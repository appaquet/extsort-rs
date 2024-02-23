// Copyright 2018 Andre-Philippe Paquet
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    cmp::Ordering,
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::{BufWriter, Error},
    path::PathBuf,
};

use rayon::slice::ParallelSliceMut;

use crate::{Sortable, SortedIterator};

/// External sorter that uses a "push" pattern instead of consuming an iterator.
///
/// It is used internally by normal pull iterator (`ExternalSorter`), but can
/// also be used directly to sort items in a push pattern.
pub struct PushExternalSorter<T, F>
where
    T: Sortable,
    F: Fn(&T, &T) -> Ordering + Send + Sync + Clone,
{
    segment_size: usize,
    sort_dir: Option<PathBuf>,
    parallel: bool,
    tempdir: Option<tempfile::TempDir>,
    count: u64,
    segments_file: Vec<File>,
    buffer: Vec<T>,
    cmp: F,
}

impl<T, F> PushExternalSorter<T, F>
where
    T: Sortable,
    F: Fn(&T, &T) -> Ordering + Send + Sync + Clone,
{
    pub(crate) fn new(
        segment_size: usize,
        sort_dir: Option<PathBuf>,
        parallel: bool,
        cmp: F,
    ) -> PushExternalSorter<T, F> {
        PushExternalSorter {
            segment_size,
            sort_dir,
            parallel,
            tempdir: None,
            count: 0,
            segments_file: Vec::new(),
            buffer: Vec::new(),
            cmp,
        }
    }

    /// Pushes all items from an iterator into the sorter.
    ///
    /// This can be called multiple times to push more items into the sorter.
    pub fn push_iter<I>(&mut self, iterator: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = T>,
    {
        for next_item in iterator.into_iter() {
            self.push(next_item)?;
        }
        Ok(())
    }

    /// Push a single item into the sorter.
    pub fn push(&mut self, item: T) -> Result<(), Error> {
        self.buffer.push(item);
        self.count += 1;

        if self.buffer.len() > self.segment_size {
            self.sort_and_write_segment()?;
        }

        Ok(())
    }

    pub fn done(mut self) -> Result<SortedIterator<T, F>, Error> {
        // Write any items left in buffer, but only if we had at least 1 segment
        // written. Otherwise we use the buffer itself to iterate from memory
        let pass_through_queue = if !self.buffer.is_empty() && !self.segments_file.is_empty() {
            self.sort_and_write_segment()?;
            None
        } else {
            let cmp = self.cmp.clone();
            self.buffer.sort_unstable_by(cmp);
            Some(VecDeque::from(self.buffer))
        };

        SortedIterator::new(
            self.tempdir,
            pass_through_queue,
            self.segments_file,
            self.count,
            self.cmp,
        )
    }

    fn sort_and_write_segment(&mut self) -> Result<(), Error> {
        let cmp = self.cmp.clone();
        if self.parallel {
            self.buffer.par_sort_unstable_by(|a, b| cmp(a, b));
        } else {
            self.buffer.sort_unstable_by(|a, b| cmp(a, b));
        }

        let sort_dir = self.get_sort_dir()?;
        let segment_path = sort_dir.join(format!("{}", self.segments_file.len()));
        let segment_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(segment_path)?;
        let mut buf_writer = BufWriter::new(segment_file);

        for item in self.buffer.drain(0..) {
            item.encode(&mut buf_writer)?;
        }

        let file = buf_writer.into_inner()?;
        self.segments_file.push(file);

        Ok(())
    }

    /// We only want to create directory if it's needed (i.e. if the dataset
    /// doesn't fit in memory) to prevent filesystem latency
    fn get_sort_dir(&mut self) -> Result<PathBuf, Error> {
        if let Some(sort_dir) = self.sort_dir.as_ref() {
            return Ok(sort_dir.clone());
        }

        self.sort_dir = if let Some(ref sort_dir) = self.sort_dir {
            Some(sort_dir.to_path_buf())
        } else {
            self.tempdir = Some(tempfile::TempDir::new()?);
            Some(self.tempdir.as_ref().unwrap().path().to_path_buf())
        };

        Ok(self.sort_dir.as_ref().unwrap().clone())
    }
}
