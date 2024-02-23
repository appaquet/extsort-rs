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
    fs::File,
    io::{BufReader, Error, Seek, SeekFrom},
};

use crate::Sortable;

/// Iterator over sorted items that may have been written to disk during the
/// sorting process.
///
/// If items could fit into memory buffer, there won't be any disk access and
/// the iterator will be as fast as a regular `Iterator`.
pub struct SortedIterator<T: Sortable, F> {
    _tempdir: Option<tempfile::TempDir>,
    pass_through_queue: Option<VecDeque<T>>,
    segment_files: Vec<BufReader<File>>,
    next_values: Vec<Option<T>>,
    count: u64,
    cmp: F,
}

impl<T: Sortable, F: Fn(&T, &T) -> Ordering + Send + Sync> SortedIterator<T, F> {
    pub(crate) fn new(
        tempdir: Option<tempfile::TempDir>,
        pass_through_queue: Option<VecDeque<T>>,
        mut segment_files: Vec<File>,
        count: u64,
        cmp: F,
    ) -> Result<SortedIterator<T, F>, Error> {
        for segment in &mut segment_files {
            segment.seek(SeekFrom::Start(0))?;
        }

        let mut next_values = Vec::with_capacity(segment_files.len());
        for file in segment_files.iter_mut() {
            next_values.push(Some(T::decode(file)?));
        }

        let segment_files = segment_files.into_iter().map(BufReader::new).collect();

        Ok(SortedIterator {
            _tempdir: tempdir,
            pass_through_queue,
            segment_files,
            next_values,
            count,
            cmp,
        })
    }

    /// Returns the number of items in the sorted iterator.
    pub fn sorted_count(&self) -> u64 {
        self.count
    }

    /// Returns the number of segments on disk.
    ///
    /// May be 0 if the whole iterator fit in memory buffer.
    pub fn disk_segment_count(&self) -> usize {
        self.segment_files.len()
    }
}

impl<T: Sortable, F: Fn(&T, &T) -> Ordering> Iterator for SortedIterator<T, F> {
    type Item = std::io::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        // if we have a pass through, we dequeue from it directly
        if let Some(ptb) = self.pass_through_queue.as_mut() {
            return ptb.pop_front().map(Ok);
        }

        // otherwise, we iter from segments on disk
        let mut smallest_idx: Option<usize> = None;
        {
            let mut smallest: Option<&T> = None;
            for idx in 0..self.segment_files.len() {
                let next_value = self.next_values[idx].as_ref();
                if next_value.is_none() {
                    continue;
                }

                if smallest.is_none()
                    || (self.cmp)(next_value.unwrap(), smallest.unwrap()) == Ordering::Less
                {
                    smallest = Some(next_value.unwrap());
                    smallest_idx = Some(idx);
                }
            }
        }

        if let Some(idx) = smallest_idx {
            let file = &mut self.segment_files[idx];
            let value = self.next_values[idx].take().unwrap();

            match T::decode(file) {
                Ok(value) => {
                    self.next_values[idx] = Some(value);
                }
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                    self.next_values[idx] = None;
                }
                Err(err) => {
                    return Some(Err(err));
                }
            };

            Some(Ok(value))
        } else {
            None
        }
    }
}
