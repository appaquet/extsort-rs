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

use rayon::prelude::*;
use std::{
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Error, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

/// Exposes external sorting (i.e. on disk sorting) capability on arbitrarily
/// sized iterator, even if the generated content of the iterator doesn't fit in
/// memory.
///
/// It uses an in-memory buffer sorted and flushed to disk in segment files when
/// full. Once sorted, it returns a new sorted iterator with all items. In order
/// to remain efficient for all implementations, the crate doesn't handle
/// serialization, but leaves that to the user.
pub struct ExternalSorter {
    segment_size: usize,
    sort_dir: Option<PathBuf>,
    parallel: bool,
}

impl ExternalSorter {
    pub fn new() -> ExternalSorter {
        ExternalSorter {
            segment_size: 10000,
            sort_dir: None,
            parallel: false,
        }
    }

    /// Sets the maximum size of each segment in number of sorted items.
    ///
    /// This number of items needs to fit in memory. While sorting, a
    /// in-memory buffer is used to collect the items to be sorted. Once
    /// it reaches the maximum size, it is sorted and then written to disk.
    ///
    /// Using a higher segment size makes sorting faster by leveraging
    /// faster in-memory operations.
    pub fn with_segment_size(mut self, size: usize) -> Self {
        self.segment_size = size;
        self
    }

    /// Sets directory in which sorted segments will be written (if it doesn't
    /// fit in memory).
    pub fn with_sort_dir(mut self, path: PathBuf) -> Self {
        self.sort_dir = Some(path);
        self
    }

    /// Uses Rayon to sort the in-memory buffer.
    ///
    /// This may not be needed if the buffer isn't big enough for parallelism to
    /// be gainful over the overhead of multithreading.
    pub fn with_parallel_sort(mut self) -> Self {
        self.parallel = true;
        self
    }

    /// Sorts a given iterator, returning a new iterator with items
    pub fn sort<T, I>(&self, iterator: I) -> Result<SortedIterator<T>, Error>
    where
        T: Sortable,
        I: Iterator<Item = T>,
    {
        let mut tempdir: Option<tempfile::TempDir> = None;
        let mut sort_dir: Option<PathBuf> = None;

        let mut count = 0;
        let mut segments_file: Vec<File> = Vec::new();
        let mut buffer: Vec<T> = Vec::with_capacity(self.segment_size);
        for next_item in iterator {
            count += 1;
            buffer.push(next_item);
            if buffer.len() > self.segment_size {
                let sort_dir = self.lazy_create_dir(&mut tempdir, &mut sort_dir)?;
                self.sort_and_write_segment(sort_dir, &mut segments_file, &mut buffer)?;
            }
        }

        // Write any items left in buffer, but only if we had at least 1 segment
        // written. Otherwise we use the buffer itself to iterate from memory
        let pass_through_queue = if !buffer.is_empty() && !segments_file.is_empty() {
            let sort_dir = self.lazy_create_dir(&mut tempdir, &mut sort_dir)?;
            self.sort_and_write_segment(sort_dir, &mut segments_file, &mut buffer)?;
            None
        } else {
            buffer.sort_unstable();
            Some(VecDeque::from(buffer))
        };

        SortedIterator::new(tempdir, pass_through_queue, segments_file, count)
    }

    /// We only want to create directory if it's needed (i.e. if the dataset
    /// doesn't fit in memory) to prevent filesystem latency
    fn lazy_create_dir<'a>(
        &self,
        tempdir: &mut Option<tempfile::TempDir>,
        sort_dir: &'a mut Option<PathBuf>,
    ) -> Result<&'a Path, Error> {
        if let Some(sort_dir) = sort_dir {
            return Ok(sort_dir);
        }

        *sort_dir = if let Some(ref sort_dir) = self.sort_dir {
            Some(sort_dir.to_path_buf())
        } else {
            *tempdir = Some(tempfile::TempDir::new()?);
            Some(tempdir.as_ref().unwrap().path().to_path_buf())
        };

        Ok(sort_dir.as_ref().unwrap())
    }

    fn sort_and_write_segment<T>(
        &self,
        sort_dir: &Path,
        segments: &mut Vec<File>,
        buffer: &mut Vec<T>,
    ) -> Result<(), Error>
    where
        T: Sortable,
    {
        if self.parallel {
            buffer.par_sort_unstable();
        } else {
            buffer.sort_unstable();
        }

        let segment_path = sort_dir.join(format!("{}", segments.len()));
        let segment_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&segment_path)?;
        let mut buf_writer = BufWriter::new(segment_file);

        for item in buffer.drain(0..) {
            item.encode(&mut buf_writer);
        }

        let file = buf_writer.into_inner()?;
        segments.push(file);

        Ok(())
    }
}

impl Default for ExternalSorter {
    fn default() -> Self {
        ExternalSorter::new()
    }
}

pub trait Sortable: Eq + Ord + Sized + Send {
    fn encode<W: Write>(&self, writer: &mut W);
    fn decode<R: Read>(reader: &mut R) -> Option<Self>;
}

pub struct SortedIterator<T: Sortable> {
    _tempdir: Option<tempfile::TempDir>,
    pass_through_queue: Option<VecDeque<T>>,
    segments_file: Vec<BufReader<File>>,
    next_values: Vec<Option<T>>,
    count: u64,
}

impl<T: Sortable> SortedIterator<T> {
    fn new(
        tempdir: Option<tempfile::TempDir>,
        pass_through_queue: Option<VecDeque<T>>,
        mut segments_file: Vec<File>,
        count: u64,
    ) -> Result<SortedIterator<T>, Error> {
        for segment in &mut segments_file {
            segment.seek(SeekFrom::Start(0))?;
        }

        let next_values = segments_file
            .iter_mut()
            .map(|file| T::decode(file))
            .collect();

        let segments_file_buffered = segments_file.into_iter().map(BufReader::new).collect();

        Ok(SortedIterator {
            _tempdir: tempdir,
            pass_through_queue,
            segments_file: segments_file_buffered,
            next_values,
            count,
        })
    }

    pub fn sorted_count(&self) -> u64 {
        self.count
    }
}

impl<T: Sortable> Iterator for SortedIterator<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        // if we have a pass through, we dequeue from it directly
        if let Some(ptb) = self.pass_through_queue.as_mut() {
            return ptb.pop_front();
        }

        // otherwise, we iter from segments on disk
        let mut smallest_idx: Option<usize> = None;
        {
            let mut smallest: Option<&T> = None;
            for idx in 0..self.segments_file.len() {
                let next_value = self.next_values[idx].as_ref();
                if next_value.is_none() {
                    continue;
                }

                if smallest.is_none() || *next_value.unwrap() < *smallest.unwrap() {
                    smallest = Some(next_value.unwrap());
                    smallest_idx = Some(idx);
                }
            }
        }

        smallest_idx.map(|idx| {
            let file = &mut self.segments_file[idx];
            let value = self.next_values[idx].take().unwrap();
            self.next_values[idx] = T::decode(file);
            value
        })
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    use byteorder::{ReadBytesExt, WriteBytesExt};

    #[test]
    fn test_smaller_than_segment() {
        let sorter = ExternalSorter::new();
        let data: Vec<u32> = (0..100u32).collect();
        let data_rev: Vec<u32> = data.iter().rev().cloned().collect();

        let sorted_iter = sorter.sort(data_rev.into_iter()).unwrap();

        // should not have used any segments (all in memory)
        assert_eq!(sorted_iter.segments_file.len(), 0);
        let sorted_data: Vec<u32> = sorted_iter.collect();

        assert_eq!(data, sorted_data);
    }

    #[test]
    fn test_multiple_segments() {
        let sorter = ExternalSorter::new().with_segment_size(100);
        let data: Vec<u32> = (0..1000u32).collect();

        let data_rev: Vec<u32> = data.iter().rev().cloned().collect();
        let sorted_iter = sorter.sort(data_rev.into_iter()).unwrap();
        assert_eq!(sorted_iter.segments_file.len(), 10);

        let sorted_data: Vec<u32> = sorted_iter.collect();
        assert_eq!(data, sorted_data);
    }

    #[test]
    fn test_parallel() {
        let sorter = ExternalSorter::new()
            .with_segment_size(100)
            .with_parallel_sort();
        let data: Vec<u32> = (0..1000u32).collect();

        let data_rev: Vec<u32> = data.iter().rev().cloned().collect();
        let sorted_iter = sorter.sort(data_rev.into_iter()).unwrap();
        assert_eq!(sorted_iter.segments_file.len(), 10);

        let sorted_data: Vec<u32> = sorted_iter.collect();
        assert_eq!(data, sorted_data);
    }

    impl Sortable for u32 {
        fn encode<W: Write>(&self, writer: &mut W) {
            writer.write_u32::<byteorder::LittleEndian>(*self).unwrap();
        }

        fn decode<R: Read>(reader: &mut R) -> Option<u32> {
            reader.read_u32::<byteorder::LittleEndian>().ok()
        }
    }
}
