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

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Error, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use tempdir;

pub struct ExternalSorter {
    max_size: usize,
    sort_dir: Option<PathBuf>,
}

impl ExternalSorter {
    pub fn new() -> ExternalSorter {
        ExternalSorter {
            max_size: 10000,
            sort_dir: None,
        }
    }

    /// Set maximum number of items we can buffer in memory
    pub fn set_max_size(&mut self, max_size: usize) {
        self.max_size = max_size;
    }

    /// Set directory in which sorted segments will be written (if it doesn't fit in memory)
    pub fn set_sort_dir(&mut self, path: PathBuf) {
        self.sort_dir = Some(path);
    }

    /// Sort a given iterator, returning a new iterator with items
    pub fn sort<T, I>(&self, iterator: I) -> Result<SortedIterator<T>, Error>
    where
        T: Sortable<T>,
        I: Iterator<Item = T>,
    {
        let mut tempdir: Option<tempdir::TempDir> = None;
        let mut sort_dir: Option<PathBuf> = None;

        let mut count = 0;
        let mut segments_file: Vec<File> = Vec::new();
        let mut buffer: Vec<T> = Vec::with_capacity(self.max_size);
        for next_item in iterator {
            count += 1;
            buffer.push(next_item);
            if buffer.len() > self.max_size {
                let sort_dir = self.lazy_create_dir(&mut tempdir, &mut sort_dir)?;
                Self::sort_and_write_segment(sort_dir, &mut segments_file, &mut buffer)?;
            }
        }

        // Write any items left in buffer, but only if we had at least 1 segment writen.
        // Otherwise we use the buffer itself to iterate from memory
        let pass_through_queue = if !buffer.is_empty() && !segments_file.is_empty() {
            let sort_dir = self.lazy_create_dir(&mut tempdir, &mut sort_dir)?;
            Self::sort_and_write_segment(sort_dir, &mut segments_file, &mut buffer)?;
            None
        } else {
            buffer.sort();
            Some(VecDeque::from(buffer))
        };

        SortedIterator::new(tempdir, pass_through_queue, segments_file, count)
    }

    /// We only want to create directory if it's needed (i.e. if the dataset doesn't fit in memory)
    /// to prevent filesystem latency
    fn lazy_create_dir<'a>(
        &self,
        tempdir: &mut Option<tempdir::TempDir>,
        sort_dir: &'a mut Option<PathBuf>,
    ) -> Result<&'a Path, Error> {
        if let Some(sort_dir) = sort_dir {
            return Ok(sort_dir);
        }

        *sort_dir = if let Some(ref sort_dir) = self.sort_dir {
            Some(sort_dir.to_path_buf())
        } else {
            *tempdir = Some(tempdir::TempDir::new("sort")?);
            Some(tempdir.as_ref().unwrap().path().to_path_buf())
        };

        Ok(sort_dir.as_ref().unwrap())
    }

    fn sort_and_write_segment<T>(
        sort_dir: &Path,
        segments: &mut Vec<File>,
        buffer: &mut Vec<T>,
    ) -> Result<(), Error>
    where
        T: Sortable<T>,
    {
        buffer.sort();

        let segment_path = sort_dir.join(format!("{}", segments.len()));
        let segment_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&segment_path)?;
        let mut buf_writer = BufWriter::new(segment_file);

        for item in buffer.drain(0..) {
            <T as Sortable<T>>::encode(item, &mut buf_writer);
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

pub trait Sortable<T>: Eq + Ord {
    fn encode(item: T, output: &mut Write);
    fn decode(intput: &mut Read) -> Option<T>;
}

pub struct SortedIterator<T: Sortable<T>> {
    _tempdir: Option<tempdir::TempDir>,
    pass_through_queue: Option<VecDeque<T>>,
    segments_file: Vec<BufReader<File>>,
    next_values: Vec<Option<T>>,
    count: u64,
}

impl<T: Sortable<T>> SortedIterator<T> {
    fn new(
        tempdir: Option<tempdir::TempDir>,
        pass_through_queue: Option<VecDeque<T>>,
        mut segments_file: Vec<File>,
        count: u64,
    ) -> Result<SortedIterator<T>, Error> {
        for segment in &mut segments_file {
            segment.seek(SeekFrom::Start(0))?;
        }

        let next_values = segments_file
            .iter_mut()
            .map(|file| Self::read_item(file))
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

    fn read_item(file: &mut Read) -> Option<T> {
        <T as Sortable<T>>::decode(file)
    }

    pub fn sorted_count(&self) -> u64 {
        self.count
    }
}

impl<T: Sortable<T>> Iterator for SortedIterator<T> {
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
            self.next_values[idx] = Self::read_item(file);
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
        let mut sorter = ExternalSorter::new();
        sorter.set_max_size(100);
        let data: Vec<u32> = (0..1000u32).collect();

        let data_rev: Vec<u32> = data.iter().rev().cloned().collect();
        let sorted_iter = sorter.sort(data_rev.into_iter()).unwrap();
        assert_eq!(sorted_iter.segments_file.len(), 10);

        let sorted_data: Vec<u32> = sorted_iter.collect();
        assert_eq!(data, sorted_data);
    }

    impl Sortable<u32> for u32 {
        fn encode(item: u32, write: &mut Write) {
            write.write_u32::<byteorder::LittleEndian>(item).unwrap();
        }

        fn decode(read: &mut Read) -> Option<u32> {
            read.read_u32::<byteorder::LittleEndian>().ok()
        }
    }
}
