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

use itertools::{Itertools, KMerge};
use rayon::prelude::*;
use std::{
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Error, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
};

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
        T: Sortable,
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

        // Write any items left in buffer, but only if we had at least 1 segment written.
        // Otherwise we use the buffer itself to iterate from memory
        let pass_through_queue = if !buffer.is_empty() && !segments_file.is_empty() {
            let sort_dir = self.lazy_create_dir(&mut tempdir, &mut sort_dir)?;
            Self::sort_and_write_segment(sort_dir, &mut segments_file, &mut buffer)?;
            None
        } else {
            buffer.sort_unstable();
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
        T: Sortable,
    {
        buffer.par_sort_unstable();

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

        let mut file = buf_writer.into_inner()?;

        // seek back to beginning of file for reading stage
        file.seek(SeekFrom::Start(0))?;

        segments.push(file);

        Ok(())
    }
}

impl Default for ExternalSorter {
    fn default() -> Self {
        ExternalSorter::new()
    }
}

pub trait Sortable: Eq + Ord + Send + Sized {
    fn encode<W: Write>(&self, writer: &mut W);
    fn decode<R: Read>(reader: &mut R) -> Option<Self>;
}

pub struct SortedIterator<T: Sortable> {
    _tempdir: Option<tempdir::TempDir>,
    pass_through_queue: Option<VecDeque<T>>,
    chunks_iter: KMerge<ChunkReader<T>>,
    segment_count: usize,
    item_count: u64,
}

impl<T: Sortable> SortedIterator<T> {
    fn new(
        tempdir: Option<tempdir::TempDir>,
        pass_through_queue: Option<VecDeque<T>>,
        segment_files: Vec<File>,
        item_count: u64,
    ) -> Result<SortedIterator<T>, Error> {
        let segment_count = segment_files.len();
        let chunk_readers = segment_files.into_iter().map(|file| {
            let buf_file = BufReader::new(file);
            ChunkReader {
                reader: buf_file,
                phantom: PhantomData,
            }
        });
        let chunks_iter = chunk_readers.into_iter().kmerge();

        Ok(SortedIterator {
            _tempdir: tempdir,
            pass_through_queue,
            chunks_iter,
            segment_count,
            item_count,
        })
    }

    pub fn sorted_count(&self) -> u64 {
        self.item_count
    }

    pub fn segment_count(&self) -> usize {
        self.segment_count
    }
}

impl<T: Sortable> Iterator for SortedIterator<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        // if we have a pass through, we dequeue from it directly
        if let Some(ptb) = self.pass_through_queue.as_mut() {
            return ptb.pop_front();
        }

        self.chunks_iter.next()
    }
}

struct ChunkReader<T: Sortable> {
    reader: BufReader<std::fs::File>,
    phantom: PhantomData<T>,
}

impl<T: Sortable> Iterator for ChunkReader<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        T::decode(&mut self.reader)
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
        assert_eq!(sorted_iter.segment_count(), 0);
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
        assert_eq!(sorted_iter.segment_count(), 10);

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
