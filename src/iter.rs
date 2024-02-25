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
    collections::{BinaryHeap, VecDeque},
    fs::File,
    io::{BufReader, Error, Seek, SeekFrom},
};

use crate::Sortable;

/// Iterator over sorted items that may have been written to disk during the
/// sorting process.
///
/// If items could fit into memory buffer, there won't be any disk access and
/// the iterator will be as fast as a regular `Iterator`.
pub struct SortedIterator<T, F>
where
    T: Sortable,
    F: Fn(&T, &T) -> Ordering + Send + Sync + Clone,
{
    _tempdir: Option<tempfile::TempDir>,
    pass_through_queue: Option<VecDeque<T>>,
    segments: Vec<Segment>,
    heap: BinaryHeap<Item<T, F>>,
    count: u64,
    cmp: F,
}

struct Segment {
    reader: BufReader<File>,
    heap_count: usize,
    done: bool,
}

impl<T, F> SortedIterator<T, F>
where
    T: Sortable,
    F: Fn(&T, &T) -> Ordering + Send + Sync + Clone,
{
    pub(crate) fn new(
        tempdir: Option<tempfile::TempDir>,
        pass_through_queue: Option<VecDeque<T>>,
        mut segment_files: Vec<File>,
        count: u64,
        cmp: F,
    ) -> Result<SortedIterator<T, F>, Error> {
        for segment_file in &mut segment_files {
            segment_file.seek(SeekFrom::Start(0))?;
        }

        let segments = segment_files
            .into_iter()
            .map(|file| Segment {
                reader: BufReader::new(file),
                heap_count: 0,
                done: false,
            })
            .collect();

        Ok(SortedIterator {
            _tempdir: tempdir,
            pass_through_queue,
            segments,
            heap: BinaryHeap::new(),
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
        self.segments.len()
    }

    /// Fills the heap with the next values from the segments on disk.
    fn fill_heap(&mut self) -> std::io::Result<()> {
        for (segment_index, segment) in self.segments.iter_mut().enumerate() {
            if segment.done {
                continue;
            }

            if segment.heap_count == 0 {
                for _i in 0..20 {
                    let value = match T::decode(&mut segment.reader) {
                        Ok(value) => value,
                        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                            segment.done = true;
                            continue;
                        }
                        Err(err) => return Err(err),
                    };

                    segment.heap_count += 1;

                    self.heap.push(Item {
                        segment_index,
                        value,
                        cmp: self.cmp.clone(),
                    });
                }
            }
        }

        Ok(())
    }
}

impl<T, F> Iterator for SortedIterator<T, F>
where
    T: Sortable,
    F: Fn(&T, &T) -> Ordering + Send + Sync + Clone,
{
    type Item = std::io::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        // if we have a pass through, we dequeue from it directly
        if let Some(ptb) = self.pass_through_queue.as_mut() {
            return ptb.pop_front().map(Ok);
        }

        if self.heap.is_empty() {
            if let Err(err) = self.fill_heap() {
                return Some(Err(err));
            }
        }

        if self.heap.is_empty() {
            return None;
        }

        let item = self.heap.pop().unwrap();
        let segment = &mut self.segments[item.segment_index];
        segment.heap_count -= 1;

        if segment.heap_count == 0 {
            if let Err(err) = self.fill_heap() {
                return Some(Err(err));
            }
        }

        Some(Ok(item.value))
    }
}

struct Item<T, F>
where
    T: Sortable,
    F: Fn(&T, &T) -> Ordering + Send + Sync,
{
    segment_index: usize,
    value: T,
    cmp: F,
}

impl<T, F> PartialOrd for Item<T, F>
where
    T: Sortable,
    F: Fn(&T, &T) -> Ordering + Send + Sync,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T, F> Ord for Item<T, F>
where
    T: Sortable,
    F: Fn(&T, &T) -> Ordering + Send + Sync,
{
    fn cmp(&self, other: &Self) -> Ordering {
        (self.cmp)(&self.value, &other.value).reverse()
    }
}

impl<T, F> PartialEq for Item<T, F>
where
    T: Sortable,
    F: Fn(&T, &T) -> Ordering + Send + Sync,
{
    fn eq(&self, other: &Self) -> bool {
        (self.cmp)(&self.value, &other.value) == Ordering::Equal
    }
}

impl<T, F> Eq for Item<T, F>
where
    T: Sortable,
    F: Fn(&T, &T) -> Ordering + Send + Sync,
{
}
