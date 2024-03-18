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

use crate::{ExternalSorterOptions, Sortable};

/// Iterator over sorted items that may have been written to disk during the
/// sorting process.
///
/// The iterator operates in 3 modes based on the number of items and segments on disk:
/// - If the items fit into a memory buffer, the iterator dequeues directly from
///   a sorted VecDeque.
/// - If there aren't a lot of segments on disk, the iterator peeks from the
///   segments and returns the smallest item.  This is faster than using a binary
///   heap since the cost of peeking over all segments at each iteration is less
///   than the cost of maintaining a binary heap.
/// - Otherwise, the iterator uses a binary heap to keep track of the smallest
///   item from each segment.
pub struct SortedIterator<T, F>
where
    T: Sortable,
    F: Fn(&T, &T) -> Ordering + Send + Sync + Clone,
{
    _tempdir: Option<tempfile::TempDir>,
    segments: Vec<Segment>,
    mode: Mode<T, F>,
    count: u64,
    cmp: F,
    options: ExternalSorterOptions,
}

enum Mode<T, F>
where
    T: Sortable,
    F: Fn(&T, &T) -> Ordering + Send + Sync + Clone,
{
    Passthrough(VecDeque<T>),
    Heap(BinaryHeap<HeapItem<T, F>>),
    Peek(Vec<Option<T>>),
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
        options: ExternalSorterOptions,
    ) -> Result<SortedIterator<T, F>, Error> {
        for segment_file in &mut segment_files {
            segment_file.seek(SeekFrom::Start(0))?;
        }

        let mut segments: Vec<Segment> = segment_files
            .into_iter()
            .map(|file| Segment {
                reader: BufReader::new(file),
                heap_count: 0,
                done: false,
            })
            .collect();

        let mode = if let Some(queue) = pass_through_queue {
            Mode::Passthrough(queue)
        } else if segments.len() < options.heap_iter_segment_count {
            let mut next_values = Vec::with_capacity(segments.len());
            for segment in segments.iter_mut() {
                next_values.push(Some(T::decode(&mut segment.reader)?));
            }
            Mode::Peek(next_values)
        } else {
            Mode::Heap(BinaryHeap::new())
        };

        Ok(SortedIterator {
            _tempdir: tempdir,
            segments,
            mode,
            count,
            cmp,
            options,
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

    /// In heap mode, fills the heap with the next values from the segments on
    /// disk.
    fn fill_heap(
        heap: &mut BinaryHeap<HeapItem<T, F>>,
        segments: &mut [Segment],
        cmp: F,
    ) -> std::io::Result<()> {
        for (segment_index, segment) in segments.iter_mut().enumerate() {
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

                    heap.push(HeapItem {
                        segment_index,
                        value,
                        cmp: cmp.clone(),
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
        match &mut self.mode {
            Mode::Passthrough(queue) => queue.pop_front().map(Ok),
            Mode::Heap(heap) => {
                if heap.is_empty() {
                    if let Err(err) = Self::fill_heap(heap, &mut self.segments, self.cmp.clone()) {
                        return Some(Err(err));
                    }
                }

                if heap.is_empty() {
                    return None;
                }

                let item = heap.pop().unwrap();
                let segment = &mut self.segments[item.segment_index];
                segment.heap_count -= 1;

                if segment.heap_count == 0 {
                    if let Err(err) = Self::fill_heap(heap, &mut self.segments, self.cmp.clone()) {
                        return Some(Err(err));
                    }
                }

                Some(Ok(item.value))
            }
            Mode::Peek(next_values) => {
                // otherwise, we iter from segments on disk
                let mut smallest_idx: Option<usize> = None;
                {
                    let mut smallest: Option<&T> = None;
                    for (idx, next_value) in next_values.iter().enumerate() {
                        let Some(next_value) = next_value else {
                            continue;
                        };

                        if smallest.is_none()
                            || (self.cmp)(next_value, smallest.unwrap()) == Ordering::Less
                        {
                            smallest = Some(next_value);
                            smallest_idx = Some(idx);
                        }
                    }
                }

                if let Some(idx) = smallest_idx {
                    let segment = &mut self.segments[idx];
                    let value = next_values[idx].take().unwrap();

                    match T::decode(&mut segment.reader) {
                        Ok(value) => {
                            next_values[idx] = Some(value);
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                            next_values[idx] = None;
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
    }
}

struct HeapItem<T, F>
where
    T: Sortable,
    F: Fn(&T, &T) -> Ordering + Send + Sync,
{
    segment_index: usize,
    value: T,
    cmp: F,
}

impl<T, F> PartialOrd for HeapItem<T, F>
where
    T: Sortable,
    F: Fn(&T, &T) -> Ordering + Send + Sync,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T, F> Ord for HeapItem<T, F>
where
    T: Sortable,
    F: Fn(&T, &T) -> Ordering + Send + Sync,
{
    fn cmp(&self, other: &Self) -> Ordering {
        (self.cmp)(&self.value, &other.value).reverse()
    }
}

impl<T, F> PartialEq for HeapItem<T, F>
where
    T: Sortable,
    F: Fn(&T, &T) -> Ordering + Send + Sync,
{
    fn eq(&self, other: &Self) -> bool {
        (self.cmp)(&self.value, &other.value) == Ordering::Equal
    }
}

impl<T, F> Eq for HeapItem<T, F>
where
    T: Sortable,
    F: Fn(&T, &T) -> Ordering + Send + Sync,
{
}
