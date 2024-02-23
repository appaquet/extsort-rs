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

use std::{cmp::Ordering, io::Error, path::PathBuf};

use crate::{iter::SortedIterator, push::PushExternalSorter, Sortable};

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
    pub fn sort<T, I>(
        self,
        iterator: I,
    ) -> Result<SortedIterator<T, impl Fn(&T, &T) -> Ordering + Send + Sync>, Error>
    where
        T: Sortable + Ord,
        I: IntoIterator<Item = T>,
    {
        self.sort_by(iterator, |a, b| a.cmp(b))
    }

    /// Sorts a given iterator with a key extraction function, returning a new iterator with items
    pub fn sort_by_key<T, I, F, K>(
        self,
        iterator: I,
        f: F,
    ) -> Result<SortedIterator<T, impl Fn(&T, &T) -> Ordering + Send + Sync>, Error>
    where
        T: Sortable,
        I: IntoIterator<Item = T>,
        F: Fn(&T) -> K + Send + Sync + Clone,
        K: Ord,
    {
        self.sort_by(iterator, move |a, b| f(a).cmp(&f(b)))
    }

    /// Sorts a given iterator with a comparator function, returning a new iterator with items
    pub fn sort_by<T, I, F>(self, iterator: I, cmp: F) -> Result<SortedIterator<T, F>, Error>
    where
        T: Sortable,
        I: IntoIterator<Item = T>,
        F: Fn(&T, &T) -> Ordering + Send + Sync + Clone,
    {
        let mut sorter =
            PushExternalSorter::new(self.segment_size, self.sort_dir, self.parallel, cmp);
        sorter.push_iter(iterator)?;
        sorter.done()
    }

    /// Creates a pushed external sorter, which will consume items in a push
    /// pattern and compare them using the default comparator.
    pub fn pushed<T>(
        self,
    ) -> PushExternalSorter<T, impl Fn(&T, &T) -> Ordering + Send + Sync + Clone>
    where
        T: Sortable + Ord,
    {
        self.pushed_by::<T, _>(|a, b| a.cmp(b))
    }

    /// Creates a pushed external sorter, which will consume items in a push
    /// pattern and compare them using the given comparator function.
    pub fn pushed_by<T, F>(self, cmp: F) -> PushExternalSorter<T, F>
    where
        T: Sortable,
        F: Fn(&T, &T) -> Ordering + Send + Sync + Clone,
    {
        PushExternalSorter::new(self.segment_size, self.sort_dir, self.parallel, cmp)
    }

    /// Creates a pushed external sorter, which will consume items in a push
    /// pattern and compare them using the given key extraction function.
    pub fn pushed_by_key<T, F, K>(
        self,
        f: F,
    ) -> PushExternalSorter<T, impl Fn(&T, &T) -> Ordering + Send + Sync + Clone>
    where
        T: Sortable,
        F: Fn(&T) -> K + Send + Sync + Clone,
        K: Ord,
    {
        self.pushed_by(move |a, b| f(a).cmp(&f(b)))
    }
}

impl Default for ExternalSorter {
    fn default() -> Self {
        ExternalSorter::new()
    }
}
