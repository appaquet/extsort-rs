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

//! The `extsort` crate exposes external sorting (i.e. on-disk sorting)
//! capability on arbitrarily sized iterators, even if the generated content of
//! the iterator doesn't fit in memory. Once sorted, it returns a new sorted
//! iterator.
//!
//! In order to remain efficient for all implementations, `extsort` doesn't
//! handle serialization but leaves that to the user.
//!
//! The sorter can optionally use [`rayon`](https://crates.io/crates/rayon) to
//! sort the in-memory buffer. It is generally faster when the buffer size is big
//! enough for parallelism to have an impact over its overhead.
//!
//! # Examples
//! ```rust
//! extern crate extsort;
//! extern crate byteorder;
//!
//! use extsort::*;
//! use byteorder::{ReadBytesExt, WriteBytesExt};
//! use std::io::{Read, Write};
//!
//! #[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
//! struct MyStruct(u32);
//!
//! impl Sortable for MyStruct {
//!     fn encode<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
//!         writer.write_u32::<byteorder::LittleEndian>(self.0)?;
//!         Ok(())
//!     }
//!
//!     fn decode<R: Read>(reader: &mut R) -> std::io::Result<MyStruct> {
//!         reader.read_u32::<byteorder::LittleEndian>()
//!             .map(MyStruct)
//!     }
//! }
//!
//! let sorter = ExternalSorter::new();
//! let reversed_data = (0..1000).rev().map(MyStruct).into_iter();
//! let sorted_iter = sorter.sort(reversed_data).unwrap();
//! let sorted_data = sorted_iter.collect::<std::io::Result<Vec<MyStruct>>>().unwrap();
//!
//! let expected_data = (0..1000).map(MyStruct).collect::<Vec<MyStruct>>();
//! assert_eq!(sorted_data, expected_data);
//! ```

use std::io::{Read, Write};

pub mod iter;
pub mod push;
pub mod sorter;

pub use crate::iter::SortedIterator;
pub use crate::push::PushExternalSorter;
pub use crate::sorter::ExternalSorter;

pub trait Sortable: Sized + Send {
    /// Encodes the item to the given writer.
    fn encode<W: Write>(&self, writer: &mut W) -> std::io::Result<()>;

    /// Decodes the item from the given reader.
    ///
    /// Important: the implementation relies on the `UnexpectedEof` error from
    /// `std::io::Read` to detect the end of the stream.
    fn decode<R: Read>(reader: &mut R) -> std::io::Result<Self>;
}

#[derive(Clone)]
pub(crate) struct ExternalSorterOptions {
    pub segment_size: usize,
    pub heap_iter_segment_count: usize,
    pub sort_dir: Option<std::path::PathBuf>,
    pub parallel: bool,
}

impl Default for ExternalSorterOptions {
    fn default() -> Self {
        ExternalSorterOptions {
            segment_size: 10_000,
            heap_iter_segment_count: 20,
            sort_dir: None,
            parallel: false,
        }
    }
}

#[cfg(test)]
pub mod test {
    use std::io::{Read, Result, Write};

    use super::*;

    use byteorder::{ReadBytesExt, WriteBytesExt};

    #[test]
    fn test_smaller_than_segment() {
        let sorter = ExternalSorter::new();
        let data: Vec<u32> = (0..100u32).collect();
        let data_rev: Vec<u32> = data.iter().rev().cloned().collect();

        let sorted_iter = sorter.sort(data_rev).unwrap();

        // should not have used any segments (all in memory)
        assert_eq!(sorted_iter.disk_segment_count(), 0);
        let sorted_data = sorted_iter.collect::<Result<Vec<u32>>>().unwrap();

        assert_eq!(data, sorted_data);
    }

    #[test]
    fn test_multiple_segments() {
        let sorter = ExternalSorter::new().with_segment_size(100);
        let data: Vec<u32> = (0..1000u32).collect();

        let data_rev: Vec<u32> = data.iter().rev().cloned().collect();
        let sorted_iter = sorter.sort(data_rev).unwrap();
        assert_eq!(sorted_iter.disk_segment_count(), 10);

        let sorted_data = sorted_iter.collect::<Result<Vec<u32>>>().unwrap();
        assert_eq!(data, sorted_data);
    }

    #[test]
    fn test_parallel() {
        let sorter = ExternalSorter::new()
            .with_segment_size(100)
            .with_parallel_sort();
        let data: Vec<u32> = (0..1000u32).collect();

        let data_rev: Vec<u32> = data.iter().rev().cloned().collect();
        let sorted_iter = sorter.sort(data_rev).unwrap();
        assert_eq!(sorted_iter.disk_segment_count(), 10);

        let sorted_data = sorted_iter.collect::<Result<Vec<u32>>>().unwrap();
        assert_eq!(data, sorted_data);
    }

    #[test]
    fn test_pushed() {
        let mut sorter = ExternalSorter::new().pushed();
        for item in (0..1000u32).rev() {
            sorter.push(item).unwrap();
        }

        let sorted_iter = sorter.done().unwrap();
        assert_sorted(sorted_iter);
    }

    #[test]
    fn test_error_propagation() {
        #[derive(PartialEq, Eq, PartialOrd, Ord)]
        struct ErrStruct(u32);
        impl Sortable for ErrStruct {
            fn encode<W: Write>(&self, writer: &mut W) -> Result<()> {
                writer.write_u32::<byteorder::LittleEndian>(self.0)?;
                Ok(())
            }

            fn decode<R: Read>(reader: &mut R) -> std::io::Result<ErrStruct> {
                let value = reader.read_u32::<byteorder::LittleEndian>()?;
                if value == 1 {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "MyStruct::decode error",
                    ))
                } else {
                    Ok(ErrStruct(value))
                }
            }
        }

        let mut sorter = ExternalSorter::new().with_segment_size(10).pushed();
        for item in 0..100 {
            sorter.push(ErrStruct(item)).unwrap();
        }

        // first value is fine, but second should fail
        let sorted_iter = sorter.done().unwrap();
        let res = sorted_iter.take(1).next().unwrap();
        assert!(res.is_err());
    }

    impl Sortable for u32 {
        fn encode<W: Write>(&self, writer: &mut W) -> Result<()> {
            writer.write_u32::<byteorder::LittleEndian>(*self)?;
            Ok(())
        }

        fn decode<R: Read>(reader: &mut R) -> std::io::Result<u32> {
            reader.read_u32::<byteorder::LittleEndian>()
        }
    }

    fn assert_sorted(iter: impl Iterator<Item = std::io::Result<u32>>) {
        let mut last = 0;
        for item in iter {
            let item = item.unwrap();
            assert!(item >= last);
            last = item;
        }
    }
}
