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

//! The `extsort` crate exposes external sorting (i.e. on disk sorting)
//! capability on arbitrarily sized iterator, even if the generated content of
//! the iterator doesn't fit in memory. Once sorted, it returns a new sorted
//! iterator.
//!
//! In order to remain efficient for all implementations, `extsort` doesn't
//! handle serialization, but leaves that to the user.
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
//!     fn encode<W: Write>(&self, write: &mut W) {
//!         write.write_u32::<byteorder::LittleEndian>(self.0).unwrap();
//!     }
//!
//!     fn decode<R: Read>(read: &mut R) -> Option<MyStruct> {
//!         read.read_u32::<byteorder::LittleEndian>()
//!             .ok()
//!             .map(MyStruct)
//!     }
//! }
//!
//! let sorter = ExternalSorter::new();
//! let reversed_data = (0..1000).rev().map(MyStruct).into_iter();
//! let sorted_iter = sorter.sort(reversed_data).unwrap();
//! let sorted_data: Vec<MyStruct> = sorted_iter.collect();
//!
//! let expected_data = (0..1000).map(MyStruct).collect::<Vec<MyStruct>>();
//! assert_eq!(sorted_data, expected_data);
//! ```

pub mod sorter;
pub use crate::sorter::{ExternalSorter, Sortable, SortedIterator};
