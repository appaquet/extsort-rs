extsort [![crates.io](https://img.shields.io/crates/v/extsort.svg)](https://crates.io/crates/extsort)
==========

Exposes external sorting (i.e. on disk sorting) capability on arbitrarily sized iterator, even if the
generated content of the iterator doesn't fit in memory. Once sorted, it returns a new sorted iterator.
In order to remain efficient for all implementations, the crate doesn't handle serialization, but leaves that to the user.

# Example
```rust
extern crate extsort;
extern crate byteorder;

use extsort::*;
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct MyStruct(u32);

impl Sortable<MyStruct> for MyStruct {
    fn encode<W: Write>(item: MyStruct, write: &mut W) {
        write.write_u32::<byteorder::LittleEndian>(item.0).unwrap();
    }

    fn decode<R: Read>(read: &mut R) -> Option<MyStruct> {
        read.read_u32::<byteorder::LittleEndian>()
            .ok()
            .map(MyStruct)
    }
}

fn main() {
    let sorter = ExternalSorter::new();
    let reversed_data = (0..1000).rev().map(MyStruct).into_iter();
    let sorted_iter = sorter.sort(reversed_data).unwrap();
    let sorted_data: Vec<MyStruct> = sorted_iter.collect();

    let expected_data = (0..1000).map(MyStruct).collect::<Vec<MyStruct>>();
    assert_eq!(sorted_data, expected_data);
}
```
