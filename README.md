# extsort

[![crates.io](https://img.shields.io/crates/v/extsort.svg)](https://crates.io/crates/extsort)
[![dependency status](https://deps.rs/repo/github/appaquet/extsort-rs/status.svg)](https://deps.rs/repo/github/appaquet/extsort-rs)

Exposes external sorting (i.e. on-disk sorting) capability on arbitrarily sized iterators, even if the
generated content of the iterator doesn't fit in memory. Once sorted, it returns a new sorted iterator.

To remain efficient for all implementations, the crate doesn't handle serialization but leaves that to the user.

The sorter can optionally use [`rayon`](https://crates.io/crates/rayon) to sort the in-memory buffer. It is generally
faster when the buffer size is big enough for parallelism to have an impact on its overhead.

## Example

```rust
extern crate extsort;
extern crate byteorder;

use extsort::*;
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct MyStruct(u32);

impl Sortable for MyStruct {
    fn encode<W: Write>(&self, write: &mut W) -> std::io::Result<()> {
        write.write_u32::<byteorder::LittleEndian>(self.0)?;
        Ok(())
    }

    fn decode<R: Read>(read: &mut R) -> std::io::Result<MyStruct> {
        read.read_u32::<byteorder::LittleEndian>().map(MyStruct)
    }
}

fn main() {
    let sorter = ExternalSorter::new();
    let reversed_data = (0..1000).rev().map(MyStruct).into_iter();
    let sorted_iter = sorter.sort(reversed_data).unwrap();
    let sorted_data = sorted_iter.collect::<std::io::Result<Vec<MyStruct>>>().unwrap();

    let expected_data = (0..1000).map(MyStruct).collect::<Vec<MyStruct>>();
    assert_eq!(sorted_data, expected_data);
}
```
