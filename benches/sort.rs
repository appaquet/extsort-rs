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

use std::io::{Read, Write};

use byteorder::{ReadBytesExt, WriteBytesExt};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

use extsort::*;

fn bench_vec_sort_1000_sorted(c: &mut Criterion) {
    c.bench_function("bench_vec_sort_1000_sorted", |b| {
        b.iter(|| {
            let mut sorted_vec: Vec<MyStruct> = (0..1000).map(MyStruct).collect();
            sorted_vec.sort();
            black_box(sorted_vec);
        });
    });
}

fn bench_ext_sort_1000_sorted(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1000_sorted", |b| {
        b.iter(|| {
            let sorter = ExternalSorter::new();
            let sorted_iter = sorter.sort((0..1000).map(MyStruct)).unwrap();
            black_box(sorted_iter.count());
        })
    });
}

fn bench_vec_sort_1000_rev(c: &mut Criterion) {
    c.bench_function("bench_vec_sort_1000_rev", |b| {
        b.iter(|| {
            let mut sorted_vec: Vec<MyStruct> = (0..1000).map(MyStruct).rev().collect();
            sorted_vec.sort();
            black_box(sorted_vec);
        });
    });
}

fn bench_ext_sort_1000_rev(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1000_rev", |b| {
        b.iter(|| {
            let sorter = ExternalSorter::new();
            let sorted_iter = sorter.sort((0..1000).map(MyStruct).rev()).unwrap();
            black_box(sorted_iter.count());
        })
    });
}

fn bench_vec_sort_1000_rand(c: &mut Criterion) {
    c.bench_function("bench_vec_sort_1000_rand", |b| {
        b.iter(|| {
            let mut sorted_vec: Vec<MyStruct> =
                (0..1000).map(|_| MyStruct(rand::random())).rev().collect();
            sorted_vec.sort();
            black_box(sorted_vec);
        })
    });
}

fn bench_ext_sort_1000_rand(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1000_rand", |b| {
        b.iter(|| {
            let sorter = ExternalSorter::new();
            let sorted_iter = sorter
                .sort((0..1000).map(|_| MyStruct(rand::random())).rev())
                .unwrap();
            black_box(sorted_iter.count());
        })
    });
}

fn bench_vec_sort_100_000_sorted(c: &mut Criterion) {
    c.bench_function("bench_vec_sort_100_000_sorted", |b| {
        b.iter(|| {
            let mut sorted_vec: Vec<MyStruct> = (0..100_000).map(MyStruct).collect();
            sorted_vec.sort();
            black_box(sorted_vec);
        })
    });
}

fn bench_ext_sort_100_000_sorted(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_100_000_sorted", |b| {
        b.iter(|| {
            let sorter = ExternalSorter::new();
            let sorted_iter = sorter.sort((0..100_000).map(MyStruct)).unwrap();
            black_box(sorted_iter.count());
        })
    });
}

fn bench_vec_sort_100_000_rev(c: &mut Criterion) {
    c.bench_function("bench_vec_sort_100_000_rev", |b| {
        b.iter(|| {
            let mut sorted_vec: Vec<MyStruct> = (0..100_000).map(MyStruct).rev().collect();
            sorted_vec.sort();
            black_box(sorted_vec);
        })
    });
}

fn bench_ext_sort_100_000_rev(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_100_000_rev", |b| {
        b.iter(|| {
            let sorter = ExternalSorter::new();
            let sorted_iter = sorter.sort((0..100_000).map(MyStruct).rev()).unwrap();
            black_box(sorted_iter.count());
        })
    });
}

fn bench_vec_sort_100_000_rand(c: &mut Criterion) {
    c.bench_function("bench_vec_sort_100_000_rand", |b| {
        b.iter(|| {
            let mut sorted_vec: Vec<MyStruct> = (0..100_000)
                .map(|_| MyStruct(rand::random()))
                .rev()
                .collect();
            sorted_vec.sort();
            black_box(sorted_vec);
        })
    });
}

fn bench_ext_sort_100_000_rand(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_100_000_rand", |b| {
        b.iter(|| {
            let sorter = ExternalSorter::new();
            let sorted_iter = sorter
                .sort((0..100_000).map(|_| MyStruct(rand::random())).rev())
                .unwrap();
            black_box(sorted_iter.count());
        })
    });
}

fn bench_ext_sort_1million_max10k_sorted(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1million_max10k_sorted", |b| {
        b.iter(|| {
            let sorter = ExternalSorter::new().with_segment_size(10_000);
            let sorted_iter = sorter.sort((0..1_000_000).map(MyStruct)).unwrap();
            black_box(sorted_iter.count());
        })
    });
}

fn bench_ext_sort_1million_max10k_rev(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1million_max10k_rev", |b| {
        b.iter(|| {
            let sorter = ExternalSorter::new().with_segment_size(10_000);
            let sorted_iter = sorter.sort((0..1_000_000).map(MyStruct).rev()).unwrap();
            black_box(sorted_iter.count());
        })
    });
}

fn bench_ext_sort_1million_max10k_rand(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1million_max10k_rand", |b| {
        b.iter(|| {
            let sorter = ExternalSorter::new().with_segment_size(10_000);
            let sorted_iter = sorter
                .sort((0..1_000_000).map(|_| MyStruct(rand::random())).rev())
                .unwrap();
            black_box(sorted_iter.count());
        })
    });
}

fn bench_ext_sort_1million_max10k_rand_parallel(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1million_max10k_rand_parallel", |b| {
        b.iter(|| {
            let sorter = ExternalSorter::new()
                .with_segment_size(10_000)
                .with_parallel_sort();

            let sorted_iter = sorter
                .sort((0..1_000_000).map(|_| MyStruct(rand::random())).rev())
                .unwrap();
            black_box(sorted_iter.count());
        })
    });
}
fn bench_ext_sort_1million_max100k_sorted(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1million_max100k_sorted", |b| {
        b.iter(|| {
            let sorter = ExternalSorter::new().with_segment_size(100_000);
            let sorted_iter = sorter.sort((0..1_000_000).map(MyStruct)).unwrap();
            black_box(sorted_iter.count());
        })
    });
}

fn bench_ext_sort_1million_max100k_rev(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1million_max100k_rev", |b| {
        b.iter(|| {
            let sorter = ExternalSorter::new().with_segment_size(100_000);
            let sorted_iter = sorter.sort((0..1_000_000).map(MyStruct).rev()).unwrap();
            black_box(sorted_iter.count());
        })
    });
}

fn bench_ext_sort_1million_max100k_rand(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1million_max100k_rand", |b| {
        b.iter(|| {
            let sorter = ExternalSorter::new().with_segment_size(100_000);
            let sorted_iter = sorter
                .sort((0..1_000_000).map(|_| MyStruct(rand::random())).rev())
                .unwrap();
            black_box(sorted_iter.count());
        })
    });
}

fn bench_ext_sort_1million_max100k_rand_parallel(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1million_max100k_rand_parallel", |b| {
        b.iter(|| {
            let sorter = ExternalSorter::new()
                .with_segment_size(100_000)
                .with_parallel_sort();

            let sorted_iter = sorter
                .sort((0..1_000_000).map(|_| MyStruct(rand::random())).rev())
                .unwrap();
            black_box(sorted_iter.count());
        })
    });
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct MyStruct(u32);

impl Sortable for MyStruct {
    fn encode<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u32::<byteorder::LittleEndian>(self.0)?;
        Ok(())
    }

    fn decode<R: Read>(reader: &mut R) -> std::io::Result<MyStruct> {
        reader.read_u32::<byteorder::LittleEndian>().map(MyStruct)
    }
}

criterion_group!(
    benches,
    bench_vec_sort_1000_sorted,
    bench_ext_sort_1000_sorted,
    bench_vec_sort_1000_rev,
    bench_ext_sort_1000_rev,
    bench_vec_sort_1000_rand,
    bench_ext_sort_1000_rand,
    bench_vec_sort_100_000_sorted,
    bench_ext_sort_100_000_sorted,
    bench_vec_sort_100_000_rev,
    bench_ext_sort_100_000_rev,
    bench_vec_sort_100_000_rand,
    bench_ext_sort_100_000_rand,
    bench_ext_sort_1million_max10k_sorted,
    bench_ext_sort_1million_max10k_rev,
    bench_ext_sort_1million_max10k_rand,
    bench_ext_sort_1million_max10k_rand_parallel,
    bench_ext_sort_1million_max100k_sorted,
    bench_ext_sort_1million_max100k_rev,
    bench_ext_sort_1million_max100k_rand,
    bench_ext_sort_1million_max100k_rand_parallel
);
criterion_main!(benches);
