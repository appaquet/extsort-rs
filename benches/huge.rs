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
use criterion::{criterion_group, criterion_main, Criterion};

use extsort::*;

fn bench_vec_sort_1000_sorted(c: &mut Criterion) {
    c.bench_function("bench_vec_sort_1000_sorted", |b| {
        b.iter(|| {
            let mut sorted_iter: Vec<MyStruct> = (0..1000).map(MyStruct).collect();
            sorted_iter.sort();
        });
    });
}

fn bench_vec_sort_1000_rev(c: &mut Criterion) {
    c.bench_function("bench_vec_sort_1000_rev", |b| {
        b.iter(|| {
            let mut sorted_iter: Vec<MyStruct> = (0..1000).map(MyStruct).rev().collect();
            sorted_iter.sort();
        });
    });
}

fn bench_vec_sort_1000_rand(c: &mut Criterion) {
    c.bench_function("bench_vec_sort_1000_rand", |b| {
        b.iter(|| {
            let mut sorted_iter: Vec<MyStruct> =
                (0..1000).map(|_| MyStruct(rand::random())).rev().collect();
            sorted_iter.sort();
        })
    });
}

fn bench_ext_sort_1000_sorted(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1000_sorted", |b| {
        let sorter = ExternalSorter::new();
        b.iter(|| {
            let sorted_iter = sorter.sort((0..1000).map(MyStruct)).unwrap();
            sorted_iter.sorted_count();
        })
    });
}

fn bench_ext_sort_1000_rev(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1000_rev", |b| {
        let sorter = ExternalSorter::new();
        b.iter(|| {
            let sorted_iter = sorter.sort((0..1000).map(MyStruct).rev()).unwrap();
            sorted_iter.sorted_count();
        })
    });
}

fn bench_ext_sort_1000_rand(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1000_rand", |b| {
        let sorter = ExternalSorter::new();
        b.iter(|| {
            let sorted_iter = sorter
                .sort((0..1000).map(|_| MyStruct(rand::random())).rev())
                .unwrap();
            sorted_iter.sorted_count();
        })
    });
}

fn bench_vec_sort_100_000_sorted(c: &mut Criterion) {
    c.bench_function("bench_vec_sort_100_000_sorted", |b| {
        b.iter(|| {
            let mut sorted_iter: Vec<MyStruct> = (0..100_000).map(MyStruct).collect();
            sorted_iter.sort();
        })
    });
}

fn bench_vec_sort_100_000_rev(c: &mut Criterion) {
    c.bench_function("bench_vec_sort_100_000_rev", |b| {
        b.iter(|| {
            let mut sorted_iter: Vec<MyStruct> = (0..100_000).map(MyStruct).rev().collect();
            sorted_iter.sort();
        })
    });
}

fn bench_vec_sort_100_000_rand(c: &mut Criterion) {
    c.bench_function("bench_vec_sort_100_000_rand", |b| {
        b.iter(|| {
            let mut sorted_iter: Vec<MyStruct> = (0..100_000)
                .map(|_| MyStruct(rand::random()))
                .rev()
                .collect();
            sorted_iter.sort();
        })
    });
}

fn bench_ext_sort_100_000_sorted(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_100_000_sorted", |b| {
        let sorter = ExternalSorter::new();
        b.iter(|| {
            let sorted_iter = sorter.sort((0..100_000).map(MyStruct)).unwrap();
            sorted_iter.sorted_count();
        })
    });
}

fn bench_ext_sort_100_000_rev(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_100_000_rev", |b| {
        let sorter = ExternalSorter::new();
        b.iter(|| {
            let sorted_iter = sorter.sort((0..100_000).map(MyStruct).rev()).unwrap();
            sorted_iter.sorted_count();
        })
    });
}

fn bench_ext_sort_100_000_rand(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_100_000_rand", |b| {
        let sorter = ExternalSorter::new();
        b.iter(|| {
            let sorted_iter = sorter
                .sort((0..100_000).map(|_| MyStruct(rand::random())).rev())
                .unwrap();
            sorted_iter.sorted_count();
        })
    });
}

fn bench_ext_sort_1million_max1k_sorted(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1million_max1k_sorted", |b| {
        let sorter = ExternalSorter::new().with_segment_size(1000);
        b.iter(|| {
            let sorted_iter = sorter.sort((0..1_000_000).map(MyStruct)).unwrap();
            sorted_iter.sorted_count();
        })
    });
}

fn bench_ext_sort_1million_max1k_rev(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1million_max1k_rev", |b| {
        let sorter = ExternalSorter::new().with_segment_size(1000);

        b.iter(|| {
            let sorted_iter = sorter.sort((0..1_000_000).map(MyStruct).rev()).unwrap();
            sorted_iter.sorted_count();
        })
    });
}

fn bench_ext_sort_1million_max1k_rand(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1million_max1k_rand", |b| {
        let sorter = ExternalSorter::new().with_segment_size(1000);

        b.iter(|| {
            let sorted_iter = sorter
                .sort((0..1_000_000).map(|_| MyStruct(rand::random())).rev())
                .unwrap();
            sorted_iter.sorted_count();
        })
    });
}

fn bench_ext_sort_1million_max1k_rand_parallel(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1million_max1k_rand_parallel", |b| {
        let sorter = ExternalSorter::new()
            .with_segment_size(1000)
            .with_parallel_sort();

        b.iter(|| {
            let sorted_iter = sorter
                .sort((0..1_000_000).map(|_| MyStruct(rand::random())).rev())
                .unwrap();
            sorted_iter.sorted_count();
        })
    });
}
fn bench_ext_sort_1million_max100k_sorted(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1million_max100k_sorted", |b| {
        let sorter = ExternalSorter::new().with_segment_size(100_000);

        b.iter(|| {
            let sorted_iter = sorter.sort((0..1_000_000).map(MyStruct)).unwrap();
            sorted_iter.sorted_count();
        })
    });
}

fn bench_ext_sort_1million_max100k_rev(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1million_max100k_rev", |b| {
        let sorter = ExternalSorter::new().with_segment_size(100_000);

        b.iter(|| {
            let sorted_iter = sorter.sort((0..1_000_000).map(MyStruct).rev()).unwrap();
            sorted_iter.sorted_count();
        })
    });
}

fn bench_ext_sort_1million_max100k_rand(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1million_max100k_rand", |b| {
        let sorter = ExternalSorter::new().with_segment_size(100_000);

        b.iter(|| {
            let sorted_iter = sorter
                .sort((0..1_000_000).map(|_| MyStruct(rand::random())).rev())
                .unwrap();
            sorted_iter.sorted_count();
        })
    });
}

fn bench_ext_sort_1million_max100k_rand_parallel(c: &mut Criterion) {
    c.bench_function("bench_ext_sort_1million_max100k_rand_parallel", |b| {
        let sorter = ExternalSorter::new()
            .with_segment_size(100_000)
            .with_parallel_sort();

        b.iter(|| {
            let sorted_iter = sorter
                .sort((0..1_000_000).map(|_| MyStruct(rand::random())).rev())
                .unwrap();
            sorted_iter.sorted_count();
        })
    });
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct MyStruct(u32);

impl Sortable for MyStruct {
    fn encode<W: Write>(&self, writer: &mut W) {
        writer.write_u32::<byteorder::LittleEndian>(self.0).unwrap();
    }

    fn decode<R: Read>(reader: &mut R) -> Option<MyStruct> {
        reader
            .read_u32::<byteorder::LittleEndian>()
            .ok()
            .map(MyStruct)
    }
}

criterion_group!(
    benches,
    bench_vec_sort_1000_sorted,
    bench_vec_sort_1000_rev,
    bench_vec_sort_1000_rand,
    bench_ext_sort_1000_sorted,
    bench_ext_sort_1000_rev,
    bench_ext_sort_1000_rand,
    bench_vec_sort_100_000_sorted,
    bench_vec_sort_100_000_rev,
    bench_vec_sort_100_000_rand,
    bench_ext_sort_100_000_sorted,
    bench_ext_sort_100_000_rev,
    bench_ext_sort_100_000_rand,
    bench_ext_sort_1million_max1k_sorted,
    bench_ext_sort_1million_max1k_rev,
    bench_ext_sort_1million_max1k_rand,
    bench_ext_sort_1million_max1k_rand_parallel,
    bench_ext_sort_1million_max100k_sorted,
    bench_ext_sort_1million_max100k_rev,
    bench_ext_sort_1million_max100k_rand,
    bench_ext_sort_1million_max100k_rand_parallel
);
criterion_main!(benches);
