# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] - 2024-02-23

- Breaking: now expose underlying error via std::io::Result on the iterator and sortable trait
  - The `Sortable` trait now returns `std::io::Result` on both `encode` and `decode` methods.
  - The `SortedIterator` iterator now returns `std::io::Result<T>` instead of `T` directly, allowing
    the underlying error to be propagated.

- Breaking: comparator methods or key extractor requires to be `Clone`. This
  shouldn't impact most users since closure are `Clone` if they don't capture any
  variable.

- Now exposing a new "pushed" iterator, allowing to push new elements instead of
  consuming them through an iterator. This is useful when the data is not
  available as an iterator.

- Methods accepting iterators now accept `IntoIterator` for flexibility.

## [0.4.2] - 2021-02-04

- Added `sort_by` and `sort_by_key` (by @NieDzejkob [#10](https://github.com/appaquet/extsort-rs/pull/10))

## [0.4.0] - 2020-12-23

- Added support for parallel sorting of the in-memory buffer (see `ExternalSorter::with_parallel_sort`).
  This feature is not always beneficial if the in-memory buffer is not big enough for parallelism to
  have an impact, so benchmark your workload.

- Breaking: cleaner `Sortable` trait ([commit](https://github.com/appaquet/extsort-rs/commit/6ab89a2c1a981c5715235c293d9a1122f22bd2dc))
  The trait also requires the implementer to be `Send` to support the new parallel sorting feature.

- Breaking: replaced setter styles methods with builder style methods (see [PR #9](https://github.com/appaquet/extsort-rs/pull/9))
  - `set_max_size` is now `with_segment_size`
  - `set_sort_dir` is now `with_sort_dir`
