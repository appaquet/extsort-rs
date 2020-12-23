# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2020-12-23
### Added
- Support for parallel sorting of the in-memory buffer (see `ExternalSorter::with_parallel_sort`).
  This feature is not always beneficial if the in-memory buffer is not big enough for parallelism to
  have an impact, so benchmark your workload.

### Changed
- Breaking: cleaner `Sortable` trait ([commit](https://github.com/appaquet/extsort-rs/commit/6ab89a2c1a981c5715235c293d9a1122f22bd2dc))
  The trait also requires the implementer to be `Send` to support the new parallel sorting feature.

- Breaking: replaced setter styles methods with builder style methods (see [PR #9](https://github.com/appaquet/extsort-rs/pull/9))
  - `set_max_size` is now `with_segment_size`
  - `set_sort_dir` is now `with_sort_dir`