# Change log

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

* When fibmap fails to verify sparseness of files, the backup will be empty instead of complete.  Do verify that your filesystem supports checking for sparseness, to benefit from the improvements in performances that `pitreos` provides.

## [v1.1.0]

### Added

* Added support for all `dstore` backends (https://github.com/dfuse-io/dstore), including Azure Blob Storage, AWS S3, GCP and local file storage.

### Changed

* Import paths have been changed. The binary now lives under `cmd/pitreos`, and the library is now at the root (instead of `lib`).
