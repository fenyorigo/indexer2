# Changelog

## 1.6.2 - 2026-03-13

### Added
- Added single-file refresh support for worker-driven media updates via `--refresh-file` and explicit `--config`.

### Fixed
- Removed interactive dry-run prompting from non-interactive CLI usage so worker-triggered refresh runs can complete unattended.
- Added refresh-file specific text output handling and JSON-safe worker integration.

## 1.6.1 - 2026-02-22

### Fixed
- Added automatic post-scan orphan tag pruning so `tags` rows without any `file_tags` link are removed after indexing.
- Added regression test ensuring orphan tags are cleaned up automatically at the end of a scan.

## 1.6.0 - 2026-02-22

### Added
- CLI path split with `--media-root` (scan source) and `--db-media-path` (stored DB base path).
- Backward-compatible `--root` alias for CLI.
- Scanner path mapping so `roots.path`, `directories.path`, and `files.path` can differ from filesystem scan path.
- Replaced single `--images-only` behavior with independent include toggles:
  - `--include-videos` (default `yes`)
  - `--include-docs` (default `no`)
  - `--include-audio` (default `no`)
- Added `--video-tags` (default `no`) so videos can be indexed into `files` without creating video-derived tag rows.
- Added optional `--video-tag-blacklist` (newline-separated UTF-8), applied when `--video-tags yes`.
- Added scan-option persistence in `meta`:
  - `indexer_version`, `include_videos`, `include_docs`, `include_audio`, `video_tags`, `video_tag_blacklist_sha256`.
- Added tests for:
  - DB media path mapping
  - new CLI arguments
  - video indexed but no `file_tags` when `video_tags=no`
  - docs/audio excluded by default.

## 1.1.1 - 2026-02-21

### Fixed
- Scanner now classifies supported document/audio extensions as `doc` / `audio` instead of `other` when `images_only` is disabled.
- Added regression test for mixed file-type classification (`image`, `video`, `doc`, `audio`).

## 1.1.0 - 2026-02-18

### Added
- CLI option `--images-only yes|no` (default `yes`) to limit scans to image files.
- GUI `Images only` checkbox (default checked) to match CLI behavior.
- Tests for CLI parsing and scanner default image-only filtering.

## 1.0.1 - 2026-02-09

### Added
- Warnings counter and non-fatal handling for ExifTool exit code 1.
- JSONL error logging for per-file failures with configurable path.

### Fixed
- Treat ExifTool warnings as non-fatal when JSON parses successfully.

## 1.0.0 - 2026-02-09

### Added
- PyQt GUI for DB selection, scan root discovery, tri-state directory selection, progress, and reports.
- Headless CLI mode with interactive prompts, JSON/text reports, and cancellation handling.
- SQLite schema with migrations, indexing, and scan status tracking.
- ExifTool integration with JSON capture and normalized tag extraction.
- taken_ts derivation with provenance tracking and taken_src distribution reporting.
- Configurable hashing and MIME detection.
- Scan reports with export to JSON/CSV and taken_src distribution.

### Changed
- Unified scan engine for GUI and CLI with dry-run support.
- Per-directory transactional scanning with cancel-safe rollback.

### Fixed
- Robust tag normalization and rescan synchronization for file_tags.
