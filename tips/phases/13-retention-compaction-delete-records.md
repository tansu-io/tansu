# Phase 13 - Retention Compaction Delete Records

Parallelism: MCP-PARALLEL

Depends on: Phase 06 - Storage Log Contract; Phase 08 - Fetch ListOffsets Leader Epoch

Can run with: Phase 07, Phase 09, Phase 10, Phase 11, and Phase 14 after log-start and list-offset semantics are grounded.

Goal: Implement retention, compaction, DeleteRecords, tombstones, cleanup policy combinations, log-start movement, and background cleaner behavior across certified engines.

Current code anchors:
- `tansu-storage/src/service/delete_records.rs` and `tansu-broker/src/service/storage.rs` route DeleteRecords.
- `tansu-broker/tests/policy_compact_delete.rs` is the main existing policy test surface.
- `tansu-storage/src/pg.rs`, `lite.rs`, `dynostore.rs`, `slate/storage.rs`, and `limbo.rs` each own engine-specific cleanup behavior.
- `tansu-storage/src/lib.rs` includes topic configs, offset stages, list offsets, and maintenance hooks.
- `tansu-storage/src/service.rs` has a `Maintain(SystemTime)` request shape.

Implementation steps:
- Define retention and compaction semantics in the Phase 06 log contract, including log start offset movement and timestamp boundaries.
- Implement cleanup policies `delete`, `compact`, and `compact,delete` with Kafka-compatible config parsing and defaults.
- Make DeleteRecords move log start offset and affect Fetch/ListOffsets exactly.
- Preserve tombstone semantics, delete retention, min/max compaction lag, dirty ratio, and key equality rules.
- Add background cleaner scheduling, cancellation, observability, and storage-engine capability reporting.
- Ensure compacted topics preserve the latest record per key and required tombstones while not breaking offsets.
- Coordinate with Phase 12 so transaction markers and aborted records are retained or compacted safely.

Tests:
- Add differential tests for DeleteRecords, earliest/latest offsets after deletion, and fetch from deleted ranges.
- Add compaction tests for tombstones, repeated keys, null keys, delete retention, and compact/delete combined policy.
- Add retention tests by time and bytes with deterministic clocks.
- Add engine conformance tests for PostgreSQL first, then SQLite, S3/dynostore, SlateDB, and memory based on certification tier.
- Add metrics/maintenance tests for background cleaner progress and cancellation.

Acceptance gate: Compacted and delete-policy topics behave like Kafka across certified engines, including log-start movement, tombstones, and ListOffsets/Fetch after cleanup.

Do not do:
- Do not compact by changing visible offsets.
- Do not discard tombstones before the configured retention window.
- Do not let DeleteRecords succeed without moving the log start offset in Fetch/ListOffsets.
- Do not claim compaction parity for object-backed or embedded engines until their conformance suite passes.

Fresh session handoff: Start with PostgreSQL DeleteRecords plus log-start conformance, then extend policy tests to compaction and retention. Keep capability flags explicit for engines that lag.
