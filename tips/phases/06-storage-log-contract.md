# Phase 06 - Storage Log Contract

Parallelism: MCP-PARALLEL

Depends on: Phase 01 - Compatibility Contract

Can run with: Phase 02, Phase 03, Phase 04, and Phase 05 after storage certification tiers are named.

Goal: Define the Kafka log/storage contract across PostgreSQL, SQLite/libSQL, S3/dynostore, SlateDB, Turso, and memory. PostgreSQL is the first production parity target; every other engine gets an explicit certification tier per feature.

Current code anchors:
- `tansu-storage/src/lib.rs` defines `Storage`, `StorageContainer`, `OffsetStage`, `ListOffsetResponse`, producer, transaction, group, and metadata methods.
- `tansu-storage/src/pg.rs`, `tansu-storage/src/lite.rs`, `tansu-storage/src/dynostore.rs`, `tansu-storage/src/slate/storage.rs`, `tansu-storage/src/limbo.rs`, and `tansu-storage/src/null.rs` implement storage engines.
- `tansu-storage/src/batch.rs` contains batch-oriented storage wrappers.
- `tansu-storage/src/sql/` contains SQL query assets for offset and list-offset behavior.
- `tansu-broker/tests/policy_compact_delete.rs`, `tansu-broker/tests/list_offsets.rs`, and `tansu-broker/tests/produce_fetch.rs` already exercise some log invariants.

Implementation steps:
- Write a `KafkaLog` contract document and map it to the current `Storage` trait.
- Define required invariants: contiguous offsets, log start offset, log end offset, high watermark, last stable offset, leader epoch cache, timestamp lookup, batch CRC validation, compression handling, producer state, transaction visibility, tombstones, retention, compaction, delete-records, and crash recovery.
- Identify which invariants are already represented by `Storage` and which require new methods or internal engine tables/indexes.
- Make PostgreSQL the first production parity engine and specify what SQLite, S3/dynostore, SlateDB, Turso, and memory can claim.
- Add feature capability reporting so services can choose Kafka-compatible errors when a storage engine cannot support required semantics.
- Define migration expectations for storage schema changes before phases 07 through 13 land.

Tests:
- Add a shared storage conformance suite that each engine can opt into by capability.
- Add produce/fetch/list-offset invariant tests for contiguous offsets, empty partitions, timestamp lookup, log start movement, high watermark, and last stable offset.
- Add crash/restart or simulated partial-write tests for engines that claim production parity.
- Add capability tests that unsupported engine features return Kafka-compatible errors rather than fake success.

Acceptance gate: Produce/fetch/list-offset invariants are testable per engine, PostgreSQL has a production parity path, and every other engine has an explicit tier with documented gaps.

Do not do:
- Do not claim every storage engine has the same Kafka semantics unless it passes the same conformance suite.
- Do not hide engine limitations behind the stateless broker abstraction.
- Do not implement leader epochs, transactions, compaction, or retention separately in services when they belong in the storage/log contract.
- Do not optimize performance before invariants are specified.

Fresh session handoff: Start by documenting current `Storage` methods against Kafka log invariants, then add the first shared conformance tests for offsets, high watermark, and list offsets.
