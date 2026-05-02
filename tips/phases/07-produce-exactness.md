# Phase 07 - Produce Exactness

Parallelism: MCP-PARALLEL

Depends on: Phase 03 - Request Lifecycle No Hangs; Phase 06 - Storage Log Contract

Can run with: Phase 08, Phase 09, Phase 10, Phase 13, and Phase 14 after shared lifecycle and storage invariants are stable.

Goal: Make Produce behavior Kafka-exact for normal producer configs before idempotence and transactions are claimed complete.

Current code anchors:
- `tansu-storage/src/service/produce.rs` owns Produce request handling.
- `tansu-storage/src/lib.rs` has `Storage::produce` and producer initialization hooks.
- `tansu-sans-io` owns record batch encode/decode, compression, CRC, request/response schemas, and API version metadata.
- `tansu-broker/tests/produce_fetch.rs` and `tansu-broker/tests/person.rs` cover current produce/fetch behavior.
- `tansu-broker/tests/pg_init_producer.rs` and `tansu-broker/tests/txn.rs` cover some producer/transaction-adjacent behavior.
- `tansu-schema/` integrates schema validation and must remain compatible with Kafka produce semantics.

Implementation steps:
- Define exact Produce version caps in Phase 02 registry based on tests, not descriptor max versions.
- Implement `acks=0`, `acks=1`, and `acks=all` semantics for Tansu's storage-backed profiles, including durable acknowledgment mapping per engine.
- Validate required acks, partition existence, topic authorization, message sizes, request sizes, record batch sizes, timestamps, magic, CRC, and compression.
- Support gzip, snappy, lz4, and zstd according to Kafka record-batch behavior and crate capabilities.
- Make per-partition errors exact for unknown topic, invalid partition, not leader or leader unavailable facade cases, record too large, invalid timestamp, invalid required acks, and storage failures.
- Ensure schema validation failures map to stable Kafka-compatible errors and do not corrupt offset assignment.
- Add throttling fields and quota hooks without weakening lifecycle guarantees.
- Leave full idempotent duplicate suppression and fencing to Phase 11, but reject invalid idempotent state truthfully until then.

Tests:
- Add differential Produce tests using Java client and librdkafka normal producer configs.
- Add request-version matrix tests for advertised Produce versions.
- Add compression and CRC tests for every supported codec.
- Add negative tests for size limits, invalid partition, unknown topic, bad acks, invalid timestamp, and schema validation failure.
- Add no-response regression tests for `acks=0` and request lifecycle.
- Add storage conformance tests that verify offsets are assigned exactly once after partial errors.

Acceptance gate: Java and librdkafka producers run normal non-transactional configs without workarounds, and every advertised Produce version has exact success and error tests.

Do not do:
- Do not advertise idempotent or transactional Produce semantics just because `InitProducerId` exists.
- Do not silently accept unsupported compression or invalid batches.
- Do not map all storage errors to a generic success or generic server error.
- Do not let schema validation mutate storage before failure is final.

Fresh session handoff: Start in `ProduceService` and the record-batch validation path. Use Phase 04 differential tests to decide the advertised Produce cap before broadening behavior.
