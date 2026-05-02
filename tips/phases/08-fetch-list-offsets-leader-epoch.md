# Phase 08 - Fetch ListOffsets Leader Epoch

Parallelism: MCP-PARALLEL

Depends on: Phase 03 - Request Lifecycle No Hangs; Phase 06 - Storage Log Contract

Can run with: Phase 07, Phase 09, Phase 10, Phase 13, and Phase 14 after storage/log invariants are defined.

Goal: Make Fetch, ListOffsets, and OffsetForLeaderEpoch exact, including timestamp lookups, log start offsets, high watermark, last stable offset, leader epoch history, and fencing. API 23 must not be advertised until these semantics exist.

Current code anchors:
- `tansu-storage/src/service/fetch.rs` handles Fetch, high watermark, last stable offset, log start offset, and empty responses.
- `tansu-storage/src/service/list_offsets.rs` handles ListOffsets and currently returns a fixed leader epoch value.
- `tansu-storage/src/lib.rs` exposes `offset_stage`, `fetch`, and `list_offsets`.
- `tansu-storage/src/sql/list_latest_offset_*.sql` contains SQL offset query assets.
- `tansu-broker/tests/fetch.rs` and `tansu-broker/tests/list_offsets.rs` cover current behavior.
- The reverted `OffsetForLeaderEpoch` service showed a useful starting concept but was unsafe because it lacked leader-epoch history and truthful ApiVersions.

Implementation steps:
- Define Fetch and ListOffsets advertised version caps from Phase 04 client behavior.
- Add leader epoch storage to the Phase 06 log contract, including epoch start offsets, truncation boundaries, and recovery behavior.
- Implement `OffsetForLeaderEpoch` only after storage can answer historical epochs and current leader epoch validation.
- Return Kafka-equivalent errors for `FencedLeaderEpoch`, `UnknownLeaderEpoch`, `OffsetOutOfRange`, unknown topic/partition, invalid topic ID, and unsupported storage capability.
- Make Fetch long polling exact for `max_wait_ms`, `min_bytes`, `max_bytes`, `partition_max_bytes`, empty fetches, and cancellation.
- Implement incremental fetch sessions or lower advertised Fetch versions until sessions are supported.
- Complete topic ID behavior for modern Fetch/ListOffsets versions.
- Implement `read_committed` visibility with last stable offset and aborted transaction lists in coordination with Phase 12.
- Ensure ListOffsets supports earliest, latest, max timestamp, timestamp lookup, isolation level, and log start movement.

Tests:
- Add differential tests for consumer seek beginning/end/timestamp with Java client and librdkafka.
- Add Fetch timeout and no-hang tests from Phase 03 into the advertised version matrix.
- Add leader epoch history tests before routing API 23.
- Add negative tests for stale/current leader epoch, unknown epoch, fenced epoch, and truncation-like scenarios.
- Add storage conformance tests for timestamp lookup, high watermark, last stable offset, and log start offset.

Acceptance gate: Consumers seek, fetch, timestamp-search, and leader-epoch paths match Kafka for every advertised version; `OffsetForLeaderEpoch` is advertised only after true leader-epoch semantics are implemented.

Do not do:
- Do not re-add the unsafe `OffsetForLeaderEpochRequest::KEY` route that only returns high watermark and echoes the requested epoch.
- Do not advertise Fetch versions that require incremental sessions unless sessions are implemented or proved unnecessary for those clients.
- Do not fake leader epoch history with a constant zero.
- Do not return empty success for cases Kafka treats as fencing, unknown epoch, or offset errors.

Fresh session handoff: Start by adding leader epoch requirements to the storage/log contract, then implement ListOffsets and Fetch fixes. Leave API 23 unadvertised until historical epoch tests pass.
