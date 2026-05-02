# Phase 10 - Consumer Groups Offsets

Parallelism: MCP-PARALLEL

Depends on: Phase 03 - Request Lifecycle No Hangs; Phase 06 - Storage Log Contract

Can run with: Phase 07, Phase 08, Phase 09, Phase 13, and Phase 14 after request cancellation and storage offset invariants are stable.

Goal: Complete committed offsets and classic consumer group coordinator behavior under churn, static membership, cooperative rebalance, and offset retention.

Current code anchors:
- `tansu-broker/src/service/coordinator.rs` routes JoinGroup, SyncGroup, Heartbeat, LeaveGroup, OffsetCommit, and OffsetFetch.
- `tansu-storage/src/service.rs` includes group and offset request/response types plus `UpdateGroup` and offset commit/fetch requests.
- `tansu-storage/src/lib.rs` exposes group detail, named group detail, committed offsets, and group update methods.
- `tansu-broker/tests/cg.rs`, `cg_dynamic.rs`, `cg_static.rs`, and `auth.rs` cover current group and auth interactions.
- `tansu-broker/tests/common/` has shared broker/storage test helpers.

Implementation steps:
- Define advertised caps for classic group APIs based on actual coordinator semantics.
- Complete JoinGroup, SyncGroup, Heartbeat, LeaveGroup, OffsetCommit, OffsetFetch, DeleteGroups, DescribeGroups, ListGroups, and ConsumerGroupDescribe behavior.
- Implement generation fencing, member IDs, static membership, rebalance timeout, session timeout, member expiration, leader selection, assignment bytes, and cooperative rebalance rules.
- Make offset commits exact for metadata, leader epoch, retention time, commit timestamp, expire timestamp, stable offsets, and group state constraints.
- Add offset retention and cleanup as a storage/log invariant.
- Ensure coordinator lookup and group operations behave under multiple stateless brokers.
- Decide when to support Kafka's newer ConsumerGroupHeartbeat protocol; until then, steer clients safely through ApiVersions/configuration.

Tests:
- Add multi-client Java consumer group tests with dynamic membership, static membership, rolling restarts, delayed heartbeats, and cooperative rebalance.
- Add librdkafka and Sarama group churn tests.
- Add offset commit/fetch differential tests for metadata, missing groups, deleted groups, stale generations, and require-stable.
- Add no-hang tests for coordinator requests under disconnect and storage stall.
- Add storage conformance tests for committed offset retention and group state update conflict handling.

Acceptance gate: Java, librdkafka, and Sarama consumer groups rebalance repeatedly without stuck members, lost committed offsets, false success, or client-specific settings.

Do not do:
- Do not advertise new group protocol support until ConsumerGroupHeartbeat semantics are implemented.
- Do not collapse all group errors into generic coordinator errors.
- Do not allow stale members or generations to commit offsets successfully.
- Do not treat group state as in-memory broker state only; stateless broker correctness must survive broker restart.

Fresh session handoff: Start by building a group behavior matrix from Java client expectations. Add differential churn tests first, then adjust coordinator state transitions and storage conflict handling.
