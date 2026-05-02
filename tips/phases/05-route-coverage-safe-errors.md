# Phase 05 - Route Coverage Safe Errors

Parallelism: MCP-PARALLEL

Depends on: Phase 02 - ApiVersions Truth

Can run with: Phase 03, Phase 04, Phase 06, and Phase 14 when route advertisement is registry-gated.

Goal: Add safe route coverage for missing stable Kafka ecosystem APIs. When semantics are incomplete, the route must return exact Kafka "not supported yet" behavior and must not be advertised as semantically supported.

Current code anchors:
- `tansu-broker/src/service/storage.rs` routes storage-backed APIs.
- `tansu-broker/src/service/coordinator.rs` routes classic consumer group APIs.
- `tansu-broker/src/service/auth.rs` routes SASL APIs.
- `tansu-storage/src/service.rs` exports storage-backed service modules.
- `tansu-sans-io` has generated request/response structs for many Kafka APIs not yet routed.
- `tansu-service/src/api.rs` and the Phase 02 registry determine whether route presence is advertised.

Implementation steps:
- Generate a route coverage report from `RootMessageMeta` API keys, broker route registration, and the Phase 01 ledger.
- Classify each missing stable Kafka 4.2 API as implement now, safe error route, intentionally unrouteable client-internal API, or future target.
- Add skeleton services for ecosystem-probed APIs that common clients/tools call even when applications do not.
- Return Kafka-compatible structured errors for incomplete APIs, with correct response schemas for the requested version.
- Add explicit support notes for APIs such as CreatePartitions, DeleteAcls, AlterConfigs legacy path, DescribeLogDirs, AlterReplicaLogDirs, AlterPartitionReassignments, OffsetDelete, ElectLeaders, client quota APIs, DescribeProducers, DescribeTransactions, ListTransactions, EndTxn, WriteTxnMarkers, UpdateFeatures, PushTelemetry, and ListConfigResources.
- Keep skeleton services out of advertised semantic support until Phase 04 differential tests prove behavior.

Tests:
- Add route coverage snapshots comparing Kafka 4.2 API keys, generated codecs, registered routes, and advertised support.
- Add AdminClient smoke tests that probe unsupported APIs and receive structured errors without crashing or hanging.
- Add CLI tests for tools that probe broad admin surfaces.
- Add versioned response-schema tests for each safe-error route.

Acceptance gate: AdminClient and common Kafka tools do not crash or hang on missing routes, and unsupported semantics are observable as Kafka-compatible errors rather than unknown services or silent closes.

Do not do:
- Do not add a route if it causes ApiVersions to over-advertise.
- Do not implement "success with empty response" where Kafka would return an error.
- Do not treat broker-internal Kafka/KRaft APIs as production client support without a product decision.
- Do not combine safe-error skeletons with partial state mutations.

Fresh session handoff: Start by generating the API coverage report and choose the first AdminClient-probed APIs. Wire them behind Phase 02 advertising controls and add tests before expanding the route list.
