# Phase 01 - Compatibility Contract

Parallelism: SERIAL

Depends on: none

Can run with: none. This phase defines the contract that every later parallel phase must obey.

Goal: Define the public Kafka replacement contract before more feature work lands. Kafka 4.2 client and protocol behavior is the baseline for the first official replacement target. Kafka 4.3 should be tracked as a future target until it has an official release. Tansu remains stateless and storage-backed internally, but replacement claims must be per certified profile, not blanket claims.

Current code anchors:
- `README.md` currently positions Tansu as a stateless Kafka-compatible broker with PostgreSQL, SQLite, S3, and memory storage.
- `docs/` and `docs/sarama.md` already show ecosystem-facing usage examples.
- `tansu-sans-io/src/lib.rs` owns generated Kafka protocol metadata through `RootMessageMeta`.
- `tansu-service/src/api.rs` builds ApiVersions from routed API keys today.
- `tansu-broker/src/service/storage.rs`, `tansu-broker/src/service/coordinator.rs`, and `tansu-broker/src/service/auth.rs` define the broker's routed API surface.
- `tansu-storage/src/lib.rs` defines the `Storage` trait and storage-engine abstraction.
- Existing research notes in `tips/tansu_phases/` and `tips/stage_11/` are inputs, not official phase plans.
- Compatibility references: https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/, https://kafka.apache.org/community/downloads/, https://cwiki.apache.org/confluence/display/KAFKA/Release%2BPlan%2B4.3.0, and https://kafka.apache.org/42/design/protocol/.

Implementation steps:
- Create a compatibility ledger that is generated or checked from code, not hand-waved in prose.
- Track every Kafka API key and version with columns for codec support, route presence, advertised range, semantic status, Java client status, librdkafka status, franz-go or Sarama status, Kafka CLI status, storage-engine status, and failure-mode coverage.
- Define product profiles: `tansu-native`, `kafka-parity-postgres`, `kafka-parity-sqlite`, `kafka-parity-s3-dynostore`, `kafka-parity-slatedb`, and `compat-preview`.
- Define storage certification tiers: production parity, limited parity, development/test only, and unsupported for a given feature.
- Make "no advertised API without proof" the rule that blocks Phase 02 and every API route added later.
- Define how Tansu's stateless broker model maps to Kafka-visible terms: broker ID, controller ID, leader, ISR, high watermark, leader epoch, and coordinator.
- Add a public status page under `docs/` or `tips/compatibility/` and wire a CI check that fails if routed APIs, advertised APIs, and the ledger drift.

Tests:
- Add a unit or snapshot test that compares broker route keys against the compatibility ledger.
- Add a snapshot test that compares `RootMessageMeta` API keys against ledger rows so missing Kafka APIs are explicit.
- Add a docs test or script that fails on missing product-profile status for any routed API.
- Add a CI job that publishes or stores the ledger artifact for review.

Acceptance gate: A public ledger exists for API/version/client/storage status, Kafka 4.2 is the documented baseline, Kafka 4.3 is tracked as future work, and no route can be called fully supported unless the ledger points to semantic tests.

Do not do:
- Do not claim blanket "drop-in Kafka replacement" across every storage engine.
- Do not use descriptor metadata or route presence as proof of compatibility.
- Do not delete the research notes under `tips/tansu_phases/` or `tips/stage_11/`.
- Do not start broad feature implementation before this contract exists.

Fresh session handoff: Start by creating the ledger format and an initial generated route/API inventory. Use this file as the authority for wording, and keep all later phases tied back to the "no advertised API without proof" rule.
