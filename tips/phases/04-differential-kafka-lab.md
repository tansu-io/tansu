# Phase 04 - Differential Kafka Lab

Parallelism: MCP-PARALLEL

Depends on: Phase 01 - Compatibility Contract

Can run with: Phase 02, Phase 03, Phase 05, and Phase 06 after the Phase 01 ledger shape is stable.

Goal: Build an Apache Kafka vs Tansu differential lab that runs the same client and protocol workloads against both systems, then records response, error, offset, metadata, timing, and lifecycle differences.

Current code anchors:
- `compose.yaml`, `justfile`, and `README.md` already support local Tansu, PostgreSQL, S3/MinIO, Prometheus, and Kafka CLI-style workflows.
- `tansu-broker/tests/` contains broker behavior tests for auth, consumer groups, fetch, list offsets, metadata, produce/fetch, topics, transactions, and policies.
- `tansu-sans-io/tests/` contains protocol tests and proptest coverage.
- `fuzz/` is available for protocol and malformed input expansion.
- `docs/sarama.md` documents a Go/Sarama path that can become a client fixture.
- `tansu-perf/` can become the seed for workload drivers and measurement.

Implementation steps:
- Add a test harness that can start Apache Kafka 4.2 and Tansu side by side with isolated bootstrap addresses.
- Create common workload descriptions for produce, fetch, list offsets, metadata, admin, consumer groups, idempotence, and transactions.
- Run Java client and librdkafka first, then franz-go and Sarama where practical.
- Capture ApiVersions negotiation, request versions chosen by clients, response errors, offsets, timestamps, high watermarks, group states, and timeout behavior.
- Add Kafka CLI fixtures for `kafka-topics`, `kafka-configs`, `kafka-get-offsets`, `kafka-console-producer`, `kafka-console-consumer`, and `kafka-consumer-groups`.
- Store differential results as CI artifacts and feed pass/fail status back to the Phase 01 ledger.
- Make the harness reusable by later phases instead of one-off scripts.

Tests:
- Add a smoke test that creates a topic, produces records, consumes records, and commits offsets against both Kafka and Tansu.
- Add an ApiVersions diff test that compares Tansu advertised APIs to the Phase 01 ledger and Kafka 4.2 reference behavior.
- Add negative differential tests for unsupported versions, unknown topics, invalid partitions, bad config names, and timeout boundaries.
- Add client matrix jobs that can be run in CI or manually when external tools are not installed.

Acceptance gate: CI can compare Tansu behavior against real Kafka for each advertised API and publish a result that updates or validates the compatibility ledger.

Do not do:
- Do not rely only on Rust unit tests for compatibility claims.
- Do not make a harness that only tests the happy path.
- Do not require external services without documenting exact versions and startup commands.
- Do not let differences be ignored without an explicit ledger entry.

Fresh session handoff: Start with one end-to-end workload using Java client or Kafka CLI against Kafka 4.2 and Tansu. Make result capture boring and repeatable before adding more clients.
