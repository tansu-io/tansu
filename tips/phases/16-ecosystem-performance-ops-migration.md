# Phase 16 - Ecosystem Performance Ops Migration

Parallelism: SERIAL

Depends on: Phase 12 - Transactions EOS; Phase 15 - Cluster Metadata Modern Kafka. Measurement work can start early, but the final release gate depends on all core semantics.

Can run with: Measurement, packaging, docs, and harness preparation can run alongside Phases 07 through 15. Final certification is last.

Goal: Prove the Kafka replacement story through ecosystem certification, performance, chaos, operations, packaging, migration, rollback, and a public compatibility matrix.

Current code anchors:
- `README.md`, `docs/`, and `demo/` are the user-facing migration and usage surfaces.
- `compose.yaml`, `Dockerfile`, `example.env`, and `justfile` are the local operations baseline.
- `etc/prometheus.yaml`, `etc/grafana/`, `tansu-otel/`, and tracing/metrics usage are observability anchors.
- `tansu-perf/` is the performance benchmark seed.
- `tansu-cli/`, `tansu-topic/`, `tansu-cat/`, and `tansu-proxy/` are operational tooling surfaces.
- Phase 04 differential lab artifacts and the Phase 01 compatibility ledger become release evidence.

Implementation steps:
- Certify ecosystem clients and tools: Java producer/consumer/AdminClient, librdkafka, franz-go, Sarama, Kafka CLI, Kafka Connect, MirrorMaker 2, Kafka Streams, Debezium, Flink/Spark Kafka connectors, KafkaJS or aiokafka where targeted, and common UI/monitoring tools.
- Publish compatibility by profile, client version, Kafka protocol version, storage engine, and feature area.
- Benchmark producer throughput/latency, consumer throughput/latency, group rebalance latency, transaction latency, compaction cost, storage-engine throughput, CPU, RAM, file/object/DB growth, and tail latency.
- Run chaos tests for broker kill, storage stall, storage disconnect, partial write, network interruption, client retry storms, rolling upgrades, and config changes.
- Finish operational packaging: Docker image, Helm chart or manifests, health/readiness, backup/restore, storage migrations, rolling upgrades, metrics dashboards, logs, and alert examples.
- Write migration guides for Kafka to Tansu by profile, including prerequisites, incompatibilities, bootstrap change, validation, rollback, and mixed operation.
- Add release gates that require the compatibility ledger, differential lab, performance reports, and ops docs to agree.

Tests:
- Add end-to-end ecosystem suites for Connect, MirrorMaker, Streams, CLI tools, and selected language clients.
- Add performance regression jobs with documented hardware/storage profiles.
- Add chaos test jobs for broker/storage/client failures.
- Add packaging smoke tests for Docker, compose, and deployment manifests.
- Add migration dry-run tests from Kafka-exported data or MirrorMaker flows into Tansu.

Acceptance gate: A migration guide can honestly say when changing only `bootstrap.servers` is supported, with a public matrix proving the client, protocol, feature, and storage profile boundaries.

Do not do:
- Do not publish a GA replacement claim based only on unit tests or small demos.
- Do not collapse all storage engines into one performance or durability claim.
- Do not omit rollback and failure-mode documentation.
- Do not let lakehouse, schema, or proxy differentiators distract from Kafka-core replacement proof.

Fresh session handoff: Start by collecting Phase 01 and Phase 04 artifacts into a release certification page. Use that as the checklist for ecosystem, performance, ops, and migration work.
