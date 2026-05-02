# Phase 15 - Cluster Metadata Modern Kafka

Parallelism: SERIAL

Depends on: Phase 06 - Storage Log Contract; Phase 08 - Fetch ListOffsets Leader Epoch; Phase 10 - Consumer Groups Offsets

Can run with: none for the metadata/controller facade. Phase 16 measurement and packaging prep can run around it.

Goal: Build a cluster metadata and controller facade with real broker IDs, leader/ISR-like metadata, reassignment behavior, and targeted modern Kafka 4.2 APIs such as ConsumerGroupHeartbeat, share groups, and streams groups where the compatibility profile requires them.

Current code anchors:
- `README.md` documents the current simplification that all brokers appear as node 111 in examples.
- `tansu-broker/src/broker.rs` registers brokers and owns listener identity.
- `tansu-storage/src/lib.rs` exposes broker registration, cluster ID, node, advertised listener, metadata, and group/transaction state.
- `tansu-storage/src/service/metadata.rs`, `describe_cluster.rs`, `describe_topic_partitions.rs`, and `list_partition_reassignments.rs` expose cluster metadata to clients.
- `tansu-broker/tests/metadata.rs` and `describe_cluster.rs` verify current metadata shape.
- `tansu-sans-io` includes Kafka 4.2 APIs such as ConsumerGroupHeartbeat, share-group APIs, streams-group APIs, telemetry, and feature APIs.

Implementation steps:
- Define the Kafka-visible cluster facade for stateless Tansu brokers: stable broker IDs, controller ID, rack, listener names, advertised listeners, broker liveness, leader identity, ISR-like fields, leader epoch, and partition availability.
- Decide how replication factor, min.insync.replicas, acks=all, reassignment, and leader election map to storage-backed durability profiles.
- Implement metadata refresh behavior and errors such as leader not available, not leader or follower, fenced leader epoch, and unknown topic/partition with Kafka timing.
- Complete DescribeCluster, Metadata, DescribeTopicPartitions, partition reassignment, and broker registration behavior for multi-broker Tansu.
- Decide and implement targeted Kafka 4.2 modern APIs: ConsumerGroupHeartbeat, ShareFetch, ShareAcknowledge, ShareGroupHeartbeat, ShareGroupDescribe, StreamsGroupHeartbeat, StreamsGroupDescribe, telemetry PushTelemetry, and UpdateFeatures.
- Keep non-targeted broker-internal KRaft APIs explicitly unsupported or safe-error routed.

Tests:
- Add multi-broker tests with distinct broker IDs, advertised listeners, metadata refresh, broker restart, and client reconnect.
- Add AdminClient tests for describe cluster, metadata, partition reassignment, and leader/ISR-like fields.
- Add client tests for modern consumer group protocol if targeted, otherwise tests proving clients are steered to supported classic behavior.
- Add share group and streams group differential tests only when those profiles are enabled.
- Add chaos tests around broker liveness and metadata changes without storage corruption.

Acceptance gate: Multi-broker Tansu is Kafka-shaped to clients and AdminClient, with stable metadata, honest modern API support, and profile-specific behavior documented in the compatibility ledger.

Do not do:
- Do not expose all brokers as node 111 in a production parity profile.
- Do not pretend Tansu has Kafka's internal ISR/controller model unless the facade gives clients equivalent observable behavior.
- Do not advertise share groups, streams groups, or ConsumerGroupHeartbeat until the state machines are implemented and tested.
- Do not let KRaft-internal APIs become accidental public support.

Fresh session handoff: Start by writing the cluster facade semantics in the ledger, then update broker registration and metadata services to expose stable multi-broker identities.
