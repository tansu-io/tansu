# Phase 09 - Topic Config Admin

Parallelism: MCP-PARALLEL

Depends on: Phase 05 - Route Coverage Safe Errors; Phase 06 - Storage Log Contract

Can run with: Phase 07, Phase 08, Phase 10, Phase 13, and Phase 14 after safe route behavior exists.

Goal: Finish topic, config, and AdminClient semantics so Kafka CLI and AdminClient can manage Tansu without special cases.

Current code anchors:
- `tansu-storage/src/service/create_topics.rs`, `delete_topics.rs`, `delete_records.rs`, `describe_configs.rs`, `incremental_alter_configs.rs`, `describe_topic_partitions.rs`, `list_partition_reassignments.rs`, and `metadata.rs`.
- `tansu-broker/src/service/storage.rs` wires current admin routes.
- `tansu-broker/tests/topic.rs`, `metadata.rs`, `describe_configs.rs`, and `policy_compact_delete.rs` cover existing admin behavior.
- `justfile` includes Kafka CLI targets for topics, configs, offsets, produce, consume, and groups.
- `README.md` documents Kafka CLI workflows and the current "all brokers are node 111" simplification.

Implementation steps:
- Certify CreateTopics, DeleteTopics, Metadata, DescribeConfigs, IncrementalAlterConfigs, DescribeTopicPartitions, ListPartitionReassignments, and DeleteRecords by API version.
- Add or complete CreatePartitions, legacy AlterConfigs if required by clients, DeleteRecords edge cases, config synonyms, config defaults, dynamic configs, validation-only behavior, and unknown config errors.
- Define exact behavior for replication factor and assignments under Tansu's stateless/storage-backed profiles.
- Implement topic ID behavior consistently across Metadata, Fetch, ListOffsets, DeleteTopics, and admin APIs.
- Make config support explicit for cleanup policy, retention, compaction, min.insync.replicas, max message bytes, timestamp type, segment-like compatibility aliases, and unsupported JVM-specific configs.
- Add metadata refresh tests for topic creation, deletion, partition changes, and storage-engine capability changes.
- Wire route coverage and ledger status for AdminClient-probed APIs.

Tests:
- Add Kafka CLI tests for `kafka-topics`, `kafka-configs`, `kafka-get-offsets`, and `kafka-reassign-partitions` where targeted.
- Add Java AdminClient differential tests for topic lifecycle, configs, validation-only, and delete records.
- Add negative tests for invalid topic names, invalid partitions, invalid replication factors, duplicate topics, unknown configs, unsupported configs, and unauthorized operations once Phase 14 lands.
- Add storage conformance tests for config-driven retention and compaction behavior that overlaps Phase 13.

Acceptance gate: Kafka CLI and Java AdminClient can create, describe, alter, delete, partition, configure, and inspect Tansu topics without Tansu-specific flags or client workarounds.

Do not do:
- Do not accept unknown configs as successful if Kafka would reject or report them.
- Do not silently ignore replication factor or assignment inputs without a documented profile rule.
- Do not let topic IDs drift across metadata APIs.
- Do not implement AdminClient happy paths without exact error behavior.

Fresh session handoff: Start with the AdminClient operation matrix, then fill gaps in service modules one API at a time. Update the Phase 01 ledger as each admin path moves from safe-error to supported.
