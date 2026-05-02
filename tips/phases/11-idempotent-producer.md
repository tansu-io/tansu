# Phase 11 - Idempotent Producer

Parallelism: MCP-PARALLEL

Depends on: Phase 07 - Produce Exactness

Can run with: Phase 09, Phase 10, Phase 13, and Phase 14. It must finish before Phase 12.

Goal: Finish idempotent producer correctness: producer IDs, epochs, per-partition sequence windows, duplicate suppression, restart recovery, and Kafka-equivalent fencing errors.

Current code anchors:
- `tansu-storage/src/service/init_producer_id.rs` owns InitProducerId behavior.
- `tansu-storage/src/service/produce.rs` must enforce idempotent append rules.
- `tansu-storage/src/lib.rs` exposes producer initialization and transaction-adjacent storage methods.
- `tansu-broker/tests/pg_init_producer.rs`, `txn.rs`, and `pg_txn.rs` cover current producer and transaction-adjacent behavior.
- `tansu-sans-io` defines producer ID, producer epoch, sequence, and error-code fields in Produce and transaction APIs.

Implementation steps:
- Define storage contract for producer state: producer ID allocation, producer epoch, transactional ID mapping, per-topic-partition sequence windows, last offset, and snapshot/recovery.
- Implement duplicate suppression, out-of-order sequence rejection, epoch fencing, unknown producer ID behavior, and sequence-number wrap handling.
- Make producer state survive broker restart and storage reconnection for production-parity engines.
- Keep non-idempotent Produce fast and exact while adding idempotent checks.
- Return exact errors such as `OutOfOrderSequenceNumber`, `DuplicateSequenceNumber`, `UnknownProducerId`, `InvalidProducerEpoch`, `ProducerFenced`, and transaction-related errors where applicable.
- Add compatibility behavior for Java producer defaults where `enable.idempotence=true` is common.
- Feed producer state into Phase 12 transaction state rather than building incompatible parallel state.

Tests:
- Add Java and librdkafka idempotent producer differential tests under retries, duplicate sends, request timeout, broker restart, and storage stall.
- Add per-partition sequence tests for duplicate, gap, stale epoch, new epoch, unknown producer, and wraparound boundaries.
- Add storage conformance tests for producer state recovery.
- Add Produce version tests that prove idempotent features are advertised only when exact.

Acceptance gate: `enable.idempotence=true` survives retries and broker restarts with no duplicates, no lost successful appends, and Kafka-equivalent fencing/error behavior.

Do not do:
- Do not treat InitProducerId success as idempotent producer support by itself.
- Do not keep producer sequence windows only in broker memory.
- Do not allow duplicate batches to append again after a retried Produce.
- Do not start Phase 12 EOS claims until idempotent producer state is durable and fenced correctly.

Fresh session handoff: Start with storage producer-state design and a Java idempotent retry test. Make `ProduceService` consult durable sequence windows before appending.
