# Phase 12 - Transactions EOS

Parallelism: SERIAL

Depends on: Phase 08 - Fetch ListOffsets Leader Epoch; Phase 10 - Consumer Groups Offsets; Phase 11 - Idempotent Producer

Can run with: none for core implementation. Measurement and client fixture preparation in Phase 16 can continue separately.

Goal: Implement Kafka exactly-once semantics: transaction coordinator, EndTxn, WriteTxnMarkers, transaction state, last stable offset, aborted index, read_committed fetches, timeout expiration, fencing, and crash recovery.

Current code anchors:
- `tansu-storage/src/service/txn/` contains AddOffsetsToTxn, AddPartitionsToTxn, and TxnOffsetCommit services.
- `tansu-storage/src/lib.rs` includes transaction request variants and storage methods such as transaction add, commit, and end concepts.
- `tansu-broker/src/service/storage.rs` routes transaction-adjacent APIs that already exist.
- `tansu-broker/tests/txn.rs` and `pg_txn.rs` cover current transaction-adjacent behavior.
- `tansu-sans-io` has generated schemas for transaction APIs including EndTxn, WriteTxnMarkers, DescribeTransactions, and ListTransactions.

Implementation steps:
- Define a transaction coordinator state model compatible with Kafka: transactional ID, producer ID, producer epoch, timeout, partitions, pending offsets, state transitions, and fencing.
- Route and implement EndTxn, WriteTxnMarkers, DescribeTransactions, and ListTransactions with truthful ApiVersions caps.
- Persist transaction state and recovery records in storage engines that claim EOS support.
- Add commit/abort markers or an equivalent visibility index that makes Fetch `read_committed` exact.
- Implement last stable offset and aborted transaction lists for Fetch.
- Make TxnOffsetCommit atomic with transaction state and group offsets.
- Implement transaction timeout expiration and coordinator failover/restart behavior under stateless brokers.
- Define safe errors for storage engines that cannot support EOS.

Tests:
- Add Java transactional producer differential tests for begin, send, send offsets to transaction, commit, abort, timeout, and fencing.
- Add Kafka Streams EOS tests once basic Java producer semantics pass.
- Add `read_committed` vs `read_uncommitted` consumer tests with aborted transactions.
- Add crash/restart tests for transaction state, producer state, group offsets, and last stable offset.
- Add versioned response tests for EndTxn, WriteTxnMarkers, DescribeTransactions, and ListTransactions.

Acceptance gate: Kafka Streams EOS and transactional producers pass differential tests, read_committed consumers match Kafka visibility, and crash recovery preserves transaction decisions.

Do not do:
- Do not claim EOS with only InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, and TxnOffsetCommit.
- Do not expose `read_committed` as equivalent to `read_uncommitted`.
- Do not ignore abort markers or aborted transaction lists.
- Do not implement transactions for an engine whose storage contract cannot make them durable and recoverable.

Fresh session handoff: Start from the transaction state machine and storage schema. Route EndTxn only when transaction decisions can affect Fetch visibility and offset commits atomically.
