# librdkafka v2.14.2 sweep findings (2026-06-11)

Initial sweep of the 36 tests that create topics through the Admin API
(the rest of the ~144 test suite needs Metadata auto-topic-creation),
against `tansu broker --storage-engine=memory://` from commit 6db7a49.

Passing tests are in `tests.allow`. Failures, most impactful first:

1. **No automatic topic creation on MetadataRequest.** Kafka with
   `auto.create.topics.enable=true` (the suite's documented prerequisite)
   creates a topic when Metadata names it with
   `allow_auto_topic_creation=true`; tansu only returns
   `UNKNOWN_TOPIC_OR_PARTITION`. Blocks ~100 further tests
   (0001, 0011 subtest, 0042, ...). Implementing this unlocks most of the
   remaining suite.

2. **CreatePartitions returns an empty topics array** —
   `CreatePartitions_result_topics returned NULL` (0044, 0069, 0112).

3. **ListOffsets-style queries hang the client** — 0031 (get_offsets) and
   0034 (offset_reset) time out after the topic is created and messages
   produced.

4. **OffsetCommit `metadata` is not round-tripped** — committed metadata
   string comes back null from OffsetFetch (0099).

5. **InitProducerId does not validate `transaction.timeout.ms`** — Kafka
   rejects values above `transaction.max.timeout.ms` (default 15 min) with
   fatal `INVALID_TRANSACTION_TIMEOUT`; tansu accepts them (0103
   `do_test_misuse_txn`; the rest of 0103 passes).

6. **Excess consumer-group rebalances** — consumers observed one more
   rebalance than Kafka produces in the same scenario (0091, 0093).

7. **CreateTopics corner cases** — 0081 (admin) fails in
   `do_test_CreateTopics` waiting for a metadata update after creating
   topics with mixed valid/invalid configs.

Also observed: 0086 (purge) failed once against a broker that had served
~50 prior tests but passes on a fresh broker — possible sensitivity to
accumulated state, unconfirmed.

Skipped (environmental, not compatibility): 0028/0075/0088 (sockem,
filtered by `-E`), 0101 (needs RapidJSON), 0107 (interactive), 0142
(needs SASL listener).
