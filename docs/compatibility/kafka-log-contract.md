# Kafka Log Contract

Phase 06 makes the storage contract explicit. This file is the decision-grade source of truth for what the storage layer may claim today, what it only partially represents, and which future phase owns the unfinished work.

Phase 06 scope:
- Lock down the core log invariants for produce, fetch, list-offsets, and offset-stage.
- Record the current `Storage` method mapping.
- Publish the engine certification matrix used by the ledger.
- Keep Phase 07+ behavior out of scope.

## Invariant Matrix

| Kafka invariant | Current storage representation | Current status | Phase owner |
| --- | --- | --- | --- |
| Contiguous offsets | `produce` assigns append-only offsets and `fetch` reads by offset. | represented | Phase 06 |
| Log start offset | `offset_stage`, `list_offsets`, and `delete_records` expose or move the start boundary. | represented | Phase 06 now, Phase 13 for movement semantics |
| Log end offset | `offset_stage` and append visibility expose the current end boundary. | represented | Phase 06 |
| High watermark | `offset_stage` and `fetch` visibility use the committed end boundary. | represented | Phase 06 |
| Last stable offset | `offset_stage`, `fetch`, and transaction APIs expose the current stable read boundary. | partially represented | Phase 06 for basic reporting, Phase 12 for full transactional semantics |
| Timestamp lookup | `list_offsets` can resolve timestamps to offsets on the basic path. | partially represented | Phase 06 for baseline lookup, Phase 08 for exactness |
| Batch validation | `produce` validates basic record batches before append. | partially represented | Phase 07/11 |
| Compression | `produce` accepts compressed payloads when the engine can store them. | partially represented | Phase 07/11 |
| Producer state | No first-class producer-state model yet. | not first-class | Phase 07/11 |
| Tombstones | `fetch` and append preserve delete markers as log records. | represented | Phase 06 |
| Transactions | Transactional methods are routed, but full visibility rules are not contracted yet. | not first-class | Phase 12 |
| Retention | Background cleanup exists only as an engine concern. | not first-class | Phase 13 |
| Compaction | Compaction is engine-dependent and not a Phase 06 contract claim. | not first-class | Phase 13 |
| Delete-records | `delete_records` can truncate a log start boundary when the engine supports it. | partially represented | Phase 13 |
| Crash recovery | Durable engines can reopen and continue from stored log state. | partially represented | Phase 06 for the core slice, later phases for stronger guarantees |
| Leader epoch cache | No explicit leader-epoch history model is exposed yet. | not first-class | Phase 08/15 |

## Current `Storage` Mapping

| Storage method | Kafka log concern | Current status |
| --- | --- | --- |
| `produce` | Append ordering, contiguous offsets, batch validation, compression handling, producer state, crash recovery. | represented for the Phase 06 append slice; partially represented for the rest |
| `fetch` | Read visibility, high watermark, last stable offset, tombstones, and offset-based reads. | represented |
| `offset_stage` | Log start, log end, high watermark, and last stable offset snapshots. | represented |
| `list_offsets` | Earliest/latest lookup, timestamp lookup, and log boundary reporting. | represented for earliest/latest; partially represented for timestamp exactness |
| `delete_records` | Log start offset movement and retention-style truncation. | partially represented |
| `txn_add_offsets` / `txn_add_partitions` / `txn_offset_commit` / `txn_end` | Transactional visibility and committed-read semantics. | not first-class |
| `metadata` | Topic and partition metadata used by fetch and list-offset correctness. | represented |
| `maintain` | Recovery, retention, compaction, and cleanup hooks. | partially represented |

## Current Status Vocabulary

| Status | Meaning |
| --- | --- |
| represented | The trait surface exists and the storage layer already exposes a usable slice of the invariant. |
| partially represented | The trait surface exists, but the semantic contract is incomplete or engine-specific. |
| not first-class | The behavior is not yet modeled as a storage contract claim. |

## Phase Owner Map

| Phase | Owns |
| --- | --- |
| Phase 06 | Core log contract: contiguous offsets, log start/end, high watermark, tombstones, and baseline timestamp lookup. |
| Phase 07/11 | Produce exactness, batch validation, compression, and producer-state fidelity. |
| Phase 08/15 | Leader epoch cache, truncation history, and exact timestamp lookup semantics. |
| Phase 12 | Transactions, LSO rules, and transactional visibility. |
| Phase 13 | Delete-records, retention, and compaction semantics. |

## Engine Claims

| Engine | Phase 06 claim | Condition |
| --- | --- | --- |
| PostgreSQL | `production-parity` | Only for the Phase 06 core invariants proven by the shared storage conformance tests. |
| SQLite / libSQL | `limited-parity` | Only if the feature-gated Phase 06 conformance path passes; otherwise `uncertified`. |
| S3 / dynostore | `limited-parity` | Only if the feature-gated Phase 06 conformance path passes; otherwise `uncertified`. |
| SlateDB | `limited-parity` | Only if the feature-gated Phase 06 conformance path passes; otherwise `uncertified`. |
| Turso | `uncertified` | Remains uncertified unless the implementation has a reliable feature-gated Phase 06 test path. |
| Memory | `development-test-only` | Suitable for local and CI conformance only. |
| Null | `unsupported` | Does not provide log-storage invariants; metadata-only bootstrap remains separate. |

## Schema And Migration Expectations

- Any future schema change for leader epochs, compaction, retention, producer state, or transactions must update this contract before implementation lands.
- Any migration that changes log-start, log-end, high-watermark, or LSO behavior must add or update the Phase 06 proof entries in the ledger.
- Engine claims may only move upward when the corresponding gated conformance tests exist and are recorded in the ledger.
- Null storage may continue to support metadata-only broker bootstrap, but it must not fake log-storage success.
- Phase 06 does not widen advertised Kafka APIs and does not claim Phase 07+ behavior.
