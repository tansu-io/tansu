# Phase 03 - Request Lifecycle No Hangs

Parallelism: SERIAL

Depends on: Phase 02 - ApiVersions Truth

Can run with: none while request lifecycle invariants are being introduced. Later data-plane phases may run after this gate.

Goal: Enforce the invariant that every accepted request responds, closes, or is cancelled. Fetch long polls and storage waits must be bounded, cancellable, and observable so no Kafka client sees silent hangs.

Current code anchors:
- `tansu-service/src/frame.rs` owns Kafka frame request/response decode and encode.
- `tansu-service/src/api.rs` dispatches frames through `FrameRouteService`.
- `tansu-broker/src/broker.rs` owns listener setup and connection handling.
- `tansu-storage/src/service/fetch.rs` loops over storage fetches and currently sleeps around `max_wait_ms`.
- `tansu-storage/src/service.rs` has `RequestChannelService`, `RequestStorageService`, and `CancellationToken` usage.
- `tansu-storage/src/proxy.rs` wraps storage calls and metrics.
- `tansu-broker/tests/fetch.rs` and `tansu-broker/tests/produce_fetch.rs` cover common fetch paths.

Implementation steps:
- Add a request lifecycle model with explicit states: accepted, dispatched, responded, closed, cancelled, timed out.
- Instrument correlation ID, API key, API version, client ID, and elapsed time around every route.
- Enforce per-connection and global in-flight limits before dispatch.
- Add route-level deadlines and storage-call cancellation for long-running calls.
- Make Fetch long polls use cancellable waits and return empty FetchResponse at the requested timeout boundary.
- Ensure disconnect cancels pending Fetch waits and storage proxy work.
- Make oversized frames, malformed frames, unsupported versions, auth failures, and storage stalls deterministic.
- Add metrics for in-flight requests, timed-out requests, cancelled waits, storage latency, and response bytes.

Tests:
- Add no-hang tests for Fetch with empty topics, low `min_bytes`, `max_wait_ms`, disconnect, and storage stall.
- Add request-pipeline tests showing response ordering by correlation ID.
- Add tests that every route either returns a response or a typed error that the frame layer turns into close/error behavior.
- Add cancellation tests for dropped client connections and broker shutdown.
- Add stress tests with many pending Fetch requests and bounded memory.

Acceptance gate: No client-visible hangs under timeout, disconnect, malformed request, unsupported version, or storage stall; every accepted request has a measured terminal state.

Do not do:
- Do not paper over hangs by reducing advertised Fetch versions alone.
- Do not spawn untracked background tasks for request handling.
- Do not let Fetch waiters survive connection close or broker shutdown.
- Do not convert Kafka-compatible errors into silent socket closes.

Fresh session handoff: Start by adding lifecycle instrumentation around frame dispatch, then harden Fetch wait/cancel behavior. Keep tests focused on externally visible no-hang guarantees.
