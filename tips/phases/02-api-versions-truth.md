# Phase 02 - ApiVersions Truth

Parallelism: SERIAL

Depends on: Phase 01 - Compatibility Contract

Can run with: none while the compatibility contract and ApiVersions enforcement are being changed. Later route work depends on this gate.

Goal: Replace descriptor-derived ApiVersions advertising with explicit implemented, tested, and advertised version caps. Unsupported versions and unsupported APIs must return Kafka-compatible errors instead of causing clients to select paths Tansu cannot honor.

Current code anchors:
- `tansu-service/src/api.rs` has `ApiVersionsService` returning `RootMessageMeta` min/max versions for every supported route key.
- `tansu-service/src/api.rs` has `FrameRouteBuilder::with_route` and `with_api_versions` where route registration and advertised APIs meet.
- `tansu-sans-io/src/lib.rs` exposes `RootMessageMeta::messages()` and descriptor version ranges.
- `tansu-service/src/frame.rs` decodes request versions from frames and is the right place to preserve requested-version context.
- `tansu-broker/src/service/storage.rs`, `tansu-broker/src/service/coordinator.rs`, and `tansu-broker/src/service/auth.rs` register routed APIs.
- `tansu-broker/src/broker.rs` already inspects protocol metadata for broker-visible behavior.

Implementation steps:
- Introduce an explicit compatibility registry, for example `ApiSupport { api_key, implemented, tested, advertised, notes }`, loaded from code or generated from the Phase 01 ledger.
- Make `ApiVersionsService` use advertised caps from the registry, never raw descriptor max versions.
- Keep descriptor metadata for encoding and decoding only; do not use it as semantic proof.
- Ensure ApiVersions itself follows Kafka 4.2 behavior when a client asks for an unsupported ApiVersions request version, including a version 0 response with `UNSUPPORTED_VERSION` where required by Kafka compatibility.
- Make request dispatch reject unsupported request versions before invoking a service whose semantics do not cover that version.
- Add a helper for exact unsupported-version responses per API and requested response schema version.
- Gate route additions so a new route must also add compatibility registry entries and tests.
- Rebuild any future `OffsetForLeaderEpoch` support here or later behind truthful API 23 advertisement; do not reintroduce the unsafe route reverted for this phase plan.

Tests:
- Add unit tests for `ApiVersionsService` showing advertised ranges are the registry caps, not `RootMessageMeta` caps.
- Add negative tests for supported API keys with unsupported versions returning `UNSUPPORTED_VERSION`.
- Add tests for unknown API keys and semantically incomplete APIs returning Kafka-compatible errors without panics or hangs.
- Add route/registry consistency tests: routed but unadvertised is allowed only with an explicit preview or disabled status; advertised without semantic tests is forbidden.
- Add client negotiation smoke tests with Java client and librdkafka once Phase 04 exists.

Acceptance gate: No route advertises a version without semantic tests, and every advertised version is traceable to an explicit compatibility registry entry.

Do not do:
- Do not advertise a full descriptor range because a request/response codec exists.
- Do not add placeholder routes that cause ApiVersions to expose unsupported semantics.
- Do not hide unsupported behavior by closing the connection when Kafka would return a structured error.
- Do not route `OffsetForLeaderEpochRequest::KEY` until leader-epoch history, fencing, and errors are implemented.

Fresh session handoff: Start in `tansu-service/src/api.rs`. Separate route registration from ApiVersions advertisement, add the smallest registry shape that can satisfy Phase 01, and prove it with tests before changing any feature service.
