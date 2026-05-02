# Phase 14 - Security ACLs Quotas

Parallelism: MCP-PARALLEL

Depends on: Phase 02 - ApiVersions Truth; Phase 03 - Request Lifecycle No Hangs

Can run with: Phase 05, Phase 07, Phase 08, Phase 09, Phase 10, Phase 11, and Phase 13 as long as every new route calls the same authorization and quota hooks.

Goal: Finish TLS/mTLS listener handling, SASL/SCRAM, ACL enforcement on every API, quotas, throttling, and audit visibility.

Current code anchors:
- `tansu-broker/src/service/auth.rs` routes SASL handshake and authenticate flows.
- `tansu-auth/` owns authentication implementation details.
- `tansu-storage/src/service/create_acls.rs`, `describe_acls.rs`, `alter_user_scram_credentials.rs`, and `describe_user_scram_credentials.rs` cover ACL/SCRAM admin surfaces.
- `command-plain.properties`, `command-scram-256.properties`, `command-scram-512.properties`, `jaas-plain.conf`, and `jaas-scram.conf` support manual client testing.
- `tansu-broker/tests/auth.rs` covers current auth behavior.
- `tansu-storage/src/proxy.rs` and OpenTelemetry metrics provide hooks for quotas and throttling.

Implementation steps:
- Complete TLS and mTLS listener handling before Kafka protocol requests are processed on SSL listeners.
- Certify SASL/PLAIN and SASL/SCRAM SHA-256/SHA-512 with Java client and librdkafka.
- Decide whether OAUTHBEARER and delegation tokens are in scope for Kafka 4.2 parity profiles.
- Add ACL enforcement hooks for every route, including topic, group, cluster, transactional ID, delegation token, and user resources.
- Ensure denied operations return Kafka-compatible authorization errors, not generic failures.
- Implement producer, consumer, request, connection, and controller/admin quotas with response throttle fields where Kafka uses them.
- Add audit logs with principal, listener, client ID, API key/version, resource, decision, and reason.
- Make security behavior profile-aware but never weaker than documented.

Tests:
- Add Java and librdkafka SASL_SSL/SCRAM tests with successful and failed auth.
- Add ACL matrix tests for Produce, Fetch, Metadata, Admin, group, transaction, and SCRAM/ACL management APIs.
- Add quota tests that verify throttle fields, delayed responses, bounded memory, and client retry compatibility.
- Add TLS handshake tests proving no Kafka protocol request is accepted before SSL handshake on SSL listeners.
- Add audit snapshot tests for allowed and denied operations.

Acceptance gate: Existing SASL_SSL/SCRAM Kafka clients work without weakening security, unauthorized operations fail with Kafka-compatible errors, and quotas throttle clients predictably.

Do not do:
- Do not leave any route outside ACL enforcement once the security profile is enabled.
- Do not accept plaintext Kafka protocol on TLS listeners.
- Do not implement throttling only as socket slowdown when Kafka clients expect throttle fields.
- Do not log secrets, SCRAM credentials, or raw passwords in audit output.

Fresh session handoff: Start by mapping every routed API to required ACL operations and resources. Then harden TLS/SASL listener behavior and add enforcement middleware before route-specific changes continue.
