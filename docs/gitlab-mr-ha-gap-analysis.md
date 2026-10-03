# GitLab merge request ingestion and highly available consumers

This is the pre-implementation audit. The producer implementation now adds
provider-neutral Standard Webhooks verification, selected header forwarding,
numeric JSONPath keys, replay barriers, transactional fencing, bounded forwarding,
and shutdown draining. See [the producer setup](../demo/gitlab/README.md) for the
current configuration and consumer handoff. Consumer implementation remains
owned by the application team; this audit does not qualify a deployed HA system.

Review date: 2026-10-02.
Gateway revision: `677419af8c8f4520a80389f45601cc8f106ee1e3`.
Pinned Rust client revision: `c4ce581587772b3750d88c61f3389d9626a20235`.
Operator checkout reviewed: `503d548d5c0841d17249b142ef8a317d5086e87f`.

Target: the latest GitLab.com SaaS, with a new signed webhook.
Legacy GitLab authentication and Self-Managed deployment support are outside scope.

This review uses source code and official GitLab documentation.
It does not include a live GitLab deployment or a failure test.
Findings identify implementation gaps and application responsibilities.
They do not qualify the complete system for production.

## Proposed flow

```text
GitLab -> HTTPS load balancer -> gateway replicas -> replicated MR topic
                                                     |
                                          consumer group per service
                                                     |
                                          durable application effects
```

Replicas of one service share a consumer group.
Different services use different groups when each service needs every event.
Prefer native krabka consumers for server microservices.
This avoids an additional gateway connection between each worker and the broker.
The native client already supports groups, manual commits, rebalance callbacks,
timeouts, retry policies, and committed reads.
[Source: pinned consumer implementation](https://github.com/krabka-io/krabka-client-rs/blob/c4ce581587772b3750d88c61f3389d9626a20235/crates/client-consumer/src/consumer.rs#L1020).

## Existing components

- Named webhook routes accept raw JSON and have configurable body limits.
- Header extraction can supply the idempotency key.
- Plain publishing uses producer idempotence and `acks=all`.
- The dedup engine writes the payload and claim in one transaction.
- Replicas divide dedup partitions and forward requests to the owner.
- Consumer groups, committed reads, and explicit acknowledgment frames exist.
- The operator supports replicas, webhook configuration, and secret references.
- The gateway exports health probes, request metrics, and transaction metrics.

Sources: [webhook handler](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/gateway/src/webhook.rs#L85),
[producer](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/gateway/src/produce.rs#L35),
[dedup transaction](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/gateway/src/dedup/mod.rs#L200),
[subscription contract](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/gateway/proto/krabka/gateway/v1/gateway.proto#L105),
and [operator CRD](https://github.com/krabka-io/krabka-operator/blob/503d548d5c0841d17249b142ef8a317d5086e87f/crates/krabka-operator/src/crd/grpc_gateway.rs#L30).

## Gateway blockers

### 1. GitLab authentication

The verifier supports body-only HMAC-SHA256 with raw secret bytes.
It does not implement the Standard Webhooks verification used by current GitLab.com.
That protocol signs the message ID, timestamp, and raw body together.
It decodes the signing key and accepts a list of versioned signatures.
Add Standard Webhooks verification with a signing token.
Use `webhook-id` for delivery deduplication and retain it in the consumer record.
Legacy secret-token support is not needed for this application.
Verify before payload conversion. Keep the secret out of stored event headers.
Pass the new settings through the operator webhook configuration.

Sources: [gateway verifier](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/gateway/src/webhook_config.rs#L273),
[GitLab authentication](https://docs.gitlab.com/user/project/integrations/webhooks/#configure-webhook-authentication).

### 2. Deduplication and routing recovery

The dedup ownership consumer says it never commits and rebuilds from earliest.
Its builder does not disable automatic commits.
The pinned client enables automatic commits when a group is present.
It commits during polls, group joins, and close.
The membership reader uses the same pattern.

Inference from these code paths: a replacement process can resume after existing
claims or membership entries instead of rebuilding its empty local maps.
This can admit duplicates or omit routing entries.
`auto_offset_reset(Earliest)` applies when no valid committed offset exists;
it does not force an existing group to replay its whole log.

Disable automatic commits for readers that rebuild local state.
Establish the replay start and completion point for each acquired partition.
The current warm gate counts empty polls; it does not compare a replay position
with a captured log boundary. Test replacement owners against existing claims.

Sources: [ownership reader](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/gateway/src/dedup/store.rs#L111),
[membership reader](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/gateway/src/dedup/membership.rs#L154),
and [pinned client defaults](https://github.com/krabka-io/krabka-client-rs/blob/c4ce581587772b3750d88c61f3389d9626a20235/crates/client-consumer/src/consumer.rs#L1030).

### 3. Event metadata and merge request keys

For ordinary JSON, the webhook handler stores the body with no record headers.
It uses the delivery ID for deduplication but does not retain that ID in the
consumer record. Consumers then lack a stable identity for application effects.
Preserve an allowlist of message ID, event type, and instance metadata.
The raw GitLab body can remain the record value; CloudEvents are optional.

The JSONPath helper returns strings only. GitLab MR IDs and IIDs are numbers.
A key source such as `json:$.object_attributes.id` silently becomes no key.
The handler also permits a missing configured key.
Add scalar extraction and a stable key such as instance/project/MR IID.
Reject malformed required keys. The key keeps one MR in one partition;
it cannot restore GitLab events that arrive in a different chronological order.

Sources: [key extraction](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/gateway/src/webhook_config.rs#L314),
[record construction](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/gateway/src/webhook.rs#L169),
[ordinary JSON translation](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/gateway/src/webhook.rs#L314),
and [MR payload](https://docs.gitlab.com/user/project/integrations/webhook_events/#merge-request-events).

### 4. Processing acknowledgments for gateway consumers

`ConsumeSession` does not disable the native client's automatic commits.
This bypasses the explicit acknowledgment frontier during polls or close.
The Rust, Go, Java, and TypeScript messaging facades request auto-commit.
The server commits in that mode before the application confirms its work.

For reliable gateway consumption, disable underlying automatic commits and expose
explicit post-processing acknowledgment in the application SDKs.
Add reconnection with the same group and bounded retry delay.
Native consumers already expose manual commits; set `enable_auto_commit(false)`
and `ReadCommitted` explicitly, then commit after durable application work.

Sources: [session construction](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/gateway/src/consume.rs#L149),
[Rust facade](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/app-sdk/src/messaging.rs#L215),
[Go facade](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/sdks/go/messaging.go#L123),
[Java facade](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/sdks/java/src/main/kotlin/dev/krabka/sdk/internal/GatewayCore.kt#L28),
[TypeScript facade](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/sdks/ts/src/messaging.ts#L201),
and [server commit behavior](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/gateway/src/streaming.rs#L574).

### 5. Receiver deadlines and shutdown

The forwarding HTTP client has no configured request deadline.
The binary handles Ctrl-C but has no SIGTERM handler.
Listener shutdown stops acceptance without awaiting spawned connection tasks.
The readiness watcher marks a replica ready once it has ever warmed.

Add bounded ingress and forwarding deadlines, SIGTERM handling, and request drain.
Coordinate readiness with ownership recovery and process drain.
The handler already waits for publishing before returning success; preserve that
behavior. A durable local spool is optional if broker-outage acceptance is required.

GitLab.com has a 10-second webhook timeout. Set a receiver deadline below that
limit, including owner forwarding and the broker transaction.
Return success after durable acceptance. Return a failure when acceptance fails.
Do not rely on GitLab to retain every event during a broker outage.
Source: [GitLab.com webhook limits](https://docs.gitlab.com/user/gitlab_com/#webhooks).

Sources: [forwarding client](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/gateway/src/forward.rs#L179),
[signal handling](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/gateway/src/bin/gateway.rs#L711),
[listener](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/gateway/src/serve.rs#L40),
and [readiness watcher](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/gateway/src/bin/gateway.rs#L854).

## Application work

| Requirement | Minimum application behavior |
| --- | --- |
| Duplicate-safe effects | Persist delivery ID and business changes in one database transaction, or use the target system's idempotency support. Commit the consumer offset afterward. |
| Failed work | Retry with bounded delay; retain failed payloads in a DLQ; support explicit replay. A poison record should not block its partition forever. |
| State repair | Monitor hook delivery status and periodically reconcile current MR state through the GitLab API. Persist a scan checkpoint and use overlapping windows. |
| Event interpretation | Interpret `object_attributes.action` and `changes`; tolerate version-dependent fields and stale deliveries. Subscribe to separate note or pipeline hooks if those changes matter. |
| Operational visibility | Alert on ingestion failures, consumer lag, processing age, retries, DLQ growth, and disabled hooks. Keep the delivery ID in logs and records. |

These requirements belong in the service or its deployment.
Gateway deduplication does not make a database write or GitLab API action exactly once.
API reconciliation repairs current state; it does not reconstruct all missed transitions.
GitLab documents stable retry IDs and disabled hooks, but the reviewed sources do not
promise ordered delivery or unlimited replay.
Sources: [webhook receiver requirements](https://docs.gitlab.com/user/project/integrations/webhooks/#webhook-receiver-requirements),
[webhook management API](https://docs.gitlab.com/api/project_webhooks/),
and [MR API](https://docs.gitlab.com/api/merge_requests/).

## Deployment and qualification

Use multiple gateway and service replicas in separate failure domains.
Share the gateway dedup topic, ownership group, and transaction ID prefix.
Give each gateway a distinct advertised address.
Replicate user and internal topics; configure the minimum in-sync replicas.
Disable the gateway's internal-topic replication fallback for HA deployments.
Match topic retention and dedup retention to the accepted replay horizon.

The operator defaults to one gateway replica and requires client TLS authentication.
Its serving certificate uses the cluster CA.
For GitLab.com, provide publicly trusted HTTPS through ingress and configure an
appropriate backend authentication mode. GitLab.com does not expose the
Self-Managed administrator setting for a webhook client certificate.
Set body limits for the accepted payload size instead of treating the default 1 MiB
as a GitLab limit. Check the broker record and frame limits too.

Sources: [gateway runtime defaults](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/gateway/src/config.rs#L195),
[operator TLS defaults](https://github.com/krabka-io/krabka-operator/blob/503d548d5c0841d17249b142ef8a317d5086e87f/crates/krabka-operator/src/crd/grpc_gateway.rs#L250),
and [GitLab client TLS availability](https://docs.gitlab.com/user/project/integrations/webhooks/#configure-webhooks-to-support-mutual-tls).

Before qualification, exercise response loss after commit, owner failure, replica
replacement with existing claims, broker failover, worker failure before and after
its database transaction, and rolling updates during ingestion.
Existing ownership and forwarding tests use an in-process single broker.
They establish useful component behavior but do not qualify this complete flow.
Sources: [ownership tests](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/gateway/tests/ownership.rs),
and [forwarding tests](https://github.com/krabka-io/krabka-gateway/blob/677419af8c8f4520a80389f45601cc8f106ee1e3/crates/gateway/tests/forwarding.rs).

Build authentication, recovery, and metadata/key support first.
Use native manual-commit consumers for the initial services.
Add SDK processing acknowledgments if those services consume through the gateway.
The separate [GitLab requirements note](gitlab-webhook-requirements.md) records upstream details.
