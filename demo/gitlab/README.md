# GitLab.com merge request producer

This setup sends signed GitLab.com merge request deliveries through gateway
replicas into `gitlab.merge-requests`. The consumer belongs to the application
team; this example adds no consumer service.

## Configure the hook

1. Copy [webhooks.toml.example](webhooks.toml.example) into a secret-mounted file,
   for example `/run/secrets/gitlab-webhooks.toml`.
2. In the GitLab project or group, open **Settings > Webhooks** and add
   `https://hooks.example.com/v1/webhooks/gitlab-merge-requests`.
3. Select **Generate signing token** and save the complete `whsec_...` token
   into the file's `secret` field. The placeholder is intentionally invalid.
   Keep the populated file out of source control and logs.
4. Enable only **Merge request events**, leave the default JSON payload, and
   keep SSL verification enabled. Do not configure a custom payload template.

Use a public HTTPS edge with a trusted certificate. Preserve the original body
and the `webhook-id`, `webhook-timestamp`, and `webhook-signature` headers through
the proxy. JSON reserialization invalidates the signature. Keep gateway clocks
synchronized: this example accepts signed timestamps within five minutes.
[GitLab signing tokens](https://docs.gitlab.com/user/project/integrations/webhooks/#signing-tokens).

## Run the gateway replicas

Provision `gitlab.merge-requests`, `__gitlab_gateway_dedup`, and
`__gitlab_gateway_membership` on a cluster with at least three brokers. Use
replication factor **3** and `min.insync.replicas=2` for all three topics;
configure the broker transaction-state topic with the same durability policy.
The dedup topic needs eight partitions and `cleanup.policy=compact,delete` with
`retention.ms=86400000`; the membership topic needs one partition and
`cleanup.policy=compact`. Keep user-topic retention long enough for the consumer
team's recovery and replay requirements.

The gateway can create internal topics but does not repair existing topic
replication or minimum ISR. Disable its replication fallback and verify these
settings before routing traffic.

With the built `krabka-gateway` binary on `PATH`, run one copy of this command
per replica, changing `--client-id` and `--advertised-addr` each time:

```sh
krabka-gateway \
  --bootstrap-servers broker-0:9092,broker-1:9092,broker-2:9092 \
  --listen-addr 0.0.0.0:9500 \
  --client-id gitlab-gateway-0 \
  --advertised-addr gitlab-gateway-0.internal:9500 \
  --webhooks-config /run/secrets/gitlab-webhooks.toml \
  --dedup-topic __gitlab_gateway_dedup \
  --dedup-partitions 8 \
  --dedup-window 24h \
  --dedup-ownership-group gitlab-gateway-owners \
  --dedup-txn-id-prefix gitlab-gateway-dedup \
  --membership-topic __gitlab_gateway_membership \
  --internal-topic-replication-factor 3 \
  --internal-topic-allow-replication-fallback false \
  --forward-max-body 32MiB \
  --client-frame-max 32MiB \
  --tls-cert /run/secrets/gateway-cert.pem \
  --tls-key /run/secrets/gateway-key.pem \
  --tls-client-ca /run/secrets/gateway-ca.pem \
  --tls-trust-roots /run/secrets/gateway-ca.pem \
  --tls-client-auth optional
```

Share the webhook secret, topics, ownership group, transaction prefix, partition
count, and dedup window across replicas. Use unique client IDs and private peer
addresses reachable by every replica. Certificates must cover those addresses
and permit both server and client authentication. Spread replicas across failure
domains, route only ready replicas using `/readyz`, and configure the process
supervisor to restart replicas after any exit.

This command uses private TLS listeners with optional client certificates so
the public edge can forward a signed hook without a GitLab client certificate.
Gateway peers present certificates for mTLS forwarding; the internal forward
endpoint requires an authenticated peer. Keep `/internal/v1/forward` private,
allow only the named webhook path through the public edge, and let the edge
verify the backend certificate. The optional mode does not require certificates
on other gateway APIs; restrict those APIs to authorized private clients.
Enable `--authz simple` when broker ACLs are provisioned, granting
`webhook:gitlab-merge-requests` write access to `gitlab.merge-requests`. Configure
broker-facing TLS/authentication separately using the gateway's broker security
flags for the deployment.

GitLab.com permits 25 MB payloads and times out after 10 seconds. The endpoint
accepts `25MB` (25,000,000 bytes). Internal base64 value encoding expands that to
about 33.3 MB, so `--forward-max-body 32MiB` leaves room for the small MR key and
allowlisted headers. Align proxy limits, client frames, broker request and record
limits, topic limits, and consumer fetch limits to accept the raw payload plus
protocol overhead. Larger extra headers require additional forwarding headroom.
[GitLab.com limits](https://docs.gitlab.com/user/gitlab_com/#other-limits).

## Delivery contract for the consumer team

| Field | Contract |
| --- | --- |
| Topic | `gitlab.merge-requests` |
| Value | Exact raw JSON bytes from the default GitLab payload; no schema framing or CloudEvents envelope. |
| Key | UTF-8 decimal `object_attributes.id`, the global numeric MR ID on GitLab.com. A missing or invalid configured key fails with HTTP 400. |
| Signed metadata | `webhook-id` and `webhook-timestamp`, preserved automatically as record headers. |
| Additional metadata | The example's `forward_headers` allowlist, preserved when supplied. These headers are outside the signature and are informational. |
| Delivery identity | `webhook-id`; do not use `x-gitlab-event-uuid` as a deduplication key. |
| Acceptance | HTTP 200 with partition, offset, and deduplicated status after transactional acceptance; duplicate deliveries return the existing outcome within the 24h dedup window. |

Authentication verifies HMAC-SHA256 over the signed ID, timestamp, and original
body before publishing. Neither the signature nor authentication secrets are
stored in record headers. Only the configured endpoint identity grants topic
write permission; extra HTTP metadata must not become authorization input.
[GitLab delivery headers](https://docs.gitlab.com/user/project/integrations/webhooks/#delivery-headers).

The receiver bounds ingress and publishing waits below GitLab's timeout; failed
or uncertain acceptance returns a failure. A transaction may complete after its
HTTP response times out, so the sender must reuse the delivery ID on retry. An
uncertain transaction outcome stops acceptance and exits the affected replica
for supervisor restart and claim replay before it can accept more work. This
protects deduplication without promising acceptance during a broker outage.

Native consumers should use `ReadCommitted`, disable automatic offset commits,
and commit after durable application work. Deduplication is bounded by retention;
the consumer must make its own effects duplicate-safe using the delivery ID.
One MR's key preserves partition order of accepted records, not chronological
order of GitLab events. Interpret the payload's `object_attributes.action` and
`changes`; note and pipeline events use separate hooks.
[MR event payload](https://docs.gitlab.com/user/project/integrations/webhook_events/#merge-request-events).

The application team owns retry/DLQ policy, offset management, and GitLab API
reconciliation after missed deliveries. Monitor delivery failures and disabled
hooks; GitLab can disable a repeatedly failing hook. Current-state reconciliation
cannot reconstruct every missed transition.
[Failure behavior](https://docs.gitlab.com/user/project/integrations/webhooks/#auto-disabled-webhooks).

## Local verification

From the repository root:

```sh
cargo test -p krabka-gateway --no-default-features --features vendored-protoc \
  --test webhook standard_webhooks_producer_path
```

This integration test runs against an in-process broker. It checks signed
acceptance, invalid-signature rejection, numeric key and raw payload preservation,
metadata, and repeated-delivery deduplication.
It also replaces the owner and verifies that a retry finds the replayed claim.
It does not qualify a live GitLab.com webhook, a deployed ingress, or a
three-broker HA installation.
Before deployment, exercise response loss after commit, replica replacement,
owner failure, broker failover, and rolling shutdown under ingestion load.
