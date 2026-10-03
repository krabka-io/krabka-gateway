# GitLab.com webhook ingestion

This setup sends signed GitLab.com merge request deliveries through gateway
replicas into `gitlab.merge-requests`. The application team owns its production
consumer. [streams-go](streams-go) provides a runnable Go example using
[krabka-streams-go](https://github.com/krabka-io/krabka-streams-go).
For every enabled GitLab event, use the
[franz-go JSON printer](#all-gitlab-events-with-franz-go).

## Configure the hook

1. In the GitLab project or group, open **Settings > Webhooks** and add
   `https://hooks.example.com/v1/webhooks/gitlab-merge-requests`.
2. Select **Generate signing token** and store the complete `whsec_...` token
   as a text value in your external secret backend. Keep it out of source
   control and logs; do not base64-encode or decode it yourself.
3. Enable only **Merge request events**, leave the default JSON payload, and
   keep SSL verification enabled. Do not configure a custom payload template.

Use a public HTTPS edge with a trusted certificate. Preserve the original body
and the `webhook-id`, `webhook-timestamp`, and `webhook-signature` headers through
the proxy. JSON reserialization invalidates the signature. Keep gateway clocks
synchronized: this example accepts signed timestamps within five minutes.
[GitLab signing tokens](https://docs.gitlab.com/user/project/integrations/webhooks/#signing-tokens).

## Load the signing token with External Secrets Operator

Install ESO with the `external-secrets.io/v1` API and configure a
`ClusterSecretStore` for your backend using its supported identity mechanism.
The example assumes an existing store named `gitlab-webhooks` and a remote
secret named `gitlab-webhook-signing-token`. Change `secretStoreRef` and
`remoteRef.key` in [external-secret.yaml](external-secret.yaml) to match your
store. For a namespaced `SecretStore`, change the reference's `kind` as well.
If the provider stores a JSON object, set `remoteRef.property` to the token's
field instead of fetching the whole object.

From the repository root, publish the non-secret TOML template as a ConfigMap
and apply the ExternalSecret in the namespace where the gateway runs:

```sh
kubectl -n gitlab create configmap gitlab-webhooks-template \
  --from-file=webhooks.toml=demo/gitlab/webhooks.toml.example \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n gitlab apply -f demo/gitlab/external-secret.yaml
kubectl -n gitlab wait --for=condition=Ready externalsecret/gitlab-webhooks \
  --timeout=120s
```

Create the namespace first if needed. ESO fetches the signing token and renders
Secret `gitlab-webhooks`, containing `webhooks.toml` and `signing-token`.
The template uses JSON string quoting, which is valid for this TOML token value.
The ConfigMap and ExternalSecret contain no credentials.
[ESO configuration templates](https://external-secrets.io/latest/guides/templating/).

Add this fragment to the gateway Deployment's `spec.template.spec`, merging it
with its existing containers and volumes. It mounts the generated configuration
at `/run/secrets/gitlab-webhooks/webhooks.toml` for the command below:

```yaml
securityContext:
  fsGroup: 65532
containers:
  - name: gateway
    volumeMounts:
      - name: gitlab-webhooks
        mountPath: /run/secrets/gitlab-webhooks
        readOnly: true
volumes:
  - name: gitlab-webhooks
    secret:
      secretName: gitlab-webhooks
      defaultMode: 0440
      items:
        - key: webhooks.toml
          path: webhooks.toml
```

Use the gateway container's existing name and security group if they differ.
The volume is required: pods wait for the Secret before starting. When using
`KafkaGrpcGateway`, reference the same ESO-generated token in the endpoint's
`secretRef: {name: gitlab-webhooks, key: signing-token}` instead; the Krabka
operator renders its own gateway configuration.

ESO refreshes the Secret hourly. The gateway reads webhook configuration at
startup, so wait for a successful ESO refresh and roll the gateway pods when
rotating the GitLab signing token. Coordinate the rollout with GitLab's token
change; refreshing the mounted file alone does not reload the verifier.
[ESO refresh behavior](https://external-secrets.io/latest/api/externalsecret/).

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
  --webhooks-config /run/secrets/gitlab-webhooks/webhooks.toml \
  --dedup-topic __gitlab_gateway_dedup \
  --dedup-partitions 8 \
  --dedup-window 24h \
  --dedup-ownership-group gitlab-gateway-owners \
  --dedup-txn-id-prefix gitlab-gateway-dedup \
  --membership-topic __gitlab_gateway_membership \
  --internal-topic-replication-factor 3 \
  --internal-topic-allow-replication-fallback false \
  --forward-max-body 128MiB \
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
accepts `25MB` (25,000,000 bytes). Internal JSON byte arrays can expand that to
100 MB, so `--forward-max-body 128MiB` leaves room for the small MR key and
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

## Go consumer example

The example consumes directly from the broker with an ordinary consumer group,
uses a `krabka-streams-go` columnar topology to decode merge request JSON, and
writes a JSON summary with its delivery ID to stdout. It uses read-committed
isolation and commits offsets only after the complete polled batch is processed
and written successfully. Decode, output, and commit failures stop the process;
SIGINT/SIGTERM cancel polling and close the consumer.

With Go 1.26.5 or newer, run from this directory:

```sh
cd streams-go
go run . -brokers 127.0.0.1:9092 \
  -topic gitlab.merge-requests -group gitlab-mr-example
```

Instances with the same group share partitions; use a different group for a
separate application that needs every event. The dependency is pinned to a
specific upstream revision in `go.mod` and `go.sum`.

Stdout is an example output, and delivery is at least once: a crash or uncertain
offset commit can repeat already printed summaries. Replace output with a
durable, duplicate-safe application effect before using this as a service.
The example intentionally stops on an invalid record rather than silently
skipping it; the application team owns retries, dead-letter handling, broker
TLS/authentication, and reconciliation.

## All GitLab events with franz-go

[consumer-franz-go](consumer-franz-go) consumes `gitlab.events` directly with
franz-go and prints every record's complete JSON payload, indented for reading.
It applies no event-type filter or MR schema and preserves numeric IDs without
converting them to floating point. The MR template and streams example above
remain available; this section selects an alternate receiver configuration.

Add a GitLab hook at
`https://hooks.example.com/v1/webhooks/gitlab-events`, enable every event type
offered for that project or group hook, and retain the default JSON payload and
SSL verification. The consumer does not enable events in GitLab. Store this
hook's generated signing token in the same external-secret backend entry used
above. Provision `gitlab.events` with the same replication, minimum ISR,
retention, and size policies; grant `webhook:gitlab-events` write access when
ACLs are enabled, and expose only the new named webhook path through the public
HTTPS edge.
[Available project and group hook events](https://docs.gitlab.com/user/project/integrations/webhook_events/).

Use [webhooks-all-events.toml.example](webhooks-all-events.toml.example), which
omits `key_source` so push, tag, issue, note, and other payloads need no MR fields.
Signed delivery IDs still provide transactional deduplication, and the same
signed and allowlisted metadata are preserved. From the repository root,
replace the existing ConfigMap's template and request an ESO refresh:

```sh
kubectl -n gitlab create configmap gitlab-webhooks-template \
  --from-file=webhooks.toml=demo/gitlab/webhooks-all-events.toml.example \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n gitlab apply -f demo/gitlab/external-secret.yaml
kubectl -n gitlab annotate externalsecret gitlab-webhooks \
  force-sync="$(date +%s)" --overwrite
kubectl -n gitlab wait --for=condition=Ready externalsecret/gitlab-webhooks \
  --timeout=120s
kubectl -n gitlab get externalsecret gitlab-webhooks \
  -o jsonpath='{.status.refreshTime}{"\n"}'
```

Before rolling pods, confirm `refreshTime` is newer than the template update;
`Ready` alone can describe a previous successful synchronization. Repeat this
check until it succeeds: it verifies the generated endpoint without printing
the Secret's contents.

```sh
kubectl -n gitlab get secret gitlab-webhooks \
  -o jsonpath='{.data.webhooks\.toml}' \
  | base64 --decode | rg --quiet '^name = "gitlab-events"$'
```

Then roll the gateway Deployment, replacing `gitlab-gateway` with its actual
name:

```sh
kubectl -n gitlab rollout restart deployment/gitlab-gateway
kubectl -n gitlab rollout status deployment/gitlab-gateway --timeout=120s
```

The gateway startup command and Secret volume stay the same. Choosing this
alternate template replaces the mounted MR endpoint with `gitlab-events`.
[ESO manual refresh and sync status](https://external-secrets.io/latest/introduction/faq/#can-i-manually-trigger-a-secret-refresh).

With Go 1.26.5 or newer, run from this directory:

```sh
cd consumer-franz-go
go run . -brokers 127.0.0.1:9092 \
  -topic gitlab.events -group gitlab-json-example
```

The example uses read-committed isolation and manual offset commits after
successful output. Instances in the same group share partitions; a different
group receives every event independently. Stdout is at least once: a restart
or uncertain commit can repeat printed records. Invalid JSON, output errors,
and commit failures halt the example instead of skipping data. The application
team owns production effects, retries, broker TLS/authentication, and recovery.

To check this example locally, run from the repository root:

```sh
cd demo/gitlab/consumer-franz-go
go test ./...
go vet ./...
```

## Local verification

From the repository root:

```sh
cargo test -p krabka-gateway --no-default-features --features vendored-protoc \
  --test webhook standard_webhooks_producer_path
cd demo/gitlab/streams-go
go test ./...
go vet ./...
```

This integration test runs against an in-process broker. It checks signed
acceptance, invalid-signature rejection, numeric key and raw payload preservation,
metadata, and repeated-delivery deduplication.
It also replaces the owner and verifies that a retry finds the replayed claim.
It does not qualify a live GitLab.com webhook, a deployed ingress, or a
three-broker HA installation.
Before deployment, exercise response loss after commit, replica replacement,
owner failure, broker failover, and rolling shutdown under ingestion load.
