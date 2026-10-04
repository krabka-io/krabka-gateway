# GitHub webhook firehose

GitHub → signed gateway endpoint → `github-webhooks` →
[`krabka-streams-rs`](https://github.com/krabka-io/krabka-streams-rs) topology →
`github-firehose`.

The runnable Rust example is
[`github_firehose.rs`](../../crates/gateway/examples/github_firehose.rs).
It parses the body with [Octocrab](https://docs.rs/octocrab/0.54.2/octocrab/models/webhook_events/)
and matches `WebhookEventType`. It covers all 76 event names in the
[GitHub webhook catalog](https://docs.github.com/en/webhooks/webhook-events-and-payloads),
including repository, organization, GitHub App, security, Projects and Actions
events. Each enum arm provides a place to add event-specific processing. All actions
pass through, including actions GitHub adds later; actionless events such as
`push`, `status` and `ping` work too.

Each output contains the original event name, a readable `kind`, the optional
`action`, and the complete JSON `payload`. `typed_payload` contains Octocrab's
serialized `WebhookEvent`, including its event-specific model (for example the
`Push` object), and `parse_error` is null when decoding succeeds. The original
`payload` retains fields that Octocrab's models omit. Parsing needs no GitHub API
token or network calls.

Octocrab 0.54.2 has no typed model for ten catalog events:
`branch_protection_configuration`, `custom_property`, `custom_property_values`,
`deployment_review`, `issue_dependencies`, `issue_relates_to`,
`projects_v2_status_update`, `repository_ruleset`, `secret_scanning_scan`, and
`sub_issues`. These use its `Unknown` variant, retain their catalog labels, and
have `typed_payload: null`. Future event names also pass through, with
`kind: null`.

When a typed model rejects a body or a new action, the record still retains the
original JSON and reports the reason in `parse_error`, with `typed_payload: null`.
Invalid JSON is forwarded with `raw_payload` and an `error`. The raw input topic
always retains the original body bytes.

## Run locally

Use a running Krabka broker at `127.0.0.1:9092` with the Streams group protocol
(KIP-1071) enabled. The topology uses raw UTF-8 keys and JSON values through
`StringSerde`; it needs no Schema Registry.

Create the two demo topics once (one partition, one replica):

```bash
cargo run -p krabka-gateway --example github_firehose -- --create-topics
```

Start the gateway from the repository root:

```bash
KRABKA_GATEWAY_ADVERTISED_ADDR=127.0.0.1:9500 \
cargo run -p krabka-gateway --bin krabka-gateway -- \
  --bootstrap-servers 127.0.0.1:9092 \
  --listen-addr 127.0.0.1:9500 \
  --webhooks-config demo/github-firehose/webhooks.toml
```

Start the Streams example in another terminal before sending deliveries:

```bash
cargo run -p krabka-gateway --example github_firehose
```

It prints each processed JSON envelope and writes it to `github-firehose`.
Ctrl-C closes the Streams runtime and commits its progress. Set
`KRABKA_BOOTSTRAP_SERVERS` or pass `--bootstrap-servers` to use another broker.

Send signed examples with Python's standard library:

```bash
python3 - <<'PY'
import hashlib
import hmac
import json
import urllib.request
import uuid

secret = b"github-firehose-demo-secret"
samples = {
    "ping": {"zen": "Keep it logically awesome.", "hook_id": 1},
    "push": {
        "ref": "refs/heads/main", "before": "abc", "after": "def", "commits": [],
        "compare": "https://github.com/octocat/demo/compare/abc...def",
        "created": False, "deleted": False, "forced": False,
        "pusher": {"name": "octocat"},
    },
    "create": {
        "ref": "feature", "ref_type": "branch", "master_branch": "main", "pusher_type": "user",
    },
    "workflow_dispatch": {"ref": "refs/heads/main", "workflow": "CI", "inputs": {}},
    "installation": {"action": "created", "installation": {"id": 1}},
    "future_event": {"action": "future_action", "new_field": True},
}
for event, payload in samples.items():
    body = json.dumps(payload).encode()
    signature = "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()
    request = urllib.request.Request(
        "http://127.0.0.1:9500/v1/webhooks/github",
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": event,
            "X-GitHub-Delivery": str(uuid.uuid4()),
            "X-Hub-Signature-256": signature,
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        print(event, response.status)
PY
```

These small synthetic payloads include the fields required by Octocrab's models.
For example, the `ping` delivery produces:

```json
{
  "event": "ping",
  "kind": "Webhook ping",
  "action": null,
  "payload": {"zen": "Keep it logically awesome.", "hook_id": 1},
  "typed_payload": {
    "sender": null,
    "repository": null,
    "organization": null,
    "installation": null,
    "Ping": {"zen": "Keep it logically awesome.", "hook_id": 1, "hook": null}
  },
  "parse_error": null
}
```

## Connect GitHub

Replace the demo secret in `webhooks.toml`, expose the gateway through your HTTPS
ingress, and configure a GitHub webhook:

- Payload URL: `https://YOUR_GATEWAY/v1/webhooks/github`.
- Content type: `application/json`.
- Secret: the same value as `secret` in the endpoint configuration.
- Events: **Send me everything** for repository or organization webhooks, or
  select all required events and permissions in your GitHub App settings.

GitHub restricts which events each webhook type can receive. To receive App-only
or organization-only events, configure those webhook types as well; a repository
webhook alone cannot produce the whole catalog. They can share this endpoint.

The gateway verifies `X-Hub-Signature-256` against the original request bytes and
deduplicates `X-GitHub-Delivery` within its configured dedup window. It uses
`X-GitHub-Event` as the Kafka key because event names are HTTP headers, not a
universal field in GitHub's JSON bodies. Ordinary HTTP headers are not copied to
the Kafka record; the delivery ID is used for dedup, not included in the output.
The endpoint accepts bodies up to `25MiB`; broker and gateway produce limits must
also allow the records you intend to ingest.

Records of the same event type share a key. The Streams runtime defaults to
at-least-once processing, so downstream readers must tolerate repeated output
after a restart. The demo prints full payloads; choose your log and topic access
policies accordingly before sending private repository events.

## Verify

The broker-free checks run records through the actual Streams topology for every
catalog event, typed Ping/Create/Push/WorkflowDispatch/Installation bodies,
unknown fields, invalid typed fields, absent/new actions, unknown events, missing
keys and malformed JSON:

```bash
cargo test -p krabka-gateway --example github_firehose
bazel test //crates/gateway:github_firehose_test
```

The Bazel runnable target is `//crates/gateway:github_firehose`.
