# GitLab merge request webhook requirements

Research date: 2026-10-02. Sources are official GitLab documentation.
Target: the latest GitLab.com SaaS, with a new signed webhook.
Older-version authentication is outside the requested scope.

## Request format and event scope

GitLab sends JSON with `X-Gitlab-Event: Merge Request Hook`.
The body contains `object_kind`, `event_type`, `object_attributes`, and `changes`.
The first two fields contain `merge_request`.
The action identifies open, update, close, reopen, merge, and approval changes.
An event can have an empty `changes` object.
Comments use separate note events. Pipelines use separate pipeline events.
`actioned_at` appeared in GitLab 18.10. Timestamp formats changed in 19.0.
Auto-merge events appeared in 19.2.
[Source: webhook events](https://docs.gitlab.com/user/project/integrations/webhook_events/#merge-request-events).

Inference: the default payload is not a CloudEvent.
A receiver should accept GitLab JSON and build its own event envelope.
Use the GitLab instance, target project ID, and merge request IID as the key for each merge request.
Keep the original payload so future consumers can use fields outside the current schema.

## Authentication, identity, and acknowledgment

GitLab 19.0 introduced signing tokens; 19.1 removed the feature flag.
HMAC-SHA256 signs `{webhook-id}.{webhook-timestamp}.{raw-body}`.
Strip `whsec_` from the token, then base64-decode the key.
Compare against each space-separated `v1,{base64}` signature with constant-time comparison.
Check timestamp freshness against replay attacks.
Use a signing token for this application. Legacy `X-Gitlab-Token` support is not needed.

Use `webhook-id` as the stable message ID. GitLab also sends it as `Idempotency-Key`.
`X-Gitlab-Event-UUID` can identify a recursive chain shared by multiple events.
Do not use that header alone to remove duplicates.

GitLab recommends quick `200` or `201` responses and asynchronous work.
Timeouts can produce duplicates.
Group and project hooks can both deliver the same event.
[Source: webhooks](https://docs.gitlab.com/user/project/integrations/webhooks/).

Gateway comparison supplied by the repository review: the existing generic verifier signs the body with the raw secret.
It accepts one signature, with an optional prefix removed.
These rules do not implement GitLab's Standard Webhooks signature format.
Inference: add Standard Webhooks verification before JSON conversion. Preserve the stable ID in the stored record.
Send success after durable acceptance. Consumer effects should remain safe when delivery repeats.

## Failure behavior and service limits

GitLab temporarily disables a hook after four consecutive failures.
Backoff starts at one minute and reaches a one-day limit.
Forty failures permanently disable the hook.
[Source: webhook developer guide](https://docs.gitlab.com/development/webhooks/#webhook-execution-safety).

GitLab.com sets a 10-second timeout and a 25 MB maximum payload.
All hooks in a top-level namespace share a plan-dependent rate limit.
GitLab temporarily disables them when they reach this limit, then enables them in the next minute.
[Source: GitLab.com settings](https://docs.gitlab.com/user/gitlab_com/#webhooks).

The reviewed documentation does not promise ordered delivery or an unlimited retry schedule.
Temporary re-enablement does not establish that GitLab replays every missed event.
Inference: use bounded receiver latency, alerts for disabled hooks, and a separate repair process.
Partition order cannot repair events that arrive from GitLab in a different order.

## Recovery and reconciliation

The project webhook API exposes `alert_status` and `disabled_until`.
It lists recent events with their delivery results and supports explicit resend.
The documented event window is seven days; resend has a rate limit.
[Source: project webhooks API](https://docs.gitlab.com/api/project_webhooks/).

The merge requests API supplies current state for a project and merge request IID.
List endpoints support `updated_after`, `updated_before`, `order_by`, and `sort`.
[Source: merge requests API](https://docs.gitlab.com/api/merge_requests/).

Inference: periodic API scans can repair current application state after outages.
Use overlapping time windows and handle pagination.
They cannot reconstruct every missed transition.
Applications that need an exact event history should retain received payloads and treat upstream gaps as observable failures.
