//! Connect-RPC handlers.
//!
//! Each handler is a thin adapter. It takes proto in, sends a `GatewayRecord`
//! to the core, and turns the `RecordOutcome` back into proto.

use std::{net::SocketAddr, sync::Arc};

use axum::Extension;
use connectrpc_axum::message::{ConnectError, ConnectRequest, ConnectResponse};
use futures_util::{FutureExt as _, future::BoxFuture};
use krabka_authz::{AuthorizationRequest, AuthorizationResult};
use krabka_metadata::{AclOperation, ResourceType};
use krabka_security::{AuthMethod, Principal};
use krabka_units::prelude::*;

use crate::{metrics::metrics, pb, state::AppState};

pub(crate) fn producer_acks_from_pb(acks: i32) -> Option<krabka_client_producer::Acks> {
    match pb::Acks::try_from(acks) {
        Ok(pb::Acks::All) => Some(krabka_client_producer::Acks::All),
        Ok(pb::Acks::Leader) => Some(krabka_client_producer::Acks::One),
        Ok(pb::Acks::None) => Some(krabka_client_producer::Acks::Zero),
        Err(_) => None,
    }
}

/// The principal used when no authenticated identity is present on the request
/// (plaintext listener, or no proxy-injected identity). Mirrors Kafka's
/// `ANONYMOUS` principal so ACLs can target it explicitly.
pub(crate) fn anonymous_principal() -> Principal {
    Principal {
        name: "ANONYMOUS".into(),
        auth_method: AuthMethod::Anonymous,
        groups: vec![],
    }
}

/// The host the gateway uses when the request carries no caller peer address.
/// It matches the broker's "unknown host" convention for ACL host matching, and
/// the `*` host pattern still matches it.
pub(crate) fn unknown_host() -> SocketAddr {
    "0.0.0.0:0".parse().expect("constant socket addr parses")
}

/// Authorize a single `(resource_type, resource_name, operation)` for the
/// effective principal and host against the gateway's ACL cache.
///
/// This function writes an audit log line for the decision. It returns the
/// binary result, and the caller decides how to surface a `Deny`.
///
/// With the default `AllowAllAuthorizer` it always returns `Allow`, which keeps
/// the gateway's pre-authz behavior.
pub(crate) fn authorize_resource(
    state: &AppState,
    principal: &Principal,
    host: &SocketAddr,
    resource_type: ResourceType,
    resource_name: &str,
    operation: AclOperation,
) -> AuthorizationResult {
    let req = AuthorizationRequest {
        principal,
        host,
        resource_type,
        resource_name,
        operation,
    };
    let cache = state.authz.cache();
    let result = state.authz.authorizer().authorize(&**cache, &req);
    tracing::info!(
        target: "gateway::audit",
        principal = %principal.name,
        op = ?operation,
        resource_type = ?resource_type,
        resource = %resource_name,
        allowed = matches!(result, AuthorizationResult::Allow),
        "gateway authz",
    );
    result
}

/// Map a produce error to a per-record `RecordResult`.
///
/// `Unavailable` is retriable, and the caller should re-route to another
/// replica. `Unauthorized` is a non-retriable `PERMISSION_DENIED`. Every other
/// error is reported as non-retriable with a generic code.
///
/// Codec errors split by cause. A `Registry` transport or availability failure
/// is retriable, because the registry can recover. A `Serialize`, `Validate`,
/// or `Framing` fault is non-retriable, because the same bytes fail the same
/// way.
pub(crate) fn error_result(e: &crate::error::GatewayError) -> crate::pb::RecordResult {
    use crate::{codec::CodecError, error::GatewayError};
    let retriable = matches!(e, GatewayError::Unavailable)
        || matches!(e, GatewayError::Codec(CodecError::Registry(_)));
    let code = match e {
        // gRPC UNAVAILABLE: re-route (Unavailable) or registry transport retry.
        GatewayError::Unavailable | GatewayError::Codec(CodecError::Registry(_)) => 14,
        GatewayError::Unauthorized(_) => 7, // gRPC PERMISSION_DENIED
        GatewayError::Codec(_) => 3,        // gRPC INVALID_ARGUMENT (payload fault)
        _ => 1,
    };
    crate::pb::RecordResult {
        partition: -1,
        offset: -1,
        deduplicated: false,
        error: Some(crate::pb::ErrorInfo {
            code,
            message: e.to_string(),
            retriable,
        }),
    }
}

/// Map a wire [`pb::SchemaFormat`], which is an i32, to the codec
/// [`SchemaFormat`]. `SCHEMA_FORMAT_UNSPECIFIED` and any unknown value default
/// to Avro, the Confluent default `schemaType`.
fn schema_format_from_pb(format: i32) -> crate::codec::SchemaFormat {
    use crate::codec::SchemaFormat;
    match crate::pb::SchemaFormat::try_from(format) {
        Ok(crate::pb::SchemaFormat::Json) => SchemaFormat::Json,
        Ok(crate::pb::SchemaFormat::Protobuf) => SchemaFormat::Protobuf,
        // AVRO, UNSPECIFIED, or an unknown enum value ⇒ Avro (Confluent default).
        Ok(crate::pb::SchemaFormat::Avro | crate::pb::SchemaFormat::Unspecified) | Err(_) => {
            SchemaFormat::Avro
        }
    }
}

/// Map a wire [`pb::SchemaSelector`] to the codec [`SchemaSelector`]. An empty
/// `subject` ⇒ `None`, which selects `TopicNameStrategy`. A zero `id` ⇒ `None`,
/// which resolves the latest schema.
fn schema_selector_from_pb(sel: crate::pb::SchemaSelector) -> crate::codec::SchemaSelector {
    crate::codec::SchemaSelector {
        subject: (!sel.subject.is_empty()).then_some(sel.subject),
        id: (sel.id != 0).then_some(sel.id),
        format: schema_format_from_pb(sel.format),
    }
}

/// Convert a wire [`pb::Record`] into the transport-agnostic [`GatewayRecord`].
///
/// The `body` oneof splits raw from structured. `raw`, or an absent oneof,
/// keeps `value` and carries no structured body. `structured` carries the JSON
/// and the record's `schema` selector, and it leaves `value` empty. The codec
/// serializes that JSON on the produce path.
pub(crate) fn to_gateway_record(r: crate::pb::Record) -> crate::types::GatewayRecord {
    use crate::pb::record::Body;
    let selector = r.schema.map(schema_selector_from_pb);
    let (value, body_structured) = match r.body {
        Some(Body::Structured(sv)) => {
            let json = bytes::Bytes::from(sv.json);
            // A structured body without a schema selector defaults to Avro via
            // TopicNameStrategy (subject/id resolved by the codec).
            let schema = selector.unwrap_or(crate::codec::SchemaSelector {
                subject: None,
                id: None,
                format: crate::codec::SchemaFormat::Avro,
            });
            (bytes::Bytes::new(), Some((json, schema)))
        }
        Some(Body::Raw(raw)) => (bytes::Bytes::from(raw), None),
        None => (bytes::Bytes::new(), None),
    };
    crate::types::GatewayRecord {
        topic: r.topic,
        key: r.key.map(bytes::Bytes::from),
        value,
        body_structured,
        headers: r
            .headers
            .into_iter()
            .map(|header| (header.key, header.value.map(bytes::Bytes::from)))
            .collect(),
        partition: r.partition,
        timestamp_ms: r.timestamp_ms,
        idempotency_key: r.idempotency_key,
    }
}

// RPC request entry (info): one span per Send. `skip_all` keeps the (large)
// request + identity out of the span; `records` carries the batch size. The
// per-record produce loop is NOT separately instrumented (tight loop).
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(records = req.0.records.len()),
)]
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub async fn send(
    Extension(state): Extension<Arc<AppState>>,
    principal: Option<Extension<Principal>>,
    peer: Option<Extension<SocketAddr>>,
    req: ConnectRequest<pb::SendRequest>,
) -> Result<ConnectResponse<pb::SendResponse>, ConnectError> {
    // RED signals: count this request as in-flight for its whole lifetime and
    // time the end-to-end handler latency under the `send` method label. The
    // guard decrements + observes on drop (covering every return path).
    let _req = metrics().begin_request("send");
    let msg = req.0;
    let acks = producer_acks_from_pb(msg.acks).ok_or_else(|| {
        ConnectError::new_invalid_argument(format!("unknown acknowledgement mode {}", msg.acks))
    })?;
    // Effective identity: the proxy-injected principal (P4 mTLS / parallel
    // bearer task) or ANONYMOUS; the caller's peer address or the unknown host.
    let eff = principal.map_or_else(anonymous_principal, |Extension(p)| p);
    let host = peer.map_or_else(unknown_host, |Extension(a)| a);
    let mut window = SendWindow::new(msg.records.len());
    for r in msg.records {
        let rec = crate::handlers::to_gateway_record(r);
        // Per-record Write ACL on the target topic. Deny ⇒ skip producing and
        // surface a non-retriable PERMISSION_DENIED for that record only.
        if authorize_resource(
            &state,
            &eff,
            &host,
            ResourceType::Topic,
            &rec.topic,
            AclOperation::Write,
        ) == AuthorizationResult::Deny
        {
            metrics().record_send("unauthorized");
            let result = error_result(&crate::error::GatewayError::Unauthorized(format!(
                "Write Topic:{}",
                rec.topic
            )));
            window.push(async move { result }.boxed()).await;
            continue;
        }
        if rec.idempotency_key.is_some() {
            // A dedup transaction or forwarding operation remains a barrier in
            // input order, including when plain and keyed records are mixed.
            window.drain().await;
            let t0 = std::time::Instant::now();
            let result = state.produce.produce_with_acks(rec, &eff, acks).await;
            window.results.push(send_result(result, t0));
        } else {
            // Enqueue in input order so per-partition ordering does not depend
            // on how concurrently polled futures happen to be scheduled.
            let t0 = std::time::Instant::now();
            let delivery = state.produce.enqueue_plain_with_acks(&rec, acks).await;
            window
                .push(
                    async move {
                        let result = match delivery {
                            Ok(delivery) => delivery
                                .await
                                .map(|meta| crate::types::RecordOutcome {
                                    partition: crate::ids::PartitionIndex(meta.partition),
                                    offset: crate::ids::Offset(meta.offset),
                                    deduplicated: false,
                                })
                                .map_err(crate::error::GatewayError::Producer),
                            Err(error) => Err(error),
                        };
                        send_result(result, t0)
                    }
                    .boxed(),
                )
                .await;
        }
    }
    window.drain().await;
    let results = window.results;
    Ok(ConnectResponse::new(pb::SendResponse { results }))
}

const SEND_WINDOW_CAPACITY: usize = 64;

/// Bounded delivery window; futures represent records already enqueued.
struct SendWindow<T> {
    pending: Vec<BoxFuture<'static, T>>,
    results: Vec<T>,
}

impl<T: 'static> SendWindow<T> {
    fn new(records: usize) -> Self {
        Self {
            pending: Vec::with_capacity(SEND_WINDOW_CAPACITY),
            results: Vec::with_capacity(records),
        }
    }

    async fn push(&mut self, delivery: BoxFuture<'static, T>) {
        self.pending.push(delivery);
        if self.pending.len() == SEND_WINDOW_CAPACITY {
            self.drain().await;
        }
    }

    async fn drain(&mut self) {
        for delivery in self.pending.drain(..) {
            self.results.push(delivery.await);
        }
    }
}

fn send_result(
    result: Result<crate::types::RecordOutcome, crate::error::GatewayError>,
    started: std::time::Instant,
) -> pb::RecordResult {
    metrics().observe_produce_latency(started.elapsed().as_time());
    match result {
        Ok(outcome) => {
            metrics().record_send(if outcome.deduplicated {
                "deduplicated"
            } else {
                "ok"
            });
            pb::RecordResult {
                partition: outcome.partition.into(),
                offset: outcome.offset.into(),
                deduplicated: outcome.deduplicated,
                error: None,
            }
        }
        Err(error) => {
            metrics().record_send(
                if matches!(error, crate::error::GatewayError::Unauthorized(_)) {
                    "unauthorized"
                } else {
                    "error"
                },
            );
            error_result(&error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_window_enqueues_before_waiting_and_keeps_input_order() {
        let mut window = SendWindow::new(130);
        for base in [0, 64] {
            let mut acknowledgements = Vec::new();
            for index in base..base + 63 {
                let (tx, rx) = tokio::sync::oneshot::channel();
                acknowledgements.push((index, tx));
                window.push(async move { rx.await.unwrap() }.boxed()).await;
            }
            // Delivery handles can remain unresolved while the next 63 records
            // enqueue. A serial send loop would stop on the first handle.
            assert2::assert!(window.results.len() == base);
            let (tx, rx) = tokio::sync::oneshot::channel();
            acknowledgements.push((base + 63, tx));
            // Complete out of order, including a per-record error. Response
            // positions must still match the input positions.
            for (index, tx) in acknowledgements.into_iter().rev() {
                tx.send(if index == 17 { Err(index) } else { Ok(index) })
                    .unwrap();
            }
            window.push(async move { rx.await.unwrap() }.boxed()).await;
            assert2::assert!(window.pending.is_empty());
        }
        window.push(async { Ok(128) }.boxed()).await;
        window.push(async { Ok(129) }.boxed()).await;
        window.drain().await;
        assert2::assert!(
            window.results
                == (0..130)
                    .map(|index| if index == 17 { Err(index) } else { Ok(index) })
                    .collect::<Vec<_>>()
        );
    }

    #[test]
    fn producer_ack_wire_values_map_to_native_modes() {
        assert2::assert!(
            producer_acks_from_pb(pb::Acks::All as i32).expect("ACKS_ALL maps")
                == krabka_client_producer::Acks::All
        );
        assert2::assert!(
            producer_acks_from_pb(pb::Acks::Leader as i32).expect("ACKS_LEADER maps")
                == krabka_client_producer::Acks::One
        );
        assert2::assert!(
            producer_acks_from_pb(pb::Acks::None as i32).expect("ACKS_NONE maps")
                == krabka_client_producer::Acks::Zero
        );
        assert2::assert!(producer_acks_from_pb(i32::MAX).is_none());
    }
}
