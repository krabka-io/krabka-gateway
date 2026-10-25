//! [`crate::codec::RecordCodec`] implementation backed by the Confluent Schema Registry.
//!
//! [`SchemaRegistryCodec`] wraps a [`SchemaResolver`], which in production is a
//! [`super::client::SchemaRegistryClient`]. On the encode path the codec
//! resolves the schema, serializes the structured body into the wire format,
//! and prepends the Confluent frame. On the decode path it strips the frame,
//! looks up the schema by id, and deserializes the payload to a JSON view.
//!
//! The codec depends on the registry through the small [`SchemaResolver`] trait
//! rather than the concrete client. A unit test can therefore drive the encode
//! and decode logic against a fake resolver, with no live registry. The
//! integration test exercises the live client-to-HTTP wiring.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use bytes::Bytes;

use super::{client::SchemaRegistryClient, format::PreparedSchema};
use crate::codec::{CodecError, Decoded, EncodeBody, SchemaFormat, SchemaMeta};

/// Schema lookups the codec needs, held apart from the concrete client so a
/// unit test can drive the encode and decode paths against a fake.
///
/// [`SchemaRegistryClient`] implements this trait, and each method delegates to
/// its caching HTTP methods.
#[async_trait::async_trait]
pub trait SchemaResolver: Send + Sync + 'static {
    /// Resolve `(schema string, format)` by numeric schema id.
    async fn by_id(&self, id: i32) -> Result<(String, SchemaFormat), CodecError>;
    /// Resolve the latest `(id, schema string, format)` for `subject`.
    async fn latest(&self, subject: &str) -> Result<(i32, String, SchemaFormat), CodecError>;
}

#[async_trait::async_trait]
impl SchemaResolver for SchemaRegistryClient {
    async fn by_id(&self, id: i32) -> Result<(String, SchemaFormat), CodecError> {
        self.schema_by_id(id).await
    }

    async fn latest(&self, subject: &str) -> Result<(i32, String, SchemaFormat), CodecError> {
        SchemaRegistryClient::latest(self, subject).await
    }
}

/// A [`crate::codec::RecordCodec`] that encodes and decodes record values with
/// a Confluent Schema Registry.
///
/// The gateway constructs it once at startup and shares it across all
/// connection handlers in an `Arc`.
pub struct SchemaRegistryCodec {
    /// The schema resolver, which in production is the caching HTTP client.
    pub client: Arc<dyn SchemaResolver>,
    /// When `true`, the codec Confluent-frames `EncodeBody::Raw` bytes with the
    /// subject's latest registered schema id. It assumes those bytes are
    /// already serialized in that format. When `false`, the default, raw bytes
    /// pass through unchanged.
    pub frame_raw: bool,
    prepared: Mutex<PreparedCache>,
}

impl SchemaRegistryCodec {
    /// Construct a codec over the concrete [`SchemaRegistryClient`].
    ///
    /// The gateway wiring uses this constructor. It boxes the client behind the
    /// [`SchemaResolver`] trait object.
    #[must_use]
    pub fn new(client: Arc<SchemaRegistryClient>, frame_raw: bool) -> Self {
        Self::with_resolver(client, frame_raw)
    }

    /// Construct a codec over any [`SchemaResolver`]. Tests use it with a fake
    /// resolver.
    #[must_use]
    pub fn with_resolver(client: Arc<dyn SchemaResolver>, frame_raw: bool) -> Self {
        Self {
            client,
            frame_raw,
            prepared: Mutex::new(PreparedCache::default()),
        }
    }

    fn prepared(
        &self,
        id: i32,
        fmt: SchemaFormat,
        schema: &str,
    ) -> Result<Arc<PreparedSchema>, CodecError> {
        let slot = self
            .prepared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .slot(id);
        slot.get_or_prepare(fmt, schema)
    }
}

const PREPARED_SCHEMA_CAPACITY: usize = 1024;

#[derive(Default)]
struct PreparedCache {
    entries: HashMap<i32, Arc<PreparedSlot>>,
    insertion_order: VecDeque<i32>,
}

impl PreparedCache {
    fn slot(&mut self, id: i32) -> Arc<PreparedSlot> {
        if let Some(slot) = self.entries.get(&id) {
            return Arc::clone(slot);
        }
        let slot = Arc::new(PreparedSlot::default());
        if self.entries.len() == PREPARED_SCHEMA_CAPACITY
            && let Some(oldest) = self.insertion_order.pop_front()
        {
            self.entries.remove(&oldest);
        }
        self.entries.insert(id, Arc::clone(&slot));
        self.insertion_order.push_back(id);
        slot
    }
}

#[derive(Default)]
struct PreparedSlot(Mutex<Option<Arc<PreparedSchema>>>);

impl PreparedSlot {
    fn get_or_prepare(
        &self,
        fmt: SchemaFormat,
        schema: &str,
    ) -> Result<Arc<PreparedSchema>, CodecError> {
        // Compile once per id without blocking lookups for other schemas.
        // Errors leave the slot empty so the next call can retry preparation.
        let mut entry = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(prepared) = entry.as_ref() {
            return Ok(Arc::clone(prepared));
        }
        let prepared = Arc::new(PreparedSchema::new(fmt, schema)?);
        *entry = Some(Arc::clone(&prepared));
        Ok(prepared)
    }
}

/// `TopicNameStrategy`: the default subject for a topic's value schema.
fn subject_for(topic: &str) -> String {
    format!("{topic}-value")
}

#[async_trait::async_trait]
impl crate::codec::RecordCodec for SchemaRegistryCodec {
    async fn encode(&self, topic: &str, body: EncodeBody) -> Result<Bytes, CodecError> {
        match body {
            EncodeBody::Raw(bytes) => {
                if self.frame_raw {
                    // Bytes are assumed already serialized in the registered
                    // format; frame them against the subject's latest schema.
                    let subject = subject_for(topic);
                    let (id, _schema, fmt) = self.client.latest(&subject).await?;
                    Ok(super::wire::encode_frame(id, fmt, &bytes))
                } else {
                    // Passthrough (default).
                    Ok(bytes)
                }
            }
            EncodeBody::Structured { json, schema } => {
                let subject = schema.subject.clone().unwrap_or_else(|| subject_for(topic));

                // Resolve (id, schema string, format). Trust the REGISTRY's
                // format over the selector hint.
                let (id, schema_str, fmt) = match schema.id {
                    Some(id) => {
                        let (schema_str, fmt) = self.client.by_id(id).await?;
                        (id, schema_str, fmt)
                    }
                    None => self.client.latest(&subject).await.map_err(|e| match e {
                        // A 404 from `latest` means no schema is registered for
                        // this subject — surface a clear, actionable message.
                        CodecError::Registry(_) => CodecError::Registry(format!(
                            "no schema registered for subject {subject}"
                        )),
                        other => other,
                    })?,
                };

                let payload = self.prepared(id, fmt, &schema_str)?.serialize(&json)?;
                Ok(super::wire::encode_frame(id, fmt, &payload))
            }
        }
    }

    async fn decode(&self, topic: &str, value: Bytes) -> Result<Decoded, CodecError> {
        // Not Confluent-framed (empty or missing magic byte) → lenient
        // passthrough with no schema metadata.
        if value.is_empty() || value[0] != 0x00 {
            return Ok(Decoded {
                value,
                schema: None,
                json: None,
            });
        }

        let (id, rest) = super::wire::decode_frame_bytes(&value)?;
        let (schema_str, fmt) = self.client.by_id(id).await?;

        // Protobuf carries a message-index prefix after the header; strip it
        // before deserialization.
        let payload = if fmt == SchemaFormat::Protobuf {
            super::wire::strip_proto_index_bytes(&rest)?
        } else {
            rest
        };

        let json = self.prepared(id, fmt, &schema_str)?.deserialize(&payload)?;

        Ok(Decoded {
            value: payload,
            schema: Some(SchemaMeta {
                subject: subject_for(topic),
                id,
                format: fmt,
            }),
            json: Some(json),
        })
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{RecordCodec, SchemaSelector};

    /// An Avro record schema reused across the structured encode/decode tests.
    const AVRO_SCHEMA: &str = r#"{
        "type": "record",
        "name": "R",
        "fields": [
            {"name": "id",   "type": "long"},
            {"name": "name", "type": "string"}
        ]
    }"#;

    /// A canned-schema resolver for unit tests, with no live registry. It
    /// records the id that `latest` answers with, and it serves a single schema
    /// for both lookup paths.
    struct FakeResolver {
        id: i32,
        schema: String,
        fmt: SchemaFormat,
    }

    #[async_trait::async_trait]
    impl SchemaResolver for FakeResolver {
        async fn by_id(&self, id: i32) -> Result<(String, SchemaFormat), CodecError> {
            if id == self.id {
                Ok((self.schema.clone(), self.fmt))
            } else {
                Err(CodecError::Registry(format!("schema id {id} not found")))
            }
        }

        async fn latest(&self, _subject: &str) -> Result<(i32, String, SchemaFormat), CodecError> {
            Ok((self.id, self.schema.clone(), self.fmt))
        }
    }

    fn avro_codec(frame_raw: bool) -> SchemaRegistryCodec {
        SchemaRegistryCodec::with_resolver(
            Arc::new(FakeResolver {
                id: 5,
                schema: AVRO_SCHEMA.to_owned(),
                fmt: SchemaFormat::Avro,
            }),
            frame_raw,
        )
    }

    #[tokio::test]
    async fn schema_decode_shares_frame_storage_and_keeps_metadata() {
        for (format, schema, json) in [
            (
                SchemaFormat::Avro,
                AVRO_SCHEMA,
                br#"{"id":1,"name":"a"}"#.as_slice(),
            ),
            (
                SchemaFormat::Json,
                r#"{"type":"object","required":["id"]}"#,
                br#"{"id":1}"#.as_slice(),
            ),
            (
                SchemaFormat::Protobuf,
                r#"syntax = "proto3"; message R { int32 id = 1; }"#,
                br#"{"id":1}"#.as_slice(),
            ),
        ] {
            let codec = SchemaRegistryCodec::with_resolver(
                Arc::new(FakeResolver {
                    id: 5,
                    schema: schema.to_string(),
                    fmt: format,
                }),
                false,
            );
            let payload = codec
                .prepared(5, format, schema)
                .unwrap()
                .serialize(json)
                .unwrap();
            let frame = super::super::wire::encode_frame(5, format, &payload);
            let decoded = codec.decode("orders", frame.clone()).await.unwrap();
            let start = 5 + usize::from(format == SchemaFormat::Protobuf);
            assert2::assert!(decoded.value.as_ref() == payload);
            if !payload.is_empty() {
                assert2::assert!(decoded.value.as_ptr() == frame[start..].as_ptr());
            }
            assert2::assert!(
                decoded.schema
                    == Some(SchemaMeta {
                        subject: "orders-value".to_string(),
                        id: 5,
                        format
                    })
            );
            assert2::assert!(decoded.json.is_some());
            assert2::assert!(frame == super::super::wire::encode_frame(5, format, &payload));
        }
    }

    #[tokio::test]
    async fn registry_lookup_still_precedes_proto_index_validation() {
        let codec = SchemaRegistryCodec::with_resolver(
            Arc::new(FakeResolver {
                id: 5,
                schema: r#"syntax = "proto3"; message R { int32 id = 1; }"#.to_string(),
                fmt: SchemaFormat::Protobuf,
            }),
            false,
        );
        for (frame, registry_error) in [
            (Bytes::from_static(&[0, 0, 0, 0, 6]), true),
            (Bytes::from_static(&[0, 0, 0, 0, 5]), false),
            (Bytes::from_static(&[0, 0, 0, 0, 5, 0x80]), false),
        ] {
            let error = codec.decode("orders", frame).await.unwrap_err();
            assert2::assert!(matches!(error, CodecError::Registry(_)) == registry_error);
            assert2::assert!(matches!(error, CodecError::Framing(_)) != registry_error);
        }
    }

    #[test]
    fn prepared_schemas_are_shared_across_concurrent_misses_and_bounded() {
        let codec = Arc::new(avro_codec(false));
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let codec = Arc::clone(&codec);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    codec.prepared(5, SchemaFormat::Avro, AVRO_SCHEMA).unwrap()
                })
            })
            .collect();
        let prepared: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        for schema in &prepared {
            assert2::assert!(Arc::ptr_eq(&prepared[0], schema));
        }
        // Errors are retried, rather than poisoning a schema id permanently.
        assert2::assert!(
            codec
                .prepared(6, SchemaFormat::Json, "invalid JSON")
                .is_err()
        );
        assert2::assert!(codec.prepared(6, SchemaFormat::Json, "true").is_ok());
        for id in 7..1031 {
            codec.prepared(id, SchemaFormat::Json, "true").unwrap();
        }
        assert2::assert!(codec.prepared.lock().unwrap().entries.len() == PREPARED_SCHEMA_CAPACITY);
        let recompiled = codec.prepared(5, SchemaFormat::Avro, AVRO_SCHEMA).unwrap();
        assert2::assert!(!Arc::ptr_eq(&prepared[0], &recompiled));
        // An outstanding reference remains usable after its cache entry evicts.
        let payload = prepared[0].serialize(br#"{"id":1,"name":"a"}"#).unwrap();
        assert2::assert!(
            prepared[0].deserialize(&payload).unwrap() == recompiled.deserialize(&payload).unwrap()
        );
    }

    #[tokio::test]
    async fn encode_and_decode_share_the_same_preparation() {
        for (fmt, schema, payload) in [
            (
                SchemaFormat::Avro,
                AVRO_SCHEMA,
                br#"{"id":1,"name":"a"}"#.as_slice(),
            ),
            (
                SchemaFormat::Json,
                r#"{"type":"object","required":["id"]}"#,
                br#"{"id":1}"#.as_slice(),
            ),
            (
                SchemaFormat::Protobuf,
                r#"syntax = "proto3"; message R { int32 id = 1; }"#,
                br#"{"id":1}"#.as_slice(),
            ),
        ] {
            let codec = SchemaRegistryCodec::with_resolver(
                Arc::new(FakeResolver {
                    id: 5,
                    schema: schema.into(),
                    fmt,
                }),
                false,
            );
            let body = EncodeBody::Structured {
                json: Bytes::copy_from_slice(payload),
                schema: SchemaSelector {
                    id: Some(5),
                    subject: None,
                    format: fmt,
                },
            };
            let encoded = codec.encode("orders", body).await.unwrap();
            let prepared = codec.prepared(5, fmt, schema).unwrap();
            let decoded = codec.decode("orders", encoded).await.unwrap();
            assert2::assert!(
                serde_json::from_slice::<serde_json::Value>(&decoded.json.unwrap()).unwrap()
                    == serde_json::from_slice::<serde_json::Value>(payload).unwrap()
            );
            assert2::assert!(Arc::ptr_eq(
                &prepared,
                &codec.prepared(5, fmt, schema).unwrap()
            ));
        }
    }

    #[tokio::test]
    async fn latest_schema_changes_select_a_new_prepared_entry() {
        struct LatestResolver(std::sync::atomic::AtomicI32);
        #[async_trait::async_trait]
        impl SchemaResolver for LatestResolver {
            async fn by_id(&self, id: i32) -> Result<(String, SchemaFormat), CodecError> {
                Ok((
                    if id == 1 {
                        r#"{"required":["id"]}"#
                    } else {
                        r#"{"required":["name"]}"#
                    }
                    .into(),
                    SchemaFormat::Json,
                ))
            }
            async fn latest(
                &self,
                _subject: &str,
            ) -> Result<(i32, String, SchemaFormat), CodecError> {
                let id = self.0.load(std::sync::atomic::Ordering::SeqCst);
                let (schema, fmt) = self.by_id(id).await?;
                Ok((id, schema, fmt))
            }
        }
        let resolver = Arc::new(LatestResolver(std::sync::atomic::AtomicI32::new(1)));
        let codec = SchemaRegistryCodec::with_resolver(resolver.clone(), false);
        let body = |json: &'static [u8], id| EncodeBody::Structured {
            json: Bytes::from_static(json),
            schema: SchemaSelector {
                id,
                subject: None,
                format: SchemaFormat::Json,
            },
        };
        let first = codec
            .encode("orders", body(br#"{"id":1}"#, None))
            .await
            .unwrap();
        assert2::assert!(super::super::wire::decode_frame(&first).unwrap().0 == 1);
        resolver.0.store(2, std::sync::atomic::Ordering::SeqCst);
        assert2::assert!(
            codec
                .encode("orders", body(br#"{"id":1}"#, None))
                .await
                .is_err()
        );
        let second = codec
            .encode("orders", body(br#"{"name":"a"}"#, None))
            .await
            .unwrap();
        assert2::assert!(super::super::wire::decode_frame(&second).unwrap().0 == 2);
        assert2::assert!(
            codec
                .encode("orders", body(br#"{"id":1}"#, Some(1)))
                .await
                .unwrap()
                == first
        );
    }

    #[test]
    #[ignore = "manual codec latency comparison"]
    fn prepared_schema_latency_comparison() {
        let payload = br#"{"id":1,"kind":"v0"}"#;
        for width in [10, 100, 500] {
            let avro = serde_json::json!({
                "type": "record", "name": "R", "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "kind", "type": {"type": "enum", "name": "Kind", "symbols": (0..width).map(|n| format!("v{n}")).collect::<Vec<_>>()}}
                ]
            }).to_string();
            let json = serde_json::json!({"type": "object", "properties": (0..width).map(|n| (format!("optional{n}"), serde_json::json!({"type":"string"}))).collect::<serde_json::Map<_, _>>()}).to_string();
            let protobuf = format!(
                r#"syntax = "proto3"; message R {{ int32 id = 1; string kind = 2; {} }}"#,
                (0..width)
                    .map(|n| format!("string optional{n} = {};", n + 3))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
            for (fmt, schema) in [
                (SchemaFormat::Avro, avro),
                (SchemaFormat::Json, json),
                (SchemaFormat::Protobuf, protobuf),
            ] {
                let mut cache = PreparedCache::default();
                cache.slot(1).get_or_prepare(fmt, &schema).unwrap();
                let start = std::time::Instant::now();
                for _ in 0..100 {
                    let compiled = cache.slot(1).get_or_prepare(fmt, &schema).unwrap();
                    let encoded = compiled.serialize(std::hint::black_box(payload)).unwrap();
                    std::hint::black_box(compiled.deserialize(&encoded).unwrap());
                }
                let warm = start.elapsed();
                let start = std::time::Instant::now();
                for _ in 0..100 {
                    let encoded = super::super::format::serialize(
                        fmt,
                        &schema,
                        std::hint::black_box(payload),
                    )
                    .unwrap();
                    std::hint::black_box(
                        super::super::format::deserialize(fmt, &schema, &encoded).unwrap(),
                    );
                }
                eprintln!(
                    "format={fmt:?} width={width} payload_bytes={} records=100 warm={warm:?} reprepare={:?}",
                    payload.len(),
                    start.elapsed()
                );
            }
        }
    }

    #[test]
    fn subject_for_appends_value_suffix() {
        assert2::assert!(subject_for("orders") == "orders-value");
    }

    #[tokio::test]
    async fn raw_passthrough_when_frame_raw_false() {
        let codec = avro_codec(false);
        let input = Bytes::from_static(b"\x09opaque-bytes");
        let out = codec
            .encode("orders", EncodeBody::Raw(input.clone()))
            .await
            .unwrap();
        assert2::assert!(out == input);
    }

    #[tokio::test]
    async fn raw_frames_when_frame_raw_true() {
        let codec = avro_codec(true);
        let input = Bytes::from_static(b"already-avro");
        let out = codec
            .encode("orders", EncodeBody::Raw(input.clone()))
            .await
            .unwrap();
        // Confluent frame: 0x00 then id=5 big-endian, then the payload verbatim
        // (Avro adds no message-index prefix).
        let expected = [&[0x00, 0x00, 0x00, 0x00, 0x05][..], input.as_ref()].concat();
        assert2::assert!(out == Bytes::from(expected));
    }

    #[tokio::test]
    async fn structured_avro_encode_produces_framing() {
        let codec = avro_codec(false);
        let json = Bytes::from_static(br#"{"id": 1, "name": "a"}"#);
        let out = codec
            .encode(
                "orders",
                EncodeBody::Structured {
                    json,
                    schema: SchemaSelector {
                        subject: None,
                        id: None,
                        format: SchemaFormat::Avro,
                    },
                },
            )
            .await
            .unwrap();
        // [00][id=5 BE][avro datum…]
        assert2::assert!(&out[..5] == &[0x00, 0x00, 0x00, 0x00, 0x05]);
        assert2::assert!(out.len() > 5);
    }

    #[tokio::test]
    async fn structured_encode_then_decode_round_trips() {
        let codec = avro_codec(false);
        let json_in = br#"{"id": 1, "name": "a"}"#;
        let framed = codec
            .encode(
                "orders",
                EncodeBody::Structured {
                    json: Bytes::from_static(json_in),
                    schema: SchemaSelector {
                        subject: None,
                        id: None,
                        format: SchemaFormat::Avro,
                    },
                },
            )
            .await
            .unwrap();

        let expected_value = framed.slice(5..);
        let decoded = codec.decode("orders", framed).await.unwrap();
        let json_out = decoded
            .json
            .as_deref()
            .expect("framed value carries a JSON view");
        let expected: serde_json::Value = serde_json::from_slice(json_in).unwrap();
        let actual: serde_json::Value = serde_json::from_slice(json_out).unwrap();
        assert2::assert!(decoded.value == expected_value);
        assert2::assert!(
            decoded.schema
                == Some(SchemaMeta {
                    subject: "orders-value".to_string(),
                    id: 5,
                    format: SchemaFormat::Avro,
                })
        );
        assert2::assert!(actual == expected);
    }

    #[tokio::test]
    async fn structured_encode_by_pinned_id() {
        let codec = avro_codec(false);
        let json = Bytes::from_static(br#"{"id": 7, "name": "b"}"#);
        let out = codec
            .encode(
                "orders",
                EncodeBody::Structured {
                    json,
                    schema: SchemaSelector {
                        subject: None,
                        id: Some(5),
                        format: SchemaFormat::Avro,
                    },
                },
            )
            .await
            .unwrap();
        assert2::assert!(&out[..5] == &[0x00, 0x00, 0x00, 0x00, 0x05]);
    }

    #[tokio::test]
    async fn decode_unframed_values_pass_through() {
        for (_name, value) in [
            ("non-magic", Bytes::from_static(b"\x01not-confluent-framed")),
            ("empty", Bytes::new()),
        ] {
            let codec = avro_codec(false);
            let decoded = codec.decode("orders", value.clone()).await.unwrap();
            assert2::assert!(decoded.value == value);
            assert2::assert!(decoded.schema == None);
            assert2::assert!(decoded.json == None);
        }
    }
}
