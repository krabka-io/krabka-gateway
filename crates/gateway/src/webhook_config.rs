//! Operator-supplied webhook-endpoint config (TOML), compiled at load time.
//!
//! The loader parses each `JSONPath` expression once and validates the
//! signature settings. This module follows the broker's `file_config` pattern.

use std::collections::HashMap;

use base64::{Engine, engine::general_purpose::STANDARD as B64STD};
use hmac::{Hmac, KeyInit, Mac};
use jsonpath_rust::{parser::model::JpQuery, query::js_path_process};
use krabka_units::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::{
    codec::SchemaFormat,
    config_value::{non_negative_time, positive_byte_size},
};

/// Raw TOML form, with one entry in `[[endpoints]]` per named endpoint.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebhooksFile {
    #[serde(default)]
    pub endpoints: Vec<WebhookEndpoint>,
}

/// One named webhook endpoint as written in the TOML config file.
///
/// The dimensioned settings carry their unit: `timestamp_tolerance = "5m"`,
/// `max_body = "1MiB"`. A bare number is rejected.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct WebhookEndpoint {
    pub name: String,
    pub target_topic: String,
    /// `standard_webhooks` signs the message ID, timestamp, and original body.
    pub signature_mode: Option<SignatureMode>,
    /// Service principal this endpoint produces as, for authz. Default
    /// `webhook:{name}`.
    pub principal: Option<String>,
    /// HMAC-SHA256 shared secret. If set, `signature_header` is required.
    pub secret: Option<String>,
    /// HTTP header that carries the HMAC signature (e.g. `X-Hub-Signature-256`).
    pub signature_header: Option<String>,
    /// `"hex"` (default) or `"base64"`.
    pub signature_encoding: Option<String>,
    /// Optional literal prefix to strip before decoding, for example
    /// `"sha256="` for GitHub.
    pub signature_prefix: Option<String>,
    /// Optional replay guard: header that carries the request timestamp.
    pub timestamp_header: Option<String>,
    /// Max age of a valid timestamp, e.g. `"5m"` (default 5 minutes).
    #[serde(
        default,
        with = "krabka_units::serde_units::human::option_time",
        skip_serializing_if = "Option::is_none"
    )]
    pub timestamp_tolerance: Option<Time>,
    /// `header:<Name>` or `json:<JSONPath expr>`. Absent ⇒ no dedup, which is a
    /// plain produce.
    pub idempotency_source: Option<String>,
    /// Optional record-key source: `header:<Name>` or `json:<JSONPath expr>`.
    pub key_source: Option<String>,
    /// HTTP headers to preserve as record metadata. Secret headers are rejected.
    pub forward_headers: Option<Vec<String>>,
    /// Maximum accepted body size, e.g. `"1MiB"` (default 1 MiB).
    #[serde(
        default,
        with = "krabka_units::serde_units::human::option_byte_size",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_body: Option<ByteSize>,
    /// Optional Schema Registry subject. When set, the injected codec validates
    /// and serializes the request body against this subject's schema, then
    /// produces it as a STRUCTURED record. A validation failure returns `400`.
    pub schema_subject: Option<String>,
    /// Payload format of the schema: `"avro"`, `"json"` (default), or
    /// `"protobuf"`. Only meaningful when `schema_subject` is set.
    pub schema_format: Option<String>,
}

/// A value source: an HTTP header or a compiled `JSONPath` into the JSON body.
#[derive(Debug, Clone)]
pub enum Source {
    Header(String),
    JsonPath(JpQuery),
}

impl Source {
    /// Parse a `header:<Name>` or `json:<expr>` spec. This compiles the
    /// `JSONPath` at load time.
    fn parse(spec: &str, ctx: &str) -> Result<Self, String> {
        if let Some(h) = spec.strip_prefix("header:") {
            Ok(Source::Header(h.to_string()))
        } else if let Some(jp) = spec.strip_prefix("json:") {
            let q = jsonpath_rust::parser::parse_json_path(jp)
                .map_err(|e| format!("{ctx}: invalid JSONPath {jp:?}: {e}"))?;
            Ok(Source::JsonPath(q))
        } else {
            Err(format!("{ctx}: must start with 'header:' or 'json:'"))
        }
    }
}

/// How the HMAC digest is encoded in the signature header value.
#[derive(Debug, Clone)]
pub enum SigEncoding {
    Hex,
    Base64,
}

/// Signature protocol for a named webhook endpoint.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SignatureMode {
    /// Standard Webhooks HMAC-SHA256 authentication.
    StandardWebhooks,
}

/// Validated and compiled endpoint config, the runtime form.
#[derive(Debug, Clone)]
pub struct CompiledWebhook {
    pub target_topic: String,
    pub signature_mode: Option<SignatureMode>,
    pub principal: String,
    /// Raw secret bytes. `None` means signature verification is disabled.
    pub secret: Option<Vec<u8>>,
    /// HTTP header carrying the HMAC signature.
    pub signature_header: Option<String>,
    pub signature_encoding: SigEncoding,
    /// Literal prefix to strip before hex or base64 decoding, for example
    /// `"sha256="`.
    pub signature_prefix: Option<String>,
    pub timestamp_header: Option<String>,
    pub timestamp_tolerance: Time,
    pub idempotency_source: Option<Source>,
    pub key_source: Option<Source>,
    pub forward_headers: Vec<axum::http::HeaderName>,
    pub max_body: ByteSize,
    /// Schema Registry subject to validate and serialize the request body
    /// against. `None` ⇒ the gateway produces the body raw, with no schema
    /// validation.
    pub schema_subject: Option<String>,
    /// The schema's payload format. Defaults to [`SchemaFormat::Json`]. The
    /// gateway reads it only when `schema_subject` is `Some`.
    pub schema_format: SchemaFormat,
}

impl WebhooksFile {
    /// Compile and validate every endpoint. Returns `name -> CompiledWebhook`.
    ///
    /// # Errors
    ///
    /// Returns a human-readable message describing the first invalid endpoint.
    pub fn compile(&self) -> Result<HashMap<String, CompiledWebhook>, String> {
        let mut out = HashMap::new();
        for e in &self.endpoints {
            let ctx = format!("[webhooks {}]", e.name);

            let secret = if e.signature_mode == Some(SignatureMode::StandardWebhooks) {
                if e.signature_header.is_some()
                    || e.signature_encoding.is_some()
                    || e.signature_prefix.is_some()
                    || e.timestamp_header.is_some()
                {
                    return Err(format!(
                        "{ctx}: standard_webhooks sets signature and timestamp headers"
                    ));
                }
                let token = e.secret.as_deref().ok_or_else(|| {
                    format!("{ctx}: standard_webhooks requires a signing token in secret")
                })?;
                let key = B64STD
                    .decode(token.strip_prefix("whsec_").unwrap_or(token))
                    .map_err(|_| format!("{ctx}: invalid Standard Webhooks signing token"))?;
                if key.is_empty() {
                    return Err(format!("{ctx}: signing token must not be empty"));
                }
                Some(key)
            } else {
                e.secret.as_ref().map(|s| s.clone().into_bytes())
            };

            // Generic HMAC endpoints need both settings.
            if e.signature_mode.is_none() && e.secret.is_some() != e.signature_header.is_some() {
                return Err(format!(
                    "{ctx}: `secret` and `signature_header` must be set together"
                ));
            }

            // An explicitly supplied secret must not be empty.
            if e.secret.as_deref() == Some("") {
                return Err(format!("{ctx}: secret must not be empty"));
            }

            let signature_encoding = match e.signature_encoding.as_deref() {
                None | Some("hex") => SigEncoding::Hex,
                Some("base64") => SigEncoding::Base64,
                Some(o) => {
                    return Err(format!(
                        "{ctx}: signature_encoding must be 'hex' or 'base64', got {o:?}"
                    ));
                }
            };

            if e.signature_mode.is_some()
                && e.idempotency_source
                    .as_deref()
                    .is_some_and(|source| source != "header:webhook-id")
            {
                return Err(format!(
                    "{ctx}: standard_webhooks uses the signed webhook-id for deduplication"
                ));
            }
            let idempotency_source = e
                .idempotency_source
                .as_deref()
                .or_else(|| e.signature_mode.map(|_| "header:webhook-id"))
                .map(|s| Source::parse(s, &format!("{ctx}.idempotency_source")))
                .transpose()?;

            let key_source = e
                .key_source
                .as_deref()
                .map(|s| Source::parse(s, &format!("{ctx}.key_source")))
                .transpose()?;

            let forward_headers =
                e.forward_headers
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(|name| {
                        let header: axum::http::HeaderName = name
                            .parse()
                            .map_err(|_| format!("{ctx}: invalid forward_headers name"))?;
                        if matches!(
                            header.as_str(),
                            "authorization" | "cookie" | "x-gitlab-token" | "webhook-signature"
                        ) || e.signature_header.as_deref().is_some_and(|signature| {
                            signature.eq_ignore_ascii_case(header.as_str())
                        }) {
                            return Err(format!("{ctx}: cannot forward authentication headers"));
                        }
                        Ok(header)
                    })
                    .collect::<Result<Vec<_>, String>>()?;

            // Validate the schema format string (defaults to JSON). This is
            // checked even when `schema_subject` is absent so a stray
            // `schema_format` typo still surfaces at load time.
            let schema_format = parse_schema_format(e.schema_format.as_deref(), &ctx)?;
            let timestamp_tolerance = non_negative_time(
                "timestamp_tolerance",
                e.timestamp_tolerance.unwrap_or_else(|| minutes(5)),
            )
            .map_err(|error| format!("{ctx}: {error}"))?;
            let max_body =
                positive_byte_size("max_body", e.max_body.unwrap_or_else(|| mebibytes(1)))
                    .map_err(|error| format!("{ctx}: {error}"))?;

            out.insert(
                e.name.clone(),
                CompiledWebhook {
                    target_topic: e.target_topic.clone(),
                    signature_mode: e.signature_mode,
                    principal: e
                        .principal
                        .clone()
                        .unwrap_or_else(|| format!("webhook:{}", e.name)),
                    secret,
                    signature_header: e.signature_header.clone(),
                    signature_encoding,
                    signature_prefix: e.signature_prefix.clone(),
                    timestamp_header: e.timestamp_header.clone(),
                    timestamp_tolerance,
                    idempotency_source,
                    key_source,
                    forward_headers,
                    max_body,
                    schema_subject: e.schema_subject.clone(),
                    schema_format,
                },
            );
        }
        Ok(out)
    }
}

/// Parse a schema-format string into a [`SchemaFormat`].
///
/// `None` and `"json"` both map to JSON, the default for webhook bodies,
/// because those bodies are JSON on the wire. The function returns a
/// human-readable error for an unknown value.
fn parse_schema_format(spec: Option<&str>, ctx: &str) -> Result<SchemaFormat, String> {
    match spec {
        None | Some("json") => Ok(SchemaFormat::Json),
        Some("avro") => Ok(SchemaFormat::Avro),
        Some("protobuf") => Ok(SchemaFormat::Protobuf),
        Some(o) => Err(format!(
            "{ctx}: schema_format must be 'avro', 'json', or 'protobuf', got {o:?}"
        )),
    }
}

// ---------------------------------------------------------------------------
// Runtime helpers (pub(crate) — used by webhook.rs + outbound.rs)
// ---------------------------------------------------------------------------

/// Compute HMAC-SHA256(`secret`, `body`) and return the digest as a lowercase
/// hex string. The outbound webhook delivery layer calls this to sign every
/// `X-Krabka-Signature` header.
#[allow(dead_code)] // used by outbound.rs
pub(crate) fn sign_hmac_hex(secret: &[u8], body: &[u8]) -> String {
    let mut mac = <Hmac<Sha256>>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

/// Compute HMAC-SHA256(`secret`, `body`) and return the digest as a standard
/// base64 string, with padding.
#[allow(dead_code)] // used by outbound.rs
pub(crate) fn sign_hmac_base64(secret: &[u8], body: &[u8]) -> String {
    let mut mac = <Hmac<Sha256>>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(body);
    B64STD.encode(mac.finalize().into_bytes())
}

/// Verify an HMAC-SHA256 signature over `body` using `secret`.
///
/// * `provided` is the raw header value, which can carry a prefix.
/// * `prefix` is an optional literal to strip before decoding, for example
///   `"sha256="`.
/// * The comparison is constant-time, which prevents timing side-channels.
///
/// Returns `false` on any decoding failure, so callers can treat it as an
/// authentication failure without telling the error kinds apart.
#[allow(dead_code)] // used by webhook.rs
pub(crate) fn verify_signature(
    secret: &[u8],
    body: &[u8],
    provided: &str,
    encoding: &SigEncoding,
    prefix: Option<&str>,
) -> bool {
    // Compute HMAC-SHA256(secret, body).
    let mut mac = <Hmac<Sha256>>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(body);
    let computed = mac.finalize().into_bytes();

    // Strip the optional prefix (e.g. "sha256=").
    let sig_str = if let Some(p) = prefix {
        match provided.strip_prefix(p) {
            Some(s) => s,
            None => return false,
        }
    } else {
        provided
    };

    // Decode the provided signature.
    let decoded = match encoding {
        SigEncoding::Hex => match hex::decode(sig_str) {
            Ok(b) => b,
            Err(_) => return false,
        },
        SigEncoding::Base64 => match B64STD.decode(sig_str) {
            Ok(b) => b,
            Err(_) => return false,
        },
    };

    // Constant-time compare — wrong length also returns false without branching
    // on secret material.
    if computed.len() != decoded.len() {
        return false;
    }
    computed.as_slice().ct_eq(decoded.as_slice()).unwrap_u8() == 1
}

/// Extract a value from an HTTP header or a `JSONPath` expression over the body.
///
/// Returns `None` when the header is absent, when the header is not UTF-8, or
/// when the `JSONPath` gives no string or number result.
#[allow(dead_code)] // used by webhook.rs
pub(crate) fn extract_source(
    src: &Source,
    headers: &axum::http::HeaderMap,
    body_json: Option<&serde_json::Value>,
) -> Option<String> {
    match src {
        Source::Header(h) => headers.get(h)?.to_str().ok().map(str::to_string),
        Source::JsonPath(q) => {
            let json = body_json?;
            let refs = js_path_process(q, json).ok()?;
            for r in refs {
                match r.val {
                    serde_json::Value::String(value) if !value.is_empty() => {
                        return Some(value.clone());
                    }
                    serde_json::Value::Number(value) => return Some(value.to_string()),
                    _ => {}
                }
            }
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use assert2::check;
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    use super::*;

    // Helper: compute HMAC-SHA256(secret, body) as a hex string.
    fn hmac_hex(secret: &[u8], body: &[u8]) -> String {
        let mut mac = <Hmac<Sha256>>::new_from_slice(secret).unwrap();
        mac.update(body);
        hex::encode(mac.finalize().into_bytes())
    }

    // Helper: compute HMAC-SHA256(secret, body) as a base64 string.
    fn hmac_b64(secret: &[u8], body: &[u8]) -> String {
        let mut mac = <Hmac<Sha256>>::new_from_slice(secret).unwrap();
        mac.update(body);
        B64STD.encode(mac.finalize().into_bytes())
    }

    // -----------------------------------------------------------------------
    // sign_hmac_hex / sign_hmac_base64 round-trip tests
    // -----------------------------------------------------------------------

    #[test]
    fn signed_hmac_values_verify_only_for_the_signed_body() {
        for (_name, secret, signed_body, verified_body, encoding, expected) in [
            (
                "hex-round-trip",
                b"outbound-secret".as_slice(),
                b"{\"topic\":\"events\",\"offset\":42}".as_slice(),
                b"{\"topic\":\"events\",\"offset\":42}".as_slice(),
                SigEncoding::Hex,
                true,
            ),
            (
                "base64-round-trip",
                b"another-secret".as_slice(),
                b"hello world".as_slice(),
                b"hello world".as_slice(),
                SigEncoding::Base64,
                true,
            ),
            (
                "hex-wrong-body",
                b"sec".as_slice(),
                b"correct body".as_slice(),
                b"wrong body".as_slice(),
                SigEncoding::Hex,
                false,
            ),
        ] {
            let signature = match &encoding {
                SigEncoding::Hex => sign_hmac_hex(secret, signed_body),
                SigEncoding::Base64 => sign_hmac_base64(secret, signed_body),
            };
            assert2::assert!(
                verify_signature(secret, verified_body, &signature, &encoding, None) == expected
            );
        }
    }

    // -----------------------------------------------------------------------
    // verify_signature tests
    // -----------------------------------------------------------------------

    #[test]
    fn signature_verification_variants() {
        let hex = hmac_hex(b"my-secret", b"{\"event\":\"push\"}");
        let base64 = hmac_b64(b"s3cr3t", b"payload");
        let prefixed = format!("sha256={}", hmac_hex(b"key", b"data"));
        let missing_prefix = hmac_hex(b"key", b"data");

        for (_name, secret, body, signature, encoding, prefix, expected) in [
            (
                "valid-hex",
                b"my-secret".as_slice(),
                b"{\"event\":\"push\"}".as_slice(),
                hex.as_str(),
                SigEncoding::Hex,
                None,
                true,
            ),
            (
                "tampered-hex-body",
                b"my-secret".as_slice(),
                b"{\"event\":\"pull\"}".as_slice(),
                hex.as_str(),
                SigEncoding::Hex,
                None,
                false,
            ),
            (
                "garbage-hex",
                b"my-secret".as_slice(),
                b"hello".as_slice(),
                "zzznothex!!",
                SigEncoding::Hex,
                None,
                false,
            ),
            (
                "short-hex",
                b"my-secret".as_slice(),
                b"hello".as_slice(),
                "deadbeef",
                SigEncoding::Hex,
                None,
                false,
            ),
            (
                "valid-base64",
                b"s3cr3t".as_slice(),
                b"payload".as_slice(),
                base64.as_str(),
                SigEncoding::Base64,
                None,
                true,
            ),
            (
                "tampered-base64-body",
                b"s3cr3t".as_slice(),
                b"different".as_slice(),
                base64.as_str(),
                SigEncoding::Base64,
                None,
                false,
            ),
            (
                "prefixed-hex",
                b"key".as_slice(),
                b"data".as_slice(),
                prefixed.as_str(),
                SigEncoding::Hex,
                Some("sha256="),
                true,
            ),
            (
                "missing-prefix",
                b"key".as_slice(),
                b"data".as_slice(),
                missing_prefix.as_str(),
                SigEncoding::Hex,
                Some("sha256="),
                false,
            ),
        ] {
            assert2::assert!(
                verify_signature(secret, body, signature, &encoding, prefix) == expected
            );
        }
    }

    // -----------------------------------------------------------------------
    // WebhooksFile::compile tests
    // -----------------------------------------------------------------------

    #[test]
    fn compile_full_endpoint() {
        let toml = r#"
[[endpoints]]
name = "github"
target_topic = "events"
secret = "s3cr3t"
signature_header = "X-Hub-Signature-256"
signature_prefix = "sha256="
idempotency_source = "json:$.id"
key_source = "header:X-Delivery"
"#;
        let file: WebhooksFile = toml::from_str(toml).expect("parse");
        let compiled = file.compile().expect("compile");
        let ep = compiled.get("github").expect("key present");

        assert2::assert!(ep.target_topic.as_str() == "events");
        assert2::assert!(ep.principal.as_str() == "webhook:github");
        assert2::assert!(ep.secret.as_deref() == Some(b"s3cr3t".as_slice()));
        assert2::assert!(ep.signature_header.as_deref() == Some("X-Hub-Signature-256"));
        assert2::assert!(ep.signature_prefix.as_deref() == Some("sha256="));
        assert2::assert!(ep.timestamp_header.as_deref() == None);
        assert2::assert!(ep.timestamp_tolerance == minutes(5));
        assert2::assert!(ep.idempotency_source.is_some());
        assert2::assert!(ep.key_source.is_some());
        assert2::assert!(ep.max_body == mebibytes(1));
        assert2::assert!(ep.schema_subject.as_deref() == None);
        assert2::assert!(ep.schema_format == SchemaFormat::Json);
    }

    #[test]
    fn compile_explicit_principal() {
        let toml = r#"
[[endpoints]]
name = "stripe"
target_topic = "payments"
principal = "svc:stripe-ingest"
"#;
        let file: WebhooksFile = toml::from_str(toml).expect("parse");
        let compiled = file.compile().expect("compile");
        let ep = &compiled["stripe"];
        assert2::assert!(ep.target_topic.as_str() == "payments");
        assert2::assert!(ep.principal.as_str() == "svc:stripe-ingest");
        assert2::assert!(ep.secret.as_deref() == None);
        assert2::assert!(ep.signature_header.as_deref() == None);
        assert2::assert!(ep.signature_prefix.as_deref() == None);
        assert2::assert!(ep.timestamp_header.as_deref() == None);
        assert2::assert!(ep.timestamp_tolerance == minutes(5));
        assert2::assert!(ep.idempotency_source.is_none());
        assert2::assert!(ep.key_source.is_none());
        assert2::assert!(ep.max_body == mebibytes(1));
        assert2::assert!(ep.schema_subject.as_deref() == None);
        assert2::assert!(ep.schema_format == SchemaFormat::Json);
    }

    #[test]
    fn compile_standard_webhooks_decodes_key_and_uses_signed_message_id() {
        for prefix in ["", "whsec_"] {
            let input = format!(
                r#"
[[endpoints]]
name = "standard"
target_topic = "events"
signature_mode = "standard_webhooks"
secret = "{prefix}c3RhbmRhcmRfd2ViaG9va3NfdGVzdF9zZWNyZXRfMzIh"
key_source = "json:$.object_attributes.id"
forward_headers = ["X-Gitlab-Event", "webhook-id"]
"#
            );
            let file: WebhooksFile = toml::from_str(&input).expect("parse");
            let compiled = file.compile().expect("compile");
            let ep = &compiled["standard"];
            assert2::assert!(ep.signature_mode == Some(SignatureMode::StandardWebhooks));
            assert2::assert!(
                ep.secret.as_deref() == Some(b"standard_webhooks_test_secret_32!".as_slice())
            );
            assert2::assert!(
                matches!(ep.idempotency_source.as_ref(), Some(Source::Header(name)) if name == "webhook-id")
            );
            let body = serde_json::json!({"object_attributes": {"id": 17}});
            assert2::assert!(
                extract_source(
                    ep.key_source.as_ref().expect("key source"),
                    &axum::http::HeaderMap::new(),
                    Some(&body)
                ) == Some("17".to_string())
            );
            assert2::assert!(
                ep.forward_headers
                    .iter()
                    .map(axum::http::HeaderName::as_str)
                    .collect::<Vec<_>>()
                    == vec!["x-gitlab-event", "webhook-id"]
            );
        }
    }

    #[test]
    fn compile_standard_webhooks_rejects_invalid_or_conflicting_settings() {
        let base = r#"
[[endpoints]]
name = "standard"
target_topic = "events"
signature_mode = "standard_webhooks"
"#;
        for settings in [
            "",
            "secret = 'not-base64!'",
            "secret = 'whsec_'",
            "secret = 'a2V5'\nsignature_header = 'X-Sig'",
            "secret = 'a2V5'\nsignature_encoding = 'base64'",
            "secret = 'a2V5'\nsignature_prefix = 'v1,'",
            "secret = 'a2V5'\ntimestamp_header = 'webhook-timestamp'",
            "secret = 'a2V5'\nidempotency_source = 'json:$.id'",
            "secret = 'a2V5'\nforward_headers = ['invalid name']",
        ] {
            let file: WebhooksFile = toml::from_str(&format!("{base}{settings}")).expect("parse");
            assert2::assert!(file.compile().is_err());
        }
    }

    #[test]
    fn compile_rejects_forwarded_authentication_headers() {
        for header in [
            "Authorization",
            "Cookie",
            "X-Gitlab-Token",
            "Webhook-Signature",
            "X-Custom-Signature",
        ] {
            let input = format!(
                r#"
[[endpoints]]
name = "custom"
target_topic = "events"
secret = "key"
signature_header = "x-custom-signature"
forward_headers = ["{header}"]
"#
            );
            let file: WebhooksFile = toml::from_str(&input).expect("parse");
            assert2::assert!(file.compile().is_err());
        }
    }

    #[test]
    fn compile_error_cases() {
        let secret_without_header = r#"
[[endpoints]]
name = "bad"
target_topic = "t"
secret = "oops"
"#;
        let header_without_secret = r#"
[[endpoints]]
name = "bad"
target_topic = "t"
signature_header = "X-Sig"
"#;
        let invalid_jsonpath = r#"
[[endpoints]]
name = "bad"
target_topic = "t"
idempotency_source = "json:@.unterminated["
"#;
        let bad_encoding = r#"
[[endpoints]]
name = "bad"
target_topic = "t"
secret = "s"
signature_header = "X-Sig"
signature_encoding = "md5"
"#;
        let negative_timestamp_tolerance = r#"
[[endpoints]]
name = "bad"
target_topic = "t"
timestamp_tolerance = "-1s"
"#;
        let zero_body_limit = r#"
[[endpoints]]
name = "bad"
target_topic = "t"
max_body = "0B"
"#;
        for (_name, input, needle) in [
            (
                "secret_without_header",
                secret_without_header,
                "signature_header",
            ),
            (
                "header_without_secret",
                header_without_secret,
                "signature_header",
            ),
            ("invalid_jsonpath", invalid_jsonpath, "JSONPath"),
            ("bad_encoding", bad_encoding, "signature_encoding"),
            (
                "negative_timestamp_tolerance",
                negative_timestamp_tolerance,
                "timestamp_tolerance",
            ),
            ("zero_body_limit", zero_body_limit, "max_body"),
        ] {
            let file: WebhooksFile = toml::from_str(input).expect("parse");
            let error = file.compile().expect_err("case must fail");
            assert2::assert!(error.contains(needle));
        }
    }

    #[test]
    fn compile_empty_file() {
        let file: WebhooksFile = toml::from_str("").expect("parse");
        let compiled = file.compile().expect("compile");
        assert2::assert!(compiled.is_empty());
    }

    // -----------------------------------------------------------------------
    // Dimensioned config encoding
    // -----------------------------------------------------------------------

    #[test]
    fn dimensioned_settings_are_read_in_their_human_form() {
        let toml = r#"
[[endpoints]]
name = "sized"
target_topic = "t"
timestamp_tolerance = "90s"
max_body = "512KiB"
"#;
        let file: WebhooksFile = toml::from_str(toml).expect("parse");
        let compiled = file.compile().expect("compile");
        let ep = &compiled["sized"];
        check!(ep.timestamp_tolerance == secs(90));
        check!(ep.max_body == kibibytes(512));
    }

    /// A dimensioned setting must carry its unit: `300` is neither seconds nor
    /// milliseconds until it says so, and `1048576` is not self-evidently bytes.
    #[test]
    fn unitless_dimensioned_settings_are_rejected() {
        for (_name, field, value) in [
            ("tolerance", "timestamp_tolerance", "300"),
            ("body limit", "max_body", "1048576"),
        ] {
            let input = format!(
                r#"
[[endpoints]]
name = "unitless"
target_topic = "t"
{field} = {value}
"#
            );
            check!(toml::from_str::<WebhooksFile>(&input).is_err());
        }
    }

    #[test]
    fn endpoint_round_trips_through_its_serde_encoding() {
        let endpoint = WebhookEndpoint {
            name: "github".to_string(),
            target_topic: "events".to_string(),
            signature_mode: None,
            principal: None,
            secret: None,
            signature_header: None,
            signature_encoding: None,
            signature_prefix: None,
            timestamp_header: Some("X-Ts".to_string()),
            timestamp_tolerance: Some(minutes(5)),
            idempotency_source: None,
            key_source: None,
            forward_headers: None,
            max_body: Some(mebibytes(3)),
            schema_subject: None,
            schema_format: None,
        };

        let encoded = serde_json::to_string(&endpoint).expect("serialize");
        let decoded: WebhookEndpoint = serde_json::from_str(&encoded).expect("deserialize");

        check!(encoded.contains(r#""timestamp_tolerance":"5m""#));
        check!(encoded.contains(r#""max_body":"3MiB""#));
        check!(decoded == endpoint);
    }

    // -----------------------------------------------------------------------
    // extract_source tests
    // -----------------------------------------------------------------------

    #[test]
    fn extract_header_source_cases() {
        use axum::http::HeaderMap;

        for (_name, source_name, header, expected) in [
            (
                "present",
                "x-delivery",
                Some(("x-delivery", "abc-123")),
                Some("abc-123"),
            ),
            ("missing", "x-missing", None, None),
        ] {
            let mut headers = HeaderMap::new();
            if let Some((header_name, value)) = header {
                headers.insert(header_name, value.parse().unwrap());
            }
            let source = Source::Header(source_name.to_string());
            assert2::assert!(
                extract_source(&source, &headers, None) == expected.map(str::to_string)
            );
        }
    }

    #[test]
    fn extract_jsonpath_source_cases() {
        use axum::http::HeaderMap;
        use jsonpath_rust::parser::parse_json_path;

        let headers = HeaderMap::new();
        for (_name, query, body, expected) in [
            (
                "match",
                "$.id",
                Some(serde_json::json!({"id": "event-42", "type": "push"})),
                Some("event-42"),
            ),
            (
                "numeric",
                "$.id",
                Some(serde_json::json!({"id": 17})),
                Some("17"),
            ),
            (
                "negative",
                "$.id",
                Some(serde_json::json!({"id": -17})),
                Some("-17"),
            ),
            ("empty", "$.id", Some(serde_json::json!({"id": ""})), None),
            (
                "boolean",
                "$.id",
                Some(serde_json::json!({"id": true})),
                None,
            ),
            (
                "object",
                "$.id",
                Some(serde_json::json!({"id": {"nested": 17}})),
                None,
            ),
            (
                "no_match",
                "$.missing_field",
                Some(serde_json::json!({"id": "event-42"})),
                None,
            ),
            ("no_body", "$.id", None, None),
        ] {
            let source = Source::JsonPath(parse_json_path(query).expect("compile"));
            assert2::assert!(
                extract_source(&source, &headers, body.as_ref()) == expected.map(str::to_string)
            );
        }
    }
}
