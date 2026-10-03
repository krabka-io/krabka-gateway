//! Standard Webhooks HMAC-SHA256 verification over the original request bytes.

use axum::http::HeaderMap;
use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, KeyInit, Mac};
use krabka_units::prelude::*;
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::webhook_config::CompiledWebhook;

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?.to_str().ok()?;
    (!value.is_empty() && values.next().is_none()).then_some(value)
}

pub(crate) fn authenticate<'a>(
    cfg: &CompiledWebhook,
    headers: &'a HeaderMap,
    body: &[u8],
    now: i64,
) -> Option<(&'a str, &'a str)> {
    let id = header(headers, "webhook-id")?;
    if id.len() > 512 {
        return None;
    }
    let timestamp = header(headers, "webhook-timestamp")?;
    if !timestamp.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let epoch = timestamp.parse::<i64>().ok()?;
    let skew = (i128::from(now) - i128::from(epoch)).abs();
    if Time::from_secs(i64::try_from(skew).ok()?) > cfg.timestamp_tolerance {
        return None;
    }
    let signatures = header(headers, "webhook-signature")?;
    let mut mac = <Hmac<Sha256>>::new_from_slice(cfg.secret.as_deref()?).ok()?;
    mac.update(id.as_bytes());
    mac.update(b".");
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(body);
    let expected = mac.finalize().into_bytes();
    signatures
        .split_ascii_whitespace()
        .any(|signature| {
            signature
                .strip_prefix("v1,")
                .and_then(|signature| STANDARD.decode(signature).ok())
                .is_some_and(|signature| {
                    expected.as_slice().ct_eq(signature.as_slice()).unwrap_u8() == 1
                })
        })
        .then_some((id, timestamp))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::webhook_config::WebhooksFile;

    const BODY: &[u8] = br#"{"id":17,"object_kind":"merge_request"}"#;
    // Independent HMAC-SHA256 vector over delivery-42.1000.{original body}.
    const SIGNATURE: &str = "v1,d86VwQbuVCrsnuKB1vF8QoiYMXV8xRG5ImAf+uRwaKk=";

    fn config() -> CompiledWebhook {
        let file: WebhooksFile = toml::from_str(
            r#"
[[endpoints]]
name = "standard"
target_topic = "events"
signature_mode = "standard_webhooks"
secret = "whsec_c3RhbmRhcmRfd2ViaG9va3NfdGVzdF9zZWNyZXRfMzIh"
"#,
        )
        .expect("parse config");
        file.compile()
            .expect("compile config")
            .remove("standard")
            .expect("endpoint")
    }

    fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in [
            ("webhook-id", "delivery-42"),
            ("webhook-timestamp", "1000"),
            ("webhook-signature", SIGNATURE),
        ] {
            headers.insert(name, value.parse().expect("header"));
        }
        headers
    }

    #[test]
    fn independent_vector_and_timestamp_boundaries() {
        let cfg = config();
        assert2::assert!(
            cfg.secret.as_deref() == Some(b"standard_webhooks_test_secret_32!".as_slice())
        );
        for (now, valid) in [
            (699, false),
            (700, true),
            (1000, true),
            (1300, true),
            (1301, false),
            (i64::MIN, false),
        ] {
            assert2::assert!(authenticate(&cfg, &headers(), BODY, now).is_some() == valid);
        }
        assert2::assert!(
            authenticate(&cfg, &headers(), BODY, 1000) == Some(("delivery-42", "1000"))
        );
        assert2::assert!(
            authenticate(
                &cfg,
                &headers(),
                br#"{"id":18,"object_kind":"merge_request"}"#,
                1000
            )
            .is_none()
        );
    }

    #[test]
    fn signed_headers_reject_missing_duplicate_or_tampered_values() {
        let cfg = config();
        for name in ["webhook-id", "webhook-timestamp", "webhook-signature"] {
            let mut missing = headers();
            missing.remove(name);
            assert2::assert!(authenticate(&cfg, &missing, BODY, 1000).is_none());

            let mut duplicate = headers();
            duplicate.append(name, duplicate[name].clone());
            assert2::assert!(authenticate(&cfg, &duplicate, BODY, 1000).is_none());
        }
        for (name, value) in [
            ("webhook-id", "delivery-43"),
            ("webhook-id", ""),
            ("webhook-timestamp", "1001"),
            ("webhook-timestamp", "+1000"),
            ("webhook-timestamp", "1000.0"),
            ("webhook-timestamp", ""),
            ("webhook-timestamp", "9223372036854775808"),
            ("webhook-signature", "v1,not-base64!"),
            ("webhook-signature", "v1,AA=="),
            (
                "webhook-signature",
                "v2,d86VwQbuVCrsnuKB1vF8QoiYMXV8xRG5ImAf+uRwaKk=",
            ),
        ] {
            let mut headers = headers();
            headers.insert(name, value.parse().expect("header"));
            assert2::assert!(authenticate(&cfg, &headers, BODY, 1000).is_none());
        }
        let mut headers = headers();
        headers.insert("webhook-id", "x".repeat(513).parse().expect("header"));
        assert2::assert!(authenticate(&cfg, &headers, BODY, 1000).is_none());
    }

    #[test]
    fn rotation_accepts_any_matching_v1_signature() {
        let cfg = config();
        for signatures in [
            format!("v1,AA== {SIGNATURE}"),
            format!("{SIGNATURE} v1,AA=="),
            format!("v2,AA== v1,invalid! {SIGNATURE}"),
        ] {
            let mut headers = headers();
            headers.insert("webhook-signature", signatures.parse().expect("header"));
            assert2::assert!(authenticate(&cfg, &headers, BODY, 1000).is_some());
        }
    }
}
