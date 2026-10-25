//! Confluent binary framing helpers.
//!
//! All Confluent-framed values share a 5-byte header:
//!
//! ```text
//! +--------+--------------------------------+
//! | 0x00   |  magic byte                    |
//! | i32 BE |  schema id                     |
//! +--------+--------------------------------+
//! ```
//!
//! A Protobuf value also carries a variable-length message-index prefix
//! immediately after the 5-byte header. See [`strip_proto_index`] and
//! [`prepend_proto_index`].

use bytes::{BufMut, Bytes, BytesMut};

use crate::codec::{CodecError, SchemaFormat};

/// Encode a payload with the Confluent 5-byte magic and id framing header.
///
/// For `Protobuf`, a single-zero message-index byte precedes the payload.
/// That byte selects the first, top-level message
/// in the schema. For `Avro` and `Json`, this function writes the payload as it
/// is.
#[must_use]
pub fn encode_frame(id: i32, fmt: SchemaFormat, payload: &[u8]) -> Bytes {
    let protobuf = fmt == SchemaFormat::Protobuf;
    let mut out = BytesMut::with_capacity(5 + usize::from(protobuf) + payload.len());
    out.put_u8(0x00);
    out.put_i32(id);
    if protobuf {
        out.put_u8(0x00);
    }
    out.put_slice(payload);
    out.freeze()
}

/// Decode a Confluent-framed value.
///
/// Returns `(schema_id, payload)`, where `payload` is the bytes *after* the
/// 5-byte header. For Protobuf, the caller must strip the message-index prefix
/// from the returned payload with [`strip_proto_index`].
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub fn decode_frame(bytes: &[u8]) -> Result<(i32, Vec<u8>), CodecError> {
    let id = frame_schema_id(bytes)?;
    Ok((id, bytes[5..].to_vec()))
}

/// Decode a frame while retaining shared ownership of its payload bytes.
///
/// # Errors
/// Rejects a short header or a magic byte other than zero.
pub fn decode_frame_bytes(bytes: &Bytes) -> Result<(i32, Bytes), CodecError> {
    let id = frame_schema_id(bytes)?;
    Ok((id, bytes.slice(5..)))
}

fn frame_schema_id(bytes: &[u8]) -> Result<i32, CodecError> {
    if bytes.len() < 5 {
        return Err(CodecError::Framing(format!(
            "frame too short: {} bytes (need at least 5)",
            bytes.len()
        )));
    }
    if bytes[0] != 0x00 {
        return Err(CodecError::Framing(format!(
            "bad magic byte: 0x{:02x} (expected 0x00)",
            bytes[0]
        )));
    }
    Ok(i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]))
}

/// Strip the variable-length Protobuf message-index LEB128 prefix from a
/// post-header payload and return the raw proto bytes.
///
/// The encoding is:
/// - A leading varint `count` giving the number of message-index entries.
/// - If `count == 0`, the common single-first-message optimization, the
///   remainder after that one byte is the proto payload.
/// - Otherwise, `count` more varints follow, then the proto payload.
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub fn strip_proto_index(payload: &[u8]) -> Result<Vec<u8>, CodecError> {
    Ok(payload[proto_payload_start(payload)?..].to_vec())
}

/// Strip the message-index prefix without copying the owned payload.
///
/// # Errors
/// Rejects truncated or overflowing message-index varints.
pub fn strip_proto_index_bytes(payload: &Bytes) -> Result<Bytes, CodecError> {
    let start = proto_payload_start(payload)?;
    Ok(payload.slice(start..))
}

fn proto_payload_start(payload: &[u8]) -> Result<usize, CodecError> {
    let (count, mut pos) = read_varint(payload, 0)?;
    if count == 0 {
        // length-0 optimization: no index entries, rest is message
        return Ok(pos);
    }
    // skip `count` index varints
    for _ in 0..count {
        let (_, next) = read_varint(payload, pos)?;
        pos = next;
    }
    Ok(pos)
}

/// Prepend the Protobuf message-index prefix to `payload`. The prefix is a
/// single `[0x00]` for the top-level, first message in the schema.
#[must_use]
pub fn prepend_proto_index(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + payload.len());
    out.push(0x00); // count = 0 => first-message optimization
    out.extend_from_slice(payload);
    out
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Read a single unsigned LEB128 varint from `buf` starting at `pos`.
/// Returns `(value, new_pos)`.
fn read_varint(buf: &[u8], mut pos: usize) -> Result<(u64, usize), CodecError> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        if pos >= buf.len() {
            return Err(CodecError::Framing(
                "truncated varint in message-index prefix".to_string(),
            ));
        }
        let byte = buf[pos];
        pos += 1;
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, pos));
        }
        shift += 7;
        if shift >= 64 {
            return Err(CodecError::Framing(
                "varint overflow in message-index prefix".to_string(),
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::SchemaFormat;

    #[test]
    fn framed_round_trip_cases() {
        for (_name, id, format, payload) in [
            ("avro", 42_i32, SchemaFormat::Avro, b"hi".as_slice()),
            ("json", 99_i32, SchemaFormat::Json, b"hi".as_slice()),
        ] {
            let framed = encode_frame(id, format, payload);
            assert2::assert!(decode_frame(&framed).unwrap() == (id, payload.to_vec()));
        }
    }

    #[test]
    fn protobuf_encode_layout() {
        // encode_frame(7, Protobuf, b"msg") must start with 00 00 00 00 07 00
        let framed = encode_frame(7, SchemaFormat::Protobuf, b"msg");
        assert2::assert!(
            framed.as_ref() == &[0x00, 0x00, 0x00, 0x00, 0x07, 0x00, b'm', b's', b'g']
        );
    }

    #[test]
    fn protobuf_round_trip() {
        let id = 7_i32;
        let payload = b"msg";
        let framed = encode_frame(id, SchemaFormat::Protobuf, payload);
        let (got_id, remainder) = decode_frame(&framed).unwrap();
        let stripped = strip_proto_index(&remainder).unwrap();
        assert2::assert!(got_id == id);
        assert2::assert!(stripped == payload.to_vec());
    }

    #[test]
    fn owned_frame_decode_shares_payload_storage_and_preserves_golden_bytes() {
        for format in [
            SchemaFormat::Avro,
            SchemaFormat::Json,
            SchemaFormat::Protobuf,
        ] {
            for payload in [b"".as_slice(), b"message".as_slice()] {
                let frame = encode_frame(7, format, payload);
                let mut expected = vec![0, 0, 0, 0, 7];
                if format == SchemaFormat::Protobuf {
                    expected.push(0);
                }
                expected.extend_from_slice(payload);
                assert2::assert!(frame.as_ref() == expected);
                let (id, remainder) = decode_frame_bytes(&frame).unwrap();
                assert2::assert!(id == 7);
                assert2::assert!(remainder.as_ref() == &frame[5..]);
                if !remainder.is_empty() {
                    assert2::assert!(remainder.as_ptr() == frame[5..].as_ptr());
                }
                let decoded = if format == SchemaFormat::Protobuf {
                    strip_proto_index_bytes(&remainder).unwrap()
                } else {
                    remainder
                };
                assert2::assert!(decoded.as_ref() == payload);
                if !payload.is_empty() {
                    assert2::assert!(
                        decoded.as_ptr() == frame[frame.len() - payload.len()..].as_ptr()
                    );
                }
            }
        }
    }

    #[test]
    fn owned_proto_decode_retains_varint_acceptance_and_errors() {
        for (prefix, payload_start) in [
            (vec![0], 1),
            (vec![2, 3, 5], 3),
            (vec![0x80, 0], 2),
            (vec![0x82, 0, 0x83, 0, 0x85, 0], 6),
        ] {
            let bytes = Bytes::from([prefix.as_slice(), b"data"].concat());
            let decoded = strip_proto_index_bytes(&bytes).unwrap();
            assert2::assert!(decoded.as_ref() == b"data");
            assert2::assert!(decoded.as_ptr() == bytes[payload_start..].as_ptr());
            assert2::assert!(decoded.as_ref() == strip_proto_index(&bytes).unwrap());
        }
        for bytes in [vec![], vec![0x80], vec![2, 3], vec![0x80; 10]] {
            let expected = strip_proto_index(&bytes).unwrap_err().to_string();
            let actual = strip_proto_index_bytes(&Bytes::from(bytes))
                .unwrap_err()
                .to_string();
            assert2::assert!(actual == expected);
        }
    }

    #[test]
    fn strip_proto_index_first_message() {
        // [0x00] prefix + payload
        let with_index = prepend_proto_index(b"hello");
        let stripped = strip_proto_index(&with_index).unwrap();
        assert2::assert!(with_index[0] == 0x00);
        assert2::assert!(stripped == b"hello".to_vec());
    }

    #[test]
    fn strip_proto_index_non_zero_count() {
        // Manually craft count=2, indexes=[3,5], then payload b"data"
        // LEB128 for small values is the byte itself (< 128)
        let mut buf = vec![2u8, 3u8, 5u8]; // count=2, index[0]=3, index[1]=5
        buf.extend_from_slice(b"data");
        let stripped = strip_proto_index(&buf).unwrap();
        assert2::assert!(stripped == b"data");
    }

    #[test]
    fn decode_frame_rejection_cases() {
        for (_name, input) in [
            ("empty", &[][..]),
            ("bad_magic", &[0x01, 0x00, 0x00, 0x00, 0x01][..]),
            ("short", &[0x00, 0x00, 0x00, 0x01][..]),
        ] {
            assert2::assert!(matches!(decode_frame(input), Err(CodecError::Framing(_))));
            assert2::assert!(
                decode_frame_bytes(&Bytes::copy_from_slice(input))
                    .unwrap_err()
                    .to_string()
                    == decode_frame(input).unwrap_err().to_string()
            );
        }
    }
}
