//! Per-format serialize / deserialize / validate dispatch.
//!
//! [`serialize`], [`deserialize`], and [`validate`] are the three operations
//! the gateway codec needs. Each one dispatches to the correct sub-module for
//! the given [`SchemaFormat`].

pub mod avro;
pub mod json;
pub mod protobuf;

use bytes::Bytes;

use crate::codec::{CodecError, SchemaFormat};

/// Serialize the JSON-encoded value `json` into the wire format for `fmt`,
/// with `schema` as the schema descriptor.
///
/// Returns the serialized bytes ready for Confluent framing.
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub fn serialize(fmt: SchemaFormat, schema: &str, json: &[u8]) -> Result<Bytes, CodecError> {
    match fmt {
        SchemaFormat::Avro => avro::serialize(schema, json),
        SchemaFormat::Json => json::serialize(schema, json),
        SchemaFormat::Protobuf => protobuf::serialize(schema, json),
    }
}

/// Deserialize the wire payload `payload` in `fmt` back to a JSON-encoded
/// value, with `schema` as the schema descriptor.
///
/// Returns the JSON bytes.
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub fn deserialize(fmt: SchemaFormat, schema: &str, payload: &[u8]) -> Result<Bytes, CodecError> {
    match fmt {
        SchemaFormat::Avro => avro::deserialize(schema, payload),
        SchemaFormat::Json => json::deserialize(schema, payload),
        SchemaFormat::Protobuf => protobuf::deserialize(schema, payload),
    }
}

/// Validate that `json` is a valid instance of `schema` in `fmt`.
///
/// Returns `Ok(())` on success, [`CodecError::Validate`] on failure.
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub fn validate(fmt: SchemaFormat, schema: &str, json: &[u8]) -> Result<(), CodecError> {
    match fmt {
        SchemaFormat::Avro => avro::validate(schema, json),
        SchemaFormat::Json => json::validate(schema, json),
        SchemaFormat::Protobuf => protobuf::validate(schema, json),
    }
}

/// Compiled, immutable schema shared by all records carrying one registry id.
pub(super) enum PreparedSchema {
    Avro(apache_avro::Schema),
    Json(jsonschema::Validator),
    Protobuf(prost_reflect::MessageDescriptor),
}

impl PreparedSchema {
    pub(super) fn new(fmt: SchemaFormat, schema: &str) -> Result<Self, CodecError> {
        match fmt {
            SchemaFormat::Avro => avro::prepare(schema).map(Self::Avro),
            SchemaFormat::Json => json::prepare(schema).map(Self::Json),
            SchemaFormat::Protobuf => protobuf::first_message_desc(schema).map(Self::Protobuf),
        }
    }

    pub(super) fn serialize(&self, json: &[u8]) -> Result<Bytes, CodecError> {
        match self {
            Self::Avro(schema) => avro::serialize_prepared(schema, json),
            Self::Json(validator) => {
                json::validate_prepared(validator, json)?;
                Ok(Bytes::copy_from_slice(json))
            }
            Self::Protobuf(schema) => protobuf::serialize_prepared(schema, json),
        }
    }

    pub(super) fn deserialize(&self, payload: &[u8]) -> Result<Bytes, CodecError> {
        match self {
            Self::Avro(schema) => avro::deserialize_prepared(schema, payload),
            Self::Json(validator) => {
                json::validate_prepared(validator, payload)?;
                Ok(Bytes::copy_from_slice(payload))
            }
            Self::Protobuf(schema) => protobuf::deserialize_prepared(schema, payload),
        }
    }
}
