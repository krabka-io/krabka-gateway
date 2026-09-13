//! Generates Connect-RPC server stubs and prost message types from the
//! `.proto`.
//!
//! The Connect generator, connectrpc-axum-build, always invokes a `protoc`
//! binary, so `prost-build`'s `protoc_executable` supplies one. [`protoc_path`]
//! selects it. Neither build needs a system `protoc` or a network fetch.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/krabka/gateway/v1/gateway.proto";
    let protoc_path = protoc_path()?;
    connectrpc_axum_build::compile_protos(&[proto], &["proto"])
        .with_prost_config(move |config| {
            config.protoc_executable(protoc_path.clone());
        })
        .compile()?;
    normalize_generated_docs_and_builder()?;
    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}

/// The `protoc` that this build runs.
///
/// Bazel supplies a hermetic `protoc` through `PROTOC`, and that one wins. The
/// `protoc-bin-vendored` crates find their binary through
/// `env!("CARGO_MANIFEST_DIR")`, and that path does not exist in a Bazel
/// sandbox. Cargo sets no `PROTOC`, so a Cargo build uses the vendored binary.
fn protoc_path() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    if let Some(from_toolchain) = std::env::var_os("PROTOC") {
        return Ok(std::path::PathBuf::from(from_toolchain));
    }
    #[cfg(feature = "vendored-protoc")]
    {
        Ok(protoc_bin_vendored::protoc_bin_path()?)
    }
    #[cfg(not(feature = "vendored-protoc"))]
    {
        Err("no PROTOC in the environment and the vendored-protoc feature is off".into())
    }
}

fn normalize_generated_docs_and_builder() -> Result<(), Box<dyn std::error::Error>> {
    let generated =
        std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("krabka.gateway.v1.rs");
    let source = std::fs::read_to_string(&generated)?;
    let source = source
        .replace("ProtoBuf", "`Protobuf`")
        .replace("a JSONPath expression", "a `JSONPath` expression")
        .replace("via TopicNameStrategy", "via `TopicNameStrategy`")
        .replace("under\n    /// RawCodec", "under\n    /// `RawCodec`")
        .replace(
            "pub struct GatewayServiceBuilder",
            "#[must_use]\npub struct GatewayServiceBuilder",
        )
        .replace(
            "    pub fn as_str_name",
            "    #[must_use]\n    pub fn as_str_name",
        )
        .replace(
            "    pub fn from_str_name",
            "    #[must_use]\n    pub fn from_str_name",
        )
        .replace("&FIELDS)", "FIELDS)")
        .replace(
            "write!(formatter, \"expected one of: {:?}\", FIELDS)",
            "write!(formatter, \"expected one of: {FIELDS:?}\")",
        )
        .replace("[`build()`]", "`build()`");
    std::fs::write(generated, source)?;
    Ok(())
}
