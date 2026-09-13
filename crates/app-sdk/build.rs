//! Generates the prost message types from the gateway contract.
//!
//! The contract lives in `crates/gateway/proto`. Cargo runs this script from
//! the crate directory, so the relative path reaches it. Bazel runs the script
//! from a different directory and sets `KRABKA_GATEWAY_PROTO_DIR` to the
//! declared proto sources (see //crates/app-sdk:BUILD.bazel).

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=KRABKA_GATEWAY_PROTO_DIR");
    let include = std::env::var_os("KRABKA_GATEWAY_PROTO_DIR")
        .map_or_else(|| PathBuf::from("../gateway/proto"), PathBuf::from);
    let proto = include.join("krabka/gateway/v1/gateway.proto");
    let fds = protox::compile([&proto], [&include])?;
    prost_build::Config::new().compile_fds(fds)?;
    println!("cargo:rerun-if-changed={}", proto.display());
    Ok(())
}
