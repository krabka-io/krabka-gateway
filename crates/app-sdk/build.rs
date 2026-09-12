fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "../gateway/proto/krabka/gateway/v1/gateway.proto";
    let include = "../gateway/proto";
    let fds = protox::compile([proto], [include])?;
    prost_build::Config::new().compile_fds(fds)?;
    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}
