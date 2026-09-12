# krabka-gateway

[![Crates.io](https://img.shields.io/crates/v/krabka-gateway.svg)](https://crates.io/crates/krabka-gateway)
[![Docs.rs](https://docs.rs/krabka-gateway/badge.svg)](https://docs.rs/krabka-gateway)
[![CI](https://github.com/krabka-io/krabka-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/krabka-io/krabka-gateway/actions/workflows/ci.yml)

gRPC / Connect-RPC + HTTP gateway into Krabka (Kafka) topics.

This crate is part of [Krabka](https://github.com/krabka-io), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add krabka-gateway
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Run the Connect-RPC gateway in front of a Krabka cluster:

```bash
KRABKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 \
KRABKA_GATEWAY_LISTEN_ADDR=127.0.0.1:9500 \
krabka-gateway

curl -f http://127.0.0.1:9500/healthz
```

## Documentation

The API documentation is on [docs.rs/krabka-gateway](https://docs.rs/krabka-gateway). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
