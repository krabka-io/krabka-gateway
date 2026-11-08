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

## Logs and traces

The gateway writes JSON log entries to stdout. `KRABKA_OTLP_ENABLED=true`, or
an endpoint in `KRABKA_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT`, also
sends traces to an OTLP collector.

`--gcp-project-id` (`KRABKA_GCP_PROJECT_ID`) names the Google Cloud project
whose Cloud Trace holds the gateway's traces. With it set and OTLP export on,
each log entry that the gateway writes inside a traced span carries
`logging.googleapis.com/trace`, `logging.googleapis.com/spanId` and
`logging.googleapis.com/trace_sampled`, so Cloud Logging links the entry to its
trace. Unset, entries carry no trace keys. The broker reads the same variable,
so one value configures both.

## Documentation

The API documentation is on [docs.rs/krabka-gateway](https://docs.rs/krabka-gateway). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
