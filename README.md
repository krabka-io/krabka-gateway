# krabka-gateway

The Krabka gateway: a gRPC / Connect-RPC and HTTP front end to Kafka topics,
the four application SDKs that speak to it, and the conformance harness that
holds them to one contract.

A Kafka client speaks the Kafka wire protocol over a long-lived TCP connection.
An application in a browser, a serverless function or a short-lived job often
cannot. The gateway gives those callers Connect-RPC and plain HTTP instead: send
a record, subscribe to a topic, acquire and acknowledge queue messages, receive
CloudEvents over a webhook. It runs in front of a Krabka cluster and speaks
Kafka to the brokers on the application's behalf.

It layers on four sibling repositories:
[`krabka-protocol`](https://github.com/krabka-io/krabka-protocol) for ids, units
and SASL, [`krabka-client-rs`](https://github.com/krabka-io/krabka-client-rs) for
the Kafka clients the records travel over,
[`krabka-broker`](https://github.com/krabka-io/krabka-broker) for authorization,
telemetry and the broker its integration suites boot, and
[`krabka-streams-rs`](https://github.com/krabka-io/krabka-streams-rs) for the
Arrow record batches the subscription filter reads.

## Crates

| Crate | What it is |
| --- | --- |
| `krabka-gateway` | The gateway server: Connect-RPC and HTTP handlers, dedup, webhooks, TLS, authorization and the Schema Registry codecs |
| `krabka-app-sdk` | The Rust application SDK, and the Rust conformance adapter |
| `krabka-sdk-conformance` | The vectors and the runner that replay one contract against every SDK |

## SDKs

| SDK | Path | Toolchain |
| --- | --- | --- |
| C++ | [`sdks/cpp`](sdks/cpp) | CMake, nghttp2, protobuf |
| Go | [`sdks/go`](sdks/go) | Go modules, Connect |
| Java | [`sdks/java`](sdks/java) | Gradle, Kotlin, connect-kotlin |
| TypeScript | [`sdks/ts`](sdks/ts) | npm, `@bufbuild/protobuf` |

Each SDK ships a `conformance-adapter` executable. The runner drives that
executable over JSON on stdio, so one set of vectors gates all five SDKs.

## The contract

One `.proto` defines the gateway service:
[`crates/gateway/proto/krabka/gateway/v1/gateway.proto`](crates/gateway/proto/krabka/gateway/v1/gateway.proto).

The Rust server compiles it in `build.rs`. The other four SDKs read the
checked-in stubs under `sdks/<lang>/gen`, produced by `buf` from the same file:

```bash
go run github.com/bufbuild/buf/cmd/buf@v1.54.0 generate
git diff --exit-code -- sdks
```

CI runs exactly those two commands, so a change to the `.proto` that is not
regenerated fails the build rather than leaving the SDKs behind the server.

## Build

```bash
cargo test --workspace
```

Bazel targets for the Rust crates are checked in and read the same `Cargo.toml`
and `Cargo.lock` through `crate.from_cargo`, so there is no second dependency
set to keep in sync. They are not yet a gate: they were written during the
extraction, without a Bazel run to check them against. Make `bazel test //...`
the gate once one run has proved it. The SDKs are built by their own
toolchains, one workflow each.

## Sibling revisions

Sibling crates are declared against crates.io in the member manifests and pinned
by revision in one `[patch.crates-io]` table in [`Cargo.toml`](Cargo.toml). That
table is the only place a revision is bumped, and
[`sync-siblings`](.github/workflows/sync-siblings.yml) proposes those bumps as
pull requests.

Every crate each sibling publishes is listed there, not only the ones this
repository names directly. Cargo ignores a dependency's own patch table, so a
crate reached transitively would otherwise resolve from the registry while its
git twin is also in the graph.

## Conformance

```bash
cargo build -p krabka-app-sdk --bin conformance_adapter --features conformance-adapter
cargo run -p krabka-sdk-conformance --bin conformance -- \
  --adapter target/debug/conformance_adapter \
  --vectors crates/sdk-conformance/vectors/v1
```

`--live-substrate --live-compatible-only` runs the same vectors against a live
in-process gateway instead of the mock, and skips the vectors that describe
mock-only behaviour.

## Suites that need a daemon

`tests/jvm_differential.rs` starts a `cp-kafka` container and drives the JVM
consumer against the gateway, so it needs a Docker daemon. It is `#[ignore]`d
and runs on its own step in CI:

```bash
cargo nextest run -p krabka-gateway --test jvm_differential --run-ignored only
```

## CloudEvents demo

[`demo/cloudevents`](demo/cloudevents) posts a binary and a structured
CloudEvent into the gateway and waits for both egress shapes to arrive at a
local capture server. The in-process regression for the same paths is
`cargo test -p krabka-gateway --test cloudevents_roundtrip`.

## Container image

[`packaging/apko/krabka-gateway.yaml`](packaging/apko/krabka-gateway.yaml)
builds the distroless image around the `krabka-gateway` binary.

## Kubernetes

The `KafkaGrpcGateway` custom resource that runs this server on Kubernetes lives
in [`krabka-operator`](https://github.com/krabka-io/krabka-operator), with the
other nine Krabka CRDs.

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](NOTICE).
