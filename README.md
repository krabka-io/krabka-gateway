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
bazel build //...
bazel test //...
```

Bazel builds and tests the Rust crates, and CI gates on `bazel test //...`.
The Bazel targets read the same `Cargo.toml` and `Cargo.lock` through
`crate.from_cargo`, so there is no second dependency set to keep in sync.
`bazel test //...` also runs clippy on every crate through the `*_clippy`
targets. Cargo builds the same workspace:

```bash
cargo test --workspace
```

The SDKs are built by their own toolchains, one workflow each.

## Sibling revisions

Sibling crates are declared against crates.io in the member manifests and pinned
by revision in one `[patch.crates-io]` table in [`Cargo.toml`](Cargo.toml). That
table is the only place a revision is bumped.

Every crate each sibling publishes is listed there, not only the ones this
repository names directly. Cargo ignores a dependency's own patch table, so a
crate reached transitively would otherwise resolve from the registry while its
git twin is also in the graph.

## Conformance

```bash
bazel build //crates/sdk-conformance:conformance //crates/app-sdk:conformance_adapter
"$(bazel cquery --output=files //crates/sdk-conformance:conformance)" \
  --adapter "$(bazel cquery --output=files //crates/app-sdk:conformance_adapter)" \
  --vectors crates/sdk-conformance/vectors/v1
```

`--live-substrate --live-compatible-only` runs the same vectors against a live
in-process gateway instead of the mock, and skips the vectors that describe
mock-only behaviour. With Cargo, build the adapter with
`cargo build -p krabka-app-sdk --bin conformance_adapter --features conformance-adapter`.

## Suites that need a daemon

`tests/jvm_differential.rs` starts a `cp-kafka` container and drives the JVM
consumer against the gateway, so it needs a Docker daemon. It is `#[ignore]`d
under Cargo and `manual` under Bazel, and it runs in its own CI job:

```bash
bazel test //crates/gateway:jvm_differential_test --test_arg=--ignored
```

## CloudEvents demo

[`demo/cloudevents`](demo/cloudevents) posts a binary and a structured
CloudEvent into the gateway and waits for both egress shapes to arrive at a
local capture server. The in-process regression for the same paths is
`cargo test -p krabka-gateway --test cloudevents_roundtrip`.

## Container image

The gateway image is `ghcr.io/krabka-io/krabka-gateway`. Bazel builds it in
[`packaging`](packaging/BUILD.bazel): apko makes a locked Wolfi base, and
`rules_img` adds the Bazel-built `krabka-gateway` binary. The image runs as the
non-root user 65532 and has no shell.

```bash
bazel run -c opt //packaging:image_load
docker run --rm ghcr.io/krabka-io/krabka-gateway:dev --help
```

Each push to `main` publishes the image with the commit SHA as its tag. A `v*`
tag promotes that image to the version tag, and to `latest` when it is the
newest release.

## Kubernetes

The `KafkaGrpcGateway` custom resource that runs this server on Kubernetes lives
in [`krabka-operator`](https://github.com/krabka-io/krabka-operator), with the
other nine Krabka CRDs.

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](NOTICE).
