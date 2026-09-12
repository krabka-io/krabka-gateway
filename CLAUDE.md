# Krabka gateway — project-specific guidance

## Compatibility

**Krabka is greenfield and undeployed.** There are no production users, no persisted state to migrate, and no clients pinned to a specific build. Do not write backwards-compatibility shims:

- No `#[serde(default)]` on metadata fields "to keep old raft logs readable"
- No `V2` enum variants that stay alongside `V1` to support replay
- No feature flags that gate new behavior behind a default-off switch
- No migration code or one-shot upgraders for on-disk format changes
- No deprecated-but-kept API surfaces

When a schema, enum, wire format, or interface changes, change it. Delete local raft logs and data directories during development if necessary.

**Kafka compatibility is the constraint that matters.** Always keep:

- Apache Kafka wire-protocol byte exactness for request and response shapes, field order, error codes, and version negotiation
- KIP semantics for the feature that you implement
- Behavior that the JVM admin tools rely on, such as `kafka-topics`, `kafka-acls`, `kafka-leader-election`, and `kafka-reassign-partitions`

When in doubt, match Kafka. If Kafka's behavior is undocumented or version-dependent, check the behavior of the latest released cp-kafka image. Do not rely on the wiki.

## One contract, six implementations

`crates/gateway/proto/krabka/gateway/v1/gateway.proto` is the single definition of the gateway service. Six things read it:

- `crates/gateway/build.rs`, through `connectrpc-axum-build` and a vendored `protoc`
- `crates/app-sdk/build.rs`, through `protox`
- `sdks/cpp/gen`, `sdks/go/gen`, `sdks/java/gen` and `sdks/ts/gen`, through `buf generate`

The four `gen/` trees are checked in, and CI regenerates them and compares byte for byte. So when you change the `.proto`, run `buf generate` in the same commit:

```bash
go run github.com/bufbuild/buf/cmd/buf@v1.54.0 generate
```

Never hand-edit a file under `sdks/*/gen`.

A change to the contract is a change to five SDKs. Add a conformance vector under `crates/sdk-conformance/vectors/v1` for the new behavior, then make every adapter pass it. A vector that only the Rust adapter satisfies is a gap, not a pass.

## Code & Documentation Style

Follow the style guides in [`docs/style_guides/`](docs/style_guides/README.md): [code](docs/style_guides/code_style_guide.md), [rustdoc](docs/style_guides/rustdoc_style_guide.md), [README](docs/style_guides/readme_style_guide.md), [design docs](docs/style_guides/design_doc_style_guide.md), and [coverage reports](docs/style_guides/coverage_report_style_guide.md). The guides record Krabka's conventions. Examples are the pinned stable toolchain, `cargo +nightly fmt`, forbidden `unsafe`, and `clippy::pedantic`. The guides also cover workspace lints and dependencies, `krabka-<name>` crates, thiserror error enums, tokio, and `assert2`/`nextest`/mutation testing.

Do not make style-only sweeps across untouched files. Bring a file into line with the guides only when you already edit it. Keep the tidy-up proportionate to the change.

### Assertions and Clippy

- Never add `#[allow(clippy::...)]` or any equivalent Clippy suppression. Fix every Clippy warning in the code, regardless of the effort required.
- Never use Rust's plain `assert!`, `assert_eq!`, or `assert_ne!` macros. Use the `assert2` crate's `assert!` macro instead. Use it also for equality and inequality comparisons.

## Sibling repositories

The crates this repository depends on live in other `krabka-io` repositories. Each is declared against crates.io in the member manifest and pinned by revision in the one `[patch.crates-io]` table in `Cargo.toml`. Bump a revision there and nowhere else, then regenerate `Cargo.lock` in the same commit. `.github/workflows/sync-siblings.yml` proposes those bumps as pull requests.

## Execution

When you execute an implementation plan, always use **subagent-driven development in parallel batches** where the per-task file sets do not overlap. The plan groups tasks into batches. Dispatch all tasks in a batch concurrently, in one message with multiple Agent calls. Then wait for the batch to complete, review it, and move to the next batch.

Sequential dispatch of one task at a time wastes wall-clock time. Use sequential dispatch only when later tasks depend on earlier ones in the same batch.

A "conflict" between parallel implementers occurs only when both edit the same file. Tasks such as "add a webhook config field" in `webhook_config.rs` and "add a metric" in `metrics.rs` do not conflict, and you should run them together. When in doubt, list the file set that each task touches before you decide.

**Never discard working-tree state while parallel implementers run.** `git checkout -- <path>`, `git restore`, `git stash`, and `git clean` all destroy *every* uncommitted change in the files they touch, not only yours. In a shared worktree, those files usually hold the unfinished work of another agent. To undo your own edit, reverse it directly. Re-edit the region, or apply a reverse patch of your own diff. This has already destroyed the uncommitted work of one agent.

Tests must exercise behavior, not source text. Do not read source files in tests and assert against their contents. `include_str!` and `fs::read_to_string` are examples of such reads. If a behavior is hard to test, add a narrow helper or seam. Then test that behavior directly.

When you check generated protocol records or other structured values in tests, compare the whole expected struct. This is better than long chains of field-by-field assertions. Use table-driven or parameterized tests for repeated scenarios that differ only by inputs, protocol version, or expected request shape.

## Release Process

Krabka uses **release-plz** for automated semantic versioning. Conventional commits drive the version bumps:

- `feat:` gives a minor version bump
- `fix:` gives a patch version bump
- `feat!:` gives a major version bump

release-plz also generates the changelogs and publishes the crates to crates.io.
