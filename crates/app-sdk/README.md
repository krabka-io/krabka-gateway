# krabka-app-sdk

`krabka-app-sdk` is the Rust application SDK surface for Krabka serverless apps.

It is private while the contract is still being validated by the in-repository conformance suite. The SDK is intentionally separate from the native Kafka-compatible client crates: use this crate for the cross-language application contract, and use the native `krabka-client-*` crates when you need Kafka-shaped administration, produce, or consume APIs.
