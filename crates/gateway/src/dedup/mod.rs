//! Single-owner exactly-once dedup engine.

pub mod membership;
pub mod store;
pub mod topic;

/// Deterministic FNV-1a-64 over the key, modulo the partition count.
///
/// The result is stable across processes and restarts, unlike
/// `DefaultHasher`'s per-run state, so a given key always maps to the same
/// dedup partition.
///
/// # Panics
///
/// Panics when `partitions` is zero. Process configuration validates this
/// invariant before constructing the engine.
#[must_use]
pub fn partition_for(key: &str, partitions: u32) -> u32 {
    assert2::assert!(partitions > 0);
    assert2::assert!(i32::try_from(partitions).is_ok());
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // `hash % partitions` is < `partitions` (a u32), so it always fits in u32.
    u32::try_from(hash % u64::from(partitions)).expect("partition modulo fits in u32")
}

use std::sync::Arc;

use bytes::Bytes;
use krabka_client_producer::{Acks, Producer, ProducerError, ProducerRecord, RecordMetadata};
use tokio::sync::Mutex;

use self::store::{ClaimValue, DedupStore};
use crate::{
    error::GatewayError,
    ids::{Offset, PartitionIndex},
    produce::to_producer_record,
    types::{GatewayRecord, RecordOutcome},
};

/// A lazily-initialized transactional producer pinned to one dedup partition.
///
/// Only one transaction can be in flight at a time, so the `Mutex` serializes
/// that partition's record and claim transactions.
type TxnSlot = Mutex<Option<Producer>>;

pub struct DedupEngine {
    bootstrap: String,
    client_id: String,
    txn_id_prefix: String,
    dedup_topic: String,
    partitions: u32,
    slots: Vec<TxnSlot>,
    store: Arc<DedupStore>,
    security: Option<krabka_client_core::security::ClientSecurity>,
    dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    frame_max: krabka_client_core::ClientFrameMax,
}

impl DedupEngine {
    /// Fence the previous owner before the new owner captures replay boundaries.
    pub(crate) async fn prepare_partitions(
        &self,
        partitions: &[(String, i32)],
    ) -> Result<(), GatewayError> {
        for (_, partition) in partitions {
            let p = u32::try_from(*partition).map_err(|_| GatewayError::Unavailable)?;
            let mut slot = self.slots[usize::try_from(p).expect("partition fits usize")]
                .lock()
                .await;
            *slot = None;
            *slot = Some(self.init_producer(p).await?);
        }
        Ok(())
    }

    /// Wait for in-flight writes and release revoked shard producers.
    pub(crate) async fn release_partitions(&self, partitions: &[(String, i32)]) {
        for (_, partition) in partitions {
            if let Ok(p) = usize::try_from(*partition)
                && let Some(slot) = self.slots.get(p)
            {
                *slot.lock().await = None;
            }
        }
    }

    async fn init_producer(&self, p: u32) -> Result<Producer, GatewayError> {
        let producer = Producer::builder()
            .bootstrap(self.bootstrap.clone())
            .client_id(format!("{}-dedup-{}", self.client_id, p))
            .dispatch_queue_capacity(self.dispatch_queue_capacity.get())
            .frame_max(self.frame_max.size())
            .enable_idempotence(true)
            .acks(Acks::All)
            .transactional_id(format!("{}-{}", self.txn_id_prefix, p))
            .maybe_security(self.security.clone())
            .build()
            .await?;
        producer.init_transactions().await?;
        Ok(producer)
    }

    /// Construct a dedup engine for a validated non-zero partition count.
    ///
    /// # Panics
    ///
    /// Panics when `partitions` is zero.
    #[must_use]
    pub fn new(
        bootstrap: &str,
        client_id: &str,
        txn_id_prefix: &str,
        dedup_topic: String,
        partitions: u32,
        store: Arc<DedupStore>,
        security: Option<krabka_client_core::security::ClientSecurity>,
    ) -> Self {
        Self::new_with_policy(
            bootstrap,
            client_id,
            txn_id_prefix,
            dedup_topic,
            partitions,
            store,
            (security, &crate::config::GatewayRuntimeConfig::default()),
        )
    }

    /// Construct with the deployment's client resource policy.
    #[must_use]
    pub fn new_with_policy(
        bootstrap: &str,
        client_id: &str,
        txn_id_prefix: &str,
        dedup_topic: String,
        partitions: u32,
        store: Arc<DedupStore>,
        client_policy: (
            Option<krabka_client_core::security::ClientSecurity>,
            &crate::config::GatewayRuntimeConfig,
        ),
    ) -> Self {
        let (security, policy) = client_policy;
        assert2::assert!(partitions > 0);
        assert2::assert!(i32::try_from(partitions).is_ok());
        let slots = (0..partitions).map(|_| Mutex::new(None)).collect();
        Self {
            bootstrap: bootstrap.to_string(),
            client_id: client_id.to_string(),
            txn_id_prefix: txn_id_prefix.to_string(),
            dedup_topic,
            partitions,
            slots,
            store,
            security,
            dispatch_queue_capacity: policy.client_dispatch_queue_capacity,
            frame_max: policy.client_frame_max,
        }
    }

    /// The dedup partition a key hashes to. Routing decisions use it.
    #[must_use]
    pub fn partition_for_key(&self, key: &str) -> u32 {
        partition_for(key, self.partitions)
    }

    /// True if this replica currently owns dedup-partition `p`.
    #[must_use]
    pub fn owns(&self, p: u32) -> bool {
        self.store.owns(p)
    }

    /// EOS produce.
    ///
    /// A fast-path map hit returns the cached offset. A miss takes the
    /// partition's transactional producer, writes the data record and the claim
    /// atomically, then updates the local map.
    #[tracing::instrument(skip_all)]
    /// # Panics
    /// Panics if the validated partition count cannot be represented locally.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn dedup_produce(
        &self,
        rec: &GatewayRecord,
        value: Bytes,
    ) -> Result<RecordOutcome, GatewayError> {
        let key = rec.idempotency_key.as_deref().ok_or_else(|| {
            GatewayError::Other("dedup_produce called without idempotency_key".into())
        })?;
        let p = partition_for(key, self.partitions);
        // Mutual exclusion: only the owner of `p` may produce its keys, and only
        // once warmed (claim map rebuilt). Otherwise refuse so the caller retries
        // against the owning replica.
        if !self.store.owns(p) || !self.store.is_warm() {
            return Err(GatewayError::Unavailable);
        }

        // Fast path: already claimed.
        if let Some(c) = self.store.get(key) {
            crate::metrics::metrics().record_dedup_hit();
            return Ok(RecordOutcome {
                partition: c.partition,
                offset: c.offset,
                deduplicated: true,
            });
        }

        let mut slot = self.slots[usize::try_from(p).expect("u32 partition fits usize")]
            .lock()
            .await;

        // A waiter may have acquired this lock after its shard was revoked.
        if !self.store.owns(p) || !self.store.is_warm() {
            return Err(GatewayError::Unavailable);
        }

        // Re-check under the lock (another task may have just claimed it).
        if let Some(c) = self.store.get(key) {
            crate::metrics::metrics().record_dedup_hit();
            return Ok(RecordOutcome {
                partition: c.partition,
                offset: c.offset,
                deduplicated: true,
            });
        }

        // A confirmed abort can safely retry with a fresh producer. Only an
        // unresolved transaction outcome requires the store to stop serving.
        match self.txn_write(&mut slot, rec, value, key, p).await {
            Ok(outcome) => Ok(outcome),
            Err(e) => {
                *slot = None;
                Err(e)
            }
        }
    }

    /// The fallible begin, record, claim, and commit sequence for one keyed
    /// record.
    ///
    /// This method is separate so `dedup_produce` can reset the producer slot
    /// on any error. The abort-on-error logic lives here, and not in
    /// `dedup_produce`, because only the holder of the `Transaction` guard that
    /// `begin_transaction` returns can abort it. The caller must hold `slot`'s
    /// lock and must already have confirmed that the key is not claimed.
    async fn txn_write(
        &self,
        slot: &mut Option<Producer>,
        rec: &GatewayRecord,
        value: Bytes,
        key: &str,
        p: u32,
    ) -> Result<RecordOutcome, GatewayError> {
        // Lazily init the partition's transactional producer.
        if slot.is_none() {
            *slot = Some(self.init_producer(p).await?);
        }
        let producer = slot.as_ref().expect("just initialized");

        let txn = producer.begin_transaction().await.map_err(|error| {
            if matches!(error, ProducerError::RecoveryRequired) {
                // A previous request may have been cancelled during commit.
                self.store.require_restart();
            }
            GatewayError::Producer(error)
        })?;

        let sent: Result<(RecordMetadata, ClaimValue), GatewayError> = async {
            // 1. data record → user topic
            let data = to_producer_record(rec, value);
            let meta = producer.send(data).await.map_err(GatewayError::Producer)?;

            // 2. claim → dedup topic (partition p), key = idempotency key
            let claim = ClaimValue {
                topic: rec.topic.clone(),
                partition: PartitionIndex(meta.partition),
                offset: Offset(meta.offset),
            };
            let claim_rec = ProducerRecord {
                topic: self.dedup_topic.clone(),
                partition: Some(i32::try_from(p).expect("validated partition fits i32")),
                key: Some(Bytes::from(key.as_bytes().to_vec())),
                value: Some(Bytes::from(serde_json::to_vec(&claim)?)),
                headers: vec![],
                timestamp_ms: None,
            };
            producer
                .send(claim_rec)
                .await
                .map_err(GatewayError::Producer)?;

            Ok((meta, claim))
        }
        .await;

        let (meta, claim) = match sent {
            Ok(pair) => pair,
            Err(e) => {
                if let Err(abort_error) = txn.abort().await {
                    self.store.require_restart();
                    drop(abort_error);
                }
                crate::metrics::metrics().record_txn("abort");
                return Err(e);
            }
        };

        if let Err(commit_error) = txn.commit().await {
            // A definite commit failure can still be aborted. An uncertain
            // EndTxn result requires recovery and cannot confirm an abort.
            if let Err(abort_error) = commit_error.transaction.abort().await {
                self.store.require_restart();
                drop(abort_error);
            }
            crate::metrics::metrics().record_txn("abort");
            return Err(GatewayError::Producer(commit_error.source));
        }
        crate::metrics::metrics().record_txn("commit");

        // Single-owner: update the local map directly.
        self.store.apply(key.to_string(), claim);
        Ok(RecordOutcome {
            partition: PartitionIndex(meta.partition),
            offset: Offset(meta.offset),
            deduplicated: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;

    use super::{DedupEngine, partition_for, store::DedupStore};

    #[test]
    fn partition_for_rejects_zero_partitions() {
        assert!(std::panic::catch_unwind(|| partition_for("key", 0)).is_err());
    }

    #[test]
    fn partition_for_rejects_counts_kafka_cannot_represent() {
        assert!(
            std::panic::catch_unwind(|| {
                let _ = partition_for("key", i32::MAX.cast_unsigned() + 1);
            })
            .is_err()
        );
    }

    #[test]
    fn dedup_engine_rejects_zero_partitions() {
        assert!(
            std::panic::catch_unwind(|| {
                DedupEngine::new(
                    "localhost:9092",
                    "test",
                    "test-txn",
                    "dedup".into(),
                    0,
                    Arc::new(DedupStore::new(1)),
                    None,
                )
            })
            .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn confirmed_abort_preserves_connect_ingestion_and_uncertain_guard_stops_it() {
        use std::{
            collections::{BTreeMap, HashMap},
            time::Duration,
        };

        use axum::Extension;
        use connectrpc_axum::message::ConnectRequest;
        use krabka_broker::{Broker, BrokerConfig};
        use krabka_client_admin::{AdminClient, CreateTopicSpec, TopicMutationOptions};
        use krabka_client_producer::{Acks, Producer};
        use tokio_util::sync::CancellationToken;

        use crate::{
            authz::GatewayAuthz,
            codec::RawCodec,
            config::{GatewayConfig, GatewayRuntimeConfig},
            dedup::topic::{InternalTopicPolicy, ensure_dedup_topic},
            error::GatewayError,
            handlers, pb,
            produce::ProduceCore,
            state::AppState,
        };

        let dir = tempfile::TempDir::new().unwrap();
        let mut broker_config = BrokerConfig::for_tests(dir.path().to_path_buf());
        broker_config.offsets_topic_num_partitions = 1;
        broker_config.transaction_state_num_partitions = 1;
        let broker = Broker::start(broker_config).await.unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
            .await
            .unwrap();
        admin
            .create_topics(
                &[CreateTopicSpec {
                    name: "abort-events".into(),
                    partitions: 1,
                    replicas: 1,
                    configs: BTreeMap::new(),
                    replica_assignments: BTreeMap::new(),
                }],
                TopicMutationOptions::with_timeout(krabka_units::secs(10)),
            )
            .await
            .unwrap();
        ensure_dedup_topic(
            &bootstrap,
            "abort-claims",
            1,
            krabka_units::hours(1),
            &InternalTopicPolicy {
                replication_factor: 1,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        let store = Arc::new(DedupStore::new(1));
        let engine = Arc::new(DedupEngine::new(
            &bootstrap,
            "abort-test",
            "abort-txn",
            "abort-claims".into(),
            1,
            Arc::clone(&store),
            None,
        ));
        store.set_engine(&engine);
        let shutdown = CancellationToken::new();
        let reader = tokio::spawn(Arc::clone(&store).run_ownership(
            bootstrap.clone(),
            "abort-owner".into(),
            "abort-claims".into(),
            "abort-owners".into(),
            shutdown.clone(),
            None,
        ));
        tokio::time::timeout(Duration::from_secs(20), async {
            while !store.is_warm() {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();

        // Keep an invalid explicit partition's metadata wait short without
        // changing the production producer resource policy.
        let producer = Producer::builder()
            .bootstrap(bootstrap.clone())
            .client_id("abort-fast")
            .enable_idempotence(true)
            .acks(Acks::All)
            .transactional_id("abort-txn-0")
            .max_block(Duration::from_millis(200))
            .build()
            .await
            .unwrap();
        producer.init_transactions().await.unwrap();
        *engine.slots[0].lock().await = Some(producer);
        let core = ProduceCore::new_for_test(&bootstrap, "abort-connect", Arc::new(RawCodec))
            .await
            .unwrap()
            .with_dedup(Arc::clone(&engine));
        let state = Arc::new(AppState {
            produce: Arc::new(core),
            config: Arc::new(GatewayConfig {
                bootstrap,
                listen_addr: "127.0.0.1:0".parse().unwrap(),
                client_id: "abort-test".into(),
                dedup_topic: "abort-claims".into(),
                dedup_partitions: 1,
                dedup_window: krabka_units::hours(1),
                dedup_ownership_group: "abort-owners".into(),
                dedup_txn_id_prefix: "abort-txn".into(),
                advertised_addr: "127.0.0.1:0".into(),
                membership_topic: "abort-membership".into(),
                tls: None,
                broker_security: None,
                authz: None,
                webhooks: HashMap::default(),
                outbound: Vec::new(),
                schema_registry_url: None,
                runtime: GatewayRuntimeConfig::default(),
            }),
            authz: Arc::new(GatewayAuthz::new(Arc::new(
                krabka_authz::AllowAllAuthorizer,
            ))),
            codec: Arc::new(RawCodec),
            queue: Arc::default(),
        });
        let request = |partition, key: &str| {
            ConnectRequest(pb::SendRequest {
                records: vec![pb::Record {
                    topic: "abort-events".into(),
                    body: Some(pb::record::Body::Raw(b"event".to_vec())),
                    partition: Some(partition),
                    idempotency_key: Some(key.into()),
                    ..Default::default()
                }],
                acks: pb::Acks::All as i32,
            })
        };
        let failed = handlers::send(
            Extension(Arc::clone(&state)),
            None,
            None,
            request(1, "delivery-1"),
        )
        .await
        .unwrap();
        assert!(failed.0 == pb::SendResponse { results: vec![pb::RecordResult {
            partition: -1, offset: -1, deduplicated: false,
            error: Some(pb::ErrorInfo { code: 1, retriable: false,
                message: "producer error: Partition 1 of topic abort-events with partition count 1 is not present in metadata after 200 ms.".into() }),
        }] });
        assert!(store.is_warm());
        assert!(!reader.is_finished());
        let accepted = handlers::send(
            Extension(Arc::clone(&state)),
            None,
            None,
            request(0, "delivery-1"),
        )
        .await
        .unwrap();
        assert!(
            accepted.0
                == pb::SendResponse {
                    results: vec![pb::RecordResult {
                        partition: 0,
                        offset: 0,
                        deduplicated: false,
                        error: None,
                    }]
                }
        );
        assert!(store.is_warm());

        // Dropping a native unresolved guard models a cancelled request. Its
        // RecoveryRequired begin failure must not reset and retry blindly.
        {
            let slot = engine.slots[0].lock().await;
            let transaction = slot.as_ref().unwrap().begin_transaction().await.unwrap();
            drop(transaction);
        }
        let unresolved = handlers::send(Extension(state), None, None, request(0, "delivery-2"))
            .await
            .unwrap();
        assert!(unresolved.0 == pb::SendResponse { results: vec![pb::RecordResult {
            partition: -1, offset: -1, deduplicated: false,
            error: Some(pb::ErrorInfo { code: 1, retriable: false,
                message: "producer error: transaction outcome is unknown; call init_transactions before sending or beginning another transaction".into() }),
        }] });
        assert!(!store.is_warm());
        let result = tokio::time::timeout(Duration::from_secs(10), reader)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(GatewayError::Unavailable)));
        shutdown.cancel();
        broker.shutdown().await;
    }
}
