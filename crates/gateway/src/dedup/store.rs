//! Materialized view of the compacted dedup-claim topic.
//!
//! The ownership consumer, `run_ownership`, joins the owners consumer group,
//! tracks the assigned dedup partitions, and keeps the claim map warm. P3 gates
//! every produce on ownership and warmth, so only the owning replica can write.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, OnceLock, RwLock, Weak,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
};

use bytes::Bytes;
use dashmap::DashMap;
use krabka_client_consumer::{
    AutoOffsetReset, Consumer, ConsumerRebalanceListener, IsolationLevel, RebalanceListenerError,
};
use krabka_client_producer::{Acks, Producer, ProducerRecord};
use krabka_units::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    config::GatewayRuntimeConfig,
    error::GatewayError,
    ids::{Offset, PartitionIndex},
};

/// The value stored under each `idempotency_key` claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimValue {
    pub topic: String,
    pub partition: PartitionIndex,
    pub offset: Offset,
}

pub struct DedupStore {
    map: DashMap<String, ClaimValue>,
    partitions: u32,
    /// Dedup-partition ids this replica owns, from the consumer-group assignment.
    owned: RwLock<HashSet<u32>>,
    /// Caught up on reads of the owned partitions since the last assignment
    /// change.
    warm: AtomicBool,
    /// Has completed replay at least once, for startup waits.
    warmed_once: AtomicBool,
    /// An uncertain transaction requires a new process to fence and replay.
    restart_required: AtomicBool,
    /// Optional membership publisher. The binary sets it before `run_ownership`
    /// starts. In a single-owner or unit context it is `None`, and the store
    /// publishes nothing.
    membership: OnceLock<Arc<crate::dedup::membership::MembershipPublisher>>,
    /// The producer barrier fences prior owners before capturing replay ends.
    /// Weak ownership avoids a cycle with the engine's store reference.
    engine: OnceLock<Weak<crate::dedup::DedupEngine>>,
    poll_timeout: Time,
    warmup_empty_polls: u32,
    empty_polls: AtomicU32,
}

impl DedupStore {
    #[must_use]
    pub fn new(partitions: u32) -> Self {
        Self::new_with_policy(partitions, &GatewayRuntimeConfig::default())
    }

    #[must_use]
    pub fn new_with_policy(partitions: u32, runtime: &GatewayRuntimeConfig) -> Self {
        assert2::assert!(partitions > 0);
        assert2::assert!(i32::try_from(partitions).is_ok());
        Self {
            map: DashMap::new(),
            partitions,
            owned: RwLock::new(HashSet::new()),
            warm: AtomicBool::new(false),
            warmed_once: AtomicBool::new(false),
            restart_required: AtomicBool::new(false),
            membership: OnceLock::new(),
            engine: OnceLock::new(),
            poll_timeout: runtime.consumer_poll_timeout,
            warmup_empty_polls: runtime.ownership_warmup_empty_polls,
            empty_polls: AtomicU32::new(0),
        }
    }

    /// Install the membership publisher. Call this before you spawn
    /// `run_ownership`, so the store publishes the first assignment.
    pub fn set_membership(&self, publisher: Arc<crate::dedup::membership::MembershipPublisher>) {
        let _ = self.membership.set(publisher);
    }

    /// Register the transactional engine before starting ownership recovery.
    /// # Panics
    /// Panics if an engine was already registered for this store.
    pub fn set_engine(&self, engine: &Arc<crate::dedup::DedupEngine>) {
        assert2::assert!(self.engine.set(Arc::downgrade(engine)).is_ok());
    }

    /// True if this replica currently owns dedup-partition `p`.
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn owns(&self, p: u32) -> bool {
        self.owned.read().expect("owned lock").contains(&p)
    }

    /// True once the store is caught up on the owned partitions since the last
    /// assignment change.
    #[must_use]
    pub fn is_warm(&self) -> bool {
        self.warm.load(Ordering::SeqCst) && !self.restart_required.load(Ordering::SeqCst)
    }

    /// Fence requests before an errored producer is dropped. The ownership task
    /// then exits, letting its supervisor stop the replica for fresh recovery.
    pub(crate) fn require_restart(&self) {
        self.restart_required.store(true, Ordering::SeqCst);
        self.warm.store(false, Ordering::SeqCst);
    }

    /// Has completed replay at least once. Readiness uses `is_warm` instead.
    #[must_use]
    pub fn has_warmed_once(&self) -> bool {
        self.warmed_once.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn get(&self, key: &str) -> Option<ClaimValue> {
        self.map.get(key).map(|v| v.clone())
    }

    /// Apply a claim to the in-memory map. The local path calls this after a
    /// commit.
    pub fn apply(&self, key: String, value: ClaimValue) {
        self.map.insert(key, value);
    }

    /// Run the ownership consumer until `shutdown` fires.
    ///
    /// The consumer joins the owners group on the dedup topic, and its
    /// assignment is the owned-partition set. It reads owned partitions from
    /// earliest, and never commits, to build the claim map again. Each
    /// assignment change re-arms the warm gate. The task closes the consumer on
    /// exit, so the coordinator task and the group member do not leak.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub async fn run_ownership(
        self: Arc<Self>,
        bootstrap: String,
        client_id: String,
        dedup_topic: String,
        group: String,
        shutdown: tokio_util::sync::CancellationToken,
        security: Option<krabka_client_core::security::ClientSecurity>,
    ) -> Result<(), GatewayError> {
        self.run_ownership_with_policy(
            bootstrap,
            client_id,
            dedup_topic,
            group,
            shutdown,
            (security, crate::config::GatewayRuntimeConfig::default()),
        )
        .await
    }

    /// Run ownership with the deployment's client resource policy.
    /// # Errors
    /// Returns an error when consuming fails.
    /// # Panics
    /// Panics if synchronized ownership state is poisoned.
    pub async fn run_ownership_with_policy(
        self: Arc<Self>,
        bootstrap: String,
        client_id: String,
        dedup_topic: String,
        group: String,
        shutdown: tokio_util::sync::CancellationToken,
        client_policy: (
            Option<krabka_client_core::security::ClientSecurity>,
            crate::config::GatewayRuntimeConfig,
        ),
    ) -> Result<(), GatewayError> {
        let (security, policy) = client_policy;
        let replay_targets = Arc::new(RwLock::new(HashMap::new()));
        let mut consumer = Consumer::builder()
            .bootstrap(bootstrap)
            .client_id(client_id)
            .dispatch_queue_capacity(policy.client_dispatch_queue_capacity.get())
            .frame_max(policy.client_frame_max.size())
            .group_id(group)
            .subscribe(vec![dedup_topic.clone()])
            .isolation_level(IsolationLevel::ReadCommitted)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .rebalance_listener(Box::new(OwnershipReplay {
                store: Arc::clone(&self),
                replay_targets: Arc::clone(&replay_targets),
                topic: dedup_topic,
            }))
            .assignors(vec![krabka_client_consumer::Assignor::CooperativeSticky])
            .maybe_security(security)
            .build()
            .await?;

        let result = async {
            loop {
                let batch = tokio::select! {
                    () = shutdown.cancelled() => break,
                    batch = consumer.poll(self.poll_timeout) => batch?,
                };
                if self.restart_required.load(Ordering::SeqCst) {
                    return Err(GatewayError::Unavailable);
                }
                let empty_polls = if batch.is_empty() {
                    self.empty_polls.load(Ordering::SeqCst).saturating_add(1)
                } else {
                    0
                };
                self.empty_polls.store(empty_polls, Ordering::SeqCst);
                for record in batch {
                    if !u32::try_from(record.partition).is_ok_and(|p| self.owns(p)) {
                        continue;
                    }
                    let Some(key_bytes) = record.key else {
                        continue;
                    };
                    let key = String::from_utf8_lossy(&key_bytes).into_owned();
                    match record.value {
                        None => {
                            self.map.remove(&key);
                        }
                        // Skip malformed claims without killing the ownership loop.
                        Some(value) => {
                            if let Ok(claim) = serde_json::from_slice::<ClaimValue>(&value) {
                                self.map.insert(key, claim);
                            }
                        }
                    }
                }
                if !self.is_warm() && empty_polls >= self.warmup_empty_polls {
                    let targets = replay_targets.read().expect("replay lock").clone();
                    let mut positions = HashMap::new();
                    for (topic, partition) in targets.keys() {
                        positions.insert(
                            (topic.clone(), *partition),
                            consumer.position(topic.clone(), *partition).await?,
                        );
                    }
                    if replay_complete(&targets, &positions) {
                        self.warm.store(true, Ordering::SeqCst);
                        self.warmed_once.store(true, Ordering::SeqCst);
                    }
                }
            }
            Ok::<(), GatewayError>(())
        }
        .await;

        // A stopped or failed reader can no longer safely accept claims.
        self.warm.store(false, Ordering::SeqCst);
        self.owned.write().expect("owned lock").clear();
        self.map.clear();
        crate::metrics::metrics().set_owned_partitions(0);
        let _ = consumer.close().await;
        result
    }

    async fn update_assignment(&self, assigned: HashSet<u32>) {
        self.warm.store(false, Ordering::SeqCst);
        self.empty_polls.store(0, Ordering::SeqCst);
        let revoked = {
            let mut owned = self.owned.write().expect("owned lock");
            let revoked: HashSet<_> = owned.difference(&assigned).copied().collect();
            owned.clone_from(&assigned);
            revoked
        };
        if !revoked.is_empty() {
            self.map.retain(|key, _| {
                !revoked.contains(&crate::dedup::partition_for(key, self.partitions))
            });
        }
        crate::metrics::metrics()
            .set_owned_partitions(i64::try_from(assigned.len()).expect("count fits i64"));
        if let Some(publisher) = self.membership.get()
            && let Err(error) = publisher.publish(&assigned).await
        {
            tracing::warn!(%error, "membership publish failed");
        }
    }

    /// Test and helper writer that produces a single claim record to its hashed
    /// partition. On the compacted topic the key is the idempotency key and the
    /// value is a JSON `ClaimValue`.
    /// # Panics
    /// Panics if the validated partition count cannot be represented by Kafka.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn write_claim(
        &self,
        bootstrap: &str,
        client_id: &str,
        dedup_topic: &str,
        key: &str,
        value: &ClaimValue,
        security: Option<krabka_client_core::security::ClientSecurity>,
    ) -> Result<(), GatewayError> {
        let producer = Producer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(client_id.to_string())
            .enable_idempotence(true)
            .acks(Acks::All)
            .maybe_security(security)
            .build()
            .await?;
        let partition = i32::try_from(crate::dedup::partition_for(key, self.partitions))
            .expect("validated partition fits i32");
        let prec = ProducerRecord {
            topic: dedup_topic.to_string(),
            partition: Some(partition),
            key: Some(Bytes::from(key.as_bytes().to_vec())),
            value: Some(Bytes::from(serde_json::to_vec(value)?)),
            headers: vec![],
            timestamp_ms: None,
        };
        let meta = producer.send(prec).await;
        meta.map_err(GatewayError::Producer)?;
        self.apply(key.to_string(), value.clone());
        Ok(())
    }
}

type ReplayOffsets = HashMap<(String, i32), i64>;

/// Seek before returning the first record of a newly acquired partition, even
/// when the group has an old committed offset or reacquires the same assignment.
struct OwnershipReplay {
    store: Arc<DedupStore>,
    replay_targets: Arc<RwLock<ReplayOffsets>>,
    topic: String,
}

#[async_trait::async_trait]
impl ConsumerRebalanceListener for OwnershipReplay {
    async fn on_partitions_revoked(
        &mut self,
        _consumer: &Consumer,
        partitions: &[(String, i32)],
    ) -> Result<(), RebalanceListenerError> {
        self.store.warm.store(false, Ordering::SeqCst);
        self.replay_targets.write().expect("replay lock").clear();
        let mut owned = self.store.owned.read().expect("owned lock").clone();
        for (_, partition) in partitions {
            if let Ok(partition) = u32::try_from(*partition) {
                owned.remove(&partition);
            }
        }
        self.store.update_assignment(owned).await;
        if let Some(engine) = self.store.engine.get() {
            let engine = engine.upgrade().ok_or(GatewayError::Unavailable)?;
            engine.release_partitions(partitions).await;
        }
        Ok(())
    }

    async fn on_partitions_assigned(
        &mut self,
        consumer: &Consumer,
        partitions: &[(String, i32)],
    ) -> Result<(), RebalanceListenerError> {
        self.store.warm.store(false, Ordering::SeqCst);
        if let Some(engine) = self.store.engine.get() {
            let engine = engine.upgrade().ok_or(GatewayError::Unavailable)?;
            // Fence old producers and settle their transactions before capturing
            // the last stable offset: late commits must be included in replay.
            engine.prepare_partitions(partitions).await?;
        }
        // An empty seek resets every partition, including retained partitions.
        if !partitions.is_empty() {
            consumer.seek_to_beginning(partitions).await?;
        }
        let assigned: Vec<_> = consumer
            .assignment()
            .await
            .into_iter()
            .filter(|(topic, _)| *topic == self.topic)
            .collect();
        let targets = consumer.end_offsets(&assigned).await?;
        // Missing boundaries fail closed: an empty poll never proves replay.
        if targets.len() != assigned.len() {
            return Err(Box::new(GatewayError::Unavailable));
        }
        *self.replay_targets.write().expect("replay lock") = targets;
        self.store
            .update_assignment(
                assigned
                    .into_iter()
                    .filter_map(|(_, partition)| u32::try_from(partition).ok())
                    .collect(),
            )
            .await;
        Ok(())
    }
}

fn replay_complete(targets: &ReplayOffsets, positions: &ReplayOffsets) -> bool {
    !targets.is_empty()
        && targets.iter().all(|(partition, end)| {
            positions
                .get(partition)
                .is_some_and(|position| position >= end)
        })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{ReplayOffsets, replay_complete};

    #[test]
    fn replay_requires_all_assigned_partition_boundaries() {
        let targets = ReplayOffsets::from([(("claims".into(), 0), 0), (("claims".into(), 1), 9)]);
        for (positions, expected) in [
            (ReplayOffsets::new(), false),
            (ReplayOffsets::from([(("claims".into(), 0), 0)]), false),
            (
                ReplayOffsets::from([(("claims".into(), 0), 0), (("claims".into(), 1), 8)]),
                false,
            ),
            (
                ReplayOffsets::from([(("claims".into(), 0), 0), (("claims".into(), 1), 9)]),
                true,
            ),
            (
                ReplayOffsets::from([(("claims".into(), 0), 0), (("claims".into(), 1), 10)]),
                true,
            ),
        ] {
            assert!(replay_complete(&targets, &positions) == expected);
        }
        assert!(!replay_complete(
            &ReplayOffsets::new(),
            &ReplayOffsets::new()
        ));
    }

    #[test]
    fn uncertain_transaction_requires_restart_despite_delayed_warmup() {
        use std::sync::atomic::Ordering;

        use super::DedupStore;

        let store = DedupStore::new(1);
        store.warm.store(true, Ordering::SeqCst);
        assert!(store.is_warm());
        store.require_restart();
        assert!(!store.is_warm());
        // A replay check that started before the error cannot reopen writes.
        store.warm.store(true, Ordering::SeqCst);
        assert!(!store.is_warm());
    }

    #[tokio::test]
    async fn revocation_fences_writes_and_preserves_retained_claims() {
        use std::{collections::HashSet, sync::atomic::Ordering};

        use super::{ClaimValue, DedupStore};
        use crate::ids::{Offset, PartitionIndex};

        let store = DedupStore::new(2);
        let key_for = |partition| {
            (0..10_000)
                .map(|n| format!("key-{n}"))
                .find(|key| crate::dedup::partition_for(key, 2) == partition)
                .unwrap()
        };
        let revoked = key_for(0);
        let retained = key_for(1);
        let value = ClaimValue {
            topic: "events".into(),
            partition: PartitionIndex(0),
            offset: Offset(9),
        };
        store.update_assignment(HashSet::from([0, 1])).await;
        store.apply(revoked.clone(), value.clone());
        store.apply(retained.clone(), value.clone());
        store.warm.store(true, Ordering::SeqCst);
        store.update_assignment(HashSet::from([1])).await;
        assert!(!store.is_warm());
        assert!(!store.owns(0));
        assert!(store.owns(1));
        assert!(store.get(&revoked) == None);
        assert!(store.get(&retained) == Some(value));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cold_owner_replays_claims_despite_previously_committed_group_offsets() {
        use std::{sync::Arc, time::Duration};

        use krabka_broker::{Broker, BrokerConfig};
        use krabka_client_consumer::{AutoOffsetReset, Consumer};
        use tokio_util::sync::CancellationToken;

        use super::{ClaimValue, DedupStore};
        use crate::{
            dedup::topic::{InternalTopicPolicy, ensure_dedup_topic},
            ids::{Offset, PartitionIndex},
        };

        let dir = tempfile::TempDir::new().unwrap();
        let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let topic = "cold-owner-claims";
        let group = "cold-owner-group";
        ensure_dedup_topic(
            &bootstrap,
            topic,
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
        let value = ClaimValue {
            topic: "events".into(),
            partition: PartitionIndex(0),
            offset: Offset(9),
        };
        DedupStore::new(1)
            .write_claim(
                &bootstrap,
                "claim-writer",
                topic,
                "delivery-1",
                &value,
                None,
            )
            .await
            .unwrap();

        let mut prior = Consumer::builder()
            .bootstrap(bootstrap.clone())
            .group_id(group)
            .subscribe(vec![topic.into()])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .build()
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if !prior
                    .poll(krabka_units::millis(100))
                    .await
                    .unwrap()
                    .is_empty()
                {
                    break;
                }
            }
        })
        .await
        .unwrap();
        prior.commit_sync().await.unwrap();
        prior.close().await.unwrap();

        let store = Arc::new(DedupStore::new(1));
        let shutdown = CancellationToken::new();
        let reader = tokio::spawn(Arc::clone(&store).run_ownership(
            bootstrap,
            "cold-owner".into(),
            topic.into(),
            group.into(),
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
        assert!(store.get("delivery-1") == Some(value));
        assert!(store.owns(0));
        shutdown.cancel();
        reader.await.unwrap().unwrap();
        assert!(!store.is_warm());
        assert!(!store.owns(0));
        broker.shutdown().await;
    }
}
