//! Consume core: a group-subscribed session that yields records and commits
//! offsets.
//!
//! The streaming/poll wire (later plan) drives this session. The codec decodes
//! each record on the way out.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use krabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use krabka_units::prelude::*;

use crate::{
    codec::{RecordCodec, SchemaMeta},
    error::GatewayError,
    ids::{Offset, PartitionIndex, Timestamp},
};

#[derive(Debug, Clone)]
pub struct DecodedConsumerRecord {
    pub topic: String,
    pub partition: PartitionIndex,
    pub offset: Offset,
    pub timestamp: Timestamp,
    pub key: Option<bytes::Bytes>,
    /// Original Kafka bytes, preserved for byte-exact delivery.
    pub raw_value: bytes::Bytes,
    /// Registry-decoded payload used only for filter evaluation.
    pub value: bytes::Bytes,
    pub headers: Vec<krabka_client_consumer::Header>,
    pub schema: Option<SchemaMeta>,
    pub json: Option<bytes::Bytes>,
}

/// Maximum actual deliveries that can wait above an unacknowledged record in
/// one partition. The bound includes acknowledgements waiting behind it. The
/// consumer cannot pause one assigned partition independently, so exceeding
/// this bound terminates the stream instead of growing memory without limit.
pub(crate) const MAX_OUTSTANDING_PER_PARTITION: usize = 100_000;

#[derive(Debug, Default)]
struct PartitionAckState {
    /// Highest contiguously acknowledged offset. Before the first delivery is
    /// acknowledged, this is the offset immediately before that delivery.
    frontier: Option<i64>,
    delivered: BTreeSet<i64>,
    pending: BTreeSet<i64>,
    last_committed_frontier: Option<i64>,
}

impl PartitionAckState {
    /// Establish the first actually delivered offset as the lower bound. This
    /// is safe for compacted logs and prevents a later filtered-record ack from
    /// skipping an earlier delivered record that is still unacknowledged.
    fn record_delivery(&mut self, offset: i64) -> Result<(), ()> {
        if self.frontier.is_some_and(|frontier| offset <= frontier) {
            return Ok(());
        }
        if !self.delivered.contains(&offset)
            && self.delivered.len() >= MAX_OUTSTANDING_PER_PARTITION
        {
            return Err(());
        }
        if self.frontier.is_none() {
            let baseline = offset - 1;
            self.frontier = Some(baseline);
            self.last_committed_frontier = Some(baseline);
        }
        self.delivered.insert(offset);
        Ok(())
    }

    fn record_ack(&mut self, offset: i64) -> Result<(), ()> {
        if self.frontier.is_none() {
            // Preserve lazy seeding for a caller acknowledging its first
            // delivery directly. Streams register every delivery first.
            self.record_delivery(offset)?;
        }
        if self.frontier.is_some_and(|frontier| offset <= frontier) {
            return Ok(());
        }
        if !self.delivered.contains(&offset) {
            return Err(());
        }
        self.pending.insert(offset);
        self.drain();
        Ok(())
    }

    fn drain(&mut self) {
        // Kafka offsets need not be consecutive: compaction, transaction
        // markers and aborted transactions can all leave invisible offsets.
        while let Some(next) = self.delivered.first().copied() {
            if !self.pending.remove(&next) {
                break;
            }
            self.delivered.pop_first();
            self.frontier = Some(next);
        }
    }

    fn commit_value(&self) -> Option<i64> {
        self.frontier
            .filter(|frontier| self.last_committed_frontier != Some(*frontier))
            .and_then(|frontier| frontier.checked_add(1))
    }
}

pub struct ConsumeSession {
    /// Held in an `Option` so [`Drop`] can `take()` the consumer and stop its
    /// background coordinator. See the `Drop` impl. This field is always `Some`
    /// while the session is alive, and `None` only for a moment inside `drop`.
    consumer: Option<Consumer>,
    codec: Arc<dyn RecordCodec>,
    ack_tracker: HashMap<(String, i32), PartitionAckState>,
}

impl ConsumeSession {
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn new(
        bootstrap: &str,
        group_id: &str,
        client_id: &str,
        topics: Vec<String>,
        security: Option<krabka_client_core::security::ClientSecurity>,
        codec: Arc<dyn RecordCodec>,
    ) -> Result<Self, GatewayError> {
        Self::new_with_policy(
            bootstrap,
            group_id,
            client_id,
            topics,
            security,
            codec,
            &crate::config::GatewayRuntimeConfig::default(),
        )
        .await
    }

    /// Build a consume session with the deployment's client resource policy.
    /// # Errors
    /// Returns an error when client construction fails.
    pub async fn new_with_policy(
        bootstrap: &str,
        group_id: &str,
        client_id: &str,
        topics: Vec<String>,
        security: Option<krabka_client_core::security::ClientSecurity>,
        codec: Arc<dyn RecordCodec>,
        policy: &crate::config::GatewayRuntimeConfig,
    ) -> Result<Self, GatewayError> {
        let consumer = Consumer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(client_id.to_string())
            .dispatch_queue_capacity(policy.client_dispatch_queue_capacity.get())
            .frame_max(policy.client_frame_max.size())
            .group_id(group_id.to_string())
            .subscribe(topics)
            .isolation_level(IsolationLevel::ReadCommitted)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .maybe_security(security)
            .build()
            .await?;
        Ok(Self {
            consumer: Some(consumer),
            codec,
            ack_tracker: HashMap::new(),
        })
    }

    /// Poll a batch. The codec decodes each record value.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub async fn poll(
        &mut self,
        timeout: Time,
    ) -> Result<Vec<DecodedConsumerRecord>, GatewayError> {
        let batch = self
            .consumer
            .as_mut()
            .expect("ConsumeSession polled after close")
            .poll(timeout)
            .await?;
        let mut decoded_batch = Vec::with_capacity(batch.len());
        for r in batch {
            let (raw_value, value, schema, json) = match r.value {
                Some(v) => {
                    let raw_value = v.clone();
                    let decoded = self.codec.decode(&r.topic, v).await?;
                    (raw_value, decoded.value, decoded.schema, decoded.json)
                }
                None => (bytes::Bytes::new(), bytes::Bytes::new(), None, None),
            };
            decoded_batch.push(DecodedConsumerRecord {
                topic: r.topic,
                partition: PartitionIndex(r.partition),
                offset: Offset(r.offset),
                timestamp: Timestamp(r.timestamp),
                key: r.key,
                raw_value,
                value,
                headers: r.headers,
                schema,
                json,
            });
        }
        Ok(decoded_batch)
    }

    /// Commit the current positions. For at-least-once, call this after the
    /// receiver acknowledges delivery.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn commit(&self) -> Result<(), GatewayError> {
        self.consumer
            .as_ref()
            .ok_or_else(|| GatewayError::Other("consume session is closed".to_string()))?
            .commit_sync()
            .await?;
        Ok(())
    }

    /// Record an offset that the stream delivered or filtered. This establishes
    /// the partition's lazy, delivery-derived lower bound without acknowledging
    /// the record.
    /// # Errors
    /// Returns an error when outstanding deliveries exceed the partition cap.
    pub(crate) fn record_delivery(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), GatewayError> {
        if partition < 0 || offset < 0 {
            return Ok(());
        }
        self.ack_tracker
            .entry((topic.to_string(), partition))
            .or_default()
            .record_delivery(offset)
            .map_err(|()| GatewayError::TooManyUnacked {
                topic: topic.to_string(),
                partition,
                offset,
            })
    }

    /// Record one delivered-record acknowledgement in the partition's bounded
    /// contiguous frontier.
    ///
    /// # Errors
    ///
    /// Returns [`GatewayError::TooManyUnacked`] when the offset does not match
    /// a tracked outstanding delivery. Streaming callers validate delivery
    /// membership before recording acknowledgements.
    pub(crate) fn record_ack(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), GatewayError> {
        if partition < 0 || offset < 0 {
            return Ok(());
        }
        self.ack_tracker
            .entry((topic.to_string(), partition))
            .or_default()
            .record_ack(offset)
            .map_err(|()| GatewayError::TooManyUnacked {
                topic: topic.to_string(),
                partition,
                offset,
            })
    }

    pub(crate) fn ack_frontier(&self, topic: &str, partition: i32) -> Option<i64> {
        self.ack_tracker
            .get(&(topic.to_string(), partition))
            .and_then(|state| state.frontier)
    }

    fn acked_offsets(&self) -> HashMap<(String, i32), i64> {
        self.ack_tracker
            .iter()
            .filter_map(|(key, state)| state.commit_value().map(|offset| (key.clone(), offset)))
            .collect()
    }

    /// Commit `frontier + 1` for advanced partitions that this group member
    /// still owns. Revoked partition state is discarded so this member cannot
    /// regress the new owner's committed offset.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer rejects the selected commits.
    pub(crate) async fn commit_acked(&mut self) -> Result<(), GatewayError> {
        let consumer = self
            .consumer
            .as_ref()
            .ok_or_else(|| GatewayError::Other("consume session is closed".to_string()))?;
        let owned: HashSet<_> = consumer.assignment().await.into_iter().collect();
        self.ack_tracker.retain(|key, _| owned.contains(key));

        let offsets = self.acked_offsets();
        if offsets.is_empty() {
            return Ok(());
        }
        consumer
            .commit_offsets_sync(
                offsets
                    .iter()
                    .map(|(key, offset)| {
                        (
                            key.clone(),
                            krabka_client_consumer::OffsetAndMetadata::new(*offset),
                        )
                    })
                    .collect(),
            )
            .await?;
        for (key, next_offset) in offsets {
            if let Some(state) = self.ack_tracker.get_mut(&key) {
                state.last_committed_frontier = next_offset.checked_sub(1);
            }
        }
        Ok(())
    }
}

impl Drop for ConsumeSession {
    fn drop(&mut self) {
        if let Some(consumer) = self.consumer.take() {
            // The underlying `Consumer` runs a background coordinator task
            // (heartbeat + rebalance loop) that is torn down ONLY by
            // `Consumer::close()`. Merely dropping the consumer detaches that
            // task's `JoinHandle`, so it keeps heartbeating forever — leaking a
            // task + socket and orphaning a live group member (which stalls
            // rebalances for the rest of the group). Streaming drops sessions on
            // EVERY exit path (control-stream close/error, any break, or an
            // abrupt client disconnect dropping the response generator), so the
            // teardown belongs here.
            //
            // `close()` is async and consumes `self`, so spawn it detached. The
            // gateway always drops sessions inside the server's tokio runtime,
            // so a runtime is guaranteed to be available here.
            tokio::spawn(async move {
                let _ = consumer.close().await;
            });
        }
    }
}

#[cfg(test)]
mod ack_tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn first_ack_lazily_seeds_frontier() {
        let mut state = PartitionAckState::default();
        assert!(state.record_ack(5).is_ok());
        assert!(state.commit_value() == Some(6));
    }

    #[test]
    fn delivered_lower_offset_prevents_filtered_tail_from_skipping_a_gap() {
        let mut state = PartitionAckState::default();
        state.record_delivery(10).unwrap();
        state.record_delivery(11).unwrap();

        assert!(state.record_ack(11).is_ok());

        assert!(state.commit_value().is_none());
        assert!(state.pending == BTreeSet::from([11]));
    }

    #[test]
    fn in_order_and_gap_filling_acks_advance_contiguously() {
        let mut state = PartitionAckState::default();
        for offset in 10..=13 {
            state.record_delivery(offset).unwrap();
        }
        for offset in [10, 12, 13] {
            assert!(state.record_ack(offset).is_ok());
        }
        assert!(state.commit_value() == Some(11));
        assert!(state.pending == BTreeSet::from([12, 13]));

        assert!(state.record_ack(11).is_ok());

        assert!(state.commit_value() == Some(14));
        assert!(state.pending.is_empty());
    }

    #[test]
    fn duplicate_and_lower_acks_are_idempotent() {
        let mut state = PartitionAckState::default();
        for offset in [10, 10, 3] {
            assert!(state.record_ack(offset).is_ok());
        }
        assert!(state.commit_value() == Some(11));
    }

    #[test]
    fn unchanged_frontier_is_not_recommitted() {
        let mut state = PartitionAckState::default();
        assert!(state.record_ack(10).is_ok());
        state.last_committed_frontier = Some(10);
        assert!(state.commit_value().is_none());
    }

    #[test]
    fn outstanding_cap_blocks_growth_but_allows_gap_filling_acknowledgements() {
        let mut state = PartitionAckState::default();
        let cap = i64::try_from(MAX_OUTSTANDING_PER_PARTITION).unwrap();
        for offset in 0..cap {
            state.record_delivery(offset).unwrap();
        }
        for offset in 1..cap {
            assert!(state.record_ack(offset).is_ok());
        }

        assert!(state.record_delivery(cap).is_err());
        assert!(
            state.record_delivery(cap - 1).is_ok(),
            "duplicate delivery needs no capacity"
        );
        assert!(
            state.record_ack(cap - 1).is_ok(),
            "duplicate ack needs no capacity"
        );
        assert!(
            state.record_ack(0).is_ok(),
            "the oldest ack can release a full window"
        );
        assert!(state.commit_value() == Some(cap));
        assert!(state.delivered.is_empty());
        assert!(state.pending.is_empty());
        assert!(state.record_delivery(cap).is_ok());
    }

    #[test]
    fn sparse_in_order_deliveries_do_not_require_invisible_offsets() {
        let mut state = PartitionAckState::default();
        for offset in [10, 12] {
            state.record_delivery(offset).unwrap();
        }
        state.record_ack(10).unwrap();
        state.record_ack(12).unwrap();
        assert!(state.commit_value() == Some(13));
        assert!(state.delivered.is_empty());
        assert!(state.pending.is_empty());
    }

    #[test]
    fn sparse_out_of_order_ack_cannot_skip_an_earlier_visible_delivery() {
        let mut state = PartitionAckState::default();
        for offset in [10, 12] {
            state.record_delivery(offset).unwrap();
        }
        state.record_ack(12).unwrap();
        assert!(state.commit_value().is_none());
        state.record_ack(10).unwrap();
        assert!(state.commit_value() == Some(13));
    }

    #[test]
    fn complete_gapped_poll_registration_protects_deliveries_from_filtered_tail() {
        let mut state = PartitionAckState::default();
        for offset in [10, 12, 15] {
            state.record_delivery(offset).unwrap();
        }
        // The server filters 15. The client still owes acknowledgements for
        // both visible records, even though their offsets are not consecutive.
        state.record_ack(15).unwrap();
        assert!(state.commit_value().is_none());
        state.record_ack(10).unwrap();
        assert!(state.commit_value() == Some(11));
        state.record_ack(12).unwrap();
        assert!(state.commit_value() == Some(16));
    }

    #[test]
    fn repeated_delivery_below_frontier_does_not_create_an_immortal_head() {
        let mut state = PartitionAckState::default();
        for offset in [10, 12] {
            state.record_delivery(offset).unwrap();
        }
        state.record_ack(10).unwrap();
        for offset in [10, 3, 12] {
            state.record_delivery(offset).unwrap();
        }
        state.record_ack(12).unwrap();
        assert!(state.commit_value() == Some(13));
        assert!(state.delivered.is_empty());
    }

    #[test]
    fn unregistered_ack_does_not_invent_a_visible_delivery() {
        let mut state = PartitionAckState::default();
        state.record_delivery(10).unwrap();
        assert!(state.record_ack(12).is_err());
        state.record_ack(10).unwrap();
        assert!(state.commit_value() == Some(11));
    }

    #[test]
    fn maximum_offset_acknowledgement_does_not_overflow() {
        let mut state = PartitionAckState::default();
        state.record_delivery(i64::MAX).unwrap();
        state.record_ack(i64::MAX).unwrap();
        assert!(state.frontier == Some(i64::MAX));
        assert!(state.commit_value().is_none());
    }
}
