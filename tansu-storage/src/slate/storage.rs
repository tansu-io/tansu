// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Storage trait implementation for SlateDB Engine

use std::{
    collections::BTreeMap,
    iter,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use serde::Serialize;
use tansu_sans_io::{
    BatchAttribute, ConfigResource, ConfigSource, ConfigType, Encoder, ErrorCode, IsolationLevel,
    ListOffset,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::{deflated::Batch, inflated::Batch as InflatedBatch},
    to_system_time,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use tansu_schema::lake::LakeHouse as _;
use tracing::debug;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, Error, GroupDetail, ListOffsetResponse, MetadataResponse,
    NULL_TOPIC_ID, NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result,
    Storage, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, TxnState, UpdateError, Version,
};

use super::engine::Engine;
use super::types::{
    BatchKey, BrokerInfo, Brokers, GroupDetailVersion, GroupKey, OffsetCommitKey,
    OffsetCommitValue, Producers, TopicMetadata, Topics, Transactions, Txn, TxnCommitOffset,
    TxnDetail, TxnProduceOffset, Watermark, WatermarkKey,
};

#[async_trait]
impl Storage for Engine {
    /// Register a broker in the cluster.
    ///
    /// Persists broker information to SlateDB under the `BROKERS` key.
    ///
    /// # TODO
    /// - **Scalability**: Loading/Saving the entire `BROKERS` map is inefficient and causes contention.
    /// - **Liveness**: Implement broker liveness tracking (heartbeat/TTL mechanism).
    /// - **Cleanup**: Currently brokers are never removed from storage, causing stale entries to accumulate.
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        debug!(?broker_registration);

        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut brokers: Brokers = self.load_metadata(&tx, Self::BROKERS).await?;

        // Persist broker info to storage
        // NOTE: This is stored permanently - no cleanup mechanism exists yet
        let broker_info = BrokerInfo {
            broker_id: self.node,
            host: self
                .advertised_listener
                .host_str()
                .unwrap_or("0.0.0.0")
                .into(),
            port: self.advertised_listener.port().unwrap_or(9092).into(),
            rack: broker_registration.rack,
        };

        _ = brokers.insert(self.node, broker_info);
        self.save_metadata(&tx, Self::BROKERS, &brokers)?;

        tx.commit().await.map_err(Error::from)?;

        Ok(())
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let stored_brokers = self
            .db
            .get(Self::BROKERS)
            .await
            .map_err(Error::from)
            .and_then(|brokers| {
                brokers.map_or(Ok(Brokers::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })?;

        if stored_brokers.is_empty() {
            // Return self as the only broker if no registrations yet
            let broker_id = self.node;
            let host = self
                .advertised_listener
                .host_str()
                .unwrap_or("0.0.0.0")
                .into();
            let port = self.advertised_listener.port().unwrap_or(9092).into();

            Ok(vec![
                DescribeClusterBroker::default()
                    .broker_id(broker_id)
                    .host(host)
                    .port(port)
                    .rack(None),
            ])
        } else {
            Ok(stored_brokers
                .values()
                .map(|info| {
                    DescribeClusterBroker::default()
                        .broker_id(info.broker_id)
                        .host(info.host.clone())
                        .port(info.port)
                        .rack(info.rack.clone())
                })
                .collect())
        }
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        // TODO: Implement validate_only mode properly.
        // Currently, it logs a warning but proceeds with creation, which violates the protocol contract.
        // It should validate the config and return without side effects.
        if validate_only {
            tracing::warn!("validate_only mode is not implemented, proceeding with creation");
        }
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        // NOTE: Contention Hotspot
        // Reading the entire TOPICS map creates a serialization bottleneck and high conflict rate
        // for concurrent topic creation.
        let mut topics: Topics = self.load_metadata(&tx, Self::TOPICS).await?;

        let name = topic.name.clone();

        if topics.contains_key(&name[..]) {
            return Err(Error::Api(ErrorCode::TopicAlreadyExists));
        }

        let id = Uuid::now_v7();
        let td = TopicMetadata { id, topic };

        _ = topics.insert(name, td);
        self.save_metadata(&tx, Self::TOPICS, &topics)?;

        tx.commit().await.map_err(Error::from).and(Ok(id))
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let all_topics: Topics = self.load_metadata(&tx, Self::TOPICS).await?;

        let mut results = Vec::with_capacity(topics.len());

        for topic in topics {
            let mut partition_results = vec![];

            let topic_metadata = all_topics.get(&topic.name[..]);

            if let Some(partitions) = topic.partitions.as_ref() {
                for partition in partitions {
                    let error_code = if let Some(metadata) = topic_metadata {
                        if partition.partition_index < 0
                            || partition.partition_index >= metadata.topic.num_partitions
                        {
                            ErrorCode::UnknownTopicOrPartition
                        } else {
                            // Update the low watermark for this partition
                            let watermark_key = postcard::to_stdvec(&WatermarkKey::new(
                                metadata.id,
                                partition.partition_index,
                            ))?;

                            let mut watermark =
                                tx.get(&watermark_key).await.map_err(Error::from).and_then(
                                    |watermark| {
                                        watermark.map_or(Ok(Watermark::default()), |encoded| {
                                            postcard::from_bytes(&encoded[..]).map_err(Into::into)
                                        })
                                    },
                                )?;

                            // Update low watermark to the requested offset
                            // Only if it's greater than current low watermark
                            let new_low = partition.offset;
                            if new_low > watermark.low.unwrap_or(0) {
                                watermark.low = Some(new_low);

                                // Remove timestamps before the new low watermark
                                if let Some(ref mut timestamps) = watermark.timestamps {
                                    timestamps.retain(|_, offset| *offset >= new_low);
                                }

                                let watermark_value = postcard::to_stdvec(&watermark)?;
                                tx.put(&watermark_key, watermark_value)?;
                            }

                            ErrorCode::None
                        }
                    } else {
                        ErrorCode::UnknownTopicOrPartition
                    };

                    partition_results.push(
                        DeleteRecordsPartitionResult::default()
                            .partition_index(partition.partition_index)
                            .low_watermark(partition.offset)
                            .error_code(error_code.into()),
                    );
                }
            }

            results.push(
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partition_results)),
            );
        }

        tx.commit().await.map_err(Error::from)?;

        Ok(results)
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut topics: Topics = self.load_metadata(&tx, Self::TOPICS).await?;

        let topic_name = match topic {
            TopicId::Name(name) => name.clone(),
            TopicId::Id(id) => {
                if let Some((name, _)) = topics.iter().find(|(_, tm)| tm.id == *id) {
                    name.clone()
                } else {
                    return Ok(ErrorCode::UnknownTopicOrPartition);
                }
            }
        };

        if topics.remove(&topic_name).is_none() {
            return Ok(ErrorCode::UnknownTopicOrPartition);
        }

        self.save_metadata(&tx, Self::TOPICS, &topics)?;

        tx.commit().await.map_err(Error::from)?;

        Ok(ErrorCode::None)
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        // For now, just return success for all resource types
        // This matches the simple behavior in dynostore
        Ok(AlterConfigsResourceResponse::default()
            .error_code(ErrorCode::None.into())
            .error_message(Some("".into()))
            .resource_type(resource.resource_type)
            .resource_name(resource.resource_name))
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: Batch,
    ) -> Result<i64> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let topics: Topics = self.load_metadata(&tx, Self::TOPICS).await?;

        let Some(metadata) = topics.get(&topition.topic[..]) else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        if topition.partition < 0 || topition.partition >= metadata.topic.num_partitions {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        }

        // Schema validation (if schemas registry is configured)
        if let Some(ref schemas) = self.schemas {
            let inflated = InflatedBatch::try_from(deflated.clone())?;
            let attributes = BatchAttribute::try_from(inflated.attributes)?;

            // Only validate non-control batches
            if !attributes.control {
                schemas.validate(topition.topic(), &inflated).await?;
            }
        }

        // Idempotent message check
        if deflated.is_idempotent() {
            // NOTE: Contention Hotspot
            // Loading all producers to check/update sequence numbers prevents high-throughput idempotent production.
            // This map needs to be sharded or converted to per-producer keys.
            let mut producers: Producers = self.load_metadata(&tx, Self::PRODUCERS).await?;

            let Some(producer_detail) = producers.get_mut(&deflated.producer_id) else {
                return Err(Error::Api(ErrorCode::UnknownProducerId));
            };

            // Get current epoch for this producer
            let Some(current_epoch) = producer_detail.sequences.last_key_value().map(|(e, _)| *e)
            else {
                return Err(Error::Api(ErrorCode::UnknownProducerId));
            };

            // Get current sequence for this topic/partition
            let current_sequence = producer_detail
                .sequences
                .get(&deflated.producer_epoch)
                .and_then(|topics| topics.get(&topition.topic))
                .and_then(|partitions| partitions.get(&topition.partition))
                .copied()
                .unwrap_or(0);

            debug!(
                producer_id = deflated.producer_id,
                producer_epoch = deflated.producer_epoch,
                current_epoch,
                current_sequence,
                base_sequence = deflated.base_sequence,
            );

            // Check sequence validity
            let increment =
                Self::idempotent_sequence_check(&current_epoch, &current_sequence, &deflated)?;

            // Update sequence
            _ = producer_detail
                .sequences
                .entry(deflated.producer_epoch)
                .or_default()
                .entry(topition.topic.clone())
                .or_default()
                .insert(topition.partition, current_sequence + increment);

            // Save updated producers
            self.save_metadata(&tx, Self::PRODUCERS, &producers)?;
        }

        // Handle transactional produce
        if let Some(transaction_id) = transaction_id {
            // NOTE: Contention Hotspot
            // Loading all transactions to update state is not scalable.
            // Also, there is no cleanup mechanism for completed transactions, leading to unbounded growth of this blob.
            let mut transactions: Transactions =
                self.load_metadata(&tx, Self::TRANSACTIONS).await?;

            if let Some(txn) = transactions.get_mut(transaction_id) {
                if let Some(txn_detail) = txn.epochs.get_mut(&deflated.producer_epoch) {
                    let offset_start = self
                        .db
                        .get(postcard::to_stdvec(&WatermarkKey::new(
                            metadata.id,
                            topition.partition,
                        ))?)
                        .await
                        .map_err(Error::from)
                        .and_then(|watermark| {
                            watermark.map_or(Ok(Watermark::default()), |encoded| {
                                postcard::from_bytes(&encoded[..]).map_err(Into::into)
                            })
                        })?
                        .high
                        .unwrap_or(0);

                    let offset_end = offset_start + deflated.last_offset_delta as i64;

                    _ = txn_detail
                        .produces
                        .entry(topition.topic.clone())
                        .or_default()
                        .insert(
                            topition.partition,
                            Some(TxnProduceOffset {
                                offset_start,
                                offset_end,
                            }),
                        );

                    self.save_metadata(&tx, Self::TRANSACTIONS, &transactions)?;
                }
            }
        }

        let mut watermark = tx
            .get(postcard::to_stdvec(&WatermarkKey::new(
                metadata.id,
                topition.partition,
            ))?)
            .await
            .map_err(Error::from)
            .and_then(|watermark| {
                watermark.map_or(Ok(Watermark::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })?;

        let offset = watermark.high.unwrap_or_default();

        watermark.high = watermark
            .high
            .map_or(Some(deflated.last_offset_delta as i64 + 1i64), |high| {
                Some(high + deflated.last_offset_delta as i64 + 1i64)
            });

        _ = watermark
            .timestamps
            .get_or_insert_default()
            .insert(deflated.base_timestamp, offset);

        debug!(?watermark);

        let encoded = {
            let mut writer = BytesMut::new().writer();
            let mut encoder = Encoder::new(&mut writer);
            deflated.serialize(&mut encoder)?;

            Bytes::from(writer.into_inner())
        };

        let batch_key =
            postcard::to_stdvec(&BatchKey::new(metadata.id, topition.partition, offset))?;

        tx.put(batch_key, &encoded[..])?;

        // Also save the updated watermark
        let watermark_key =
            postcard::to_stdvec(&WatermarkKey::new(metadata.id, topition.partition))?;
        let watermark_value = postcard::to_stdvec(&watermark)?;
        tx.put(watermark_key, watermark_value)?;

        // Store to data lake if configured
        if let Some(ref lake) = self.lake {
            let inflated = InflatedBatch::try_from(deflated.clone())?;
            let attributes = BatchAttribute::try_from(inflated.attributes)?;

            if !attributes.control {
                // TODO: Optimization - Avoid synchronous call
                // Loading config and writing to lake synchronously inside the transaction critical path
                // increases latency and lock holding time. Consider moving this to an async background task.
                let config = self
                    .describe_config(topition.topic(), ConfigResource::Topic, None)
                    .await?;

                lake.store(
                    topition.topic(),
                    topition.partition(),
                    offset,
                    &inflated,
                    config,
                )
                .await?;
            }
        }

        tx.commit().await.map_err(Error::from).and(Ok(offset))
    }

    async fn fetch(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<Batch>> {
        // Get the high watermark based on isolation level
        let offset_stage = self.offset_stage(topition).await?;
        let high_watermark = if isolation_level == IsolationLevel::ReadCommitted {
            offset_stage.last_stable
        } else {
            offset_stage.high_watermark
        };

        debug!(
            ?isolation_level,
            high_watermark, offset, min_bytes, max_bytes
        );

        let topics = self
            .db
            .get(Self::TOPICS)
            .await
            .map_err(Error::from)
            .and_then(|topics| {
                topics.map_or(Ok(Topics::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })?;

        let Some(metadata) = topics.get(&topition.topic[..]) else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        if topition.partition < 0 || topition.partition >= metadata.topic.num_partitions {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        }

        let mut i = {
            let from = postcard::to_stdvec(&BatchKey::scan_from(
                metadata.id,
                topition.partition,
                offset,
            ))?;

            self.db.scan(from..).await?
        };

        let mut batches = vec![];
        let mut total_bytes: usize = 0;
        let min_bytes = min_bytes as usize;
        let max_bytes = max_bytes as usize;

        while let Some(kv) = i.next().await? {
            let size = kv.value.len();

            let key: BatchKey = postcard::from_bytes(&kv.key)?;

            // Stop if we've reached the high watermark (respecting isolation level)
            if key.offset >= high_watermark {
                break;
            }

            // TODO: Performance - Avoid full decode
            // We decode the entire batch just to check size limits or return it.
            // For scanning/filtering, we should only decode the header or use a lightweight check.
            let mut batch = self.decode(kv.value)?;
            batch.base_offset = key.offset;
            batches.push(batch);
            total_bytes += size;

            // Stop if we've exceeded max_bytes (unless we haven't reached min_bytes yet)
            if total_bytes >= max_bytes
                || (total_bytes >= min_bytes && size > (max_bytes - total_bytes))
            {
                break;
            }
        }

        Ok(batches)
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let topics = self.get_topics().await?;

        let Some(metadata) = topics.get(&topition.topic[..]) else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        if topition.partition < 0 || topition.partition >= metadata.topic.num_partitions {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        }

        let watermark_key =
            postcard::to_stdvec(&WatermarkKey::new(metadata.id, topition.partition))?;

        let watermark = self
            .db
            .get(&watermark_key)
            .await
            .map_err(Error::from)
            .and_then(|watermark| {
                watermark.map_or(Ok(Watermark::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })?;

        let high_watermark = watermark.high.unwrap_or(0);

        // Calculate last_stable by finding the minimum offset of any in-progress transaction
        let transactions = self.get_transactions().await?;
        let mut last_stable = high_watermark;

        for txn in transactions.values() {
            for txn_detail in txn.epochs.values() {
                // Only consider transactions that are in-progress (Begin state)
                if txn_detail.state == Some(TxnState::Begin) {
                    // Check if this transaction has produced to this topic/partition
                    if let Some(partitions) = txn_detail.produces.get(&topition.topic) {
                        if let Some(Some(offset_range)) = partitions.get(&topition.partition) {
                            // The last_stable should be the minimum of all in-flight txn start offsets
                            last_stable = last_stable.min(offset_range.offset_start);
                        }
                    }
                }
            }
        }

        Ok(OffsetStage {
            log_start: watermark.low.unwrap_or(0),
            last_stable,
            high_watermark,
        })
    }

    async fn offset_commit(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        // TODO: Implement offset retention
        // Offsets should expire after the configured retention period.
        // Currently, they are stored indefinitely, leading to storage leaks.
        if retention.is_some() {
            tracing::warn!(
                "offset retention is not implemented, offsets will be kept indefinitely"
            );
        }
        // NOTE: Reading global TOPICS map for validation is inefficient.
        let topics = self.get_topics().await?;
        let mut responses = Vec::with_capacity(offsets.len());

        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        for (topition, offset_commit) in offsets {
            // Verify topic exists
            if !topics.contains_key(&topition.topic[..]) {
                responses.push((topition.clone(), ErrorCode::UnknownTopicOrPartition));
                continue;
            }

            let key = postcard::to_stdvec(&OffsetCommitKey::new(
                group,
                &topition.topic,
                topition.partition,
            ))?;

            let value = postcard::to_stdvec(&OffsetCommitValue {
                offset: offset_commit.offset,
                leader_epoch: offset_commit.leader_epoch,
                metadata: offset_commit.metadata.clone(),
            })?;

            tx.put(key, value)?;
            responses.push((topition.clone(), ErrorCode::None));
        }

        tx.commit().await.map_err(Error::from)?;

        Ok(responses)
    }

    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        let prefix = postcard::to_stdvec(&OffsetCommitKey::group_prefix(group_id))?;

        let mut topitions = BTreeMap::new();
        let mut scan = self.db.scan(prefix.clone()..).await?;

        while let Some(kv) = scan.next().await? {
            // Check if key still has our prefix
            if !kv.key.starts_with(&prefix) {
                break;
            }

            let key: OffsetCommitKey = postcard::from_bytes(&kv.key)?;
            let value: OffsetCommitValue = postcard::from_bytes(&kv.value)?;

            _ = topitions.insert(Topition::new(key.topic, key.partition), value.offset);
        }

        Ok(topitions)
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        // TODO: Implement require_stable
        // When true, we must return the Last Stable Offset instead of High Watermark.
        // Current implementation violates READ_COMMITTED isolation by returning potentially unstable offsets.
        if require_stable == Some(true) {
            tracing::warn!(
                "require_stable is not implemented, returning potentially unstable offsets"
            );
        }
        let mut responses = BTreeMap::new();

        if let Some(group_id) = group_id {
            for topition in topics {
                let key = postcard::to_stdvec(&OffsetCommitKey::new(
                    group_id,
                    &topition.topic,
                    topition.partition,
                ))?;

                let offset = match self.db.get(&key).await {
                    Ok(Some(encoded)) => {
                        let value: OffsetCommitValue = postcard::from_bytes(&encoded)?;
                        value.offset
                    }
                    Ok(None) => -1, // No committed offset
                    Err(err) => {
                        debug!(?err, ?group_id, ?topition);
                        return Err(Error::Slate(Arc::new(err)));
                    }
                };

                _ = responses.insert(topition.clone(), offset);
            }
        }

        Ok(responses)
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        // TODO: Implement isolation_level for list_offsets
        // For IsolationLevel::ReadCommitted, we must return the Last Stable Offset.
        // Returning High Watermark here can expose uncommitted data to consumers.
        if isolation_level == IsolationLevel::ReadCommitted {
            tracing::warn!("list_offsets ignores isolation_level, always returns high_watermark");
        }
        let topics = self.get_topics().await?;
        let mut responses = Vec::with_capacity(offsets.len());

        for (topition, list_offset) in offsets {
            let Some(metadata) = topics.get(&topition.topic[..]) else {
                responses.push((
                    topition.clone(),
                    ListOffsetResponse {
                        error_code: ErrorCode::UnknownTopicOrPartition,
                        offset: None,
                        timestamp: None,
                    },
                ));
                continue;
            };

            if topition.partition < 0 || topition.partition >= metadata.topic.num_partitions {
                responses.push((
                    topition.clone(),
                    ListOffsetResponse {
                        error_code: ErrorCode::UnknownTopicOrPartition,
                        offset: None,
                        timestamp: None,
                    },
                ));
                continue;
            }

            let watermark_key =
                postcard::to_stdvec(&WatermarkKey::new(metadata.id, topition.partition))?;

            let watermark = self
                .db
                .get(&watermark_key)
                .await
                .map_err(Error::from)
                .and_then(|watermark| {
                    watermark.map_or(Ok(Watermark::default()), |encoded| {
                        postcard::from_bytes(&encoded[..]).map_err(Into::into)
                    })
                })?;

            let response = match list_offset {
                ListOffset::Earliest => {
                    if let Some((ts, off)) = watermark
                        .timestamps
                        .as_ref()
                        .and_then(|ts| ts.first_key_value())
                    {
                        ListOffsetResponse {
                            error_code: ErrorCode::None,
                            offset: Some(*off),
                            timestamp: to_system_time(*ts).ok(),
                        }
                    } else {
                        ListOffsetResponse {
                            error_code: ErrorCode::None,
                            offset: Some(0),
                            timestamp: None,
                        }
                    }
                }
                ListOffset::Latest => {
                    let offset = watermark.high.unwrap_or(0);
                    let timestamp = watermark
                        .timestamps
                        .as_ref()
                        .and_then(|ts| ts.last_key_value())
                        .and_then(|(ts, _)| to_system_time(*ts).ok());

                    ListOffsetResponse {
                        error_code: ErrorCode::None,
                        offset: Some(offset),
                        timestamp,
                    }
                }
                ListOffset::Timestamp(target_ts) => {
                    // Find the first offset with timestamp >= target
                    // target_ts is SystemTime, need to convert to i64 for comparison
                    let target_millis = target_ts
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);

                    let result = watermark.timestamps.as_ref().and_then(|ts| {
                        ts.range(target_millis..)
                            .next()
                            .map(|(ts, off)| (*off, *ts))
                    });

                    match result {
                        Some((offset, ts)) => ListOffsetResponse {
                            error_code: ErrorCode::None,
                            offset: Some(offset),
                            timestamp: to_system_time(ts).ok(),
                        },
                        None => ListOffsetResponse {
                            error_code: ErrorCode::None,
                            offset: Some(watermark.high.unwrap_or(0)),
                            timestamp: None,
                        },
                    }
                }
            };

            responses.push((topition.clone(), response));
        }

        Ok(responses)
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        // TODO: Implement topic filtering
        // Currently returns metadata for ALL topics in the cluster, which is inefficient.
        // Should respect the `topics` argument and only return requested metadata.
        if topics.is_some() {
            tracing::warn!("metadata topic filtering is not implemented, returning all topics");
        }
        let brokers = vec![
            MetadataResponseBroker::default()
                .node_id(self.node)
                .host(
                    self.advertised_listener
                        .host_str()
                        .unwrap_or("0.0.0.0")
                        .into(),
                )
                .port(self.advertised_listener.port().unwrap_or(9092).into())
                .rack(None),
        ];

        let existing = self
            .db
            .get(Self::TOPICS)
            .await
            .map_err(Error::from)
            .and_then(|existing| {
                existing.map_or(Ok(Topics::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })
            .map(|existing| {
                existing
                    .into_iter()
                    .map(|(_name, topic_metadata)| {
                        let name = Some(topic_metadata.topic.name.to_owned());
                        let error_code = ErrorCode::None.into();
                        let topic_id = Some(topic_metadata.id.into_bytes());
                        let is_internal = Some(false);
                        let partitions = topic_metadata.topic.num_partitions;
                        let replication_factor = topic_metadata.topic.replication_factor;

                        let partitions = Some(
                            (0..partitions)
                                .map(|partition_index| {
                                    let leader_id = self.node;
                                    let replica_nodes = Some(
                                        iter::repeat_n(self.node, replication_factor as usize)
                                            .collect(),
                                    );
                                    let isr_nodes = replica_nodes.clone();

                                    MetadataResponsePartition::default()
                                        .error_code(error_code)
                                        .partition_index(partition_index)
                                        .leader_id(leader_id)
                                        .leader_epoch(Some(-1))
                                        .replica_nodes(replica_nodes)
                                        .isr_nodes(isr_nodes)
                                        .offline_replicas(Some([].into()))
                                })
                                .collect(),
                        );

                        MetadataResponseTopic::default()
                            .error_code(error_code)
                            .name(name)
                            .topic_id(topic_id)
                            .is_internal(is_internal)
                            .partitions(partitions)
                            .topic_authorized_operations(Some(i32::MIN))
                    })
                    .collect()
            })?;

        Ok(MetadataResponse {
            cluster: Some(self.cluster.clone()),
            controller: Some(self.node),
            brokers,
            topics: existing,
        })
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        // TODO: Filter config entries by requested keys
        if keys.is_some() {
            tracing::warn!(
                "describe_config key filtering is not implemented, returning all configs"
            );
        }
        match resource {
            ConfigResource::Topic => match self.topic_metadata(&TopicId::Name(name.into())).await {
                Ok(Some(topic_metadata)) => {
                    let error_code = ErrorCode::None;

                    Ok(DescribeConfigsResult::default()
                        .error_code(error_code.into())
                        .error_message(Some(error_code.to_string()))
                        .resource_type(i8::from(resource))
                        .resource_name(name.into())
                        .configs(topic_metadata.topic.configs.map(|configs| {
                            configs
                                .iter()
                                .map(|config| {
                                    DescribeConfigsResourceResult::default()
                                        .name(config.name.clone())
                                        .value(config.value.clone())
                                        .read_only(false)
                                        .is_default(None)
                                        .config_source(Some(ConfigSource::DefaultConfig.into()))
                                        .is_sensitive(false)
                                        .synonyms(Some([].into()))
                                        .config_type(Some(ConfigType::String.into()))
                                        .documentation(None)
                                })
                                .collect()
                        })))
                }

                Ok(None) => {
                    let error_code = ErrorCode::UnknownTopicOrPartition;

                    Ok(DescribeConfigsResult::default()
                        .error_code(error_code.into())
                        .error_message(Some(error_code.to_string()))
                        .resource_type(i8::from(resource))
                        .resource_name(name.into())
                        .configs(Some([].into())))
                }

                Err(_) => {
                    let error_code = ErrorCode::UnknownServerError;

                    Ok(DescribeConfigsResult::default()
                        .error_code(error_code.into())
                        .error_message(Some(error_code.to_string()))
                        .resource_type(i8::from(resource))
                        .resource_name(name.into())
                        .configs(Some([].into())))
                }
            },
            _ => {
                // For other resource types, return empty config
                Ok(DescribeConfigsResult::default()
                    .error_code(ErrorCode::None.into())
                    .error_message(Some(ErrorCode::None.to_string()))
                    .resource_type(i8::from(resource))
                    .resource_name(name.into())
                    .configs(Some([].into())))
            }
        }
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        // TODO: Implement pagination with partition_limit and cursor
        // Currently returns all partitions regardless of limit
        if partition_limit > 0 || cursor.is_some() {
            tracing::warn!(
                "describe_topic_partitions pagination is not implemented, returning all partitions"
            );
        }
        let mut responses =
            Vec::with_capacity(topics.map(|topics| topics.len()).unwrap_or_default());

        for topic in topics.unwrap_or_default() {
            match self.topic_metadata(topic).await {
                Ok(Some(topic_metadata)) => {
                    responses.push(
                        DescribeTopicPartitionsResponseTopic::default()
                            .error_code(ErrorCode::None.into())
                            .name(Some(topic_metadata.topic.name))
                            .topic_id(topic.into())
                            .is_internal(false)
                            .partitions(Some(
                                (0..topic_metadata.topic.num_partitions)
                                    .map(|partition_index| {
                                        DescribeTopicPartitionsResponsePartition::default()
                                            .error_code(ErrorCode::None.into())
                                            .partition_index(partition_index)
                                            .leader_id(self.node)
                                            .leader_epoch(-1)
                                            .replica_nodes(Some(vec![
                                                self.node;
                                                topic_metadata.topic.replication_factor
                                                    as usize
                                            ]))
                                            .isr_nodes(Some(vec![
                                                self.node;
                                                topic_metadata.topic.replication_factor
                                                    as usize
                                            ]))
                                            .eligible_leader_replicas(Some(vec![]))
                                            .last_known_elr(Some(vec![]))
                                            .offline_replicas(Some(vec![]))
                                    })
                                    .collect(),
                            ))
                            .topic_authorized_operations(-2147483648),
                    );
                }

                Ok(None) => {
                    responses.push(
                        DescribeTopicPartitionsResponseTopic::default()
                            .error_code(ErrorCode::UnknownTopicOrPartition.into())
                            .name(match topic {
                                TopicId::Name(name) => Some(name.into()),
                                TopicId::Id(_) => None,
                            })
                            .topic_id(match topic {
                                TopicId::Name(_) => NULL_TOPIC_ID,
                                TopicId::Id(id) => id.into_bytes(),
                            })
                            .is_internal(false)
                            .partitions(Some([].into()))
                            .topic_authorized_operations(-2147483648),
                    );
                }

                Err(_) => {
                    responses.push(
                        DescribeTopicPartitionsResponseTopic::default()
                            .error_code(ErrorCode::UnknownServerError.into())
                            .name(match topic {
                                TopicId::Name(name) => Some(name.into()),
                                TopicId::Id(_) => None,
                            })
                            .topic_id(match topic {
                                TopicId::Name(_) => NULL_TOPIC_ID,
                                TopicId::Id(id) => id.into_bytes(),
                            })
                            .is_internal(false)
                            .partitions(Some([].into()))
                            .topic_authorized_operations(-2147483648),
                    );
                }
            }
        }

        Ok(responses)
    }

    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        // TODO: Implement states_filter - should filter groups by their state
        if states_filter.is_some() {
            tracing::warn!("list_groups state filtering is not implemented, returning all groups");
        }
        let prefix = postcard::to_stdvec(&GroupKey::scan_prefix())?;
        let mut groups = vec![];

        let mut scan = self.db.scan(prefix.clone()..).await?;

        while let Some(kv) = scan.next().await? {
            if !kv.key.starts_with(&prefix) {
                break;
            }

            if let Ok(key) = postcard::from_bytes::<GroupKey>(&kv.key) {
                groups.push(
                    ListedGroup::default()
                        .group_id(key.group_id)
                        .protocol_type("consumer".into())
                        .group_state(Some("Unknown".into()))
                        .group_type(Some("classic".into())),
                );
            }
        }

        Ok(groups)
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            let tx = self
                .db
                .begin(slatedb::IsolationLevel::SerializableSnapshot)
                .await
                .inspect_err(|err| debug!(?err))?;

            for group_id in group_ids {
                // Delete group state
                let group_key = postcard::to_stdvec(&GroupKey::new(group_id))?;
                let had_group = tx.get(&group_key).await?.is_some();

                if had_group {
                    tx.delete(&group_key)?;
                }

                // Delete committed offsets for this group
                let offset_prefix = postcard::to_stdvec(&OffsetCommitKey::group_prefix(group_id))?;
                let mut deleted_offsets = false;

                // Note: SlateDB doesn't support range deletes directly,
                // so we scan and delete individually
                let mut scan = self.db.scan(offset_prefix.clone()..).await?;
                while let Some(kv) = scan.next().await? {
                    if !kv.key.starts_with(&offset_prefix) {
                        break;
                    }
                    tx.delete(&kv.key)?;
                    deleted_offsets = true;
                }

                results.push(
                    DeletableGroupResult::default()
                        .group_id(group_id.into())
                        .error_code(
                            if had_group || deleted_offsets {
                                ErrorCode::None
                            } else {
                                ErrorCode::GroupIdNotFound
                            }
                            .into(),
                        ),
                );
            }

            tx.commit().await.map_err(Error::from)?;
        }

        Ok(results)
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        // TODO: Implement include_authorized_operations
        // Should return ACL-based authorized operations for each group
        if include_authorized_operations {
            tracing::warn!(
                "describe_groups authorized_operations is not implemented, returning empty"
            );
        }
        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                let key = postcard::to_stdvec(&GroupKey::new(group_id))?;

                match self.db.get(&key).await {
                    Ok(Some(encoded)) => {
                        match postcard::from_bytes::<GroupDetailVersion>(&encoded) {
                            Ok(gdv) => {
                                results.push(NamedGroupDetail::found(group_id.into(), gdv.detail));
                            }
                            Err(_) => {
                                results.push(NamedGroupDetail::found(
                                    group_id.into(),
                                    GroupDetail::default(),
                                ));
                            }
                        }
                    }
                    Ok(None) => {
                        results.push(NamedGroupDetail::found(
                            group_id.into(),
                            GroupDetail::default(),
                        ));
                    }
                    Err(_) => {
                        results.push(NamedGroupDetail::error_code(
                            group_id.into(),
                            ErrorCode::UnknownServerError,
                        ));
                    }
                }
            }
        }

        Ok(results)
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))
            .map_err(|err| UpdateError::Error(Error::Slate(Arc::new(err))))?;

        let key = postcard::to_stdvec(&GroupKey::new(group_id))
            .map_err(|err| UpdateError::Error(Error::Postcard(err)))?;

        // Try to load existing group
        let current_group: Option<GroupDetailVersion> = self
            .load_metadata(&tx, &key)
            .await
            .map(Some)
            .or_else(|_| Ok::<_, Error>(None))
            .map_err(|err| UpdateError::Error(err))?;

        if let Some(current) = current_group {
            // Check version if provided
            if version.is_some_and(|v| v != current.version) {
                tx.rollback();
                return Err(UpdateError::Outdated {
                    current: current.detail,
                    version: current.version,
                });
            }
        }

        let updated_version = Version::from(&Uuid::now_v7());
        let new_group = GroupDetailVersion::default()
            .detail(detail)
            .version(updated_version.clone());

        self.save_metadata(&tx, &key, &new_group)
            .map_err(|err| UpdateError::Error(err))?;

        tx.commit()
            .await
            .map_err(|err| UpdateError::Error(Error::Slate(Arc::new(err))))
            .and(Ok(updated_version))
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        if let Some(transaction_id) = transaction_id {
            // Transactional producer initialization
            let tx = self
                .db
                .begin(slatedb::IsolationLevel::SerializableSnapshot)
                .await
                .inspect_err(|err| debug!(?err))?;

            let mut transactions: Transactions =
                self.load_metadata(&tx, Self::TRANSACTIONS).await?;
            let mut producers: Producers = self.load_metadata(&tx, Self::PRODUCERS).await?;

            // Check if transaction already exists
            if transactions.contains_key(transaction_id) {
                let existing_txn = transactions.get_mut(transaction_id).unwrap();
                // Bump epoch for existing transaction
                let new_epoch = existing_txn
                    .epochs
                    .last_key_value()
                    .map(|(e, _)| e + 1)
                    .unwrap_or(0);

                _ = existing_txn.epochs.insert(
                    new_epoch,
                    TxnDetail {
                        transaction_timeout_ms,
                        started_at: Some(SystemTime::now()),
                        state: Some(TxnState::Begin),
                        ..Default::default()
                    },
                );

                let producer_id = existing_txn.producer;

                self.save_metadata(&tx, Self::TRANSACTIONS, &transactions)?;

                tx.commit().await.map_err(Error::from)?;

                return Ok(ProducerIdResponse {
                    id: producer_id,
                    epoch: new_epoch,
                    ..Default::default()
                });
            }

            // Create new transactional producer
            let new_producer_id = producers.last_key_value().map_or(1, |(k, _)| k + 1);
            let epoch = 0i16;

            let mut pd = super::types::ProducerDetail::default();
            _ = pd.sequences.insert(epoch, BTreeMap::new());
            _ = producers.insert(new_producer_id, pd);

            let mut txn = Txn {
                producer: new_producer_id,
                epochs: BTreeMap::new(),
            };
            _ = txn.epochs.insert(
                epoch,
                TxnDetail {
                    transaction_timeout_ms,
                    started_at: Some(SystemTime::now()),
                    state: Some(TxnState::Begin),
                    ..Default::default()
                },
            );
            _ = transactions.insert(transaction_id.to_string(), txn);

            self.save_metadata(&tx, Self::PRODUCERS, &producers)?;
            self.save_metadata(&tx, Self::TRANSACTIONS, &transactions)?;

            tx.commit().await.map_err(Error::from)?;

            Ok(ProducerIdResponse {
                id: new_producer_id,
                epoch,
                ..Default::default()
            })
        } else if Some(-1) == producer_id && Some(-1) == producer_epoch {
            let tx = self
                .db
                .begin(slatedb::IsolationLevel::SerializableSnapshot)
                .await
                .inspect_err(|err| debug!(?err))?;

            let mut producers: Producers = self.load_metadata(&tx, Self::PRODUCERS).await?;

            let producer = producers.last_key_value().map_or(1.into(), |(k, _v)| k + 1);

            let epoch = 0;
            let mut pd = super::types::ProducerDetail::default();
            _ = pd.sequences.insert(epoch, BTreeMap::new());
            debug!(?producer, ?pd);
            _ = producers.insert(producer, pd);

            self.save_metadata(&tx, Self::PRODUCERS, &producers)?;

            tx.commit()
                .await
                .map_err(Error::from)
                .and(Ok(ProducerIdResponse {
                    id: producer,
                    epoch,
                    ..Default::default()
                }))
        } else {
            Ok(ProducerIdResponse {
                id: -1,
                epoch: -1,
                error: ErrorCode::UnknownServerError,
            })
        }
    }

    async fn txn_add_offsets(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _group_id: &str,
    ) -> Result<ErrorCode> {
        // TODO: Implement txn_add_offsets
        //
        // This should:
        // 1. Validate the transaction exists and matches producer_id/epoch
        // 2. Add the group_id to the transaction's offset commit set
        // 3. This enables the transaction to commit offsets for this consumer group
        //
        // Currently returns an error to indicate unimplemented status
        Err(Error::Api(ErrorCode::UnknownServerError))
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        match partitions {
            TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id,
                producer_id,
                producer_epoch,
                ref topics,
            } => {
                let tx = self
                    .db
                    .begin(slatedb::IsolationLevel::SerializableSnapshot)
                    .await
                    .inspect_err(|err| debug!(?err))?;

                // Helper to create error responses for all topics/partitions
                let make_error_response =
                    |error_code: ErrorCode| -> Vec<AddPartitionsToTxnTopicResult> {
                        topics
                            .iter()
                            .map(|topic| {
                                let results_by_partition = topic
                                    .partitions
                                    .as_deref()
                                    .unwrap_or(&[])
                                    .iter()
                                    .map(|p| {
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(*p)
                                            .partition_error_code(error_code.into())
                                    })
                                    .collect();
                                AddPartitionsToTxnTopicResult::default()
                                    .name(topic.name.clone())
                                    .results_by_partition(Some(results_by_partition))
                            })
                            .collect()
                    };

                let mut transactions: Transactions =
                    self.load_metadata(&tx, Self::TRANSACTIONS).await?;

                let Some(transaction) = transactions.get_mut(&transaction_id) else {
                    return Ok(TxnAddPartitionsResponse::VersionZeroToThree(
                        make_error_response(ErrorCode::TransactionalIdNotFound),
                    ));
                };

                if transaction.producer != producer_id {
                    return Ok(TxnAddPartitionsResponse::VersionZeroToThree(
                        make_error_response(ErrorCode::UnknownProducerId),
                    ));
                }

                let Some(mut current_epoch) = transaction.epochs.last_entry() else {
                    return Ok(TxnAddPartitionsResponse::VersionZeroToThree(
                        make_error_response(ErrorCode::ProducerFenced),
                    ));
                };

                if &producer_epoch != current_epoch.key() {
                    return Ok(TxnAddPartitionsResponse::VersionZeroToThree(
                        make_error_response(ErrorCode::ProducerFenced),
                    ));
                }

                let txn_detail = current_epoch.get_mut();
                let mut results = vec![];

                for topic in topics {
                    let mut results_by_partition = vec![];
                    for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                        _ = txn_detail
                            .produces
                            .entry(topic.name.clone())
                            .or_default()
                            .insert(*partition_index, None);

                        results_by_partition.push(
                            AddPartitionsToTxnPartitionResult::default()
                                .partition_index(*partition_index)
                                .partition_error_code(ErrorCode::None.into()),
                        );
                    }
                    results.push(
                        AddPartitionsToTxnTopicResult::default()
                            .name(topic.name.clone())
                            .results_by_partition(Some(results_by_partition)),
                    );
                }

                self.save_metadata(&tx, Self::TRANSACTIONS, &transactions)?;

                tx.commit().await.map_err(Error::from)?;

                Ok(TxnAddPartitionsResponse::VersionZeroToThree(results))
            }

            TxnAddPartitionsRequest::VersionFourPlus { transactions } => {
                use tansu_sans_io::add_partitions_to_txn_response::AddPartitionsToTxnResult;

                let tx = self
                    .db
                    .begin(slatedb::IsolationLevel::SerializableSnapshot)
                    .await
                    .inspect_err(|err| debug!(?err))?;

                let mut stored_transactions: Transactions =
                    self.load_metadata(&tx, Self::TRANSACTIONS).await?;

                let mut results = Vec::with_capacity(transactions.len());

                for txn_request in transactions {
                    let transaction_id = txn_request.transactional_id.clone();
                    let producer_id = txn_request.producer_id;
                    let producer_epoch = txn_request.producer_epoch;
                    let topics = txn_request.topics.as_deref().unwrap_or(&[]);

                    let make_topic_results =
                        |error_code: ErrorCode| -> Vec<AddPartitionsToTxnTopicResult> {
                            topics
                                .iter()
                                .map(|topic| {
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(
                                            topic
                                                .partitions
                                                .as_deref()
                                                .unwrap_or(&[])
                                                .iter()
                                                .map(|p| {
                                                    AddPartitionsToTxnPartitionResult::default()
                                                        .partition_index(*p)
                                                        .partition_error_code(error_code.into())
                                                })
                                                .collect(),
                                        ))
                                })
                                .collect()
                        };

                    let topic_results = if let Some(transaction) =
                        stored_transactions.get_mut(&transaction_id)
                    {
                        if transaction.producer != producer_id {
                            make_topic_results(ErrorCode::UnknownProducerId)
                        } else if let Some(mut current_epoch) = transaction.epochs.last_entry() {
                            if &producer_epoch != current_epoch.key() {
                                make_topic_results(ErrorCode::ProducerFenced)
                            } else {
                                // Success - add partitions
                                let txn_detail = current_epoch.get_mut();
                                topics
                                    .iter()
                                    .map(|topic| {
                                        let partition_results: Vec<_> = topic
                                            .partitions
                                            .as_deref()
                                            .unwrap_or(&[])
                                            .iter()
                                            .map(|p| {
                                                _ = txn_detail
                                                    .produces
                                                    .entry(topic.name.clone())
                                                    .or_default()
                                                    .insert(*p, None);

                                                AddPartitionsToTxnPartitionResult::default()
                                                    .partition_index(*p)
                                                    .partition_error_code(ErrorCode::None.into())
                                            })
                                            .collect();

                                        AddPartitionsToTxnTopicResult::default()
                                            .name(topic.name.clone())
                                            .results_by_partition(Some(partition_results))
                                    })
                                    .collect()
                            }
                        } else {
                            // No epoch found
                            make_topic_results(ErrorCode::ProducerFenced)
                        }
                    } else {
                        // Transaction not found
                        make_topic_results(ErrorCode::TransactionalIdNotFound)
                    };

                    results.push(
                        AddPartitionsToTxnResult::default()
                            .transactional_id(transaction_id)
                            .topic_results(Some(topic_results)),
                    );
                }

                self.save_metadata(&tx, Self::TRANSACTIONS, &stored_transactions)?;

                tx.commit().await.map_err(Error::from)?;

                Ok(TxnAddPartitionsResponse::VersionFourPlus(results))
            }
        }
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut transactions: Transactions = self.load_metadata(&tx, Self::TRANSACTIONS).await?;

        let error_response = |error_code: ErrorCode| -> Vec<TxnOffsetCommitResponseTopic> {
            offsets
                .topics
                .iter()
                .map(|topic| {
                    TxnOffsetCommitResponseTopic::default()
                        .name(topic.name.clone())
                        .partitions(Some(
                            topic
                                .partitions
                                .as_deref()
                                .unwrap_or(&[])
                                .iter()
                                .map(|p| {
                                    TxnOffsetCommitResponsePartition::default()
                                        .partition_index(p.partition_index)
                                        .error_code(error_code.into())
                                })
                                .collect(),
                        ))
                })
                .collect()
        };

        let Some(transaction) = transactions.get_mut(&offsets.transaction_id) else {
            return Ok(error_response(ErrorCode::TransactionalIdNotFound));
        };

        if transaction.producer != offsets.producer_id {
            return Ok(error_response(ErrorCode::UnknownProducerId));
        }

        let Some(mut current_epoch) = transaction.epochs.last_entry() else {
            return Ok(error_response(ErrorCode::ProducerFenced));
        };

        if &offsets.producer_epoch != current_epoch.key() {
            return Ok(error_response(ErrorCode::ProducerFenced));
        }

        let txn_detail = current_epoch.get_mut();
        let mut responses = vec![];

        for topic in &offsets.topics {
            let mut partition_responses = vec![];

            if let Some(partitions) = topic.partitions.as_deref() {
                for partition in partitions {
                    _ = txn_detail
                        .offsets
                        .entry(offsets.group_id.clone())
                        .or_default()
                        .entry(topic.name.clone())
                        .or_default()
                        .insert(
                            partition.partition_index,
                            TxnCommitOffset {
                                committed_offset: partition.committed_offset,
                                leader_epoch: partition.committed_leader_epoch,
                                metadata: partition.committed_metadata.clone(),
                            },
                        );

                    partition_responses.push(
                        TxnOffsetCommitResponsePartition::default()
                            .partition_index(partition.partition_index)
                            .error_code(ErrorCode::None.into()),
                    );
                }
            }

            responses.push(
                TxnOffsetCommitResponseTopic::default()
                    .name(topic.name.clone())
                    .partitions(Some(partition_responses)),
            );
        }

        postcard::to_stdvec(&transactions)
            .map_err(Error::from)
            .and_then(|encoded| tx.put(Self::TRANSACTIONS, encoded).map_err(Into::into))?;

        tx.commit().await.map_err(Error::from)?;

        Ok(responses)
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut transactions: Transactions = self.load_metadata(&tx, Self::TRANSACTIONS).await?;

        let Some(transaction) = transactions.get_mut(transaction_id) else {
            return Err(Error::Api(ErrorCode::TransactionalIdNotFound));
        };

        if transaction.producer != producer_id {
            return Err(Error::Api(ErrorCode::UnknownProducerId));
        }

        let Some(mut current_epoch) = transaction.epochs.last_entry() else {
            return Err(Error::Api(ErrorCode::ProducerFenced));
        };

        if &producer_epoch != current_epoch.key() {
            return Err(Error::Api(ErrorCode::ProducerFenced));
        }

        let txn_detail = current_epoch.get_mut();

        if txn_detail.state == Some(TxnState::Begin) {
            txn_detail.state = Some(if committed {
                TxnState::PrepareCommit
            } else {
                TxnState::PrepareAbort
            });
        }

        // If committing, apply the offset commits
        if committed {
            for (group_id, topics) in &txn_detail.offsets {
                for (topic_name, partitions) in topics {
                    for (partition, commit_offset) in partitions {
                        let key = postcard::to_stdvec(&OffsetCommitKey::new(
                            group_id, topic_name, *partition,
                        ))?;

                        let value = postcard::to_stdvec(&OffsetCommitValue {
                            offset: commit_offset.committed_offset,
                            leader_epoch: commit_offset.leader_epoch,
                            metadata: commit_offset.metadata.clone(),
                        })?;

                        tx.put(key, value)?;
                    }
                }
            }
        }

        // Mark transaction as complete
        txn_detail.state = Some(if committed {
            TxnState::Committed
        } else {
            TxnState::Aborted
        });

        self.save_metadata(&tx, Self::TRANSACTIONS, &transactions)?;

        tx.commit().await.map_err(Error::from)?;

        Ok(ErrorCode::None)
    }

    async fn maintain(&self) -> Result<()> {
        Ok(())
    }

    async fn cluster_id(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    async fn node(&self) -> Result<i32> {
        Ok(self.node)
    }

    async fn advertised_listener(&self) -> Result<url::Url> {
        Ok(self.advertised_listener.clone())
    }
}
