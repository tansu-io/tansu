// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

mod common;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::SystemTime,
};

use bytes::Bytes;
use rama::{Context, Service as _};
use tansu_sans_io::{
    BatchAttribute, Compression, ConfigResource, ErrorCode, ProduceRequest,
    create_topics_request::CreatableTopic,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    incremental_alter_configs_request::AlterConfigsResource,
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::ProduceResponse,
    record::{
        Record,
        deflated::{self, Frame},
        inflated,
    },
};
use tansu_storage::{
    BrokerRegistrationRequest, Error as StorageError, GroupDetail, ListOffsetResponse,
    MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProduceService,
    ProducerIdResponse, Result as StorageResult, ScramCredential, Storage, StorageCapabilities,
    StorageCertification, StorageFeature, TopicId, Topition, TxnAddPartitionsRequest,
    TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError, Version,
};
use url::Url;
use uuid::Uuid;

use crate::common::{Error as TestError, build_storage, create_topic, init_tracing};

#[derive(Clone, Debug)]
struct ExactnessStorage {
    inner: Arc<Box<dyn Storage>>,
    produce_calls: Arc<AtomicUsize>,
    produce_error: Option<StorageError>,
}

impl ExactnessStorage {
    fn new(inner: Arc<Box<dyn Storage>>, produce_error: Option<StorageError>) -> Self {
        Self {
            inner,
            produce_calls: Arc::new(AtomicUsize::new(0)),
            produce_error,
        }
    }

    fn produce_calls(&self) -> usize {
        self.produce_calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl Storage for ExactnessStorage {
    fn capabilities(&self) -> StorageCapabilities {
        let mut capabilities = self.inner.capabilities();
        _ = capabilities.features.insert(
            StorageFeature::BatchValidation,
            StorageCertification::DevelopmentTestOnly,
        );
        capabilities
    }

    async fn register_broker(
        &self,
        broker_registration: BrokerRegistrationRequest,
    ) -> StorageResult<()> {
        self.inner.register_broker(broker_registration).await
    }

    async fn create_topic(
        &self,
        topic: CreatableTopic,
        validate_only: bool,
    ) -> StorageResult<Uuid> {
        self.inner.create_topic(topic, validate_only).await
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> StorageResult<
        tansu_sans_io::incremental_alter_configs_response::AlterConfigsResourceResponse,
    > {
        self.inner.incremental_alter_resource(resource).await
    }

    async fn delete_records(
        &self,
        topics: &[tansu_sans_io::delete_records_request::DeleteRecordsTopic],
    ) -> StorageResult<Vec<tansu_sans_io::delete_records_response::DeleteRecordsTopicResult>> {
        self.inner.delete_records(topics).await
    }

    async fn delete_topic(&self, topic: &TopicId) -> StorageResult<ErrorCode> {
        self.inner.delete_topic(topic).await
    }

    async fn brokers(
        &self,
    ) -> StorageResult<Vec<tansu_sans_io::describe_cluster_response::DescribeClusterBroker>> {
        self.inner.brokers().await
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> StorageResult<i64> {
        _ = self.produce_calls.fetch_add(1, Ordering::SeqCst);

        if let Some(error) = self.produce_error.clone() {
            return Err(error);
        }

        self.inner.produce(transaction_id, topition, batch).await
    }

    async fn fetch(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: tansu_sans_io::IsolationLevel,
    ) -> StorageResult<Vec<deflated::Batch>> {
        self.inner
            .fetch(topition, offset, min_bytes, max_bytes, isolation)
            .await
    }

    async fn offset_stage(&self, topition: &Topition) -> StorageResult<OffsetStage> {
        self.inner.offset_stage(topition).await
    }

    async fn list_offsets(
        &self,
        isolation_level: tansu_sans_io::IsolationLevel,
        offsets: &[(Topition, tansu_sans_io::ListOffset)],
    ) -> StorageResult<Vec<(Topition, ListOffsetResponse)>> {
        self.inner.list_offsets(isolation_level, offsets).await
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<std::time::Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> StorageResult<Vec<(Topition, ErrorCode)>> {
        self.inner
            .offset_commit(group_id, retention_time_ms, offsets)
            .await
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> StorageResult<std::collections::BTreeMap<Topition, i64>> {
        self.inner
            .offset_fetch(group_id, topics, require_stable)
            .await
    }

    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> StorageResult<std::collections::BTreeMap<Topition, i64>> {
        self.inner.committed_offset_topitions(group_id).await
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> StorageResult<MetadataResponse> {
        self.inner.metadata(topics).await
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: tansu_sans_io::ScramMechanism,
        credential: ScramCredential,
    ) -> StorageResult<()> {
        self.inner
            .upsert_user_scram_credential(user, mechanism, credential)
            .await
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: tansu_sans_io::ScramMechanism,
    ) -> StorageResult<()> {
        self.inner
            .delete_user_scram_credential(user, mechanism)
            .await
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: tansu_sans_io::ScramMechanism,
    ) -> StorageResult<Option<ScramCredential>> {
        self.inner.user_scram_credential(user, mechanism).await
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> StorageResult<DescribeConfigsResult> {
        self.inner.describe_config(name, resource, keys).await
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> StorageResult<Vec<DescribeTopicPartitionsResponseTopic>> {
        self.inner
            .describe_topic_partitions(topics, partition_limit, cursor)
            .await
    }

    async fn list_groups(
        &self,
        states: Option<&[String]>,
    ) -> StorageResult<Vec<tansu_sans_io::list_groups_response::ListedGroup>> {
        self.inner.list_groups(states).await
    }

    async fn delete_groups(
        &self,
        groups: Option<&[String]>,
    ) -> StorageResult<Vec<tansu_sans_io::delete_groups_response::DeletableGroupResult>> {
        self.inner.delete_groups(groups).await
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> StorageResult<Vec<NamedGroupDetail>> {
        self.inner
            .describe_groups(group_ids, include_authorized_operations)
            .await
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        self.inner.update_group(group_id, detail, version).await
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> StorageResult<ProducerIdResponse> {
        self.inner
            .init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            )
            .await
    }

    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> StorageResult<ErrorCode> {
        self.inner
            .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> StorageResult<TxnAddPartitionsResponse> {
        self.inner.txn_add_partitions(partitions).await
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> StorageResult<Vec<tansu_sans_io::txn_offset_commit_response::TxnOffsetCommitResponseTopic>>
    {
        self.inner.txn_offset_commit(offsets).await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> StorageResult<ErrorCode> {
        self.inner
            .txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
    }

    async fn maintain(&self, now: SystemTime) -> StorageResult<()> {
        self.inner.maintain(now).await
    }

    async fn cluster_id(&self) -> StorageResult<String> {
        self.inner.cluster_id().await
    }

    async fn node(&self) -> StorageResult<i32> {
        self.inner.node().await
    }

    async fn advertised_listener(&self) -> StorageResult<Url> {
        self.inner.advertised_listener().await
    }

    async fn ping(&self) -> StorageResult<()> {
        self.inner.ping().await
    }
}

fn single_record_batch(value: &'static [u8]) -> Result<deflated::Batch, StorageError> {
    inflated::Batch::builder()
        .record(Record::builder().value(Bytes::from_static(value).into()))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

fn two_record_batch(
    first: &'static [u8],
    second: &'static [u8],
) -> Result<deflated::Batch, StorageError> {
    inflated::Batch::builder()
        .record(Record::builder().value(Bytes::from_static(first).into()))
        .record(Record::builder().value(Bytes::from_static(second).into()))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

fn compressed_record_batch(
    value: &'static [u8],
    compression: Compression,
) -> Result<deflated::Batch, StorageError> {
    let mut inflated = inflated::Batch::builder()
        .record(Record::builder().value(Bytes::from_static(value).into()))
        .build()?;

    inflated.attributes = i16::from(BatchAttribute::default().compression(compression));
    deflated::Batch::try_from(inflated).map_err(Into::into)
}

fn partition_data(index: i32, batches: Vec<deflated::Batch>) -> PartitionProduceData {
    PartitionProduceData::default()
        .index(index)
        .records(Some(Frame { batches }))
}

fn produce_request(
    topic: &str,
    partitions: Vec<PartitionProduceData>,
    acks: i16,
) -> ProduceRequest {
    ProduceRequest::default()
        .acks(acks)
        .timeout_ms(0)
        .topic_data(Some(vec![
            TopicProduceData::default()
                .name(topic.into())
                .partition_data(Some(partitions)),
        ]))
}

fn assert_partition_response(
    response: &ProduceResponse,
    topic_index: usize,
    partition_index: i32,
    error_code: ErrorCode,
    base_offset: i64,
) {
    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());

    let partitions = topics[topic_index]
        .partition_responses
        .as_deref()
        .unwrap_or_default();
    let partition = partitions
        .iter()
        .find(|partition| partition.index == partition_index)
        .expect("missing partition response");

    assert_eq!(
        error_code,
        ErrorCode::try_from(partition.error_code).unwrap()
    );
    assert_eq!(base_offset, partition.base_offset);
}

async fn exactness_storage(
    topic: Option<(&str, i32)>,
    produce_error: Option<StorageError>,
) -> Result<(Arc<Box<dyn Storage>>, ExactnessStorage), TestError> {
    let cluster_id = Uuid::now_v7().to_string();
    let inner = build_storage(&cluster_id, 111, Url::parse("memory://tansu/")?).await?;

    if let Some((topic, partitions)) = topic {
        _ = create_topic(&inner, topic, partitions).await?;
    }

    let wrapper = ExactnessStorage::new(inner.clone(), produce_error);
    Ok((inner, wrapper))
}

#[tokio::test]
async fn invalid_required_acks_is_rejected() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let storage = build_storage(
        &Uuid::now_v7().to_string(),
        111,
        Url::parse("memory://tansu/")?,
    )
    .await?;
    let ctx = Context::with_state(storage);
    let service = ProduceService;

    let response = service
        .serve(
            ctx,
            produce_request(
                "acks-invalid",
                vec![partition_data(0, vec![single_record_batch(b"one")?])],
                2,
            ),
        )
        .await?;

    assert_partition_response(&response, 0, 0, ErrorCode::InvalidRequiredAcks, -1);

    Ok(())
}

#[tokio::test]
async fn valid_required_acks_are_accepted() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    for (topic, acks) in [("acks-leader", 1), ("acks-all", -1)] {
        let (inner, storage) = exactness_storage(Some((topic, 1)), None).await?;
        let ctx = Context::with_state(storage.clone());
        let service = ProduceService;

        let response = service
            .serve(
                ctx,
                produce_request(
                    topic,
                    vec![partition_data(0, vec![single_record_batch(b"one")?])],
                    acks,
                ),
            )
            .await?;

        assert_partition_response(&response, 0, 0, ErrorCode::None, 0);
        assert_eq!(1, storage.produce_calls());

        let stage = inner.offset_stage(&Topition::new(topic, 0)).await?;
        assert_eq!(1, stage.high_watermark());
    }

    Ok(())
}

#[tokio::test]
async fn unknown_topic_returns_unknown_topic_or_partition() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let (inner, storage) = exactness_storage(None, None).await?;
    let ctx = Context::with_state(storage);
    let service = ProduceService;
    let topic = "missing-topic";

    let response = service
        .serve(
            ctx,
            produce_request(
                topic,
                vec![partition_data(0, vec![single_record_batch(b"one")?])],
                1,
            ),
        )
        .await?;

    assert_partition_response(&response, 0, 0, ErrorCode::UnknownTopicOrPartition, -1);

    let stage = inner.offset_stage(&Topition::new(topic, 0)).await?;
    assert_eq!(0, stage.high_watermark());

    Ok(())
}

#[tokio::test]
async fn invalid_partition_returns_unknown_topic_or_partition() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let topic = "partition-missing";
    let (inner, storage) = exactness_storage(Some((topic, 1)), None).await?;
    let ctx = Context::with_state(storage.clone());
    let service = ProduceService;

    let response = service
        .serve(
            ctx,
            produce_request(
                topic,
                vec![partition_data(1, vec![single_record_batch(b"one")?])],
                1,
            ),
        )
        .await?;

    assert_partition_response(&response, 0, 1, ErrorCode::UnknownTopicOrPartition, -1);

    assert_eq!(0, storage.produce_calls());
    let stage = inner.offset_stage(&Topition::new(topic, 0)).await?;
    assert_eq!(0, stage.high_watermark());

    Ok(())
}

#[tokio::test]
async fn invalid_magic_is_rejected_without_append() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let topic = "magic-invalid";
    let (inner, storage) = exactness_storage(Some((topic, 1)), None).await?;
    let ctx = Context::with_state(storage);
    let service = ProduceService;

    let mut batch = single_record_batch(b"one")?;
    batch.magic = 1;

    let response = service
        .serve(
            ctx,
            produce_request(topic, vec![partition_data(0, vec![batch])], 1),
        )
        .await?;

    assert_partition_response(&response, 0, 0, ErrorCode::InvalidRecord, -1);
    let stage = inner.offset_stage(&Topition::new(topic, 0)).await?;
    assert_eq!(0, stage.high_watermark());

    Ok(())
}

#[tokio::test]
async fn crc_invalid_is_rejected_without_append() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let topic = "crc-invalid";
    let (inner, storage) = exactness_storage(Some((topic, 1)), None).await?;
    let ctx = Context::with_state(storage);
    let service = ProduceService;

    let mut batch = single_record_batch(b"one")?;
    batch.crc ^= 1;

    let response = service
        .serve(
            ctx,
            produce_request(topic, vec![partition_data(0, vec![batch])], 1),
        )
        .await?;

    assert_partition_response(&response, 0, 0, ErrorCode::CorruptMessage, -1);

    let stage = inner.offset_stage(&Topition::new(topic, 0)).await?;
    assert_eq!(0, stage.high_watermark());

    Ok(())
}

#[tokio::test]
async fn unsupported_compression_is_rejected_without_append() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let topic = "unsupported-compression";
    let (inner, storage) = exactness_storage(Some((topic, 1)), None).await?;
    let ctx = Context::with_state(storage);
    let service = ProduceService;

    let mut batch = single_record_batch(b"one")?;
    batch.attributes =
        i16::from(BatchAttribute::default().compression(Compression::Snappy)) | 0b101;

    let response = service
        .serve(
            ctx,
            produce_request(topic, vec![partition_data(0, vec![batch])], 1),
        )
        .await?;

    assert_partition_response(&response, 0, 0, ErrorCode::UnsupportedCompressionType, -1);

    let stage = inner.offset_stage(&Topition::new(topic, 0)).await?;
    assert_eq!(0, stage.high_watermark());

    Ok(())
}

#[tokio::test]
async fn supported_compressions_are_accepted_without_append() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    for (topic, compression) in [
        ("compression-none", Compression::None),
        ("compression-gzip", Compression::Gzip),
        ("compression-lz4", Compression::Lz4),
        ("compression-zstd", Compression::Zstd),
    ] {
        let (inner, storage) = exactness_storage(Some((topic, 1)), None).await?;
        let ctx = Context::with_state(storage.clone());
        let service = ProduceService;

        let response = service
            .serve(
                ctx,
                produce_request(
                    topic,
                    vec![partition_data(
                        0,
                        vec![compressed_record_batch(b"one", compression)?],
                    )],
                    1,
                ),
            )
            .await?;

        assert_partition_response(&response, 0, 0, ErrorCode::None, 0);
        assert_eq!(1, storage.produce_calls());

        let stage = inner.offset_stage(&Topition::new(topic, 0)).await?;
        assert_eq!(1, stage.high_watermark());
    }

    Ok(())
}

#[tokio::test]
async fn all_batches_validated_before_append() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let topic = "batch-validation";
    let (inner, storage) = exactness_storage(Some((topic, 1)), None).await?;
    let ctx = Context::with_state(storage);
    let service = ProduceService;

    let mut invalid = two_record_batch(b"two", b"three")?;
    invalid.last_offset_delta = 0;

    let response = service
        .serve(
            ctx,
            produce_request(
                topic,
                vec![partition_data(
                    0,
                    vec![single_record_batch(b"one")?, invalid],
                )],
                1,
            ),
        )
        .await?;

    assert_partition_response(&response, 0, 0, ErrorCode::InvalidRecord, -1);

    let stage = inner.offset_stage(&Topition::new(topic, 0)).await?;
    assert_eq!(0, stage.high_watermark());

    Ok(())
}

#[tokio::test]
async fn record_count_mismatch_is_rejected_without_append() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let topic = "record-count";
    let (inner, storage) = exactness_storage(Some((topic, 1)), None).await?;
    let ctx = Context::with_state(storage);
    let service = ProduceService;

    let mut batch = two_record_batch(b"one", b"two")?;
    batch.record_count = 1;

    let response = service
        .serve(
            ctx,
            produce_request(topic, vec![partition_data(0, vec![batch])], 1),
        )
        .await?;

    assert_partition_response(&response, 0, 0, ErrorCode::CorruptMessage, -1);

    let stage = inner.offset_stage(&Topition::new(topic, 0)).await?;
    assert_eq!(0, stage.high_watermark());

    Ok(())
}

#[tokio::test]
async fn batch_length_mismatch_is_rejected_without_append() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let topic = "batch-length";
    let (inner, storage) = exactness_storage(Some((topic, 1)), None).await?;
    let ctx = Context::with_state(storage);
    let service = ProduceService;

    let mut batch = single_record_batch(b"one")?;
    batch.batch_length -= 1;

    let response = service
        .serve(
            ctx,
            produce_request(topic, vec![partition_data(0, vec![batch])], 1),
        )
        .await?;

    assert_partition_response(&response, 0, 0, ErrorCode::CorruptMessage, -1);

    let stage = inner.offset_stage(&Topition::new(topic, 0)).await?;
    assert_eq!(0, stage.high_watermark());

    Ok(())
}

#[tokio::test]
async fn invalid_timestamp_is_rejected_without_append() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let topic = "timestamp-invalid";
    let (inner, storage) = exactness_storage(Some((topic, 1)), None).await?;
    let ctx = Context::with_state(storage);
    let service = ProduceService;

    let mut batch = single_record_batch(b"one")?;
    batch.max_timestamp = batch.base_timestamp - 1;

    let response = service
        .serve(
            ctx,
            produce_request(topic, vec![partition_data(0, vec![batch])], 1),
        )
        .await?;

    assert_partition_response(&response, 0, 0, ErrorCode::InvalidTimestamp, -1);

    let stage = inner.offset_stage(&Topition::new(topic, 0)).await?;
    assert_eq!(0, stage.high_watermark());

    Ok(())
}

#[tokio::test]
async fn partitions_remain_independent() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let topic = "partition-independent";
    let (inner, storage) = exactness_storage(Some((topic, 2)), None).await?;
    let ctx = Context::with_state(storage.clone());
    let service = ProduceService;

    let response = service
        .serve(
            ctx,
            ProduceRequest::default()
                .acks(1)
                .timeout_ms(0)
                .topic_data(Some(vec![
                    TopicProduceData::default()
                        .name(topic.into())
                        .partition_data(Some(vec![
                            partition_data(0, vec![single_record_batch(b"one")?]),
                            partition_data(2, vec![single_record_batch(b"two")?]),
                        ])),
                ])),
        )
        .await?;

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
    assert_eq!(2, partitions.len());
    assert_eq!(0, partitions[0].index);
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );
    assert_eq!(0, partitions[0].base_offset);
    assert_eq!(2, partitions[1].index);
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(partitions[1].error_code)?
    );

    let stage0 = inner.offset_stage(&Topition::new(topic, 0)).await?;
    assert_eq!(1, stage0.high_watermark());

    let stage1 = inner.offset_stage(&Topition::new(topic, 1)).await?;
    assert_eq!(0, stage1.high_watermark());

    let response = service
        .serve(
            Context::with_state(storage.clone()),
            produce_request(
                topic,
                vec![partition_data(0, vec![single_record_batch(b"three")?])],
                1,
            ),
        )
        .await?;

    assert_partition_response(&response, 0, 0, ErrorCode::None, 1);

    let stage0 = inner.offset_stage(&Topition::new(topic, 0)).await?;
    assert_eq!(2, stage0.high_watermark());

    Ok(())
}

#[tokio::test]
async fn schema_failure_maps_and_keeps_offsets() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let topic = "schema-failure";
    let (inner, storage) = exactness_storage(
        Some((topic, 1)),
        Some(StorageError::Schema(Arc::new(
            tansu_schema::Error::SchemaValidation,
        ))),
    )
    .await?;
    let ctx = Context::with_state(storage.clone());
    let service = ProduceService;

    let response = service
        .serve(
            ctx,
            produce_request(
                topic,
                vec![partition_data(0, vec![single_record_batch(b"one")?])],
                1,
            ),
        )
        .await?;

    assert_partition_response(&response, 0, 0, ErrorCode::InvalidRecord, -1);
    assert_eq!(1, storage.produce_calls());

    let stage = inner.offset_stage(&Topition::new(topic, 0)).await?;
    assert_eq!(0, stage.high_watermark());

    Ok(())
}
