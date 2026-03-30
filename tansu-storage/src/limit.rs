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

use std::{
    collections::BTreeMap,
    fmt::Debug,
    num::{NonZero, NonZeroU32},
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use governor::{
    DefaultDirectRateLimiter, Jitter, Quota, RateLimiter, clock::QuantaInstant,
    middleware::NoOpMiddleware,
};
use tansu_sans_io::{
    ConfigResource, ErrorCode, IsolationLevel, ListOffset, ScramMechanism,
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup,
    record::deflated::{self},
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use tracing::{debug, instrument};
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, GroupDetail, ListOffsetResponse, MetadataResponse, NamedGroupDetail,
    OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result, ScramCredential, Storage,
    TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest,
    UpdateError, Version,
};

#[derive(Clone, Debug)]
pub(crate) struct Engine<G> {
    storage: G,
    limiter: ReadWriteLimiter,
    jitter: Duration,
}

impl<G> Engine<G> {
    pub fn new(storage: G) -> Self
    where
        G: Storage,
    {
        Self {
            storage,
            limiter: Default::default(),
            jitter: Default::default(),
        }
    }

    pub fn with_limiter(self, limiter: ReadWriteLimiter) -> Self {
        Self { limiter, ..self }
    }

    pub fn with_jitter(self, jitter: Duration) -> Self {
        Self { jitter, ..self }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ReadWriteLimiter {
    read: Option<Arc<DefaultDirectRateLimiter<NoOpMiddleware<QuantaInstant>>>>,
    write: Option<Arc<DefaultDirectRateLimiter<NoOpMiddleware<QuantaInstant>>>>,
}

impl ReadWriteLimiter {
    pub fn with_reads_per_second(self, reads_per_second: Option<NonZero<u32>>) -> Self {
        Self {
            read: reads_per_second
                .map(Quota::per_second)
                .map(RateLimiter::direct)
                .map(Arc::new),
            ..self
        }
    }

    pub fn with_writes_per_second(self, writes_per_second: Option<NonZero<u32>>) -> Self {
        Self {
            write: writes_per_second
                .map(Quota::per_second)
                .map(RateLimiter::direct)
                .map(Arc::new),
            ..self
        }
    }
}

impl<G> Engine<G> {
    #[instrument(skip_all)]
    async fn read_rate_limit(&self, n_ready: Option<NonZero<u32>>) -> Result<()> {
        if let Some(ref limiter) = self.limiter.read
            && let Some(n_ready) = n_ready
        {
            let start = SystemTime::now();
            limiter
                .until_n_ready_with_jitter(n_ready, Jitter::up_to(self.jitter))
                .await
                .inspect(|_| {
                    debug!(
                        rate_limit_ms = start.elapsed().map_or(0, |duration| duration.as_millis())
                    )
                })
                .map_err(Into::into)
        } else {
            Ok(())
        }
    }

    #[instrument(skip_all)]
    async fn write_rate_limit(&self, n_ready: Option<NonZero<u32>>) -> Result<()> {
        if let Some(ref limiter) = self.limiter.write
            && let Some(n_ready) = n_ready
        {
            let start = SystemTime::now();
            limiter
                .until_n_ready_with_jitter(n_ready, Jitter::up_to(self.jitter))
                .await
                .inspect(|_| {
                    debug!(
                        rate_limit_ms = start.elapsed().map_or(0, |duration| duration.as_millis())
                    )
                })
                .map_err(Into::into)
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl<G: Storage> Storage for Engine<G>
where
    G: Storage,
{
    #[instrument(skip_all)]
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        self.read_rate_limit(NonZeroU32::new(1)).await?;
        self.register_broker(broker_registration).await
    }

    #[instrument(skip_all)]
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        self.read_rate_limit(NonZeroU32::new(1)).await?;
        self.brokers().await
    }

    #[instrument(skip_all)]
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        self.write_rate_limit(NonZeroU32::new(1)).await?;
        self.create_topic(topic, validate_only).await
    }

    #[instrument(skip_all)]
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        self.write_rate_limit(NonZeroU32::new(1)).await?;
        self.storage.delete_records(topics).await
    }

    #[instrument(skip_all)]
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        self.write_rate_limit(NonZeroU32::new(1)).await?;
        self.storage.delete_topic(topic).await
    }

    #[instrument(skip_all)]
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        self.write_rate_limit(NonZeroU32::new(1)).await?;
        self.storage.incremental_alter_resource(resource).await
    }

    #[instrument(skip_all)]
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        self.write_rate_limit(NonZeroU32::new(1)).await?;
        self.storage.produce(transaction_id, topition, batch).await
    }

    #[instrument(skip_all)]
    async fn fetch(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        self.write_rate_limit(NonZeroU32::new(1)).await?;
        self.storage
            .fetch(topition, offset, min_bytes, max_bytes, isolation_level)
            .await
    }

    #[instrument(skip_all)]
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        self.read_rate_limit(NonZeroU32::new(1)).await?;
        self.storage.offset_stage(topition).await
    }

    #[instrument(skip_all)]
    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        self.write_rate_limit(NonZeroU32::new(1)).await?;
        self.storage
            .offset_commit(group_id, retention_time_ms, offsets)
            .await
    }

    #[instrument(skip_all)]
    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        self.read_rate_limit(NonZeroU32::new(1)).await?;
        self.storage.committed_offset_topitions(group_id).await
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        self.read_rate_limit(NonZeroU32::new(1)).await?;
        self.offset_fetch(group_id, topics, require_stable).await
    }

    #[instrument(skip_all)]
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        self.read_rate_limit(NonZeroU32::new(1)).await?;
        self.list_offsets(isolation_level, offsets).await
    }

    #[instrument(skip_all)]
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        self.read_rate_limit(NonZeroU32::new(1)).await?;
        self.metadata(topics).await
    }

    #[instrument(skip_all)]
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        self.read_rate_limit(NonZeroU32::new(1)).await?;
        self.describe_config(name, resource, keys).await
    }

    #[instrument(skip_all)]
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        self.read_rate_limit(NonZeroU32::new(1)).await?;
        self.describe_topic_partitions(topics, partition_limit, cursor)
            .await
    }

    #[instrument(skip_all)]
    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        self.read_rate_limit(NonZeroU32::new(1)).await?;
        self.storage.list_groups(states_filter).await
    }

    #[instrument(skip_all)]
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        self.write_rate_limit(NonZeroU32::new(1)).await?;
        self.delete_groups(group_ids).await
    }

    #[instrument(skip_all)]
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        self.read_rate_limit(NonZeroU32::new(1)).await?;
        self.describe_groups(group_ids, include_authorized_operations)
            .await
    }

    #[instrument(skip_all)]
    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        self.write_rate_limit(NonZeroU32::new(1)).await?;
        self.update_group(group_id, detail, version).await
    }

    #[instrument(skip_all)]
    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        self.write_rate_limit(NonZeroU32::new(1)).await?;
        self.init_producer(
            transaction_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
        )
        .await
    }

    #[instrument(skip_all)]
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        self.write_rate_limit(NonZeroU32::new(1)).await?;
        self.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    #[instrument(skip_all)]
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        self.write_rate_limit(NonZeroU32::new(1)).await?;
        self.txn_add_partitions(partitions).await
    }

    #[instrument(skip_all)]
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.write_rate_limit(NonZeroU32::new(1)).await?;
        self.txn_offset_commit(offsets).await
    }

    #[instrument(skip_all)]
    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        self.write_rate_limit(NonZeroU32::new(1)).await?;
        self.storage
            .txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
    }

    #[instrument(skip_all)]
    async fn maintain(&self, now: SystemTime) -> Result<()> {
        self.storage.maintain(now).await
    }

    #[instrument(skip_all)]
    async fn cluster_id(&self) -> Result<String> {
        self.cluster_id().await
    }

    #[instrument(skip_all)]
    async fn node(&self) -> Result<i32> {
        self.storage.node().await
    }

    #[instrument(skip_all)]
    async fn advertised_listener(&self) -> Result<Url> {
        self.storage.advertised_listener().await
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        self.read_rate_limit(NonZeroU32::new(1)).await?;
        self.storage.ping().await
    }

    #[instrument(skip_all)]
    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        self.write_rate_limit(NonZeroU32::new(1)).await?;
        self.storage
            .delete_user_scram_credential(user, mechanism)
            .await
    }

    #[instrument(ret)]
    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        self.write_rate_limit(NonZeroU32::new(1)).await?;
        self.upsert_user_scram_credential(user, mechanism, credential)
            .await
    }

    #[instrument(ret)]
    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        self.read_rate_limit(NonZeroU32::new(1)).await?;
        self.user_scram_credential(user, mechanism).await
    }
}
