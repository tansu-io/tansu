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
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
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
    record::deflated::{self, combine},
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use tokio::time::sleep;
use tracing::debug;
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, Error, GroupDetail, ListOffsetResponse, MetadataResponse,
    NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result,
    ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, UpdateError, Version,
};

#[derive(Clone, Debug)]
struct Ticket<G> {
    id: Uuid,
    batcher: ProduceRequestBatcher<G>,
}

impl<G> Ticket<G> {
    fn new(batcher: ProduceRequestBatcher<G>) -> Self {
        Self {
            id: Uuid::now_v7(),
            batcher,
        }
    }
}

impl<G> AsRef<Uuid> for Ticket<G> {
    fn as_ref(&self) -> &Uuid {
        &self.id
    }
}

impl<G> Future for Ticket<G> {
    type Output = Result<i64, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut responses = self.batcher.responses.lock()?;

        match responses.remove(&self.id) {
            Some(BatchResponse::Response(response)) => Poll::Ready(Ok(response)),
            Some(BatchResponse::Waker(_)) | None => {
                _ = responses.insert(self.id, BatchResponse::Waker(cx.waker().clone()));
                Poll::Pending
            }
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TopitionProducerId {
    topition: Topition,
    producer_id: i64,
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct BatchRequest {
    id: Uuid,
    batch: deflated::Batch,
}

#[derive(Clone, Debug)]
enum BatchResponse {
    Waker(Waker),
    Response(i64),
}

const DEFAULT_DELAY: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub(crate) struct ProduceRequestBatcher<G> {
    storage: G,
    maximum_delay: Option<Duration>,
    minimum_size: Option<usize>,

    requests: Arc<Mutex<BTreeMap<TopitionProducerId, Vec<BatchRequest>>>>,
    responses: Arc<Mutex<BTreeMap<Uuid, BatchResponse>>>,
}

impl<G> ProduceRequestBatcher<G>
where
    G: Storage,
{
    pub(crate) fn new(storage: G) -> Self {
        Self {
            storage,
            minimum_size: Default::default(),
            maximum_delay: Default::default(),

            requests: Arc::new(Mutex::new(BTreeMap::new())),
            responses: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub(crate) fn with_minimum_size(self, minimum_size: Option<usize>) -> Self {
        Self {
            minimum_size,
            ..self
        }
    }

    pub(crate) fn with_maximum_delay(self, maximum_delay: Option<Duration>) -> Self {
        Self {
            maximum_delay,
            ..self
        }
    }

    async fn send_queued(
        &self,
        id: &Uuid,
        transaction_id: Option<&str>,
        topition: &Topition,
        producer_id: i64,
    ) -> Result<Option<i64>, Error> {
        let Some(queued) = self.requests.lock().map(|mut requests| {
            requests.remove(&TopitionProducerId {
                topition: topition.to_owned(),
                producer_id,
            })
        })?
        else {
            return Ok(None);
        };

        let owners = queued
            .iter()
            .map(|batch_request| batch_request.id)
            .collect::<BTreeSet<_>>();

        if let Some(queued) = combine(queued.into_iter().map(|queued| queued.batch).collect())? {
            let offset = self
                .storage
                .produce(transaction_id, topition, queued)
                .await?;

            self.responses.lock().map(|mut responses| {
                for owner in owners {
                    if &owner == id {
                        _ = responses.remove(&owner);
                    } else {
                        if let Some(BatchResponse::Waker(waker)) =
                            responses.insert(owner, BatchResponse::Response(offset))
                        {
                            waker.wake();
                        }
                    }
                }
            })?;

            Ok(Some(offset))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl<G> Storage for ProduceRequestBatcher<G>
where
    G: Storage + Clone,
{
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        self.storage.register_broker(broker_registration).await
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        self.storage.create_topic(topic, validate_only).await
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        self.storage.incremental_alter_resource(resource).await
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        self.storage.delete_records(topics).await
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        self.storage.delete_topic(topic).await
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        self.storage.brokers().await
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        let producer_id = deflated.producer_id;

        let topition_producer_id = TopitionProducerId {
            topition: topition.to_owned(),
            producer_id,
        };

        let ticket = self.requests.lock().map(|mut requests| {
            let ticket = Ticket::new(self.clone());

            requests
                .entry(topition_producer_id.clone())
                .or_default()
                .push(BatchRequest {
                    id: ticket.id,
                    batch: deflated,
                });

            ticket
        })?;

        loop {
            let ticket = ticket.clone();
            let id = ticket.id;

            let queued_bytes = self
                .requests
                .lock()
                .map(|requests| {
                    requests
                        .get(&topition_producer_id)
                        .map(|queue| {
                            queue
                                .iter()
                                .map(|batch_request| batch_request.batch.record_data.len())
                                .sum::<usize>()
                        })
                        .inspect(|queued_bytes| debug!(queued_bytes))
                        .unwrap_or_default()
                })
                .inspect(|queued_bytes| debug!(queued_bytes))?;

            if self
                .minimum_size
                .is_some_and(|minimum_size| queued_bytes > minimum_size)
                && let Some(offset) = self
                    .send_queued(&id, transaction_id, topition, producer_id)
                    .await?
            {
                return Ok(offset);
            }

            let patience = sleep(self.maximum_delay.unwrap_or(DEFAULT_DELAY));

            tokio::select! {
                response = ticket  => {
                    return response;
                }

                expired = patience => {
                    debug!(?expired);
                    if let Some(offset) = self.send_queued(&id, transaction_id, topition, producer_id).await? {
                        return Ok(offset)
                    }
                }

            }
        }
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        self.storage
            .fetch(topition, offset, min_bytes, max_bytes, isolation)
            .await
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        self.storage.offset_stage(topition).await
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        self.storage.list_offsets(isolation_level, offsets).await
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        self.storage
            .offset_commit(group_id, retention_time_ms, offsets)
            .await
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        self.storage
            .offset_fetch(group_id, topics, require_stable)
            .await
    }

    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        self.storage.committed_offset_topitions(group_id).await
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        self.storage.metadata(topics).await
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        self.storage
            .upsert_user_scram_credential(user, mechanism, credential)
            .await
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        self.storage
            .delete_user_scram_credential(user, mechanism)
            .await
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        self.storage.user_scram_credential(user, mechanism).await
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        self.storage.describe_config(name, resource, keys).await
    }

    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        self.storage.list_groups(states_filter).await
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        self.storage.delete_groups(group_ids).await
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        self.storage
            .describe_groups(group_ids, include_authorized_operations)
            .await
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        self.storage
            .describe_topic_partitions(topics, partition_limit, cursor)
            .await
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        self.storage.update_group(group_id, detail, version).await
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        self.storage
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
    ) -> Result<ErrorCode> {
        self.storage
            .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        self.storage.txn_add_partitions(partitions).await
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.storage.txn_offset_commit(offsets).await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        self.storage
            .txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
    }

    async fn maintain(&self, now: SystemTime) -> Result<()> {
        self.storage.maintain(now).await
    }

    async fn cluster_id(&self) -> Result<String> {
        self.storage.cluster_id().await
    }

    async fn node(&self) -> Result<i32> {
        self.storage.node().await
    }

    async fn advertised_listener(&self) -> Result<Url> {
        self.storage.advertised_listener().await
    }

    async fn ping(&self) -> Result<()> {
        self.storage.ping().await
    }
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;
    use tansu_sans_io::{
        BatchAttribute,
        record::{Record, deflated, inflated},
    };
    use tokio::{task::yield_now, time::advance};

    use super::*;

    #[derive(Clone, Debug, Default)]
    struct FlightRecorder {
        produced: Arc<Mutex<BTreeMap<Topition, Vec<deflated::Batch>>>>,
    }

    impl FlightRecorder {
        fn new() -> Self {
            Self {
                produced: Arc::new(Mutex::new(BTreeMap::new())),
            }
        }

        fn produced(&self, topition: &Topition) -> Result<Option<Vec<inflated::Batch>>> {
            self.produced
                .as_ref()
                .lock()
                .map_err(Into::into)
                .and_then(|produced| {
                    produced
                        .get(topition)
                        .map(|produced| {
                            produced
                                .iter()
                                .map(|deflated| {
                                    inflated::Batch::try_from(deflated).map_err(Into::into)
                                })
                                .collect::<Result<Vec<_>>>()
                        })
                        .transpose()
                })
        }
    }

    #[async_trait]
    impl Storage for FlightRecorder {
        async fn register_broker(
            &self,
            _broker_registration: BrokerRegistrationRequest,
        ) -> Result<()> {
            unimplemented!()
        }

        async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
            unimplemented!()
        }

        async fn create_topic(&self, _topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
            unimplemented!()
        }

        async fn delete_records(
            &self,
            _topics: &[DeleteRecordsTopic],
        ) -> Result<Vec<DeleteRecordsTopicResult>> {
            unimplemented!()
        }

        async fn delete_topic(&self, _topic: &TopicId) -> Result<ErrorCode> {
            unimplemented!()
        }

        async fn incremental_alter_resource(
            &self,
            _resource: AlterConfigsResource,
        ) -> Result<AlterConfigsResourceResponse> {
            unimplemented!()
        }

        async fn produce(
            &self,
            _transaction_id: Option<&str>,
            topition: &Topition,
            deflated: deflated::Batch,
        ) -> Result<i64> {
            self.produced
                .lock()
                .map(|mut produced| {
                    _ = produced
                        .entry(topition.to_owned())
                        .or_default()
                        .push(deflated);

                    0
                })
                .map_err(Into::into)
        }

        async fn fetch(
            &self,
            _topition: &Topition,
            _offset: i64,
            _min_bytes: u32,
            _max_bytes: u32,
            _isolation_level: IsolationLevel,
        ) -> Result<Vec<deflated::Batch>> {
            unimplemented!()
        }

        async fn offset_stage(&self, _topition: &Topition) -> Result<OffsetStage> {
            unimplemented!()
        }

        async fn offset_commit(
            &self,
            _group: &str,
            _retention: Option<Duration>,
            _offsets: &[(Topition, OffsetCommitRequest)],
        ) -> Result<Vec<(Topition, ErrorCode)>> {
            unimplemented!()
        }

        async fn committed_offset_topitions(
            &self,
            _group_id: &str,
        ) -> Result<BTreeMap<Topition, i64>> {
            unimplemented!()
        }

        async fn offset_fetch(
            &self,
            _group_id: Option<&str>,
            _topics: &[Topition],
            _require_stable: Option<bool>,
        ) -> Result<BTreeMap<Topition, i64>> {
            unimplemented!()
        }

        async fn list_offsets(
            &self,
            _isolation_level: IsolationLevel,
            _offsets: &[(Topition, ListOffset)],
        ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
            unimplemented!()
        }

        async fn metadata(&self, _topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
            unimplemented!()
        }

        async fn describe_config(
            &self,
            _name: &str,
            _resource: ConfigResource,
            _keys: Option<&[String]>,
        ) -> Result<DescribeConfigsResult> {
            unimplemented!()
        }

        async fn describe_topic_partitions(
            &self,
            _topics: Option<&[TopicId]>,
            _partition_limit: i32,
            _cursor: Option<Topition>,
        ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
            unimplemented!()
        }

        async fn list_groups(&self, _states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
            unimplemented!()
        }

        async fn delete_groups(
            &self,
            _group_ids: Option<&[String]>,
        ) -> Result<Vec<DeletableGroupResult>> {
            unimplemented!()
        }

        async fn describe_groups(
            &self,
            _group_ids: Option<&[String]>,
            _include_authorized_operations: bool,
        ) -> Result<Vec<NamedGroupDetail>> {
            unimplemented!()
        }

        async fn update_group(
            &self,
            _group_id: &str,
            _detail: GroupDetail,
            _version: Option<Version>,
        ) -> Result<Version, UpdateError<GroupDetail>> {
            unimplemented!()
        }

        async fn init_producer(
            &self,
            _transaction_id: Option<&str>,
            _transaction_timeout_ms: i32,
            _producer_id: Option<i64>,
            _producer_epoch: Option<i16>,
        ) -> Result<ProducerIdResponse> {
            unimplemented!()
        }

        async fn txn_add_offsets(
            &self,
            _transaction_id: &str,
            _producer_id: i64,
            _producer_epoch: i16,
            _group_id: &str,
        ) -> Result<ErrorCode> {
            unimplemented!()
        }

        async fn txn_add_partitions(
            &self,
            _partitions: TxnAddPartitionsRequest,
        ) -> Result<TxnAddPartitionsResponse> {
            unimplemented!()
        }

        async fn txn_offset_commit(
            &self,
            _offsets: TxnOffsetCommitRequest,
        ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
            unimplemented!()
        }

        async fn txn_end(
            &self,
            _transaction_id: &str,
            _producer_id: i64,
            _producer_epoch: i16,
            _committed: bool,
        ) -> Result<ErrorCode> {
            unimplemented!()
        }

        async fn maintain(&self, _now: SystemTime) -> Result<()> {
            unimplemented!()
        }

        async fn cluster_id(&self) -> Result<String> {
            unimplemented!()
        }

        async fn node(&self) -> Result<i32> {
            unimplemented!()
        }

        async fn advertised_listener(&self) -> Result<Url> {
            unimplemented!()
        }

        async fn ping(&self) -> Result<()> {
            unimplemented!()
        }

        async fn delete_user_scram_credential(
            &self,
            _user: &str,
            _mechanism: ScramMechanism,
        ) -> Result<()> {
            unimplemented!()
        }

        async fn upsert_user_scram_credential(
            &self,
            _user: &str,
            _mechanism: ScramMechanism,
            _credential: ScramCredential,
        ) -> Result<()> {
            unimplemented!()
        }

        async fn user_scram_credential(
            &self,
            _user: &str,
            _mechanism: ScramMechanism,
        ) -> Result<Option<ScramCredential>> {
            unimplemented!()
        }
    }

    fn into_batch(
        attributes: i16,
        producer_id: i64,
        producer_epoch: i16,
        base_offset: i64,
        records: &[Bytes],
    ) -> Result<deflated::Batch> {
        let base_sequence = 0;

        let mut inflated = inflated::Batch::builder()
            .attributes(attributes)
            .producer_id(producer_id)
            .producer_epoch(producer_epoch)
            .base_offset(base_offset)
            .last_offset_delta(records.len() as i32 - 1)
            .base_sequence(base_sequence);

        for (offset_delta, value) in records.iter().enumerate() {
            inflated = inflated.record(
                Record::builder()
                    .value(value.clone().into())
                    .offset_delta(offset_delta as i32),
            );
        }

        inflated
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))
            .map_err(Into::into)
    }

    #[tokio::test(start_paused = true)]
    async fn single_produce_in_window() -> Result<()> {
        const MINIMUM_DELAY: Duration = Duration::from_secs(1);
        const ADVANCE_DELAY: Duration = Duration::from_secs(5);

        let recorder = FlightRecorder::new();
        let storage =
            ProduceRequestBatcher::new(recorder.clone()).with_maximum_delay(Some(MINIMUM_DELAY));

        let producer_id = 54345;
        let producer_epoch = 32123;
        let base_offset = 0;
        let attributes: i16 = BatchAttribute::default().into();

        let transaction_id = None;
        let abc0 = Topition::new("abc", 0);

        const A: Bytes = Bytes::from_static(b"a");
        const B: Bytes = Bytes::from_static(b"b");
        const C: Bytes = Bytes::from_static(b"c");

        let batch_a = {
            let storage = storage.clone();
            let abc0 = abc0.clone();

            tokio::spawn(async move {
                storage
                    .produce(
                        transaction_id,
                        &abc0,
                        into_batch(
                            attributes,
                            producer_id,
                            producer_epoch,
                            base_offset,
                            &[A, B, C],
                        )?,
                    )
                    .await
            })
        };

        advance(ADVANCE_DELAY).await;
        yield_now().await;

        let response_a = batch_a
            .await
            .expect("join_handle")
            .inspect(|produce_response| debug!(?produce_response))?;
        assert_eq!(0, response_a);

        let sent = recorder.produced(&abc0)?.unwrap();
        assert_eq!(1, sent.len());
        assert_eq!(3, sent[0].records.len());
        assert_eq!(Some(A), sent[0].records[0].value());
        assert_eq!(Some(B), sent[0].records[1].value());
        assert_eq!(Some(C), sent[0].records[2].value());

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn two_produces_in_window() -> Result<()> {
        const MINIMUM_DELAY: Duration = Duration::from_secs(1);
        const ADVANCE_DELAY: Duration = Duration::from_secs(5);

        let recorder = FlightRecorder::new();
        let storage =
            ProduceRequestBatcher::new(recorder.clone()).with_maximum_delay(Some(MINIMUM_DELAY));

        let producer_id = 54345;
        let producer_epoch = 32123;
        let base_offset = 0;
        let attributes: i16 = BatchAttribute::default().into();

        let transaction_id = None;
        let abc0 = Topition::new("abc", 0);

        const A: Bytes = Bytes::from_static(b"a");
        const B: Bytes = Bytes::from_static(b"b");
        const C: Bytes = Bytes::from_static(b"c");

        let batch_a = {
            let storage = storage.clone();
            let abc0 = abc0.clone();

            tokio::spawn(async move {
                storage
                    .produce(
                        transaction_id,
                        &abc0,
                        into_batch(
                            attributes,
                            producer_id,
                            producer_epoch,
                            base_offset,
                            &[A, B, C],
                        )?,
                    )
                    .await
            })
        };

        const D: Bytes = Bytes::from_static(b"d");
        const E: Bytes = Bytes::from_static(b"e");

        let batch_b = {
            let storage = storage.clone();
            let abc0 = abc0.clone();

            tokio::spawn(async move {
                storage
                    .produce(
                        transaction_id,
                        &abc0,
                        into_batch(
                            attributes,
                            producer_id,
                            producer_epoch,
                            base_offset,
                            &[D, E],
                        )?,
                    )
                    .await
            })
        };

        advance(ADVANCE_DELAY).await;
        yield_now().await;

        let response_a = batch_a
            .await
            .expect("join_handle")
            .inspect(|produce_response| debug!(?produce_response))?;
        assert_eq!(0, response_a);

        let response_b = batch_b
            .await
            .expect("join_handle")
            .inspect(|produce_response| debug!(?produce_response))?;
        assert_eq!(0, response_b);

        let sent = recorder.produced(&abc0)?.unwrap();
        assert_eq!(1, sent.len());
        assert_eq!(5, sent[0].records.len());
        assert_eq!(Some(A), sent[0].records[0].value());
        assert_eq!(Some(B), sent[0].records[1].value());
        assert_eq!(Some(C), sent[0].records[2].value());
        assert_eq!(Some(D), sent[0].records[3].value());
        assert_eq!(Some(E), sent[0].records[4].value());

        Ok(())
    }
}
