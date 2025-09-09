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

use std::{collections::BTreeMap, time::Duration};

use async_trait::async_trait;
use rama::{Context, Service};
use tansu_sans_io::{
    ConfigResource, ErrorCode, IsolationLevel, ListOffset, create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult, delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup, record::deflated,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use tokio::sync::{mpsc, oneshot};
use tracing::debug;
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, Error, GroupDetail, ListOffsetResponse, MetadataResponse,
    NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result, Storage,
    TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest,
    UpdateError, Version,
};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Proxy {}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Request {
    RegisterBroker(BrokerRegistrationRequest),
    IncrementalAlterResource(AlterConfigsResource),
    CreateTopic {
        topic: CreatableTopic,
        validate_only: bool,
    },
    DeleteRecords(Vec<DeleteRecordsTopic>),
    DeleteTopic(TopicId),
    Brokers,
    Produce {
        transaction_id: Option<String>,
        topition: Topition,
        batch: deflated::Batch,
    },
    Fetch {
        topition: Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
    },
    OffsetStage(Vec<(Topition, ListOffsetResponse)>),
    ListOffsets {
        isolation_level: IsolationLevel,
        offsets: Vec<(Topition, ListOffset)>,
    },
    OffsetCommit {
        group_id: String,
        retention_time_ms: Option<Duration>,
        offsets: Vec<(Topition, OffsetCommitRequest)>,
    },
    CommittedOffsetTopitions(String),
    OffsetFetch {
        group_id: Option<String>,
        topics: Vec<Topition>,
        require_stable: Option<bool>,
    },
    Metadata(Option<Vec<TopicId>>),
    DescribeConfig {
        name: String,
        resource: ConfigResource,
        keys: Option<Vec<String>>,
    },
    DescribeTopicPartitions {
        topics: Option<Vec<TopicId>>,
        partition_limit: i32,
        cursor: Option<Topition>,
    },
    ListGroups(Option<Vec<String>>),
    DeleteGroups(Option<Vec<String>>),
    DescribeGroups {
        group_ids: Option<Vec<String>>,
        include_authorized_operations: bool,
    },
    UpdateGroup {
        group_id: String,
        detail: GroupDetail,
        version: Option<Version>,
    },
    InitProducer {
        transaction_id: Option<String>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    },
    TxnAddOffsets {
        transaction_id: String,
        producer_id: i64,
        producer_epoch: i16,
        group_id: String,
    },
    TxnAddPartitions(TxnAddPartitionsRequest),
    TxnOffsetCommit(TxnOffsetCommitRequest),
    TxnEnd {
        transaction_id: String,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    },
    Maintain,
    ClusterId,
    Node,
    AdvertisedLister,
}

#[derive(Debug)]
enum Response {
    RegisterBroker(Result<()>),
    IncrementalAlterResponse(Result<AlterConfigsResourceResponse>),
    CreateTopic(Result<Uuid>),
    DeleteRecords(Result<Vec<DeleteRecordsTopicResult>>),
    DeleteTopic(Result<ErrorCode>),
    Brokers(Result<Vec<DescribeClusterBroker>>),
    Produce(Result<i64>),
    Fetch(Result<Vec<deflated::Batch>>),
    OffsetStage(Result<OffsetStage>),
    ListOffsets(Result<Vec<(Topition, ListOffsetResponse)>>),
    OffsetCommit(Result<Vec<(Topition, ErrorCode)>>),
    CommittedOffsetTopitions(Result<BTreeMap<Topition, i64>>),
    OffsetFetch(Result<BTreeMap<Topition, i64>>),
    Metadata(Result<MetadataResponse>),
    DescribeConfig(Result<DescribeConfigsResult>),
    DescribeTopicPartitions(Result<Vec<DescribeTopicPartitionsResponseTopic>>),
    ListGroups(Result<Vec<ListedGroup>>),
    DeleteGroups(Result<Vec<DeletableGroupResult>>),
    DescribeGroups(Result<Vec<NamedGroupDetail>>),
    UpdateGroup(Result<Version, UpdateError<GroupDetail>>),
    InitProducer(Result<ProducerIdResponse>),
    TxnAddOffsets(Result<ErrorCode>),
    TxnAddPartitions(Result<TxnAddPartitionsResponse>),
    TxnOffsetCommit(Result<Vec<TxnOffsetCommitResponseTopic>>),
    TxnEnd(Result<ErrorCode>),
    Maintain(Result<()>),
    ClusterId(Result<String>),
    Node(Result<i32>),
    AdvertisedListener(Result<Url>),
}

type RequestSender = mpsc::Sender<(Request, oneshot::Sender<Response>)>;
type RequestReceiver = mpsc::Receiver<(Request, oneshot::Sender<Response>)>;

fn bounded_channel(buffer: usize) -> (RequestSender, RequestReceiver) {
    mpsc::channel::<(Request, oneshot::Sender<Response>)>(buffer)
}

#[derive(Clone, Debug)]
struct RequestProxyService {
    tx: RequestSender,
}

impl RequestProxyService {
    fn new(tx: RequestSender) -> Self {
        Self { tx }
    }
}

impl<State> Service<State, Request> for RequestProxyService
where
    State: Send + Sync + 'static,
{
    type Response = Response;
    type Error = Error;

    async fn serve(
        &self,
        _ctx: Context<State>,
        req: Request,
    ) -> Result<Self::Response, Self::Error> {
        debug!(?req);

        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx
            .send((req, resp_tx))
            .await
            .map_err(|_send_error| Error::UnableToSend)?;

        resp_rx.await.map_err(|_| Error::OneshotRecv)
    }
}

struct ProxyRequestService<S> {
    inner: S,
}

impl<S, State> Service<State, RequestReceiver> for ProxyRequestService<S>
where
    S: Service<State, Request, Response = Response, Error = Error>,
    State: Clone + Send + Sync + 'static,
{
    type Response = ();
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        mut req: RequestReceiver,
    ) -> Result<Self::Response, Self::Error> {
        todo!()
    }
}

#[async_trait]
impl Storage for Proxy {
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        todo!()
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        todo!()
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        todo!()
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        todo!()
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        todo!()
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        todo!()
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        todo!()
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        todo!()
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        todo!()
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        todo!()
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        todo!()
    }

    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        todo!()
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        todo!()
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        todo!()
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        todo!()
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        todo!()
    }

    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        todo!()
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        todo!()
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        todo!()
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        todo!()
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        todo!()
    }

    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        todo!()
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        todo!()
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        todo!()
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        todo!()
    }

    async fn maintain(&self) -> Result<()> {
        todo!()
    }

    fn cluster_id(&self) -> Result<&str> {
        todo!()
    }

    fn node(&self) -> Result<i32> {
        todo!()
    }

    fn advertised_listener(&self) -> Result<&Url> {
        todo!()
    }
}
