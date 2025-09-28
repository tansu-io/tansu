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

mod consumer_group_describe;
mod create_topics;
mod delete_groups;
mod delete_records;
mod delete_topics;
mod describe_cluster;
mod describe_configs;
mod describe_groups;
mod describe_topic_partitions;
mod fetch;
mod find_coordinator;
mod get_telemetry_subscriptions;
mod incremental_alter_configs;
mod init_producer_id;
mod list_groups;
mod list_offsets;
mod list_partition_reassignments;
mod metadata;
mod produce;
mod txn;

use std::{
    collections::BTreeMap,
    fmt::{self, Debug, Display, Formatter},
    time::Duration,
};

use async_trait::async_trait;
pub use consumer_group_describe::ConsumerGroupDescribeService;
pub use create_topics::CreateTopicsService;
pub use delete_groups::DeleteGroupsService;
pub use delete_records::DeleteRecordsService;
pub use delete_topics::DeleteTopicsService;
pub use describe_cluster::DescribeClusterService;
pub use describe_configs::DescribeConfigsService;
pub use describe_groups::DescribeGroupsService;
pub use describe_topic_partitions::DescribeTopicPartitionsService;
pub use fetch::FetchService;
pub use find_coordinator::FindCoordinatorService;
pub use get_telemetry_subscriptions::GetTelemetrySubscriptionsService;
pub use incremental_alter_configs::IncrementalAlterConfigsService;
pub use init_producer_id::InitProducerIdService;
pub use list_groups::ListGroupsService;
pub use list_offsets::ListOffsetsService;
pub use list_partition_reassignments::ListPartitionReassignmentsService;
pub use metadata::MetadataService;
pub use produce::ProduceService;
use rama::{Context, Layer, Service};
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
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument};
pub use txn::add_offsets::AddOffsetsService as TxnAddOffsetsService;
pub use txn::add_partitions::AddPartitionService as TxnAddPartitionService;
pub use txn::offset_commit::OffsetCommitService as TxnOffsetCommitService;
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, Error, GroupDetail, ListOffsetResponse, MetadataResponse,
    NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result, Storage,
    TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest,
    UpdateError, Version,
};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Request {
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
    OffsetStage(Topition),
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

#[derive(Clone, Debug)]
pub enum Response {
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

pub type RequestSender = mpsc::Sender<(Request, oneshot::Sender<Response>)>;
pub type RequestReceiver = mpsc::Receiver<(Request, oneshot::Sender<Response>)>;

pub fn bounded_channel(buffer: usize) -> (RequestSender, RequestReceiver) {
    mpsc::channel::<(Request, oneshot::Sender<Response>)>(buffer)
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum ServiceError {
    Storage(Error),
    UpdateGroupDetail(UpdateError<GroupDetail>),
}

impl Display for ServiceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<Error> for ServiceError {
    fn from(value: Error) -> Self {
        Self::Storage(value)
    }
}

impl From<UpdateError<GroupDetail>> for ServiceError {
    fn from(value: UpdateError<GroupDetail>) -> Self {
        Self::UpdateGroupDetail(value)
    }
}

impl From<ServiceError> for Error {
    fn from(value: ServiceError) -> Self {
        if let ServiceError::Storage(error) = value {
            error
        } else {
            unreachable!()
        }
    }
}

impl From<ServiceError> for UpdateError<GroupDetail> {
    fn from(value: ServiceError) -> Self {
        if let ServiceError::UpdateGroupDetail(error) = value {
            error
        } else {
            unreachable!()
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestLayer;

impl<S> Layer<S> for RequestLayer {
    type Service = RequestService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestService<S> {
    inner: S,
}

impl<State, S> Service<State, Request> for RequestService<S>
where
    S: Service<State, Request>,
    State: Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        req: Request,
    ) -> Result<Self::Response, Self::Error> {
        debug!(?req);
        self.inner.serve(ctx, req).await
    }
}

/// A [`Service`] sending [`Request`]s over a [`RequestSender`] channel
#[derive(Clone, Debug)]
pub struct RequestChannelService {
    tx: RequestSender,
}

impl RequestChannelService {
    pub fn new(tx: RequestSender) -> Self {
        Self { tx }
    }
}

impl<State> Service<State, Request> for RequestChannelService
where
    State: Send + Sync + 'static,
{
    type Response = Response;
    type Error = ServiceError;

    #[instrument(skip(ctx), ret)]
    async fn serve(
        &self,
        ctx: Context<State>,
        req: Request,
    ) -> Result<Self::Response, Self::Error> {
        let _ = ctx;
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx
            .send((req, resp_tx))
            .await
            .map_err(|_send_error| ServiceError::Storage(Error::UnableToSend))?;

        resp_rx.await.map_err(|_| Error::OneshotRecv.into())
    }
}

#[async_trait]
impl Storage for RequestChannelService {
    #[instrument(ret)]
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        self.serve(
            Context::default(),
            Request::RegisterBroker(broker_registration),
        )
        .await
        .and_then(|response| {
            if let Response::RegisterBroker(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        self.serve(
            Context::default(),
            Request::IncrementalAlterResource(resource),
        )
        .await
        .and_then(|response| {
            if let Response::IncrementalAlterResponse(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        self.serve(
            Context::default(),
            Request::CreateTopic {
                topic,
                validate_only,
            },
        )
        .await
        .and_then(|response| {
            if let Response::CreateTopic(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        self.serve(
            Context::default(),
            Request::DeleteRecords(Vec::from(topics)),
        )
        .await
        .and_then(|response| {
            if let Response::DeleteRecords(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        self.serve(Context::default(), Request::DeleteTopic(topic.to_owned()))
            .await
            .and_then(|response| {
                if let Response::DeleteTopic(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        self.serve(Context::default(), Request::Brokers)
            .await
            .and_then(|response| {
                if let Response::Brokers(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        let transaction_id = transaction_id.map(|s| s.to_string());
        let topition = topition.to_owned();

        self.serve(
            Context::default(),
            Request::Produce {
                transaction_id,
                topition,
                batch,
            },
        )
        .await
        .and_then(|response| {
            if let Response::Produce(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        let topition = topition.to_owned();

        self.serve(
            Context::default(),
            Request::Fetch {
                topition,
                offset,
                min_bytes,
                max_bytes,
                isolation,
            },
        )
        .await
        .and_then(|response| {
            if let Response::Fetch(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        self.serve(
            Context::default(),
            Request::OffsetStage(topition.to_owned()),
        )
        .await
        .and_then(|response| {
            if let Response::OffsetStage(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let offsets = Vec::from(offsets);

        self.serve(
            Context::default(),
            Request::ListOffsets {
                isolation_level,
                offsets,
            },
        )
        .await
        .and_then(|response| {
            if let Response::ListOffsets(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let group_id = group_id.to_string();
        let offsets = Vec::from(offsets);

        self.serve(
            Context::default(),
            Request::OffsetCommit {
                group_id,
                retention_time_ms,
                offsets,
            },
        )
        .await
        .and_then(|response| {
            if let Response::OffsetCommit(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        let group_id = group_id.to_string();

        self.serve(
            Context::default(),
            Request::CommittedOffsetTopitions(group_id),
        )
        .await
        .and_then(|response| {
            if let Response::CommittedOffsetTopitions(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        let group_id = group_id.map(|s| s.to_string());
        let topics = Vec::from(topics);

        self.serve(
            Context::default(),
            Request::OffsetFetch {
                group_id,
                topics,
                require_stable,
            },
        )
        .await
        .and_then(|response| {
            if let Response::OffsetFetch(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let topics = topics.map(Vec::from);

        self.serve(Context::default(), Request::Metadata(topics))
            .await
            .and_then(|response| {
                if let Response::Metadata(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let name = name.to_string();
        let keys = keys.map(Vec::from);

        self.serve(
            Context::default(),
            Request::DescribeConfig {
                name,
                resource,
                keys,
            },
        )
        .await
        .and_then(|response| {
            if let Response::DescribeConfig(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let topics = topics.map(Vec::from);

        self.serve(
            Context::default(),
            Request::DescribeTopicPartitions {
                topics,
                partition_limit,
                cursor,
            },
        )
        .await
        .and_then(|response| {
            if let Response::DescribeTopicPartitions(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        let states_filter = states_filter.map(Vec::from);

        self.serve(Context::default(), Request::ListGroups(states_filter))
            .await
            .and_then(|response| {
                if let Response::ListGroups(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let group_ids = group_ids.map(Vec::from);

        self.serve(Context::default(), Request::DeleteGroups(group_ids))
            .await
            .and_then(|response| {
                if let Response::DeleteGroups(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let group_ids = group_ids.map(Vec::from);

        self.serve(
            Context::default(),
            Request::DescribeGroups {
                group_ids,
                include_authorized_operations,
            },
        )
        .await
        .and_then(|response| {
            if let Response::DescribeGroups(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let group_id = group_id.to_string();

        self.serve(
            Context::default(),
            Request::UpdateGroup {
                group_id,
                detail,
                version,
            },
        )
        .await
        .and_then(|response| {
            if let Response::UpdateGroup(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        let transaction_id = transaction_id.map(|transaction_id| transaction_id.to_owned());

        self.serve(
            Context::default(),
            Request::InitProducer {
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            },
        )
        .await
        .and_then(|response| {
            if let Response::InitProducer(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        let transaction_id = transaction_id.to_string();
        let group_id = group_id.to_string();

        self.serve(
            Context::default(),
            Request::TxnAddOffsets {
                transaction_id,
                producer_id,
                producer_epoch,
                group_id,
            },
        )
        .await
        .and_then(|response| {
            if let Response::TxnAddOffsets(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        self.serve(Context::default(), Request::TxnAddPartitions(partitions))
            .await
            .and_then(|response| {
                if let Response::TxnAddPartitions(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.serve(Context::default(), Request::TxnOffsetCommit(offsets))
            .await
            .and_then(|response| {
                if let Response::TxnOffsetCommit(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let transaction_id = transaction_id.to_string();

        self.serve(
            Context::default(),
            Request::TxnEnd {
                transaction_id,
                producer_id,
                producer_epoch,
                committed,
            },
        )
        .await
        .and_then(|response| {
            if let Response::TxnEnd(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn maintain(&self) -> Result<()> {
        self.serve(Context::default(), Request::Maintain)
            .await
            .and_then(|response| {
                if let Response::Maintain(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn cluster_id(&self) -> Result<String> {
        self.serve(Context::default(), Request::ClusterId)
            .await
            .and_then(|response| {
                if let Response::ClusterId(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn node(&self) -> Result<i32> {
        self.serve(Context::default(), Request::Node)
            .await
            .and_then(|response| {
                if let Response::Node(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(ret)]
    async fn advertised_listener(&self) -> Result<Url> {
        self.serve(Context::default(), Request::AdvertisedLister)
            .await
            .and_then(|response| {
                if let Response::AdvertisedListener(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }
}

#[derive(Clone, Debug, Default)]
pub struct ChannelRequestLayer {
    cancellation: CancellationToken,
}

impl ChannelRequestLayer {
    pub fn new(cancellation: CancellationToken) -> Self {
        Self { cancellation }
    }
}

impl<S> Layer<S> for ChannelRequestLayer {
    type Service = ChannelRequestService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            cancellation: self.cancellation.clone(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ChannelRequestService<S> {
    inner: S,
    cancellation: CancellationToken,
}

impl<S, State> Service<State, RequestReceiver> for ChannelRequestService<S>
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
        loop {
            tokio::select! {
                Some((request, tx)) = req.recv() => {
                    debug!(?request, ?tx);

                    self.inner
                    .serve(ctx.clone(), request)
                    .await
                    .and_then(|response| {
                        tx.send(response).map_err(|_unsent| Error::UnableToSend)
                    })?
                }

                cancelled = self.cancellation.cancelled() => {
                    debug!(?cancelled);
                    break;
                }
            }
        }

        Ok(())
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestStorageService<G> {
    storage: G,
}

impl<G> RequestStorageService<G>
where
    G: Storage,
{
    pub fn new(storage: G) -> Self {
        Self { storage }
    }
}

impl<G, State> Service<State, Request> for RequestStorageService<G>
where
    G: Storage,
    State: Clone + Send + Sync + 'static,
{
    type Response = Response;
    type Error = Error;

    async fn serve(
        &self,
        _ctx: Context<State>,
        req: Request,
    ) -> Result<Self::Response, Self::Error> {
        match req {
            Request::RegisterBroker(broker_registration) => Ok(Response::RegisterBroker(
                self.storage.register_broker(broker_registration).await,
            )),
            Request::IncrementalAlterResource(alter_configs_resource) => {
                Ok(Response::IncrementalAlterResponse(
                    self.storage
                        .incremental_alter_resource(alter_configs_resource)
                        .await,
                ))
            }
            Request::CreateTopic {
                topic,
                validate_only,
            } => Ok(Response::CreateTopic(
                self.storage.create_topic(topic, validate_only).await,
            )),
            Request::DeleteRecords(delete_records_topics) => Ok(Response::DeleteRecords(
                self.storage
                    .delete_records(&delete_records_topics[..])
                    .await,
            )),
            Request::DeleteTopic(topic_id) => Ok(Response::DeleteTopic(
                self.storage.delete_topic(&topic_id).await,
            )),
            Request::Brokers => Ok(Response::Brokers(self.storage.brokers().await)),
            Request::Produce {
                transaction_id,
                topition,
                batch,
            } => Ok(Response::Produce(
                self.storage
                    .produce(transaction_id.as_deref(), &topition, batch)
                    .await,
            )),
            Request::Fetch {
                topition,
                offset,
                min_bytes,
                max_bytes,
                isolation,
            } => Ok(Response::Fetch(
                self.storage
                    .fetch(&topition, offset, min_bytes, max_bytes, isolation)
                    .await,
            )),
            Request::OffsetStage(topition) => Ok(Response::OffsetStage(
                self.storage.offset_stage(&topition).await,
            )),
            Request::ListOffsets {
                isolation_level,
                offsets,
            } => Ok(Response::ListOffsets(
                self.storage
                    .list_offsets(isolation_level, &offsets[..])
                    .await,
            )),
            Request::OffsetCommit {
                group_id,
                retention_time_ms,
                offsets,
            } => Ok(Response::OffsetCommit(
                self.storage
                    .offset_commit(&group_id, retention_time_ms, &offsets[..])
                    .await,
            )),
            Request::CommittedOffsetTopitions(group_id) => Ok(Response::CommittedOffsetTopitions(
                self.storage.committed_offset_topitions(&group_id).await,
            )),
            Request::OffsetFetch {
                group_id,
                topics,
                require_stable,
            } => Ok(Response::OffsetFetch(
                self.storage
                    .offset_fetch(group_id.as_deref(), &topics[..], require_stable)
                    .await,
            )),
            Request::Metadata(topic_ids) => Ok(Response::Metadata(
                self.storage.metadata(topic_ids.as_deref()).await,
            )),
            Request::DescribeConfig {
                name,
                resource,
                keys,
            } => Ok(Response::DescribeConfig(
                self.storage
                    .describe_config(&name, resource, keys.as_deref())
                    .await,
            )),
            Request::DescribeTopicPartitions {
                topics,
                partition_limit,
                cursor,
            } => Ok(Response::DescribeTopicPartitions(
                self.storage
                    .describe_topic_partitions(topics.as_deref(), partition_limit, cursor)
                    .await,
            )),
            Request::ListGroups(items) => Ok(Response::ListGroups(
                self.storage.list_groups(items.as_deref()).await,
            )),
            Request::DeleteGroups(items) => Ok(Response::DeleteGroups(
                self.storage.delete_groups(items.as_deref()).await,
            )),
            Request::DescribeGroups {
                group_ids,
                include_authorized_operations,
            } => Ok(Response::DescribeGroups(
                self.storage
                    .describe_groups(group_ids.as_deref(), include_authorized_operations)
                    .await,
            )),
            Request::UpdateGroup {
                group_id,
                detail,
                version,
            } => Ok(Response::UpdateGroup(
                self.storage.update_group(&group_id, detail, version).await,
            )),
            Request::InitProducer {
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            } => Ok(Response::InitProducer(
                self.storage
                    .init_producer(
                        transaction_id.as_deref(),
                        transaction_timeout_ms,
                        producer_id,
                        producer_epoch,
                    )
                    .await,
            )),
            Request::TxnAddOffsets {
                transaction_id,
                producer_id,
                producer_epoch,
                group_id,
            } => Ok(Response::TxnAddOffsets(
                self.storage
                    .txn_add_offsets(&transaction_id, producer_id, producer_epoch, &group_id)
                    .await,
            )),
            Request::TxnAddPartitions(txn_add_partitions_request) => {
                Ok(Response::TxnAddPartitions(
                    self.storage
                        .txn_add_partitions(txn_add_partitions_request)
                        .await,
                ))
            }
            Request::TxnOffsetCommit(txn_offset_commit_request) => Ok(Response::TxnOffsetCommit(
                self.storage
                    .txn_offset_commit(txn_offset_commit_request)
                    .await,
            )),
            Request::TxnEnd {
                transaction_id,
                producer_id,
                producer_epoch,
                committed,
            } => Ok(Response::TxnEnd(
                self.storage
                    .txn_end(&transaction_id, producer_id, producer_epoch, committed)
                    .await,
            )),
            Request::Maintain => Ok(Response::Maintain(self.storage.maintain().await)),
            Request::ClusterId => Ok(Response::ClusterId(self.storage.cluster_id().await)),
            Request::Node => Ok(Response::Node(self.storage.node().await)),
            Request::AdvertisedLister => Ok(Response::AdvertisedListener(
                self.storage.advertised_listener().await,
            )),
        }
    }
}
