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

use std::{collections::BTreeMap, fmt::Debug, time::Duration};

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
use rama::{Context, Layer, Service, service::BoxService};
use tansu_sans_io::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, AddPartitionsToTxnRequest,
    AddPartitionsToTxnResponse, ConsumerGroupDescribeRequest, ConsumerGroupDescribeResponse,
    CreateTopicsRequest, CreateTopicsResponse, DeleteGroupsRequest, DeleteGroupsResponse,
    DeleteRecordsRequest, DeleteRecordsResponse, DeleteTopicsRequest, DeleteTopicsResponse,
    DescribeClusterRequest, DescribeClusterResponse, DescribeConfigsRequest,
    DescribeConfigsResponse, DescribeGroupsRequest, DescribeGroupsResponse,
    DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, ErrorCode, FetchRequest,
    FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse,
    GetTelemetrySubscriptionsRequest, GetTelemetrySubscriptionsResponse,
    IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse, InitProducerIdRequest,
    InitProducerIdResponse, IsolationLevel, ListGroupsRequest, ListGroupsResponse, ListOffset,
    ListOffsetsRequest, ListOffsetsResponse, ListPartitionReassignmentsRequest,
    ListPartitionReassignmentsResponse, MetadataRequest, ProduceRequest, ProduceResponse,
    TxnOffsetCommitResponse, consumer_group_describe_response::DescribedGroup,
    create_topics_request::CreatableTopic, delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse, record::deflated,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::debug;
pub use txn::add_offsets::AddOffsetsService as TxnAddOffsetsService;
pub use txn::add_partitions::AddPartitionService as TxnAddPartitionService;
pub use txn::offset_commit::OffsetCommitService as TxnOffsetCommitService;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, Error, ListOffsetResponse, MetadataResponse, OffsetCommitRequest,
    OffsetStage, Result, Storage, TopicId, Topition, TxnOffsetCommitRequest,
};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Request {
    AddOffsetsToTxn(AddOffsetsToTxnRequest),
    AddPartitionsToTxn(AddPartitionsToTxnRequest),
    ConsumerGroupDescribe(ConsumerGroupDescribeRequest),
    CreateTopics(CreateTopicsRequest),
    DeleteGroups(DeleteGroupsRequest),
    DeleteRecords(DeleteRecordsRequest),
    DeleteTopics(DeleteTopicsRequest),
    DescribeCluster(DescribeClusterRequest),
    DescribeConfigs(DescribeConfigsRequest),
    DescribeGroups(DescribeGroupsRequest),
    DescribeTopicPartitions(DescribeTopicPartitionsRequest),
    Fetch(FetchRequest),
    FindCoordinator(FindCoordinatorRequest),
    GetTelemetrySubscriptions(GetTelemetrySubscriptionsRequest),
    IncrementalAlterConfigs(IncrementalAlterConfigsRequest),
    InitProducerId(InitProducerIdRequest),
    ListGroups(ListGroupsRequest),
    ListOffsets(ListOffsetsRequest),
    ListPartitionReassignments(ListPartitionReassignmentsRequest),
    Metadata(MetadataRequest),
    Produce(ProduceRequest),
    TxnOffsetCommit(TxnOffsetCommitRequest),
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Response {
    AddOffsetsToTxn(AddOffsetsToTxnResponse),
    AddPartitionsToTxn(AddPartitionsToTxnResponse),
    ConsumerGroupDescribe(ConsumerGroupDescribeResponse),
    CreateTopics(CreateTopicsResponse),
    DeleteGroups(DeleteGroupsResponse),
    DeleteRecords(DeleteRecordsResponse),
    DeleteTopics(DeleteTopicsResponse),
    DescribeCluster(DescribeClusterResponse),
    DescribeConfigs(DescribeConfigsResponse),
    DescribeGroups(DescribeGroupsResponse),
    DescribeTopicPartitions(DescribeTopicPartitionsResponse),
    Fetch(FetchResponse),
    FindCoordinator(FindCoordinatorResponse),
    GetTelemetrySubscriptions(GetTelemetrySubscriptionsResponse),
    IncrementalAlterConfigs(IncrementalAlterConfigsResponse),
    InitProducerId(InitProducerIdResponse),
    ListGroups(ListGroupsResponse),
    ListOffsets(ListOffsetsResponse),
    ListPartitionReassignments(ListPartitionReassignmentsResponse),
    Metadata(MetadataResponse),
    Produce(ProduceResponse),
    TxnOffsetCommit(TxnOffsetCommitResponse),
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StorageService<S> {
    inner: S,
}

impl<State, S> Service<State, Request> for StorageService<S>
where
    S: Service<State, Request, Response = Response>,
    State: Send + Sync + 'static,
{
    type Response = Response;
    type Error = S::Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        req: Request,
    ) -> Result<Self::Response, Self::Error> {
        self.inner.serve(ctx, req).await
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StorageLayer;

impl<S> Layer<S> for StorageLayer {
    type Service = StorageService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

type RequestSender = mpsc::Sender<(Request, oneshot::Sender<Response>)>;
type RequestReceiver = mpsc::Receiver<(Request, oneshot::Sender<Response>)>;

fn bounded_channel(buffer: usize) -> (RequestSender, RequestReceiver) {
    mpsc::channel::<(Request, oneshot::Sender<Response>)>(buffer)
}

#[derive(Clone, Debug, Default)]
pub struct ChannelRequestService<S> {
    inner: S,
    cancellation: CancellationToken,
}

impl<S, State> Service<State, RequestReceiver> for ChannelRequestService<S>
where
    S: Service<State, Request, Response = Response>,
    State: Clone + Send + Sync + 'static,
    S::Error: From<Error>,
{
    type Response = ();
    type Error = S::Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        mut req: RequestReceiver,
    ) -> Result<Self::Response, Self::Error> {
        loop {
            tokio::select! {
                Some((req, tx)) = req.recv() => {
                    debug!(?req, ?tx);

                    self.inner
                        .serve(ctx.clone(), req)
                        .await
                        .and_then(|response| {
                            tx.send(response)
                                .map_err(|_unsent| Error::UnableToSend)
                                .map_err(Into::into)
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

#[derive(Clone, Debug)]
pub struct RequestChannelService {
    tx: RequestSender,
}

impl<State> Service<State, Request> for RequestChannelService
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
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx
            .send((req, resp_tx))
            .await
            .map_err(|_send_error| Error::UnableToSend)?;

        resp_rx.await.map_err(|_| Error::OneshotRecv)
    }
}

pub struct RequestRouteService<State> {
    add_offsets_to_txn:
        Option<BoxService<State, AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, Error>>,
    add_partitions_to_txn:
        Option<BoxService<State, AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, Error>>,
    consumer_group_describe: Option<
        BoxService<State, ConsumerGroupDescribeRequest, ConsumerGroupDescribeResponse, Error>,
    >,
    create_topics: BoxService<State, CreateTopicsRequest, CreateTopicsResponse, Error>,
    delete_groups: BoxService<State, DeleteGroupsRequest, DeleteGroupsResponse, Error>,
    delete_records: BoxService<State, DeleteRecordsRequest, DeleteRecordsResponse, Error>,
    delete_topics: BoxService<State, DeleteTopicsRequest, DeleteTopicsResponse, Error>,
    describe_cluster: BoxService<State, DescribeClusterRequest, DescribeClusterResponse, Error>,
    describe_configs: BoxService<State, DescribeConfigsRequest, DescribeConfigsResponse, Error>,
    describe_groups: BoxService<State, DescribeGroupsRequest, DescribeGroupsResponse, Error>,
    describe_topic_partitions:
        BoxService<State, DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, Error>,
    fetch: BoxService<State, FetchRequest, FetchResponse, Error>,
    find_coordinator: BoxService<State, FindCoordinatorRequest, FindCoordinatorResponse, Error>,
    get_telemetry_subscriptions: BoxService<
        State,
        GetTelemetrySubscriptionsRequest,
        GetTelemetrySubscriptionsResponse,
        Error,
    >,
    incremental_alter_configs:
        BoxService<State, IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse, Error>,
    init_producer_id: BoxService<State, InitProducerIdRequest, InitProducerIdResponse, Error>,
    list_groups: BoxService<State, ListGroupsRequest, ListGroupsResponse, Error>,
    list_offsets: BoxService<State, ListOffsetsRequest, ListOffsetsResponse, Error>,
    list_partition_reassignments: BoxService<
        State,
        ListPartitionReassignmentsRequest,
        ListPartitionReassignmentsResponse,
        Error,
    >,
    metadata: BoxService<State, MetadataRequest, MetadataResponse, Error>,
    produce: BoxService<State, ProduceRequest, ProduceResponse, Error>,
    txn_offset_commit: BoxService<State, TxnOffsetCommitRequest, TxnOffsetCommitResponse, Error>,
}

impl<State> Service<State, Request> for RequestRouteService<State>
where
    State: Send + Sync + 'static,
{
    type Response = Response;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        req: Request,
    ) -> Result<Self::Response, Self::Error> {
        match req {
            Request::AddOffsetsToTxn(req) => {
                if let Some(ref service) = self.add_offsets_to_txn {
                    service.serve(ctx, req).await.map(Response::AddOffsetsToTxn)
                } else {
                    Ok(Response::AddOffsetsToTxn(
                        AddOffsetsToTxnResponse::default()
                            .error_code(ErrorCode::UnknownServerError.into()),
                    ))
                }
            }
            Request::AddPartitionsToTxn(req) => {
                if let Some(ref service) = self.add_partitions_to_txn {
                    service
                        .serve(ctx, req)
                        .await
                        .map(Response::AddPartitionsToTxn)
                } else {
                    Ok(Response::AddPartitionsToTxn(
                        AddPartitionsToTxnResponse::default()
                            .error_code(Some(ErrorCode::UnknownServerError.into())),
                    ))
                }
            }
            Request::ConsumerGroupDescribe(req) => {
                if let Some(ref service) = self.consumer_group_describe {
                    service
                        .serve(ctx, req)
                        .await
                        .map(Response::ConsumerGroupDescribe)
                } else {
                    Ok(Response::ConsumerGroupDescribe(
                        ConsumerGroupDescribeResponse::default().groups(Some(
                            req.group_ids
                                .unwrap_or_default()
                                .iter()
                                .map(|group_id| DescribedGroup::default())
                                .collect::<Vec<_>>(),
                        )),
                    ))
                }
            }
            Request::CreateTopics(create_topics_request) => self
                .create_topics
                .serve(ctx, create_topics_request)
                .await
                .map(Response::CreateTopics),
            Request::DeleteGroups(delete_groups_request) => self
                .delete_groups
                .serve(ctx, delete_groups_request)
                .await
                .map(Response::DeleteGroups),
            Request::DeleteRecords(delete_records_request) => self
                .delete_records
                .serve(ctx, delete_records_request)
                .await
                .map(Response::DeleteRecords),
            Request::DeleteTopics(delete_topics_request) => self
                .delete_topics
                .serve(ctx, delete_topics_request)
                .await
                .map(Response::DeleteTopics),
            Request::DescribeCluster(describe_cluster_request) => self
                .describe_cluster
                .serve(ctx, describe_cluster_request)
                .await
                .map(Response::DescribeCluster),
            Request::DescribeConfigs(describe_configs_request) => self
                .describe_configs
                .serve(ctx, describe_configs_request)
                .await
                .map(Response::DescribeConfigs),
            Request::DescribeGroups(describe_groups_request) => self
                .describe_groups
                .serve(ctx, describe_groups_request)
                .await
                .map(Response::DescribeGroups),
            Request::DescribeTopicPartitions(describe_topic_partitions_request) => self
                .describe_topic_partitions
                .serve(ctx, describe_topic_partitions_request)
                .await
                .map(Response::DescribeTopicPartitions),
            Request::Fetch(fetch_request) => self
                .fetch
                .serve(ctx, fetch_request)
                .await
                .map(Response::Fetch),
            Request::FindCoordinator(find_coordinator_request) => self
                .find_coordinator
                .serve(ctx, find_coordinator_request)
                .await
                .map(Response::FindCoordinator),
            Request::GetTelemetrySubscriptions(get_telemetry_subscriptions_request) => self
                .get_telemetry_subscriptions
                .serve(ctx, get_telemetry_subscriptions_request)
                .await
                .map(Response::GetTelemetrySubscriptions),
            Request::IncrementalAlterConfigs(incremental_alter_configs_request) => self
                .incremental_alter_configs
                .serve(ctx, incremental_alter_configs_request)
                .await
                .map(Response::IncrementalAlterConfigs),
            Request::InitProducerId(init_producer_id_request) => self
                .init_producer_id
                .serve(ctx, init_producer_id_request)
                .await
                .map(Response::InitProducerId),
            Request::ListGroups(list_groups_request) => self
                .list_groups
                .serve(ctx, list_groups_request)
                .await
                .map(Response::ListGroups),
            Request::ListOffsets(list_offsets_request) => self
                .list_offsets
                .serve(ctx, list_offsets_request)
                .await
                .map(Response::ListOffsets),
            Request::ListPartitionReassignments(list_partition_reassignments_request) => self
                .list_partition_reassignments
                .serve(ctx, list_partition_reassignments_request)
                .await
                .map(Response::ListPartitionReassignments),
            Request::Metadata(metadata_request) => self
                .metadata
                .serve(ctx, metadata_request)
                .await
                .map(Response::Metadata),
            Request::Produce(produce_request) => self
                .produce
                .serve(ctx, produce_request)
                .await
                .map(Response::Produce),
            Request::TxnOffsetCommit(txn_offset_commit_request) => self
                .txn_offset_commit
                .serve(ctx, txn_offset_commit_request)
                .await
                .map(Response::TxnOffsetCommit),
        }
    }
}
