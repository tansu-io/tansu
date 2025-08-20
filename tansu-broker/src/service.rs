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

use std::{collections::BTreeMap, fmt::Debug, sync::Arc};

use rama::{Context, Layer, Service, service::BoxService};
use tansu_sans_io::{
    AddOffsetsToTxnRequest, AddPartitionsToTxnRequest, ApiKey, ApiVersionsRequest,
    ApiVersionsResponse, Body, ConsumerGroupDescribeRequest, CreateTopicsRequest,
    DeleteGroupsRequest, DeleteRecordsRequest, DeleteTopicsRequest, DescribeClusterRequest,
    DescribeConfigsRequest, DescribeGroupsRequest, DescribeTopicPartitionsRequest, ErrorCode,
    FetchRequest, FindCoordinatorRequest, Frame, GetTelemetrySubscriptionsRequest,
    IncrementalAlterConfigsRequest, InitProducerIdRequest, ListGroupsRequest, ListOffsetsRequest,
    ListPartitionReassignmentsRequest, MetadataRequest, ProduceRequest, RootMessageMeta,
    TxnOffsetCommitRequest, api_versions_response::ApiVersion,
};
use tansu_storage::{
    Storage,
    service::{
        ConsumerGroupDescribeService, CreateTopicsService, DeleteGroupsService,
        DeleteRecordsService, DeleteTopicsService, DescribeClusterService, DescribeConfigsService,
        DescribeGroupsService, DescribeTopicPartitionsService, FetchService,
        FindCoordinatorService, GetTelemetrySubscriptionsService, IncrementalAlterConfigsService,
        InitProducerIdService, ListGroupsService, ListOffsetsService,
        ListPartitionReassignmentsService, MetadataService, ProduceService, StorageLayer,
        TxnAddOffsetsService, TxnAddPartitionService, TxnOffsetCommitService,
    },
};
use tracing::debug;

use crate::{
    Error, Result,
    broker::group::{
        heartbeat::HeartbeatService, join::JoinGroupService, leave::LeaveGroupService,
        offset_commit::OffsetCommitService, offset_fetch::OffsetFetchService,
        sync::SyncGroupService,
    },
    coordinator::{CoordinatorLayer, group::Coordinator},
    service::{frame::FramingService, tcp::TcpService},
};

pub mod frame;
pub mod storage;
pub mod tcp;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiVersionsService {
    supported: Vec<i16>,
}

impl ApiKey for ApiVersionsService {
    const KEY: i16 = ApiVersionsRequest::KEY;
}

impl<State> Service<State, Frame> for ApiVersionsService
where
    State: Clone + Send + Sync + 'static,
{
    type Response = Body;
    type Error = Error;

    async fn serve(
        &self,
        _ctx: Context<State>,
        _request: Frame,
    ) -> Result<Self::Response, Self::Error> {
        Ok(ApiVersionsResponse::default()
            .finalized_features(None)
            .finalized_features_epoch(None)
            .supported_features(None)
            .zk_migration_ready(None)
            .error_code(ErrorCode::None.into())
            .api_keys(Some(
                RootMessageMeta::messages()
                    .requests()
                    .iter()
                    .filter(|(api_key, _)| self.supported.contains(api_key))
                    .map(|(_, meta)| {
                        ApiVersion::default()
                            .api_key(meta.api_key)
                            .min_version(meta.version.valid.start)
                            .max_version(meta.version.valid.end)
                    })
                    .collect(),
            ))
            .throttle_time_ms(Some(0))
            .into())
    }
}

#[derive(Debug, Default)]
pub struct BrokerServiceBuilder {
    services: BTreeMap<i16, BoxService<(), Frame, Body, Error>>,
}

impl BrokerServiceBuilder {
    pub fn with_service<S>(mut self, service: S) -> Result<Self>
    where
        S: ApiKey + Debug + Service<(), Frame, Response = Body, Error = Error>,
    {
        self.services
            .insert(S::KEY, service.boxed())
            .map_or(Ok(self), |existing| {
                debug!(?existing);
                Err(Error::DuplicateApiService(S::KEY))
            })
    }

    pub fn build(self) -> Result<BrokerService> {
        let supported = self.services.keys().copied().collect::<Vec<_>>();

        self.with_service(ApiVersionsService { supported })
            .map(|builder| BrokerService {
                services: Arc::new(builder.services),
            })
    }
}

#[derive(Clone, Debug, Default)]
pub struct BrokerService {
    services: Arc<BTreeMap<i16, BoxService<(), Frame, Body, Error>>>,
}

impl BrokerService {
    pub fn builder() -> BrokerServiceBuilder {
        BrokerServiceBuilder::default()
    }
}

impl Service<(), Frame> for BrokerService {
    type Response = Body;
    type Error = Error;

    async fn serve(&self, ctx: Context<()>, req: Frame) -> Result<Self::Response, Self::Error> {
        let api_key = req.api_key()?;

        if let Some(service) = self.services.get(&api_key) {
            service.serve(ctx, req).await
        } else {
            Err(Error::UnsupportedApiService(api_key))
        }
    }
}

pub fn services<C, S>(
    cluster_id: &str,
    coordinator: C,
    storage: S,
) -> Result<TcpService<FramingService<BrokerService>>>
where
    S: Storage,
    C: Coordinator,
{
    api_services(coordinator, storage).map(|api_services| {
        TcpService::new(
            FramingService::new(api_services),
            None,
            cluster_id.to_owned(),
        )
    })
}

fn storage_services<S>(builder: BrokerServiceBuilder, storage: S) -> Result<BrokerServiceBuilder>
where
    S: Storage,
{
    builder
        .with_service(
            (
                ErrorAdapterLayer,
                StorageLayer::<_, ConsumerGroupDescribeRequest>::new(storage.clone()),
            )
                .into_layer(ConsumerGroupDescribeService),
        )
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, CreateTopicsRequest>::new(storage.clone()),
                )
                    .into_layer(CreateTopicsService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, DeleteGroupsRequest>::new(storage.clone()),
                )
                    .into_layer(DeleteGroupsService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, DeleteRecordsRequest>::new(storage.clone()),
                )
                    .into_layer(DeleteRecordsService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, DeleteTopicsRequest>::new(storage.clone()),
                )
                    .into_layer(DeleteTopicsService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, DescribeClusterRequest>::new(storage.clone()),
                )
                    .into_layer(DescribeClusterService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, DescribeConfigsRequest>::new(storage.clone()),
                )
                    .into_layer(DescribeConfigsService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, DescribeGroupsRequest>::new(storage.clone()),
                )
                    .into_layer(DescribeGroupsService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, DescribeTopicPartitionsRequest>::new(storage.clone()),
                )
                    .into_layer(DescribeTopicPartitionsService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, FetchRequest>::new(storage.clone()),
                )
                    .into_layer(FetchService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, FindCoordinatorRequest>::new(storage.clone()),
                )
                    .into_layer(FindCoordinatorService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, IncrementalAlterConfigsRequest>::new(storage.clone()),
                )
                    .into_layer(IncrementalAlterConfigsService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, InitProducerIdRequest>::new(storage.clone()),
                )
                    .into_layer(InitProducerIdService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, ListGroupsRequest>::new(storage.clone()),
                )
                    .into_layer(ListGroupsService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, ListOffsetsRequest>::new(storage.clone()),
                )
                    .into_layer(ListOffsetsService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, ListPartitionReassignmentsRequest>::new(storage.clone()),
                )
                    .into_layer(ListPartitionReassignmentsService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, MetadataRequest>::new(storage.clone()),
                )
                    .into_layer(MetadataService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, ProduceRequest>::new(storage.clone()),
                )
                    .into_layer(ProduceService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, GetTelemetrySubscriptionsRequest>::new(storage.clone()),
                )
                    .into_layer(GetTelemetrySubscriptionsService),
            )
        })
}

fn storage_txn_services<S>(
    builder: BrokerServiceBuilder,
    storage: S,
) -> Result<BrokerServiceBuilder>
where
    S: Storage,
{
    builder
        .with_service(
            (
                ErrorAdapterLayer,
                StorageLayer::<_, AddOffsetsToTxnRequest>::new(storage.clone()),
            )
                .into_layer(TxnAddOffsetsService),
        )
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, AddPartitionsToTxnRequest>::new(storage.clone()),
                )
                    .into_layer(TxnAddPartitionService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                (
                    ErrorAdapterLayer,
                    StorageLayer::<_, TxnOffsetCommitRequest>::new(storage.clone()),
                )
                    .into_layer(TxnOffsetCommitService),
            )
        })
}

fn consumer_group_services<C>(
    builder: BrokerServiceBuilder,
    coordinator: C,
) -> Result<BrokerServiceBuilder>
where
    C: Coordinator,
{
    builder
        .with_service(CoordinatorLayer::new(coordinator.clone()).into_layer(HeartbeatService))
        .and_then(|builder| {
            builder.with_service(
                CoordinatorLayer::new(coordinator.clone()).into_layer(JoinGroupService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                CoordinatorLayer::new(coordinator.clone()).into_layer(LeaveGroupService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                CoordinatorLayer::new(coordinator.clone()).into_layer(OffsetCommitService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                CoordinatorLayer::new(coordinator.clone()).into_layer(OffsetFetchService),
            )
        })
        .and_then(|builder| {
            builder.with_service(
                CoordinatorLayer::new(coordinator.clone()).into_layer(SyncGroupService),
            )
        })
}

fn api_services<C, S>(coordinator: C, storage: S) -> Result<BrokerService>
where
    S: Storage,
    C: Coordinator,
{
    storage_services(BrokerService::builder(), storage.clone())
        .and_then(|builder| storage_txn_services(builder, storage))
        .and_then(|builder| consumer_group_services(builder, coordinator))
        .and_then(|builder| builder.build())
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ErrorAdapterService<S> {
    inner: S,
}

impl<S> ApiKey for ErrorAdapterService<S>
where
    S: ApiKey,
{
    const KEY: i16 = S::KEY;
}

impl<State, S> Service<State, Frame> for ErrorAdapterService<S>
where
    S: Service<State, Frame>,
    State: Send + Sync + 'static,
    Error: From<S::Error>,
{
    type Response = S::Response;
    type Error = Error;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        self.inner.serve(ctx, req).await.map_err(Into::into)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ErrorAdapterLayer;

impl<S> Layer<S> for ErrorAdapterLayer {
    type Service = ErrorAdapterService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}
