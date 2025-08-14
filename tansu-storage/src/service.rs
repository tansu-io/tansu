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

use std::collections::BTreeMap;

use bytes::Bytes;
use rama::{Context, Layer, Service, service::BoxService};
use tansu_sans_io::{ApiKey, Body, Frame, Header};

use crate::{Error, Result};

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
mod incremental_alter_configs;
mod init_producer_id;
mod list_groups;
mod list_offsets;
mod list_partition_reassignments;
mod metadata;
mod produce;
mod telemetry;
mod txn;

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
pub use incremental_alter_configs::IncrementalAlterConfigsService;
pub use init_producer_id::InitProducerIdService;
pub use list_groups::ListGroupsService;
pub use list_offsets::ListOffsetsService;
pub use list_partition_reassignments::ListPartitionReassignmentsService;
pub use metadata::MetadataService;
pub use produce::ProduceService;
pub use telemetry::GetTelemetrySubscriptionsService;
pub use txn::add_offsets::AddOffsetsService as TxnAddOffsetsService;
pub use txn::add_partitions::AddPartitionService as TxnAddPartitionService;
pub use txn::offset_commit::OffsetCommitService as TxnOffsetCommitService;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesService<S> {
    inner: S,
}

impl<S, State> Service<State, Bytes> for BytesService<S>
where
    S: Service<State, Frame, Response = Frame>,
    State: Clone + Send + Sync + 'static,
    S::Error: Into<Error>,
{
    type Response = Bytes;

    type Error = Error;

    async fn serve(&self, ctx: Context<State>, req: Bytes) -> Result<Self::Response, Self::Error> {
        let request = Frame::request_from_bytes(req)?;
        let api_key = request.api_key()?;
        let api_version = request.api_version()?;

        let Frame { header, body, .. } =
            self.inner.serve(ctx, request).await.map_err(Into::into)?;

        Frame::response(header, body, api_key, api_version).map_err(Into::into)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesLayer;

impl<S> Layer<S> for BytesLayer {
    type Service = BytesService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BytesService { inner }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FramingService<S> {
    inner: S,
}

impl<S, State> Service<State, Frame> for FramingService<S>
where
    S: Service<State, Body, Response = Body>,
    State: Clone + Send + Sync + 'static,
    S::Error: Into<Error>,
{
    type Response = Frame;

    type Error = Error;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let correlation_id = req.correlation_id()?;

        self.inner
            .serve(ctx, req.body)
            .await
            .map_err(Into::into)
            .map(|body| Frame {
                header: Header::Response { correlation_id },
                body,
                size: 0,
            })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FramingLayer;

impl<S> Layer<S> for FramingLayer {
    type Service = FramingService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        FramingService { inner }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BodyService<S> {
    inner: S,
}

impl<S, State> Service<State, Body> for BodyService<S>
where
    S: Service<State, Body, Response = Body>,
    State: Clone + Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Body) -> Result<Self::Response, Self::Error> {
        self.inner.serve(ctx, req).await
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BodyLayer;

impl<S> Layer<S> for BodyLayer {
    type Service = BodyService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BodyService { inner }
    }
}

#[derive(Debug, Default)]
pub struct BodyApiService<State> {
    services: BTreeMap<i16, BoxService<State, Body, Body, Error>>,
}

impl<State> BodyApiService<State> {
    pub fn with_service<S>(mut self, service: S) -> Self
    where
        S: ApiKey,
        S: Service<State, Body, Response = Body, Error = Error>,
    {
        _ = self.services.insert(S::KEY, service.boxed());
        self
    }
}

impl<State> Service<State, Body> for BodyApiService<State>
where
    State: Clone + Send + Sync + 'static,
{
    type Response = Body;
    type Error = Error;

    async fn serve(&self, ctx: Context<State>, req: Body) -> Result<Self::Response, Self::Error> {
        if let Some(service) = self.services.get(&req.api_key()) {
            service.serve(ctx, req).await
        } else {
            todo!()
        }
    }
}
