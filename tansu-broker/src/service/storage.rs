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

use rama::{
    Layer as _, Service as _,
    layer::{MapErrLayer, MapStateLayer},
};
use tansu_sans_io::{
    AddOffsetsToTxnRequest, AddPartitionsToTxnRequest, ApiKey as _, ConsumerGroupDescribeRequest,
    CreateTopicsRequest, DeleteGroupsRequest, DeleteRecordsRequest, DeleteTopicsRequest,
    DescribeClusterRequest, DescribeConfigsRequest, DescribeGroupsRequest,
    DescribeTopicPartitionsRequest, FetchRequest, FindCoordinatorRequest,
    GetTelemetrySubscriptionsRequest, IncrementalAlterConfigsRequest, InitProducerIdRequest,
    ListGroupsRequest, ListOffsetsRequest, ListPartitionReassignmentsRequest, MetadataRequest,
    ProduceRequest, TxnOffsetCommitRequest,
};
use tansu_service::{FrameRequestLayer, FrameRouteBuilder};
use tansu_storage::{
    ConsumerGroupDescribeService, CreateTopicsService, DeleteGroupsService, DeleteRecordsService,
    DeleteTopicsService, DescribeClusterService, DescribeConfigsService, DescribeGroupsService,
    DescribeTopicPartitionsService, FetchService, FindCoordinatorService,
    GetTelemetrySubscriptionsService, IncrementalAlterConfigsService, InitProducerIdService,
    ListGroupsService, ListOffsetsService, ListPartitionReassignmentsService, MetadataService,
    ProduceService, Storage, TxnAddOffsetsService, TxnAddPartitionService, TxnOffsetCommitService,
};

use crate::Error;

pub fn services<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    [
        add_offsets_to_txn,
        add_partitions_to_txn,
        consumer_group_describe,
        create_topics,
        delete_groups,
        delete_records,
        delete_topics,
        describe_cluster,
        describe_configs,
        describe_groups,
        describe_topic_partitions,
        fetch,
        find_coordinator,
        get_telemetry_subscriptions,
        incremental_alter_configs,
        init_producer_id,
        list_groups,
        list_offsets,
        list_partition_reassignments,
        metadata,
        produce,
        txn_offset_commit_request,
    ]
    .iter()
    .try_fold(builder, |builder, service| {
        service(builder, storage.clone())
    })
}

pub fn consumer_group_describe<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            ConsumerGroupDescribeRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<ConsumerGroupDescribeRequest>::new(),
            )
                .into_layer(ConsumerGroupDescribeService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn create_topics<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            CreateTopicsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<CreateTopicsRequest>::new(),
            )
                .into_layer(CreateTopicsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn delete_groups<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            DeleteGroupsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<DeleteGroupsRequest>::new(),
            )
                .into_layer(DeleteGroupsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn delete_records<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            DeleteRecordsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<DeleteRecordsRequest>::new(),
            )
                .into_layer(DeleteRecordsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn delete_topics<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            DeleteTopicsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<DeleteTopicsRequest>::new(),
            )
                .into_layer(DeleteTopicsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn describe_cluster<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            DescribeClusterRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<DescribeClusterRequest>::new(),
            )
                .into_layer(DescribeClusterService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn describe_configs<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            DescribeConfigsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<DescribeConfigsRequest>::new(),
            )
                .into_layer(DescribeConfigsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn describe_groups<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            DescribeGroupsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<DescribeGroupsRequest>::new(),
            )
                .into_layer(DescribeGroupsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn describe_topic_partitions<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            DescribeTopicPartitionsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<DescribeTopicPartitionsRequest>::new(),
            )
                .into_layer(DescribeTopicPartitionsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn fetch<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            FetchRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<FetchRequest>::new(),
            )
                .into_layer(FetchService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn find_coordinator<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            FindCoordinatorRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<FindCoordinatorRequest>::new(),
            )
                .into_layer(FindCoordinatorService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn incremental_alter_configs<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            IncrementalAlterConfigsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<IncrementalAlterConfigsRequest>::new(),
            )
                .into_layer(IncrementalAlterConfigsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn init_producer_id<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            InitProducerIdRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<InitProducerIdRequest>::new(),
            )
                .into_layer(InitProducerIdService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn list_groups<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            ListGroupsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<ListGroupsRequest>::new(),
            )
                .into_layer(ListGroupsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn list_offsets<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            ListOffsetsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<ListOffsetsRequest>::new(),
            )
                .into_layer(ListOffsetsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn list_partition_reassignments<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            ListPartitionReassignmentsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<ListPartitionReassignmentsRequest>::new(),
            )
                .into_layer(ListPartitionReassignmentsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn metadata<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            MetadataRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<MetadataRequest>::new(),
            )
                .into_layer(MetadataService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn produce<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            ProduceRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<ProduceRequest>::new(),
            )
                .into_layer(ProduceService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn get_telemetry_subscriptions<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            GetTelemetrySubscriptionsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<GetTelemetrySubscriptionsRequest>::new(),
            )
                .into_layer(GetTelemetrySubscriptionsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn add_offsets_to_txn<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            AddOffsetsToTxnRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<AddOffsetsToTxnRequest>::new(),
            )
                .into_layer(TxnAddOffsetsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn add_partitions_to_txn<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            AddPartitionsToTxnRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<AddPartitionsToTxnRequest>::new(),
            )
                .into_layer(TxnAddPartitionService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn txn_offset_commit_request<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            TxnOffsetCommitRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<TxnOffsetCommitRequest>::new(),
            )
                .into_layer(TxnOffsetCommitService)
                .boxed(),
        )
        .map_err(Into::into)
}
