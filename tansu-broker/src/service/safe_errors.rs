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

use rama::{
    Context, Layer as _, Service as _,
    layer::{MapErrLayer, MapStateLayer},
};
use tansu_sans_io::{
    AlterConfigsRequest, AlterConfigsResponse, AlterReplicaLogDirsRequest,
    AlterReplicaLogDirsResponse, ApiKey as _, CreateDelegationTokenRequest,
    CreateDelegationTokenResponse, CreatePartitionsRequest, CreatePartitionsResponse,
    DeleteAclsRequest, DeleteAclsResponse, ElectLeadersRequest, ElectLeadersResponse,
    EndTxnRequest, EndTxnResponse, ErrorCode, ListTransactionsRequest, ListTransactionsResponse,
    OffsetDeleteRequest, OffsetDeleteResponse, PushTelemetryRequest, PushTelemetryResponse,
    UpdateFeaturesRequest, UpdateFeaturesResponse, WriteTxnMarkersRequest, WriteTxnMarkersResponse,
    alter_configs_response::AlterConfigsResourceResponse as _AlterConfigsResourceResponse,
    alter_replica_log_dirs_response::{
        AlterReplicaLogDirPartitionResult as _AlterReplicaLogDirPartitionResult,
        AlterReplicaLogDirTopicResult as _AlterReplicaLogDirTopicResult,
    },
    create_partitions_response::CreatePartitionsTopicResult as _CreatePartitionsTopicResult,
    delete_acls_response::DeleteAclsFilterResult as _DeleteAclsFilterResult,
    offset_delete_response::{
        OffsetDeleteResponsePartition as _OffsetDeleteResponsePartition,
        OffsetDeleteResponseTopic as _OffsetDeleteResponseTopic,
    },
    update_features_response::UpdatableFeatureResult,
    write_txn_markers_response::{
        WritableTxnMarkerPartitionResult as _WritableTxnMarkerPartitionResult,
        WritableTxnMarkerResult as _WritableTxnMarkerResult,
        WritableTxnMarkerTopicResult as _WritableTxnMarkerTopicResult,
    },
};
use tansu_service::{FrameRequestLayer, FrameRouteBuilder, ResponseService};

use crate::Error;

macro_rules! safe_error_route {
    ($name:ident, $request:ty, $request_var:ident, $response:ty, $body:block) => {
        pub fn $name(
            builder: FrameRouteBuilder<(), Error>,
        ) -> Result<FrameRouteBuilder<(), Error>, Error> {
            builder
                .with_route(
                    <$request>::KEY,
                    (
                        MapErrLayer::new(Error::from),
                        MapStateLayer::new(|_| ()),
                        FrameRequestLayer::<$request>::new(),
                    )
                        .into_layer(ResponseService::new(
                            |_ctx: Context<()>, $request_var: $request| {
                                Ok::<$response, Error>($body)
                            },
                        ))
                        .boxed(),
                )
                .map_err(Into::into)
        }
    };
}

fn unknown_server_error() -> i16 {
    i16::from(ErrorCode::UnknownServerError)
}

fn unknown_server_error_message() -> String {
    ErrorCode::UnknownServerError.to_string()
}

safe_error_route!(
    alter_configs,
    AlterConfigsRequest,
    req,
    AlterConfigsResponse,
    {
        let responses = req
            .resources
            .unwrap_or_default()
            .into_iter()
            .map(|resource| {
                _AlterConfigsResourceResponse::default()
                    .resource_type(resource.resource_type)
                    .resource_name(resource.resource_name)
                    .error_code(unknown_server_error())
            })
            .collect();

        AlterConfigsResponse::default()
            .throttle_time_ms(0)
            .responses(Some(responses))
    }
);

safe_error_route!(
    alter_replica_log_dirs,
    AlterReplicaLogDirsRequest,
    req,
    AlterReplicaLogDirsResponse,
    {
        let results = req
            .dirs
            .unwrap_or_default()
            .into_iter()
            .flat_map(|dir| {
                dir.topics.unwrap_or_default().into_iter().map(|topic| {
                    let partitions = topic
                        .partitions
                        .unwrap_or_default()
                        .into_iter()
                        .map(|partition_index| {
                            _AlterReplicaLogDirPartitionResult::default()
                                .partition_index(partition_index)
                                .error_code(unknown_server_error())
                        })
                        .collect();

                    _AlterReplicaLogDirTopicResult::default()
                        .topic_name(topic.name)
                        .partitions(Some(partitions))
                })
            })
            .collect();

        AlterReplicaLogDirsResponse::default()
            .throttle_time_ms(0)
            .results(Some(results))
    }
);

safe_error_route!(
    create_partitions,
    CreatePartitionsRequest,
    req,
    CreatePartitionsResponse,
    {
        let results = req
            .topics
            .unwrap_or_default()
            .into_iter()
            .map(|topic| {
                _CreatePartitionsTopicResult::default()
                    .name(topic.name)
                    .error_code(unknown_server_error())
                    .error_message(Some(unknown_server_error_message()))
            })
            .collect();

        CreatePartitionsResponse::default()
            .throttle_time_ms(0)
            .results(Some(results))
    }
);

safe_error_route!(
    create_delegation_token,
    CreateDelegationTokenRequest,
    _req,
    CreateDelegationTokenResponse,
    {
        CreateDelegationTokenResponse::default()
            .error_code(unknown_server_error())
            .throttle_time_ms(0)
    }
);

safe_error_route!(delete_acls, DeleteAclsRequest, req, DeleteAclsResponse, {
    let filter_results = req
        .filters
        .unwrap_or_default()
        .into_iter()
        .map(|_| {
            _DeleteAclsFilterResult::default()
                .error_code(unknown_server_error())
                .error_message(Some(unknown_server_error_message()))
                .matching_acls(None)
        })
        .collect();

    DeleteAclsResponse::default()
        .throttle_time_ms(0)
        .filter_results(Some(filter_results))
});

safe_error_route!(
    write_txn_markers,
    WriteTxnMarkersRequest,
    req,
    WriteTxnMarkersResponse,
    {
        let markers = req
            .markers
            .unwrap_or_default()
            .into_iter()
            .map(|marker| {
                let topics = marker
                    .topics
                    .unwrap_or_default()
                    .into_iter()
                    .map(|topic| {
                        let partitions = topic
                            .partition_indexes
                            .unwrap_or_default()
                            .into_iter()
                            .map(|partition_index| {
                                _WritableTxnMarkerPartitionResult::default()
                                    .partition_index(partition_index)
                                    .error_code(unknown_server_error())
                            })
                            .collect();

                        _WritableTxnMarkerTopicResult::default()
                            .name(topic.name)
                            .partitions(Some(partitions))
                    })
                    .collect();

                _WritableTxnMarkerResult::default()
                    .producer_id(marker.producer_id)
                    .topics(Some(topics))
            })
            .collect();

        WriteTxnMarkersResponse::default().markers(Some(markers))
    }
);

safe_error_route!(
    describe_log_dirs,
    tansu_sans_io::DescribeLogDirsRequest,
    _req,
    tansu_sans_io::DescribeLogDirsResponse,
    {
        tansu_sans_io::DescribeLogDirsResponse::default()
            .throttle_time_ms(0)
            .error_code(Some(unknown_server_error()))
            .results(Some([].into()))
    }
);

safe_error_route!(
    elect_leaders,
    ElectLeadersRequest,
    _req,
    ElectLeadersResponse,
    {
        ElectLeadersResponse::default()
            .throttle_time_ms(0)
            .error_code(Some(unknown_server_error()))
            .replica_election_results(Some([].into()))
    }
);

safe_error_route!(end_txn, EndTxnRequest, _req, EndTxnResponse, {
    EndTxnResponse::default()
        .throttle_time_ms(0)
        .error_code(unknown_server_error())
});

safe_error_route!(
    list_transactions,
    ListTransactionsRequest,
    _req,
    ListTransactionsResponse,
    {
        ListTransactionsResponse::default()
            .throttle_time_ms(0)
            .error_code(unknown_server_error())
    }
);

safe_error_route!(
    offset_delete,
    OffsetDeleteRequest,
    req,
    OffsetDeleteResponse,
    {
        let topics = req
            .topics
            .unwrap_or_default()
            .into_iter()
            .map(|topic| {
                let partitions = topic
                    .partitions
                    .unwrap_or_default()
                    .into_iter()
                    .map(|partition| {
                        _OffsetDeleteResponsePartition::default()
                            .partition_index(partition.partition_index)
                            .error_code(unknown_server_error())
                    })
                    .collect();

                _OffsetDeleteResponseTopic::default()
                    .name(topic.name)
                    .partitions(Some(partitions))
            })
            .collect();

        OffsetDeleteResponse::default()
            .error_code(unknown_server_error())
            .throttle_time_ms(0)
            .topics(Some(topics))
    }
);

safe_error_route!(
    push_telemetry,
    PushTelemetryRequest,
    _req,
    PushTelemetryResponse,
    {
        PushTelemetryResponse::default()
            .throttle_time_ms(0)
            .error_code(unknown_server_error())
    }
);

safe_error_route!(
    update_features,
    UpdateFeaturesRequest,
    req,
    UpdateFeaturesResponse,
    {
        let results = req
            .feature_updates
            .unwrap_or_default()
            .into_iter()
            .map(|feature| {
                UpdatableFeatureResult::default()
                    .feature(feature.feature)
                    .error_code(unknown_server_error())
                    .error_message(Some(unknown_server_error_message()))
            })
            .collect();

        UpdateFeaturesResponse::default()
            .throttle_time_ms(0)
            .error_code(unknown_server_error())
            .error_message(Some(unknown_server_error_message()))
            .results(Some(results))
    }
);

pub fn services(
    builder: FrameRouteBuilder<(), Error>,
) -> Result<FrameRouteBuilder<(), Error>, Error> {
    [
        alter_configs,
        create_partitions,
        delete_acls,
        describe_log_dirs,
        elect_leaders,
        end_txn,
        offset_delete,
        push_telemetry,
    ]
    .iter()
    .try_fold(builder, |builder, service| service(builder))
}
