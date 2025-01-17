// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

pub mod api_versions;
pub mod create_topic;
pub mod delete_records;
pub mod delete_topics;
pub mod describe_cluster;
pub mod describe_configs;
pub mod fetch;
pub mod find_coordinator;
pub mod group;
pub mod init_producer_id;
pub mod list_offsets;
pub mod list_partition_reassignments;
pub mod metadata;
pub mod produce;
pub mod telemetry;
pub mod txn;

use crate::{coordinator::group::Coordinator, Error, Result};
use api_versions::ApiVersionsRequest;
use create_topic::CreateTopic;
use delete_records::DeleteRecordsRequest;
use delete_topics::DeleteTopicsRequest;
use describe_cluster::DescribeClusterRequest;
use describe_configs::DescribeConfigsRequest;
use fetch::FetchRequest;
use find_coordinator::FindCoordinatorRequest;
use init_producer_id::InitProducerIdRequest;
use list_offsets::ListOffsetsRequest;
use list_partition_reassignments::ListPartitionReassignmentsRequest;
use metadata::MetadataRequest;
use produce::ProduceRequest;
use std::io::ErrorKind;
use tansu_kafka_sans_io::{
    consumer_group_describe_response, describe_groups_response, Body, ErrorCode, Frame, Header,
    IsolationLevel,
};
use tansu_storage::{BrokerRegistrationRequest, Storage};
use telemetry::GetTelemetrySubscriptionsRequest;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::{debug, debug_span, error, info, span, Instrument, Level, Span};
use txn::{add_offsets::AddOffsets, add_partitions::AddPartitions};
use url::Url;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct Broker<G, S> {
    node_id: i32,
    cluster_id: String,
    incarnation_id: Uuid,
    listener: Url,
    advertised_listener: Url,
    storage: S,
    groups: G,
}

impl<G, S> Broker<G, S>
where
    G: Coordinator,
    S: Storage + Clone + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: i32,
        cluster_id: &str,
        listener: Url,
        advertised_listener: Url,
        storage: S,
        groups: G,
    ) -> Self {
        let incarnation_id = Uuid::new_v4();

        Self {
            node_id,
            cluster_id: cluster_id.to_owned(),
            incarnation_id,
            listener,
            advertised_listener,
            storage,
            groups,
        }
    }

    pub async fn serve(&mut self) -> Result<()> {
        self.register().await?;
        self.listen().await
    }

    pub async fn register(&mut self) -> Result<()> {
        self.storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: self.node_id,
                cluster_id: self.cluster_id.clone(),
                incarnation_id: self.incarnation_id,
                rack: None,
            })
            .await
            .map_err(Into::into)
    }

    pub async fn listen(&self) -> Result<()> {
        debug!(listener = %self.listener, advertised_listener = %self.advertised_listener);

        let listener = TcpListener::bind(format!(
            "{}:{}",
            self.listener.host_str().unwrap_or("0.0.0.0"),
            self.listener.port().unwrap_or(9092)
        ))
        .await?;

        loop {
            let (stream, addr) = listener.accept().await?;
            debug!(?addr);

            let mut broker = self.clone();

            _ = tokio::spawn(async move {
                let span = span!(Level::DEBUG, "peer", addr = %addr);

                async move {
                    match broker.stream_handler(stream).await {
                        Err(Error::Io(ref io))
                            if io.kind() == ErrorKind::UnexpectedEof
                                || io.kind() == ErrorKind::BrokenPipe
                                || io.kind() == ErrorKind::ConnectionReset => {}

                        Err(error) => {
                            error!(?error);
                        }

                        Ok(_) => {}
                    }
                }
                .instrument(span)
                .await
            });
        }
    }

    async fn stream_handler(&mut self, mut stream: TcpStream) -> Result<()> {
        debug!(?stream);

        let mut size = [0u8; 4];

        loop {
            _ = stream
                .read_exact(&mut size)
                .await
                .inspect_err(|error| match error.kind() {
                    ErrorKind::UnexpectedEof | ErrorKind::ConnectionReset => (),

                    _ => error!(?error),
                })?;

            if i32::from_be_bytes(size) == 0 {
                info!("empty read!");
                continue;
            }

            let mut request: Vec<u8> = vec![0u8; i32::from_be_bytes(size) as usize + size.len()];
            request[0..4].copy_from_slice(&size[..]);

            _ = stream
                .read_exact(&mut request[4..])
                .await
                .inspect_err(|error| error!(?size, ?request, ?error))?;
            debug!(?request);

            let response = self
                .process_request(&request)
                .await
                .inspect_err(|error| error!(?request, ?error))?;
            debug!(?response);

            stream
                .write_all(&response)
                .await
                .inspect_err(|error| error!(?request, ?response, ?error))?;
        }
    }

    async fn process_request(&mut self, input: &[u8]) -> Result<Vec<u8>> {
        match Frame::request_from_bytes(input)? {
            Frame {
                header:
                    Header::Request {
                        api_key,
                        api_version,
                        correlation_id,
                        client_id,
                        ..
                    },
                body,
                ..
            } => {
                let span = request_span(api_key, api_version, correlation_id, &body);

                async move {
                    Frame::response(
                        Header::Response { correlation_id },
                        self.response_for(client_id.as_deref(), body, correlation_id)
                            .await
                            .inspect(|body| debug!(?body))
                            .inspect_err(|err| error!(?err))?,
                        api_key,
                        api_version,
                    )
                    .inspect(|response| debug!(?response))
                    .inspect_err(|err| error!(?err))
                    .map_err(Into::into)
                }
                .instrument(span)
                .await
            }

            _ => unimplemented!(),
        }
    }

    pub async fn response_for(
        &mut self,
        client_id: Option<&str>,
        body: Body,
        correlation_id: i32,
    ) -> Result<Body> {
        debug!(?body, ?correlation_id);

        match body {
            Body::AddOffsetsToTxnRequest {
                transactional_id,
                producer_id,
                producer_epoch,
                group_id,
            } => {
                debug!(?transactional_id, ?producer_id, ?producer_epoch, ?group_id);

                AddOffsets::with_storage(self.storage.clone())
                    .response(
                        transactional_id.as_str(),
                        producer_id,
                        producer_epoch,
                        group_id.as_str(),
                    )
                    .await
            }

            add_partitions @ Body::AddPartitionsToTxnRequest { .. } => {
                AddPartitions::with_storage(self.storage.clone())
                    .response(add_partitions.try_into()?)
                    .await
            }

            Body::ApiVersionsRequest {
                client_software_name,
                client_software_version,
            } => {
                debug!(?client_software_name, ?client_software_version,);

                let api_versions = ApiVersionsRequest;
                Ok(api_versions.response(
                    client_software_name.as_deref(),
                    client_software_version.as_deref(),
                ))
            }

            Body::ConsumerGroupDescribeRequest {
                group_ids,
                include_authorized_operations,
            } => self
                .storage
                .describe_groups(group_ids.as_deref(), include_authorized_operations)
                .await
                .map(|described| {
                    described
                        .iter()
                        .map(consumer_group_describe_response::DescribedGroup::from)
                        .collect::<Vec<_>>()
                })
                .map(Some)
                .map(|groups| Body::ConsumerGroupDescribeResponse {
                    throttle_time_ms: 0,
                    groups,
                })
                .map_err(Into::into),

            Body::CreateTopicsRequest {
                validate_only,
                topics,
                ..
            } => {
                debug!(?validate_only, ?topics);
                CreateTopic::with_storage(self.storage.clone())
                    .response(topics, validate_only.unwrap_or(false))
                    .await
                    .map(Some)
                    .map(|topics| Body::CreateTopicsResponse {
                        throttle_time_ms: Some(0),
                        topics,
                    })
            }

            Body::DeleteGroupsRequest { groups_names } => self
                .storage
                .delete_groups(groups_names.as_deref())
                .await
                .map(Some)
                .map(|results| Body::DeleteGroupsResponse {
                    throttle_time_ms: 0,
                    results,
                })
                .map_err(Into::into),

            Body::DeleteRecordsRequest { topics, .. } => {
                debug!(?topics);

                DeleteRecordsRequest::with_storage(self.storage.clone())
                    .request(topics.as_deref().unwrap_or(&[]))
                    .await
            }

            Body::DeleteTopicsRequest {
                topics,
                topic_names,
                timeout_ms,
            } => {
                debug!(?topics, ?topic_names, ?timeout_ms);

                Ok(Body::DeleteTopicsResponse {
                    throttle_time_ms: Some(0),
                    responses: DeleteTopicsRequest::with_storage(self.storage.clone())
                        .response(topics, topic_names)
                        .await
                        .map(Some)?,
                })
            }

            Body::DescribeClusterRequest {
                include_cluster_authorized_operations,
                endpoint_type,
            } => {
                debug!(?include_cluster_authorized_operations, ?endpoint_type);

                DescribeClusterRequest {
                    cluster_id: self.cluster_id.clone(),
                    storage: self.storage.clone(),
                }
                .response(include_cluster_authorized_operations, endpoint_type)
                .await
            }

            Body::DescribeConfigsRequest {
                resources,
                include_synonyms,
                include_documentation,
            } => {
                debug!(?resources, ?include_synonyms, ?include_documentation,);

                DescribeConfigsRequest::with_storage(self.storage.clone())
                    .response(
                        resources.as_deref(),
                        include_synonyms,
                        include_documentation,
                    )
                    .await
                    .map(Some)
                    .map(|results| Body::DescribeConfigsResponse {
                        throttle_time_ms: 0,
                        results,
                    })
            }

            Body::DescribeGroupsRequest {
                groups,
                include_authorized_operations,
            } => self
                .storage
                .describe_groups(
                    groups.as_deref(),
                    include_authorized_operations.unwrap_or(false),
                )
                .await
                .map(|described| {
                    described
                        .iter()
                        .map(describe_groups_response::DescribedGroup::from)
                        .collect::<Vec<_>>()
                })
                .map(Some)
                .map(|groups| Body::DescribeGroupsResponse {
                    throttle_time_ms: Some(0),
                    groups,
                })
                .map_err(Into::into),

            Body::FetchRequest {
                max_wait_ms,
                min_bytes,
                max_bytes,
                isolation_level,
                topics,
                ..
            } => {
                debug!(
                    ?max_wait_ms,
                    ?min_bytes,
                    ?max_bytes,
                    ?isolation_level,
                    ?topics,
                );

                FetchRequest::with_storage(self.storage.clone())
                    .response(
                        max_wait_ms,
                        min_bytes,
                        max_bytes,
                        isolation_level,
                        topics.as_deref(),
                    )
                    .await
                    .inspect(|r| debug!(?r))
                    .inspect_err(|error| error!(?error))
            }

            Body::FindCoordinatorRequest {
                key,
                key_type,
                coordinator_keys,
            } => {
                debug!(?key, ?key_type, ?coordinator_keys);

                let find_coordinator = FindCoordinatorRequest;

                Ok(find_coordinator.response(
                    key.as_deref(),
                    key_type,
                    coordinator_keys.as_deref(),
                    self.node_id,
                    &self.advertised_listener,
                ))
            }

            Body::GetTelemetrySubscriptionsRequest { client_instance_id } => {
                debug!(?client_instance_id);
                let get_telemetry_subscriptions = GetTelemetrySubscriptionsRequest;
                Ok(get_telemetry_subscriptions.response(client_instance_id))
            }

            Body::HeartbeatRequest {
                group_id,
                generation_id,
                member_id,
                group_instance_id,
            } => {
                debug!(?group_id, ?generation_id, ?member_id, ?group_instance_id,);

                self.groups
                    .heartbeat(
                        &group_id,
                        generation_id,
                        &member_id,
                        group_instance_id.as_deref(),
                    )
                    .await
            }

            Body::InitProducerIdRequest {
                transactional_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            } => {
                debug!(
                    ?transactional_id,
                    ?transaction_timeout_ms,
                    ?producer_id,
                    ?producer_epoch,
                );

                InitProducerIdRequest::with_storage(self.storage.clone())
                    .response(
                        transactional_id.as_deref(),
                        transaction_timeout_ms,
                        producer_id,
                        producer_epoch,
                    )
                    .await
                    .map(|response| Body::InitProducerIdResponse {
                        throttle_time_ms: 0,
                        error_code: response.error.into(),
                        producer_id: response.id,
                        producer_epoch: response.epoch,
                    })
            }

            Body::JoinGroupRequest {
                group_id,
                session_timeout_ms,
                rebalance_timeout_ms,
                member_id,
                group_instance_id,
                protocol_type,
                protocols,
                reason,
            } => {
                debug!(
                    ?group_id,
                    ?session_timeout_ms,
                    ?rebalance_timeout_ms,
                    ?member_id,
                    ?group_instance_id,
                    ?protocol_type,
                    ?protocols,
                    ?reason,
                );

                self.groups
                    .join(
                        client_id,
                        &group_id,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        &member_id,
                        group_instance_id.as_deref(),
                        &protocol_type,
                        protocols.as_deref(),
                        reason.as_deref(),
                    )
                    .await
            }

            Body::LeaveGroupRequest {
                group_id,
                member_id,
                members,
            } => {
                debug!(?group_id, ?member_id, ?members);

                self.groups
                    .leave(&group_id, member_id.as_deref(), members.as_deref())
                    .await
            }

            Body::ListGroupsRequest {
                states_filter,
                types_filter,
            } => {
                debug!(?states_filter, ?types_filter);
                self.storage
                    .list_groups(states_filter.as_deref())
                    .await
                    .map(Some)
                    .map(|groups| Body::ListGroupsResponse {
                        throttle_time_ms: Some(0),
                        error_code: ErrorCode::None.into(),
                        groups,
                    })
                    .map_err(Into::into)
            }

            Body::ListOffsetsRequest {
                replica_id,
                isolation_level,
                topics,
            } => {
                debug!(?replica_id, ?isolation_level, ?topics);

                let isolation_level = isolation_level
                    .map_or(Ok(IsolationLevel::ReadUncommitted), |isolation_level| {
                        IsolationLevel::try_from(isolation_level)
                    })?;

                ListOffsetsRequest::with_storage(self.storage.clone())
                    .response(replica_id, isolation_level, topics.as_deref())
                    .await
            }

            Body::ListPartitionReassignmentsRequest { topics, .. } => {
                debug!(?topics);

                ListPartitionReassignmentsRequest::with_storage(self.storage.clone())
                    .response(topics.as_deref())
                    .await
            }

            Body::MetadataRequest { topics, .. } => {
                debug!(?topics);
                MetadataRequest::with_storage(self.storage.clone())
                    .response(topics)
                    .await
            }

            Body::OffsetCommitRequest {
                group_id,
                generation_id_or_member_epoch,
                member_id,
                group_instance_id,
                retention_time_ms,
                topics,
            } => {
                debug!(
                    ?group_id,
                    ?generation_id_or_member_epoch,
                    ?member_id,
                    ?group_instance_id,
                    ?retention_time_ms,
                    ?topics
                );

                let detail = crate::coordinator::group::OffsetCommit {
                    group_id: group_id.as_str(),
                    generation_id_or_member_epoch,
                    member_id: member_id.as_deref(),
                    group_instance_id: group_instance_id.as_deref(),
                    retention_time_ms,
                    topics: topics.as_deref(),
                };

                self.groups.offset_commit(detail).await
            }

            Body::OffsetFetchRequest {
                group_id,
                topics,
                groups,
                require_stable,
            } => {
                debug!(?group_id, ?topics, ?groups, ?require_stable);
                self.groups
                    .offset_fetch(
                        group_id.as_deref(),
                        topics.as_deref(),
                        groups.as_deref(),
                        require_stable,
                    )
                    .await
            }

            Body::ProduceRequest {
                transactional_id,
                acks,
                timeout_ms,
                topic_data,
            } => {
                debug!(?transactional_id, ?acks, ?timeout_ms, ?topic_data);
                ProduceRequest::with_storage(self.storage.clone())
                    .response(transactional_id, acks, timeout_ms, topic_data)
                    .await
                    .map(|response| Body::ProduceResponse {
                        responses: response.responses,
                        throttle_time_ms: response.throttle_time_ms,
                        node_endpoints: response.node_endpoints,
                    })
            }

            Body::SyncGroupRequest {
                group_id,
                generation_id,
                member_id,
                group_instance_id,
                protocol_type,
                protocol_name,
                assignments,
            } => {
                self.groups
                    .sync(
                        &group_id,
                        generation_id,
                        &member_id,
                        group_instance_id.as_deref(),
                        protocol_type.as_deref(),
                        protocol_name.as_deref(),
                        assignments.as_deref(),
                    )
                    .await
            }

            Body::TxnOffsetCommitRequest {
                transactional_id,
                group_id,
                producer_id,
                producer_epoch,
                generation_id,
                member_id,
                group_instance_id,
                topics,
            } => {
                debug!(
                    ?transactional_id,
                    ?group_id,
                    ?producer_id,
                    ?producer_epoch,
                    ?generation_id,
                    ?member_id,
                    ?group_instance_id,
                    ?topics,
                );

                txn::offset_commit::OffsetCommit::with_storage(self.storage.clone())
                    .response(
                        transactional_id.as_str(),
                        group_id.as_str(),
                        producer_id,
                        producer_epoch,
                        generation_id,
                        member_id,
                        group_instance_id,
                        topics,
                    )
                    .await
            }

            Body::EndTxnRequest {
                transactional_id,
                producer_id,
                producer_epoch,
                committed,
            } => self
                .storage
                .txn_end(
                    transactional_id.as_str(),
                    producer_id,
                    producer_epoch,
                    committed,
                )
                .await
                .map(|error_code| Body::EndTxnResponse {
                    throttle_time_ms: 0,
                    error_code: i16::from(error_code),
                })
                .map_err(Into::into),

            request => {
                error!(?request);
                unimplemented!("{request:?}")
            }
        }
    }
}

fn request_span(api_key: i16, api_version: i16, correlation_id: i32, body: &Body) -> Span {
    match body {
        Body::AddOffsetsToTxnRequest {
            transactional_id,
            producer_id,
            producer_epoch,
            group_id,
            ..
        } => {
            debug_span!(
                "add_offsets_to_txn",
                api_key,
                api_version,
                correlation_id,
                transactional_id,
                producer_id,
                producer_epoch,
                group_id,
            )
        }

        Body::AddPartitionsToTxnRequest { .. } => {
            debug_span!(
                "add_partitions_to_txn",
                api_key,
                api_version,
                correlation_id,
            )
        }

        Body::ApiVersionsRequest { .. } => {
            debug_span!("api_versions", api_key, api_version, correlation_id,)
        }

        Body::CreateTopicsRequest { .. } => {
            debug_span!("create_topics", api_key, api_version, correlation_id)
        }

        Body::DeleteTopicsRequest { .. } => {
            debug_span!("delete_topics", api_key, api_version, correlation_id)
        }

        Body::EndTxnRequest {
            transactional_id,
            producer_id,
            producer_epoch,
            committed,
        } => {
            debug_span!(
                "end_txn",
                api_key,
                api_version,
                correlation_id,
                transactional_id,
                producer_id,
                producer_epoch,
                committed
            )
        }

        Body::FetchRequest { .. } => {
            debug_span!("fetch", api_key, api_version, correlation_id)
        }

        Body::FindCoordinatorRequest { .. } => {
            debug_span!("find_coordinator", api_key, api_version, correlation_id)
        }

        Body::InitProducerIdRequest {
            transactional_id,
            producer_id,
            producer_epoch,
            ..
        } => {
            debug_span!(
                "init_producer_id",
                api_key,
                api_version,
                correlation_id,
                transactional_id,
                producer_id,
                producer_epoch,
            )
        }

        Body::JoinGroupRequest {
            group_id,
            member_id,
            group_instance_id,
            ..
        } => debug_span!(
            "join_group",
            correlation_id,
            group_id,
            member_id,
            group_instance_id,
        ),

        Body::LeaveGroupRequest { .. } => {
            debug_span!("leave_group", api_key, api_version, correlation_id)
        }

        Body::ListOffsetsRequest { .. } => {
            debug_span!("list_offsets", api_key, api_version, correlation_id)
        }

        Body::MetadataRequest { .. } => {
            debug_span!("metadata", api_key, api_version, correlation_id,)
        }

        Body::OffsetFetchRequest { .. } => {
            debug_span!("offset_fetch", api_key, api_version, correlation_id,)
        }

        Body::ProduceRequest { .. } => {
            debug_span!("produce", api_key, api_version, correlation_id)
        }

        Body::SyncGroupRequest {
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            ..
        } => debug_span!(
            "sync_group",
            correlation_id,
            group_id,
            generation_id,
            member_id,
            group_instance_id,
        ),

        Body::TxnOffsetCommitRequest {
            transactional_id,
            group_id,
            producer_id,
            producer_epoch,
            generation_id,
            member_id,
            group_instance_id,
            ..
        } => {
            debug_span!(
                "txn_offset_commit",
                api_key,
                api_version,
                correlation_id,
                transactional_id,
                group_id,
                producer_id,
                producer_epoch,
                generation_id,
                member_id,
                group_instance_id,
            )
        }

        _ => debug_span!("request", api_key, api_version, correlation_id,),
    }
}
