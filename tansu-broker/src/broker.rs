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

use crate::{
    CancelKind, Error, METER, Result,
    coordinator::group::{Coordinator, administrator::Controller},
    otel,
};
use api_versions::ApiVersionsRequest;
use bytes::Bytes;
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
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use produce::ProduceRequest;
use std::{
    io::ErrorKind,
    marker::PhantomData,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    str::FromStr,
    sync::LazyLock,
    time::{Duration, SystemTime},
};
use tansu_sans_io::{
    Body, ErrorCode, Frame, Header, IsolationLevel,
    add_offsets_to_txn_request::AddOffsetsToTxnRequest,
    api_versions_request, consumer_group_describe_request, consumer_group_describe_response,
    create_topics_request,
    create_topics_response::CreateTopicsResponse,
    delete_groups_request::DeleteGroupsRequest,
    delete_groups_response::DeleteGroupsResponse,
    describe_groups_response::{self, DescribeGroupsResponse},
};
use tansu_schema::{Registry, lake::House};
use tansu_storage::{BrokerRegistrationRequest, Storage, StorageContainer, TopicId};
use telemetry::GetTelemetrySubscriptionsRequest;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    signal::unix::{SignalKind, signal},
    sync::broadcast::{self, Receiver},
    task::JoinSet,
    time::{self, sleep},
};
use tracing::{Instrument, Level, Span, debug, debug_span, error, info, span};
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

    #[allow(dead_code)]
    otlp_endpoint_url: Option<Url>,
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
        incarnation_id: Uuid,
    ) -> Self {
        Self {
            node_id,
            cluster_id: cluster_id.to_owned(),
            incarnation_id,
            listener,
            advertised_listener,
            storage,
            groups,
            otlp_endpoint_url: None,
        }
    }

    pub fn builder() -> PhantomBuilder {
        Builder::default()
    }

    pub async fn main(mut self) -> Result<ErrorCode> {
        let mut set = JoinSet::new();

        let (sender, receiver) = broadcast::channel(16);
        debug!(?sender, ?receiver);

        let mut interrupt_signal = signal(SignalKind::interrupt()).unwrap();
        debug!(?interrupt_signal);

        let mut terminate_signal = signal(SignalKind::terminate()).unwrap();
        debug!(?terminate_signal);

        _ = set.spawn(async move {
            self.serve(receiver)
                .await
                .inspect_err(|err| error!(?err))
                .unwrap();
        });

        let cancellation = tokio::select! {
            v = set.join_next() => {
                debug!(?v);
                None
            }

            interrupt = interrupt_signal.recv() => {
                debug!(?interrupt);
                Some(CancelKind::Interrupt)
            }

            terminate = terminate_signal.recv() => {
                debug!(?terminate);
                Some(CancelKind::Terminate)
            }
        };

        if let Some(cancellation) = cancellation {
            _ = sender.send(cancellation).inspect_err(|err| debug!(?err))?;

            let cleanup = async {
                while !set.is_empty() {
                    debug!(len = set.len());

                    _ = set.join_next().await;
                }
            };

            let patience = sleep(Duration::from(cancellation));

            tokio::select! {
                v = cleanup => {
                    debug!(?v)
                }

                _ = patience => {
                    debug!(aborting = set.len());
                    set.abort_all();

                    while !set.is_empty() {
                        _ = set.join_next().await;
                    }
                }
            }
        }

        Ok(ErrorCode::None)
    }

    pub async fn serve(&mut self, interrupts: Receiver<CancelKind>) -> Result<()> {
        self.register().await?;
        self.listen(interrupts).await
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

    pub async fn listen(&self, mut interrupts: Receiver<CancelKind>) -> Result<()> {
        debug!(listener = %self.listener, advertised_listener = %self.advertised_listener);

        let listener = TcpListener::bind(self.listener.host().map_or_else(
            || {
                SocketAddr::from((
                    IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                    self.listener.port().unwrap_or(9092),
                ))
            },
            |host| {
                let port = self.listener.port().unwrap_or(9092);

                match host {
                    url::Host::Domain(domain) => SocketAddr::from_str(&format!("{domain}:{port}"))
                        .unwrap_or(SocketAddr::from((IpAddr::V6(Ipv6Addr::UNSPECIFIED), port))),
                    url::Host::Ipv4(ipv4_addr) => SocketAddr::from((IpAddr::V4(ipv4_addr), port)),
                    url::Host::Ipv6(ipv6_addr) => SocketAddr::from((IpAddr::V6(ipv6_addr), port)),
                }
            },
        ))
        .await
        .inspect_err(|err| error!(?err, %self.advertised_listener))?;

        let mut interval = time::interval(Duration::from_millis(600_000));

        let mut set = JoinSet::new();

        loop {
            tokio::select! {
                Ok((stream, addr)) = listener.accept() => {
                    debug!(?addr);

                    let mut broker = self.clone();

                    let handle = set.spawn(async move {
                        let span = span!(Level::DEBUG, "peer", addr = %addr);

                        async move {
                            match broker.stream_handler(&addr, stream).await {
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

                    debug!(?handle);

                    continue;
                }

                _ = interval.tick() => {
                    let storage = self.storage.clone();


                    let handle = set.spawn(async move {
                        let span = span!(Level::DEBUG, "maintenance");

                        async move {
                            _ = storage.maintain().await.inspect(|maintain|debug!(?maintain)).inspect_err(|err|debug!(?err)).ok();

                        }.instrument(span).await

                    });

                    debug!(?handle);
                }

                v = set.join_next(), if !set.is_empty() => {
                    debug!(?v);
                }

                Ok(message) = interrupts.recv() => {
                    debug!(?message);
                    break;
                }
            }
        }

        while !set.is_empty() {
            debug!(len = set.len());

            _ = set.join_next().await;
        }

        Ok(())
    }

    async fn stream_handler(&mut self, peer: &SocketAddr, mut stream: TcpStream) -> Result<()> {
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

            let request_start = SystemTime::now();

            let attributes = [KeyValue::new("cluster_id", self.cluster_id.clone())];

            REQUEST_SIZE.record(request.len() as u64, &attributes);

            let response = self
                .process_request(peer, &request)
                .await
                .inspect_err(|error| error!(?request, ?error))?;
            debug!(?response);

            RESPONSE_SIZE.record(response.len() as u64, &attributes);
            REQUEST_DURATION.record(
                request_start
                    .elapsed()
                    .map_or(0, |duration| duration.as_millis() as u64),
                &attributes,
            );

            stream
                .write_all(&response)
                .await
                .inspect_err(|error| error!(?request, ?response, ?error))?;
        }
    }

    async fn process_request(&mut self, _peer: &SocketAddr, input: &[u8]) -> Result<Bytes> {
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

                {
                    let mut attributes = attributes(api_key, api_version, correlation_id, &body);
                    attributes.push(KeyValue::new("cluster_id", self.cluster_id.clone()));
                    API_REQUESTS.add(1, &attributes);
                }

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
            Body::AddOffsetsToTxnRequest(AddOffsetsToTxnRequest {
                transactional_id,
                producer_id,
                producer_epoch,
                group_id,
                ..
            }) => {
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

            Body::ApiVersionsRequest(api_versions_request::ApiVersionsRequest {
                client_software_name,
                client_software_version,
                ..
            }) => {
                debug!(?client_software_name, ?client_software_version,);

                let api_versions = ApiVersionsRequest;
                Ok(api_versions.response(
                    client_software_name.as_deref(),
                    client_software_version.as_deref(),
                ))
            }

            Body::ConsumerGroupDescribeRequest(
                consumer_group_describe_request::ConsumerGroupDescribeRequest {
                    group_ids,
                    include_authorized_operations,
                    ..
                },
            ) => self
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
                .map(|groups| {
                    consumer_group_describe_response::ConsumerGroupDescribeResponse::default()
                        .throttle_time_ms(0)
                        .groups(groups)
                        .into()
                })
                .map_err(Into::into),

            Body::CreateTopicsRequest(create_topics_request::CreateTopicsRequest {
                validate_only,
                topics,
                ..
            }) => {
                debug!(?validate_only, ?topics);
                CreateTopic::with_storage(self.storage.clone())
                    .response(topics, validate_only.unwrap_or(false))
                    .await
                    .map(Some)
                    .map(|topics| {
                        CreateTopicsResponse::default()
                            .throttle_time_ms(Some(0))
                            .topics(topics)
                            .into()
                    })
            }

            Body::DeleteGroupsRequest(DeleteGroupsRequest { groups_names, .. }) => self
                .storage
                .delete_groups(groups_names.as_deref())
                .await
                .map(Some)
                .map(|results| {
                    DeleteGroupsResponse::default()
                        .throttle_time_ms(0)
                        .results(results)
                        .into()
                })
                .map_err(Into::into),

            Body::DeleteRecordsRequest(
                tansu_sans_io::delete_records_request::DeleteRecordsRequest { topics, .. },
            ) => {
                debug!(?topics);

                DeleteRecordsRequest::with_storage(self.storage.clone())
                    .request(topics.as_deref().unwrap_or(&[]))
                    .await
            }

            Body::DeleteTopicsRequest(
                tansu_sans_io::delete_topics_request::DeleteTopicsRequest {
                    topics,
                    topic_names,
                    timeout_ms,
                    ..
                },
            ) => {
                debug!(?topics, ?topic_names, ?timeout_ms);

                Ok(
                    tansu_sans_io::delete_topics_response::DeleteTopicsResponse::default()
                        .throttle_time_ms(Some(0))
                        .responses(
                            DeleteTopicsRequest::with_storage(self.storage.clone())
                                .response(topics, topic_names)
                                .await
                                .map(Some)?,
                        )
                        .into(),
                )
            }

            Body::DescribeClusterRequest(
                tansu_sans_io::describe_cluster_request::DescribeClusterRequest {
                    include_cluster_authorized_operations,
                    endpoint_type,
                    ..
                },
            ) => {
                debug!(?include_cluster_authorized_operations, ?endpoint_type);

                DescribeClusterRequest {
                    cluster_id: self.cluster_id.clone(),
                    storage: self.storage.clone(),
                }
                .response(include_cluster_authorized_operations, endpoint_type)
                .await
            }

            Body::DescribeConfigsRequest(
                tansu_sans_io::describe_configs_request::DescribeConfigsRequest {
                    resources,
                    include_synonyms,
                    include_documentation,
                    ..
                },
            ) => {
                debug!(?resources, ?include_synonyms, ?include_documentation,);

                DescribeConfigsRequest::with_storage(self.storage.clone())
                    .response(
                        resources.as_deref(),
                        include_synonyms,
                        include_documentation,
                    )
                    .await
                    .map(Some)
                    .map(|results| {
                        tansu_sans_io::describe_configs_response::DescribeConfigsResponse::default()
                            .throttle_time_ms(0)
                            .results(results)
                            .into()
                    })
            }

            Body::DescribeGroupsRequest(
                tansu_sans_io::describe_groups_request::DescribeGroupsRequest {
                    groups,
                    include_authorized_operations,
                    ..
                },
            ) => self
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
                .map(|groups| {
                    DescribeGroupsResponse::default()
                        .throttle_time_ms(Some(0))
                        .groups(groups)
                        .into()
                })
                .map_err(Into::into),

            Body::DescribeTopicPartitionsRequest(
                tansu_sans_io::describe_topic_partitions_request::DescribeTopicPartitionsRequest {
                    topics,
                    response_partition_limit,
                    cursor,
                    ..
                },
            ) => self
                .storage
                .describe_topic_partitions(
                    topics
                        .as_ref()
                        .map(|topics| topics.iter().map(TopicId::from).collect::<Vec<_>>())
                        .as_deref(),
                    response_partition_limit,
                    cursor.map(Into::into),
                )
                .await
                .map(|topics| tansu_sans_io::describe_topic_partitions_response::DescribeTopicPartitionsResponse::default()
                    .throttle_time_ms(0)
                    .topics(Some(topics))
                    .next_cursor(None).into()
                )
                .map_err(Into::into),

            Body::FetchRequest (tansu_sans_io::fetch_request::FetchRequest {
                max_wait_ms,
                min_bytes,
                max_bytes,
                isolation_level,
                topics,
                ..
            }) => {
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

            Body::FindCoordinatorRequest(tansu_sans_io::find_coordinator_request::FindCoordinatorRequest {
                key,
                key_type,
                coordinator_keys,
                ..
            }) => {
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

            Body::GetTelemetrySubscriptionsRequest(tansu_sans_io::get_telemetry_subscriptions_request::GetTelemetrySubscriptionsRequest { client_instance_id,.. }) => {
                debug!(?client_instance_id);
                let get_telemetry_subscriptions = GetTelemetrySubscriptionsRequest;
                Ok(get_telemetry_subscriptions.response(client_instance_id))
            }

            Body::HeartbeatRequest(tansu_sans_io::heartbeat_request::HeartbeatRequest {
                group_id,
                generation_id,
                member_id,
                group_instance_id,
                ..
            }) => {
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

            Body::IncrementalAlterConfigsRequest (tansu_sans_io::incremental_alter_configs_request::IncrementalAlterConfigsRequest{
                resources,
                validate_only,
                ..
            }) => {
                debug!(?resources, ?validate_only);
                let mut responses = vec![];

                for resource in resources.unwrap_or_default() {
                    responses.push(self.storage.incremental_alter_resource(resource).await?);
                }

                Ok(tansu_sans_io::incremental_alter_configs_response::IncrementalAlterConfigsResponse::default()
                    .throttle_time_ms(0)
                    .responses(Some(responses))
                    .into()
                )
            }

            Body::InitProducerIdRequest (tansu_sans_io::init_producer_id_request::InitProducerIdRequest {
                transactional_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
                ..
            }) => {
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
                    .map(|response| tansu_sans_io::init_producer_id_response::InitProducerIdResponse::default()
                        .throttle_time_ms(0)
                        .error_code(response.error.into())
                        .producer_id(response.id)
                        .producer_epoch(response.epoch)
                        .into()
                    )
            }

            Body::JoinGroupRequest(tansu_sans_io::join_group_request::JoinGroupRequest {
                group_id,
                session_timeout_ms,
                rebalance_timeout_ms,
                member_id,
                group_instance_id,
                protocol_type,
                protocols,
                reason,
                ..
            }) => {
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

            Body::LeaveGroupRequest(tansu_sans_io::leave_group_request::LeaveGroupRequest {
                group_id,
                member_id,
                members,
                ..
            }) => {
                debug!(?group_id, ?member_id, ?members);

                self.groups
                    .leave(&group_id, member_id.as_deref(), members.as_deref())
                    .await
            }

            Body::ListGroupsRequest(tansu_sans_io::list_groups_request::ListGroupsRequest {
                states_filter,
                types_filter,
                ..
            }) => {
                debug!(?states_filter, ?types_filter);
                self.storage
                    .list_groups(states_filter.as_deref())
                    .await
                    .map(Some)
                    .map(|groups| tansu_sans_io::list_groups_response::ListGroupsResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .groups(groups)
                        .into()
                    )
                    .map_err(Into::into)
            }

            Body::ListOffsetsRequest(tansu_sans_io::list_offsets_request::ListOffsetsRequest {
                replica_id,
                isolation_level,
                topics,
                ..
            }) => {
                debug!(?replica_id, ?isolation_level, ?topics);

                let isolation_level = isolation_level
                    .map_or(Ok(IsolationLevel::ReadUncommitted), |isolation_level| {
                        IsolationLevel::try_from(isolation_level)
                    })?;

                ListOffsetsRequest::with_storage(self.storage.clone())
                    .response(replica_id, isolation_level, topics.as_deref())
                    .await
            }

            Body::ListPartitionReassignmentsRequest(tansu_sans_io::list_partition_reassignments_request::ListPartitionReassignmentsRequest { topics, .. }) => {
                debug!(?topics);

                ListPartitionReassignmentsRequest::with_storage(self.storage.clone())
                    .response(topics.as_deref())
                    .await
            }

            Body::MetadataRequest(tansu_sans_io::metadata_request::MetadataRequest { topics, .. }) => {
                debug!(?topics);
                MetadataRequest::with_storage(self.storage.clone())
                    .response(topics)
                    .await
            }

            Body::OffsetCommitRequest(tansu_sans_io::offset_commit_request::OffsetCommitRequest {
                group_id,
                generation_id_or_member_epoch,
                member_id,
                group_instance_id,
                retention_time_ms,
                topics,
                ..
            }) => {
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

            Body::OffsetFetchRequest(tansu_sans_io::offset_fetch_request::OffsetFetchRequest {
                group_id,
                topics,
                groups,
                require_stable,
                ..
            }) => {
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

            Body::ProduceRequest(tansu_sans_io::produce_request::ProduceRequest {
                transactional_id,
                acks,
                timeout_ms,
                topic_data,
                ..
            }) => {
                debug!(?transactional_id, ?acks, ?timeout_ms, ?topic_data);
                ProduceRequest::with_storage(self.storage.clone())
                    .response(transactional_id, acks, timeout_ms, topic_data)
                    .await
                    .map(|response| tansu_sans_io::produce_response::ProduceResponse::default()
                        .responses(response.responses)
                        .throttle_time_ms(response.throttle_time_ms)
                        .node_endpoints(response.node_endpoints)
                        .into()
                    )
            }

            Body::SyncGroupRequest (tansu_sans_io::sync_group_request::SyncGroupRequest {
                group_id,
                generation_id,
                member_id,
                group_instance_id,
                protocol_type,
                protocol_name,
                assignments,
                ..
            }) => {
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

            Body::TxnOffsetCommitRequest(tansu_sans_io::txn_offset_commit_request::TxnOffsetCommitRequest {
                transactional_id,
                group_id,
                producer_id,
                producer_epoch,
                generation_id,
                member_id,
                group_instance_id,
                topics,
                ..
            }) => {
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

            Body::EndTxnRequest(tansu_sans_io::end_txn_request::EndTxnRequest {
                transactional_id,
                producer_id,
                producer_epoch,
                committed,
                ..
            }) => self
                .storage
                .txn_end(
                    transactional_id.as_str(),
                    producer_id,
                    producer_epoch,
                    committed,
                )
                .await
                .map(|error_code| tansu_sans_io::end_txn_response::EndTxnResponse::default()
                    .throttle_time_ms(0)
                    .error_code(i16::from(error_code))
                    .into()
                )
                .map_err(Into::into),

            request => {
                error!(?request);
                unimplemented!("{request:?}")
            }
        }
    }
}

fn attributes(api_key: i16, api_version: i16, _correlation_id: i32, body: &Body) -> Vec<KeyValue> {
    let mut attributes = vec![
        KeyValue::new("api_key", api_key as i64),
        KeyValue::new("api_version", api_version as i64),
    ];

    match body {
        Body::AddOffsetsToTxnRequest { .. } => {
            attributes.append(&mut vec![KeyValue::new("api_name", "add_offsets_to_txn")])
        }

        Body::AddPartitionsToTxnRequest { .. } => attributes.append(&mut vec![KeyValue::new(
            "api_name",
            "add_partitions_to_txn",
        )]),

        Body::ApiVersionsRequest { .. } => {
            attributes.append(&mut vec![KeyValue::new("api_name", "api_versions")])
        }

        Body::CreateTopicsRequest { .. } => {
            attributes.append(&mut vec![KeyValue::new("api_name", "create_topics")])
        }

        Body::DeleteTopicsRequest { .. } => {
            attributes.append(&mut vec![KeyValue::new("api_name", "delete_topics")])
        }

        Body::EndTxnRequest { .. } => {
            attributes.append(&mut vec![KeyValue::new("api_name", "end_txn")])
        }

        Body::FetchRequest { .. } => {
            attributes.append(&mut vec![KeyValue::new("api_name", "fetch")]);
        }

        Body::FindCoordinatorRequest { .. } => {
            attributes.append(&mut vec![KeyValue::new("api_name", "find_coordinator")])
        }

        Body::HeartbeatRequest { .. } => {
            attributes.append(&mut vec![KeyValue::new("api_name", "heartbeat")]);
        }

        Body::InitProducerIdRequest { .. } => {
            attributes.append(&mut vec![KeyValue::new("api_name", "init_producer_id")]);
        }

        Body::JoinGroupRequest { .. } => {
            attributes.append(&mut vec![KeyValue::new("api_name", "join_group")]);
        }

        Body::LeaveGroupRequest { .. } => {
            attributes.append(&mut vec![KeyValue::new("api_name", "leave_group")]);
        }

        Body::ListOffsetsRequest { .. } => {
            attributes.append(&mut vec![KeyValue::new("api_name", "list_offsets")]);
        }

        Body::MetadataRequest { .. } => {
            attributes.append(&mut vec![KeyValue::new("api_name", "metadata")]);
        }

        Body::OffsetCommitRequest { .. } => {
            attributes.append(&mut vec![KeyValue::new("api_name", "offset_commit")]);
        }

        Body::OffsetFetchRequest { .. } => {
            attributes.append(&mut vec![KeyValue::new("api_name", "offset_fetch")]);
        }

        Body::ProduceRequest { .. } => {
            attributes.append(&mut vec![KeyValue::new("api_name", "produce")]);
        }

        Body::SyncGroupRequest { .. } => {
            attributes.append(&mut vec![KeyValue::new("api_name", "sync_group")]);
        }

        Body::TxnOffsetCommitRequest { .. } => {
            attributes.append(&mut vec![KeyValue::new("api_name", "txn_offset_commit")]);
        }

        _ => (),
    };

    attributes
}

fn request_span(api_key: i16, api_version: i16, correlation_id: i32, body: &Body) -> Span {
    match body {
        Body::AddOffsetsToTxnRequest(AddOffsetsToTxnRequest {
            transactional_id,
            producer_id,
            producer_epoch,
            group_id,
            ..
        }) => {
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

        Body::EndTxnRequest(tansu_sans_io::end_txn_request::EndTxnRequest {
            transactional_id,
            producer_id,
            producer_epoch,
            committed,
            ..
        }) => {
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

        Body::InitProducerIdRequest(
            tansu_sans_io::init_producer_id_request::InitProducerIdRequest {
                transactional_id,
                producer_id,
                producer_epoch,
                ..
            },
        ) => {
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

        Body::JoinGroupRequest(tansu_sans_io::join_group_request::JoinGroupRequest {
            group_id,
            member_id,
            group_instance_id,
            ..
        }) => debug_span!(
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

        Body::SyncGroupRequest(tansu_sans_io::sync_group_request::SyncGroupRequest {
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            ..
        }) => debug_span!(
            "sync_group",
            correlation_id,
            group_id,
            generation_id,
            member_id,
            group_instance_id,
        ),

        Body::TxnOffsetCommitRequest(
            tansu_sans_io::txn_offset_commit_request::TxnOffsetCommitRequest {
                transactional_id,
                group_id,
                producer_id,
                producer_epoch,
                generation_id,
                member_id,
                group_instance_id,
                ..
            },
        ) => {
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

static API_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_api_requests")
        .with_description("The number of API requests made")
        .build()
});

static REQUEST_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tansu_request_size")
        .with_unit("By")
        .with_description("The API request size in bytes")
        .build()
});

static RESPONSE_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tansu_response_size")
        .with_unit("By")
        .with_description("The API response size in bytes")
        .build()
});

static REQUEST_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tansu_request_duration")
        .with_unit("ms")
        .with_description("The API request latencies in milliseconds")
        .build()
});

#[derive(Clone, Debug, Default)]
pub struct Builder<N, C, I, A, S, L> {
    node_id: N,
    cluster_id: C,
    incarnation_id: I,
    advertised_listener: A,
    storage: S,
    listener: L,
    otlp_endpoint_url: Option<Url>,
    schema_registry: Option<Url>,
    lake_house: Option<House>,
}

type PhantomBuilder = Builder<
    PhantomData<i32>,
    PhantomData<String>,
    PhantomData<Uuid>,
    PhantomData<Url>,
    PhantomData<Url>,
    PhantomData<Url>,
>;

impl<N, C, I, A, S, L> Builder<N, C, I, A, S, L> {
    pub fn node_id(self, node_id: i32) -> Builder<i32, C, I, A, S, L> {
        Builder {
            node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
        }
    }

    pub fn cluster_id(self, cluster_id: impl Into<String>) -> Builder<N, String, I, A, S, L> {
        Builder {
            node_id: self.node_id,
            cluster_id: cluster_id.into(),
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
        }
    }

    pub fn incarnation_id(self, incarnation_id: impl Into<Uuid>) -> Builder<N, C, Uuid, A, S, L> {
        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: incarnation_id.into(),
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
        }
    }

    pub fn advertised_listener(
        self,
        advertised_listener: impl Into<Url>,
    ) -> Builder<N, C, I, Url, S, L> {
        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: advertised_listener.into(),
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
        }
    }

    pub fn storage(self, storage: Url) -> Builder<N, C, I, A, Url, L> {
        debug!(%storage);

        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
        }
    }

    pub fn listener(self, listener: Url) -> Builder<N, C, I, A, S, Url> {
        debug!(%listener);

        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
        }
    }

    pub fn schema_registry(self, schema_registry: Option<Url>) -> Builder<N, C, I, A, S, L> {
        _ = schema_registry
            .as_ref()
            .inspect(|schema_registry| debug!(%schema_registry));

        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry,
            lake_house: self.lake_house,
        }
    }

    pub fn lake_house(self, lake_house: Option<House>) -> Builder<N, C, I, A, S, L> {
        _ = lake_house
            .as_ref()
            .inspect(|lake_house| debug!(?lake_house));

        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house,
        }
    }

    pub fn otlp_endpoint_url(self, otlp_endpoint_url: Option<Url>) -> Builder<N, C, I, A, S, L> {
        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
        }
    }
}

impl Builder<i32, String, Uuid, Url, Url, Url> {
    pub async fn build(self) -> Result<Broker<Controller<StorageContainer>, StorageContainer>> {
        if let Some(otlp_endpoint_url) = self
            .otlp_endpoint_url
            .clone()
            .inspect(|otlp_endpoint_url| debug!(%otlp_endpoint_url))
        {
            otel::metric_exporter(otlp_endpoint_url)?;
        }

        let storage = StorageContainer::builder()
            .cluster_id(self.cluster_id.clone())
            .node_id(self.node_id)
            .advertised_listener(self.advertised_listener.clone())
            .schema_registry(
                self.schema_registry
                    .as_ref()
                    .map_or(Ok(None), |schema| Registry::try_from(schema).map(Some))?,
            )
            .lake_house(self.lake_house.clone())
            .storage(self.storage.clone())
            .build()
            .await?;

        let groups = Controller::with_storage(storage.clone())?;

        Ok(Broker {
            node_id: self.node_id,
            cluster_id: self.cluster_id.clone(),
            incarnation_id: self.incarnation_id,
            listener: self.listener,
            advertised_listener: self.advertised_listener,
            storage,
            groups,
            otlp_endpoint_url: self.otlp_endpoint_url,
        })
    }
}
