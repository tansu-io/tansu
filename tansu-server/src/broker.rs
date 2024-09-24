// Copyright ⓒ 2024 Peter Morgan <peter.james.morgan@gmail.com>
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

pub mod alter_user_scram_credentials;
pub mod api_versions;
pub mod create_topic;
pub mod delete_records;
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
pub mod sasl;
pub mod sasl_authenticate;
pub mod sasl_handshake;
pub mod telemetry;

use crate::{
    command::{Command, Request},
    coordinator::group::{Coordinator, OffsetCommit},
    raft::Applicator,
    Error, Result,
};
use alter_user_scram_credentials::AlterUserScramCredentials;
use api_versions::ApiVersionsRequest;
use bytes::Bytes;
use create_topic::CreateTopic;
use delete_records::DeleteRecordsRequest;
use describe_cluster::DescribeClusterRequest;
use describe_configs::DescribeConfigsRequest;
use fetch::FetchRequest;
use find_coordinator::FindCoordinatorRequest;
use init_producer_id::InitProducerIdRequest;
use list_offsets::ListOffsetsRequest;
use list_partition_reassignments::ListPartitionReassignmentsRequest;
use metadata::MetadataRequest;
use produce::ProduceRequest;
use rsasl::{config::SASLConfig, prelude::SASLServer};
use sasl::{Authentication, Callback, Justification};
use sasl_authenticate::SaslAuthenticate;
use sasl_handshake::SaslHandshake;
use std::{io::ErrorKind, sync::Arc};
use tansu_kafka_sans_io::{broker_registration_request::Listener, Body, Frame, Header};
use tansu_raft::{Log, Raft};
use tansu_storage::{BrokerRegistationRequest, Storage};
use telemetry::GetTelemetrySubscriptionsRequest;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::{debug, error, info};
use url::Url;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct Broker<G, S> {
    node_id: i32,
    cluster_id: String,
    incarnation_id: Uuid,
    #[allow(dead_code)]
    context: Raft,
    applicator: Applicator,
    listener: Url,
    advertised_listener: Url,
    #[allow(dead_code)]
    rack: Option<String>,
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
        context: Raft,
        applicator: Applicator,
        listener: Url,
        advertised_listener: Url,
        rack: Option<String>,
        storage: S,
        groups: G,
    ) -> Self {
        let incarnation_id = Uuid::new_v4();

        Self {
            node_id,
            cluster_id: cluster_id.to_owned(),
            incarnation_id,
            context,
            applicator,
            listener,
            advertised_listener,
            rack,
            storage,
            groups,
        }
    }

    pub async fn serve(&mut self) -> Result<()> {
        self.register().await?;

        let configuration = SASLConfig::builder()
            .with_defaults()
            .with_callback(Callback::with_storage(self.storage.clone()))?;

        self.listen(configuration).await
    }

    pub async fn register(&mut self) -> Result<()> {
        self.storage
            .register_broker(BrokerRegistationRequest {
                broker_id: self.node_id,
                cluster_id: self.cluster_id.clone(),
                incarnation_id: self.incarnation_id,
                listeners: [Listener {
                    name: "broker".into(),
                    host: self
                        .advertised_listener
                        .host_str()
                        .unwrap_or("localhost")
                        .to_owned(),
                    port: self.advertised_listener.port().unwrap_or(9092),
                    security_protocol: 0,
                }]
                .into(),
                features: [].into(),
                rack: None,
            })
            .await
            .map_err(Into::into)
    }

    pub async fn listen(&self, configuration: Arc<SASLConfig>) -> Result<()> {
        debug!("listener: {}", self.listener.as_str());

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
            let authentication =
                Authentication::Server(SASLServer::<Justification>::new(configuration.clone()));

            _ = tokio::spawn(async move {
                match broker.stream_handler(stream, authentication).await {
                    Err(ref error @ Error::Io(ref io)) if io.kind() == ErrorKind::UnexpectedEof => {
                        info!(?error);
                    }

                    Err(error) => {
                        error!(?error);
                    }

                    Ok(_) => {}
                }
            });
        }
    }

    async fn stream_handler(
        &mut self,
        mut stream: TcpStream,
        mut authentication: Authentication,
    ) -> Result<()> {
        debug!(?stream);

        let mut size = [0u8; 4];

        loop {
            _ = stream
                .read_exact(&mut size)
                .await
                .inspect_err(|error| match error.kind() {
                    ErrorKind::UnexpectedEof => {
                        info!(?error);
                    }

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

            let (authentication_response, response) = self
                .process_request(&request, authentication)
                .await
                .inspect_err(|error| error!(?request, ?error))?;
            debug!(?response);

            authentication = authentication_response;

            stream
                .write_all(&response)
                .await
                .inspect_err(|error| error!(?request, ?response, ?error))?;
        }
    }

    async fn process_request(
        &mut self,
        input: &[u8],
        authentication: Authentication,
    ) -> Result<(Authentication, Vec<u8>)> {
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
                debug!(?api_key, ?api_version, ?correlation_id);
                let (authentication, body) = self
                    .response_for(client_id.as_deref(), body, correlation_id, authentication)
                    .await
                    .inspect_err(|err| error!(?err))?;
                debug!(?body, ?correlation_id);
                Frame::response(
                    Header::Response { correlation_id },
                    body,
                    api_key,
                    api_version,
                )
                .inspect_err(|err| error!(?err))
                .map_err(Into::into)
                .map(|response| (authentication, response))
            }

            _ => unimplemented!(),
        }
    }

    #[allow(dead_code)]
    async fn when_applied(&mut self, body: Body) -> Result<Body> {
        debug!(?self, ?body);

        let id = Uuid::new_v4();
        let request = Request::new(id, body);
        let command = &request as &dyn Command;

        let json = serde_json::to_string(command)?;
        debug!(?json);

        let index = self
            .context
            .log(Bytes::copy_from_slice(json.as_bytes()))
            .await?;

        debug!(?index);

        Ok(self.applicator.when_applied(id).await)
    }

    pub async fn response_for(
        &mut self,
        client_id: Option<&str>,
        body: Body,
        correlation_id: i32,
        authentication: Authentication,
    ) -> Result<(Authentication, Body)> {
        debug!(?body, ?correlation_id);

        match body {
            Body::AlterUserScramCredentialsRequest {
                deletions,
                upsertions,
            } => AlterUserScramCredentials::with_storage(self.storage.clone())
                .response(deletions, upsertions)
                .await
                .map(|response| (authentication, response)),

            Body::ApiVersionsRequest {
                client_software_name,
                client_software_version,
            } => {
                debug!(?client_software_name, ?client_software_version,);

                let api_versions = ApiVersionsRequest;
                Ok((
                    authentication,
                    api_versions.response(
                        client_software_name.as_deref(),
                        client_software_version.as_deref(),
                    ),
                ))
            }

            Body::CreateTopicsRequest {
                validate_only,
                topics,
                ..
            } => {
                debug!(?validate_only, ?topics);
                CreateTopic::with_storage(self.storage.clone())
                    .request(topics, validate_only.unwrap_or(false))
                    .await
                    .map(|body| (authentication, body))
            }

            Body::DeleteRecordsRequest { topics, .. } => {
                debug!(?topics);

                DeleteRecordsRequest::with_storage(self.storage.clone())
                    .request(topics.as_deref().unwrap_or(&[]))
                    .await
                    .map(|body| (authentication, body))
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
                .map(|body| (authentication, body))
            }

            Body::DescribeConfigsRequest {
                resources,
                include_synonyms,
                include_documentation,
            } => {
                debug!(?resources, ?include_synonyms, ?include_documentation,);

                let describe_configs = DescribeConfigsRequest;
                Ok((
                    authentication,
                    describe_configs.response(
                        resources.as_deref(),
                        include_synonyms,
                        include_documentation,
                    ),
                ))
            }

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
                    .map(|body| (authentication, body))
            }

            Body::FindCoordinatorRequest {
                key,
                key_type,
                coordinator_keys,
            } => {
                debug!(?key, ?key_type, ?coordinator_keys);

                let find_coordinator = FindCoordinatorRequest;

                Ok((
                    authentication,
                    find_coordinator.response(
                        key.as_deref(),
                        key_type,
                        coordinator_keys.as_deref(),
                        self.node_id,
                        &self.listener,
                    ),
                ))
            }

            Body::GetTelemetrySubscriptionsRequest { client_instance_id } => {
                debug!(?client_instance_id);
                let get_telemetry_subscriptions = GetTelemetrySubscriptionsRequest;
                Ok((
                    authentication,
                    get_telemetry_subscriptions.response(client_instance_id),
                ))
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
                    .map(|body| (authentication, body))
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

                let init_producer_id = InitProducerIdRequest;
                Ok((
                    authentication,
                    init_producer_id.response(
                        transactional_id.as_deref(),
                        transaction_timeout_ms,
                        producer_id,
                        producer_epoch,
                    ),
                ))
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
                    .map(|body| (authentication, body))
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
                    .map(|body| (authentication, body))
            }

            Body::ListOffsetsRequest {
                replica_id,
                isolation_level,
                topics,
            } => {
                debug!(?replica_id, ?isolation_level, ?topics);

                ListOffsetsRequest::with_storage(self.storage.clone())
                    .response(replica_id, isolation_level, topics.as_deref())
                    .await
                    .map(|body| (authentication, body))
            }

            Body::ListPartitionReassignmentsRequest { topics, .. } => {
                debug!(?topics);

                ListPartitionReassignmentsRequest::with_storage(self.storage.clone())
                    .response(topics.as_deref())
                    .await
                    .map(|body| (authentication, body))
            }

            Body::MetadataRequest { topics, .. } => {
                debug!(?topics);
                MetadataRequest::with_storage(self.storage.clone())
                    .response(topics)
                    .await
                    .map(|body| (authentication, body))
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

                let detail = OffsetCommit {
                    group_id: group_id.as_str(),
                    generation_id_or_member_epoch,
                    member_id: member_id.as_deref(),
                    group_instance_id: group_instance_id.as_deref(),
                    retention_time_ms,
                    topics: topics.as_deref(),
                };

                self.groups
                    .offset_commit(detail)
                    .await
                    .map(|body| (authentication, body))
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
                    .map(|body| (authentication, body))
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
                    .map(|body| (authentication, body))
            }

            Body::SaslAuthenticateRequest { auth_bytes } => {
                debug!(?auth_bytes);
                let authenticate = SaslAuthenticate;

                authenticate.response(auth_bytes, authentication).await
            }

            Body::SaslHandshakeRequest { mechanism } => {
                debug!(?mechanism);
                let handshake = SaslHandshake;
                handshake.response(mechanism.as_str(), authentication).await
            }

            Body::SyncGroupRequest {
                group_id,
                generation_id,
                member_id,
                group_instance_id,
                protocol_type,
                protocol_name,
                assignments,
            } => self
                .groups
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
                .map(|body| (authentication, body)),

            _ => unimplemented!(),
        }
    }
}
