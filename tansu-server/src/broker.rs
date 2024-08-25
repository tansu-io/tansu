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

pub mod api_versions;
pub mod describe_cluster;
pub mod describe_configs;
pub mod fetch;
pub mod find_coordinator;
pub mod group;
pub mod init_producer_id;
pub mod list_partition_reassignments;
pub mod metadata;
pub mod produce;
pub mod telemetry;

use crate::{
    command::{Command, Request},
    coordinator::group::Coordinator,
    raft::Applicator,
    Error, Result,
};
use api_versions::ApiVersionsRequest;
use bytes::Bytes;
use describe_cluster::DescribeClusterRequest;
use describe_configs::DescribeConfigsRequest;
use fetch::FetchRequest;
use find_coordinator::FindCoordinatorRequest;
use group::{
    heartbeat::HeartbeatRequest, join::JoinRequest, leave::LeaveRequest,
    offset_commit::OffsetCommitRequest, offset_fetch::OffsetFetchRequest, sync::SyncGroupRequest,
};
use init_producer_id::InitProducerIdRequest;
use list_partition_reassignments::ListPartitionReassignmentsRequest;
use metadata::MetadataRequest;
use produce::ProduceRequest;
use std::{
    io::ErrorKind,
    sync::{Arc, Mutex, MutexGuard},
};
use tansu_kafka_sans_io::{Body, Frame, Header};
use tansu_raft::{Log, Raft};
use tansu_storage::Storage;
use telemetry::GetTelemetrySubscriptionsRequest;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::{debug, error, info};
use url::Url;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct Broker {
    node_id: i32,
    cluster_id: String,
    incarnation_id: Uuid,
    context: Raft,
    applicator: Applicator,
    listener: Url,
    rack: Option<String>,
    storage: Arc<Mutex<Storage>>,
    groups: Arc<Mutex<Box<dyn Coordinator>>>,
}

impl Broker {
    pub fn new<'a>(
        node_id: i32,
        cluster_id: &'a str,
        context: Raft,
        applicator: Applicator,
        listener: Url,
        rack: Option<String>,
        storage: Arc<Mutex<Storage>>,
        groups: Arc<Mutex<Box<dyn Coordinator>>>,
    ) -> Self {
        let incarnation_id = Uuid::new_v4();

        Self {
            node_id,
            cluster_id: cluster_id.to_owned(),
            incarnation_id,
            context,
            applicator,
            listener,
            rack,
            storage,
            groups,
        }
    }

    pub(crate) fn storage_lock(&self) -> Result<MutexGuard<'_, Storage>> {
        self.storage.lock().map_err(|error| error.into())
    }

    pub async fn serve(&mut self) -> Result<()> {
        _ = self.register().await?;
        self.listen().await
    }

    pub async fn register(&mut self) -> Result<Body> {
        use tansu_kafka_sans_io::broker_registration_request::Listener;

        self.when_applied(Body::BrokerRegistrationRequest {
            broker_id: self.node_id,
            cluster_id: self.cluster_id.clone(),
            incarnation_id: self.incarnation_id.as_bytes().clone(),
            listeners: Some(vec![Listener {
                name: "broker".into(),
                host: self.listener.host_str().unwrap_or("localhost").to_owned(),
                port: self.listener.port().unwrap_or(9092),
                security_protocol: 0,
            }]),
            features: Some(vec![]),
            rack: self.rack.clone(),
            is_migrating_zk_broker: Some(false),
            log_dirs: Some(vec![]),
            previous_broker_epoch: Some(-1),
        })
        .await
    }

    pub async fn listen(&self) -> Result<()> {
        debug!("listener: {}", self.listener.as_str());

        let listener = TcpListener::bind(format!(
            "{}:{}",
            self.listener.host_str().unwrap_or("localhost"),
            self.listener.port().unwrap_or(9092)
        ))
        .await?;

        loop {
            let (stream, addr) = listener.accept().await?;
            info!(?addr);

            let mut broker = self.clone();

            _ = tokio::spawn(async move {
                match broker.stream_handler(stream).await {
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

    async fn stream_handler(&mut self, mut stream: TcpStream) -> Result<()> {
        debug!(?stream);

        let mut size = [0u8; 4];

        loop {
            _ = stream.read_exact(&mut size).await?;

            if i32::from_be_bytes(size) == 0 {
                info!("empty read!");
                continue;
            }

            let mut request: Vec<u8> = vec![0u8; i32::from_be_bytes(size) as usize + size.len()];
            request[0..4].copy_from_slice(&size[..]);
            _ = stream.read_exact(&mut request[4..]).await?;
            debug!(?request);

            let mut response = self.process_request(&request).await?;
            debug!(?response);

            stream.write_all(&mut response).await?;
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
                debug!(?api_key, ?api_version, ?correlation_id);
                let body = self
                    .response_for(client_id.as_ref().map(|s| s.as_str()), body, correlation_id)
                    .await?;
                debug!(?body, ?correlation_id);
                Frame::response(
                    Header::Response { correlation_id },
                    body,
                    api_key,
                    api_version,
                )
                .map_err(Into::into)
            }

            _ => unimplemented!(),
        }
    }

    async fn when_applied(&mut self, body: Body) -> Result<Body> {
        debug!(?self, ?body);

        let id = Uuid::new_v4();
        let request = Request::new(id.clone(), body);
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
    ) -> Result<Body> {
        debug!(?body, ?correlation_id);

        match body {
            Body::ApiVersionsRequest {
                client_software_name,
                client_software_version,
            } => {
                let api_versions = ApiVersionsRequest;
                Ok(api_versions.response(
                    client_software_name
                        .as_ref()
                        .map(|client_software_name| client_software_name.as_str()),
                    client_software_version
                        .as_ref()
                        .map(|client_software_version| client_software_version.as_str()),
                ))
            }

            Body::CreateTopicsRequest {
                validate_only: Some(false),
                ..
            } => self.when_applied(body).await,

            Body::DescribeClusterRequest {
                include_cluster_authorized_operations,
                endpoint_type,
            } => {
                let state = self.applicator.with_current_state().await;

                let describe_cluster = DescribeClusterRequest;
                Ok(describe_cluster.response(
                    include_cluster_authorized_operations,
                    endpoint_type,
                    &state,
                ))
            }

            Body::DescribeConfigsRequest {
                resources,
                include_synonyms,
                include_documentation,
            } => {
                let describe_configs = DescribeConfigsRequest;
                Ok(describe_configs.response(
                    resources.as_ref().map(|resources| resources.as_slice()),
                    include_synonyms,
                    include_documentation,
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
                let storage = self.storage.clone();
                let mut fetch = FetchRequest { storage };
                let state = self.applicator.with_current_state().await;
                fetch
                    .response(
                        max_wait_ms,
                        min_bytes,
                        max_bytes,
                        isolation_level,
                        topics.as_ref().map(|topics| topics.as_slice()),
                        &state,
                    )
                    .await
            }

            Body::FindCoordinatorRequest {
                key,
                key_type,
                coordinator_keys,
            } => {
                let find_coordinator = FindCoordinatorRequest;

                Ok(find_coordinator.response(
                    key.as_ref().map(|key| key.as_str()),
                    key_type,
                    coordinator_keys
                        .as_ref()
                        .map(|coordinator_keys| coordinator_keys.as_slice()),
                    self.node_id,
                    &self.listener,
                ))
            }

            Body::GetTelemetrySubscriptionsRequest { client_instance_id } => {
                let get_telemetry_subscriptions = GetTelemetrySubscriptionsRequest;
                Ok(get_telemetry_subscriptions.response(client_instance_id))
            }

            Body::HeartbeatRequest {
                group_id,
                generation_id,
                member_id,
                group_instance_id,
            } => {
                let heartbeat = HeartbeatRequest {
                    groups: self.groups.clone(),
                };

                heartbeat.response(
                    &group_id,
                    generation_id,
                    &member_id,
                    group_instance_id
                        .as_ref()
                        .map(|group_instance_id| group_instance_id.as_str()),
                )
            }

            Body::InitProducerIdRequest {
                transactional_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            } => {
                let init_producer_id = InitProducerIdRequest;
                Ok(init_producer_id.response(
                    transactional_id
                        .as_ref()
                        .map(|transaction_id| transaction_id.as_str()),
                    transaction_timeout_ms,
                    producer_id,
                    producer_epoch,
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
                let join = JoinRequest {
                    groups: self.groups.clone(),
                };

                join.response(
                    client_id,
                    &group_id,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    &member_id,
                    group_instance_id
                        .as_ref()
                        .map(|group_instance_id| group_instance_id.as_str()),
                    &protocol_type,
                    protocols.as_ref().map(|protocols| protocols.as_slice()),
                    reason.as_ref().map(|reason| reason.as_str()),
                )
            }

            Body::LeaveGroupRequest {
                group_id,
                member_id,
                members,
            } => {
                let leave = LeaveRequest {
                    groups: self.groups.clone(),
                };

                leave.response(
                    &group_id,
                    member_id.as_ref().map(|member_id| member_id.as_str()),
                    members.as_ref().map(|members| members.as_slice()),
                )
            }

            Body::ListPartitionReassignmentsRequest { topics, .. } => {
                let state = self.applicator.with_current_state().await;
                let list_partition_reassignments = ListPartitionReassignmentsRequest;
                Ok(list_partition_reassignments
                    .response(topics.as_ref().map(|topics| topics.as_slice()), &state))
            }

            Body::MetadataRequest { topics, .. } => {
                let controller_id = Some(self.node_id);
                let state = self.applicator.with_current_state().await;

                let topics = topics.as_ref().map(|topics| topics.as_slice());

                let request = MetadataRequest;
                Ok(request.response(controller_id, topics, &state))
            }

            Body::OffsetCommitRequest {
                group_id,
                generation_id_or_member_epoch,
                member_id,
                group_instance_id,
                retention_time_ms,
                topics,
            } => {
                let offset_commit = OffsetCommitRequest {
                    groups: self.groups.clone(),
                };

                offset_commit.response(
                    &group_id,
                    generation_id_or_member_epoch,
                    member_id.as_ref().map(|s| s.as_str()),
                    group_instance_id.as_ref().map(|s| s.as_str()),
                    retention_time_ms,
                    topics.as_ref().map(|topics| topics.as_slice()),
                )
            }

            Body::OffsetFetchRequest {
                group_id,
                topics,
                groups,
                require_stable,
            } => {
                let offset_fetch = OffsetFetchRequest {
                    groups: self.groups.clone(),
                };

                offset_fetch.response(
                    group_id.as_ref().map(|group_id| group_id.as_str()),
                    topics.as_ref().map(|topics| topics.as_slice()),
                    groups.as_ref().map(|groups| groups.as_slice()),
                    require_stable,
                )
            }

            Body::ProduceRequest {
                transactional_id,
                acks,
                timeout_ms,
                topic_data,
            } => {
                let state = self.applicator.with_current_state().await;

                self.storage_lock().map(|mut manager| {
                    ProduceRequest::response(
                        transactional_id,
                        acks,
                        timeout_ms,
                        topic_data,
                        &mut manager,
                        &state,
                    )
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
                let sync_group = SyncGroupRequest {
                    groups: self.groups.clone(),
                };

                sync_group.response(
                    &group_id,
                    generation_id,
                    &member_id,
                    group_instance_id
                        .as_ref()
                        .map(|group_instance_id| group_instance_id.as_str()),
                    protocol_type
                        .as_ref()
                        .map(|protocol_type| protocol_type.as_str()),
                    protocol_name
                        .as_ref()
                        .map(|protocol_name| protocol_name.as_str()),
                    assignments
                        .as_ref()
                        .map(|assignments| assignments.as_slice()),
                )
            }

            _ => unimplemented!(),
        }
    }
}
