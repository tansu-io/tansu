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

#![allow(dead_code)]
use bytes::Bytes;
use object_store::memory::InMemory;
use rand::{
    distr::{Alphanumeric, StandardUniform},
    prelude::*,
    rng,
};
use tansu_broker::{
    Error, Result,
    coordinator::group::{Coordinator, administrator::Controller},
};
use tansu_sans_io::{
    Body, ErrorCode,
    fetch_response::{FetchableTopicResponse, NodeEndpoint},
    join_group_request::JoinGroupRequestProtocol,
    join_group_response::JoinGroupResponseMember,
    leave_group_request::MemberIdentity,
    leave_group_response::MemberResponse,
    offset_fetch_request::OffsetFetchRequestTopic,
    offset_fetch_response::OffsetFetchResponseTopic,
    sync_group_request::SyncGroupRequestAssignment,
};
use tansu_schema::Registry;
use tansu_storage::{
    BrokerRegistrationRequest, Storage, StorageContainer, dynostore::DynoStore, pg::Postgres,
};
use tracing::{debug, subscriber::DefaultGuard};
use tracing_subscriber::EnvFilter;
use url::Url;
use uuid::Uuid;

pub(crate) fn init_tracing() -> Result<DefaultGuard> {
    use std::{fs::File, sync::Arc, thread};

    Ok(tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
            )
            .with_writer(
                thread::current()
                    .name()
                    .ok_or(Error::Message(String::from("unnamed thread")))
                    .and_then(|name| {
                        File::create(format!(
                            "../logs/{}/{}::{name}.log",
                            env!("CARGO_PKG_NAME"),
                            env!("CARGO_CRATE_NAME")
                        ))
                        .map_err(Into::into)
                    })
                    .map(Arc::new)?,
            )
            .finish(),
    ))
}

pub(crate) enum StorageType {
    Postgres,
    InMemory,
}

pub(crate) fn storage_container(
    storage_type: StorageType,
    cluster: impl Into<String>,
    node: i32,
    advertised_listener: Url,
    schemas: Option<Registry>,
) -> Result<StorageContainer> {
    match storage_type {
        StorageType::Postgres => Postgres::builder("postgres://postgres:postgres@localhost")
            .map(|builder| builder.cluster(cluster))
            .map(|builder| builder.node(node))
            .map(|builder| builder.advertised_listener(advertised_listener))
            .map(|builder| builder.schemas(schemas))
            .map(|builder| builder.build())
            .map(StorageContainer::Postgres)
            .map_err(Into::into),

        StorageType::InMemory => Ok(StorageContainer::DynoStore(
            DynoStore::new(cluster.into().as_str(), node, InMemory::new())
                .advertised_listener(advertised_listener)
                .schemas(schemas),
        )),
    }
}

pub(crate) fn alphanumeric_string(length: usize) -> String {
    rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

pub(crate) fn random_bytes(length: usize) -> Bytes {
    rng()
        .sample_iter(StandardUniform)
        .take(length)
        .collect::<Vec<u8>>()
        .into()
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct JoinGroupResponse {
    pub error_code: ErrorCode,
    pub generation_id: i32,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub leader: String,
    pub skip_assignment: bool,
    pub member_id: String,
    pub members: Vec<JoinGroupResponseMember>,
}

impl TryFrom<Body> for JoinGroupResponse {
    type Error = Error;

    fn try_from(value: Body) -> Result<Self, Self::Error> {
        match value {
            Body::JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code,
                generation_id,
                protocol_type,
                protocol_name,
                leader,
                skip_assignment: Some(skip_assignment),
                members: Some(members),
                member_id,
            } => ErrorCode::try_from(error_code)
                .map(|error_code| JoinGroupResponse {
                    error_code,
                    generation_id,
                    protocol_type,
                    protocol_name,
                    leader,
                    skip_assignment,
                    member_id,
                    members,
                })
                .map_err(Into::into),
            otherwise => panic!("{otherwise:?}"),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn join_group(
    controller: &mut Controller<StorageContainer>,
    client_id: Option<&str>,
    group_id: &str,
    session_timeout_ms: i32,
    rebalance_timeout_ms: Option<i32>,
    member_id: &str,
    group_instance_id: Option<&str>,
    protocol_type: &str,
    protocols: Option<&[JoinGroupRequestProtocol]>,
    reason: Option<&str>,
) -> Result<JoinGroupResponse> {
    controller
        .join(
            client_id,
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            protocols,
            reason,
        )
        .await
        .and_then(TryInto::try_into)
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct SyncGroupResponse {
    pub error_code: ErrorCode,
    pub protocol_type: String,
    pub protocol_name: String,
    pub assignment: Bytes,
}

impl TryFrom<Body> for SyncGroupResponse {
    type Error = Error;

    fn try_from(value: Body) -> std::result::Result<Self, Self::Error> {
        match value {
            Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code,
                protocol_type: Some(protocol_type),
                protocol_name: Some(protocol_name),
                assignment,
            } => ErrorCode::try_from(error_code)
                .map(|error_code| SyncGroupResponse {
                    error_code,
                    protocol_type,
                    protocol_name,
                    assignment,
                })
                .map_err(Into::into),

            otherwise => panic!("{otherwise:?}"),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn sync_group(
    controller: &mut Controller<StorageContainer>,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    group_instance_id: Option<&str>,
    protocol_type: &str,
    protocol_name: &str,
    assignments: &[SyncGroupRequestAssignment],
) -> Result<SyncGroupResponse> {
    controller
        .sync(
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            Some(protocol_type),
            Some(protocol_name),
            Some(assignments),
        )
        .await
        .and_then(TryInto::try_into)
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct HeartbeatResponse {
    pub error_code: ErrorCode,
}

impl TryFrom<Body> for HeartbeatResponse {
    type Error = Error;

    fn try_from(value: Body) -> std::result::Result<Self, Self::Error> {
        match value {
            Body::HeartbeatResponse {
                throttle_time_ms: Some(0),
                error_code,
            } => ErrorCode::try_from(error_code)
                .map(|error_code| HeartbeatResponse { error_code })
                .map_err(Into::into),

            otherwise => panic!("{otherwise:?}"),
        }
    }
}

pub(crate) async fn heartbeat(
    controller: &mut Controller<StorageContainer>,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    group_instance_id: Option<&str>,
) -> Result<HeartbeatResponse> {
    controller
        .heartbeat(group_id, generation_id, member_id, group_instance_id)
        .await
        .and_then(TryInto::try_into)
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct OffsetFetchResponse {
    pub topics: Vec<OffsetFetchResponseTopic>,
    pub error_code: ErrorCode,
}

impl TryFrom<Body> for OffsetFetchResponse {
    type Error = Error;

    fn try_from(value: Body) -> Result<Self, Self::Error> {
        match value {
            Body::OffsetFetchResponse {
                throttle_time_ms: Some(0),
                topics: Some(topics),
                error_code: Some(error_code),
                groups: None,
            } => ErrorCode::try_from(error_code)
                .map(|error_code| OffsetFetchResponse { topics, error_code })
                .map_err(Into::into),

            otherwise => panic!("{otherwise:?}"),
        }
    }
}

pub(crate) async fn offset_fetch(
    controller: &mut Controller<StorageContainer>,
    group_id: &str,
    topics: &[OffsetFetchRequestTopic],
) -> Result<OffsetFetchResponse> {
    controller
        .offset_fetch(Some(group_id), Some(topics), None, Some(false))
        .await
        .and_then(TryInto::try_into)
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct LeaveGroupResponse {
    pub error_code: ErrorCode,
    members: Vec<MemberResponse>,
}

impl TryFrom<Body> for LeaveGroupResponse {
    type Error = Error;

    fn try_from(value: Body) -> Result<Self, Self::Error> {
        match value {
            Body::LeaveGroupResponse {
                error_code,
                members,
                ..
            } => ErrorCode::try_from(error_code)
                .map(|error_code| {
                    let members = members.unwrap_or_default();
                    LeaveGroupResponse {
                        error_code,
                        members,
                    }
                })
                .map_err(Into::into),

            otherwise => panic!("{otherwise:?}"),
        }
    }
}

pub(crate) async fn leave(
    controller: &mut Controller<StorageContainer>,
    group_id: &str,
    member_id: &str,
    group_instance_id: Option<&str>,
) -> Result<LeaveGroupResponse> {
    controller
        .leave(
            group_id,
            None,
            Some(&[MemberIdentity {
                member_id: member_id.into(),
                group_instance_id: group_instance_id.map(|s| s.to_owned()),
                reason: Some("the consumer is being closed".into()),
            }]),
        )
        .await
        .and_then(TryInto::try_into)
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum JoinResponse {
    Leader {
        id: String,
        generation: i32,
        members: Vec<JoinGroupResponseMember>,
        protocols: Vec<JoinGroupRequestProtocol>,
    },
    Follower {
        leader: String,
        id: String,
        generation: i32,
        protocols: Vec<JoinGroupRequestProtocol>,
    },
}

impl JoinResponse {
    #[allow(dead_code)]
    pub(crate) fn leader(&self) -> &str {
        match self {
            Self::Leader { id: leader, .. } | Self::Follower { leader, .. } => leader.as_str(),
        }
    }

    pub(crate) fn id(&self) -> &str {
        match self {
            Self::Leader { id, .. } | Self::Follower { id, .. } => id.as_str(),
        }
    }

    pub(crate) fn generation(&self) -> i32 {
        match self {
            Self::Leader { generation, .. } | Self::Follower { generation, .. } => *generation,
        }
    }

    pub(crate) fn is_leader(&self) -> bool {
        matches!(self, Self::Leader { .. })
    }

    pub(crate) fn protocols(&self) -> &[JoinGroupRequestProtocol] {
        match self {
            Self::Leader { protocols, .. } | Self::Follower { protocols, .. } => &protocols[..],
        }
    }
}

pub(crate) const CLIENT_ID: &str = "console-consumer";
pub(crate) const RANGE: &str = "range";
pub(crate) const COOPERATIVE_STICKY: &str = "cooperative-sticky";
pub(crate) const PROTOCOL_TYPE: &str = "consumer";

#[allow(clippy::too_many_arguments)]
pub(crate) async fn join(
    controller: &mut Controller<StorageContainer>,
    group_id: &str,
    member_id: Option<&str>,
    group_instance_id: Option<&str>,
    protocols: Option<Vec<JoinGroupRequestProtocol>>,
    session_timeout_ms: i32,
    rebalance_timeout_ms: Option<i32>,
) -> Result<JoinResponse> {
    let reason = None;

    let protocols = protocols.unwrap_or_else(|| {
        [
            JoinGroupRequestProtocol {
                name: RANGE.into(),
                metadata: random_bytes(15),
            },
            JoinGroupRequestProtocol {
                name: COOPERATIVE_STICKY.into(),
                metadata: random_bytes(15),
            },
        ]
        .into()
    });

    let join_response = join_group(
        controller,
        Some(CLIENT_ID),
        group_id,
        session_timeout_ms,
        rebalance_timeout_ms,
        member_id.unwrap_or_default(),
        group_instance_id,
        PROTOCOL_TYPE,
        Some(&protocols[..]),
        reason,
    )
    .await?;

    if member_id.is_none() && group_instance_id.is_none() {
        // join rejected as member id is required for a dynamic group
        //
        assert_eq!(ErrorCode::MemberIdRequired, join_response.error_code);
        assert_eq!(Some("".into()), join_response.protocol_name);
        assert!(join_response.leader.is_empty());
        assert!(join_response.member_id.starts_with(CLIENT_ID));
        assert_eq!(0, join_response.members.len());

        Box::pin(join(
            controller,
            group_id,
            Some(join_response.member_id.as_str()),
            group_instance_id,
            Some(protocols),
            session_timeout_ms,
            rebalance_timeout_ms,
        ))
        .await
    } else if join_response.member_id == join_response.leader {
        assert_eq!(ErrorCode::None, join_response.error_code);
        assert_eq!(Some(PROTOCOL_TYPE.into()), join_response.protocol_type);
        assert_eq!(Some(RANGE.into()), join_response.protocol_name);

        let id = join_response.leader;
        let generation = join_response.generation_id;
        let members = join_response.members;

        Ok(JoinResponse::Leader {
            id,
            generation,
            members,
            protocols,
        })
    } else {
        assert_eq!(ErrorCode::None, join_response.error_code);
        assert_eq!(Some(PROTOCOL_TYPE.into()), join_response.protocol_type);
        assert_eq!(Some(RANGE.into()), join_response.protocol_name);
        assert_ne!(join_response.member_id, join_response.leader);
        assert_eq!(0, join_response.members.len());

        let id = join_response.member_id;
        let leader = join_response.leader;
        let generation = join_response.generation_id;

        Ok(JoinResponse::Follower {
            leader,
            id,
            generation,
            protocols,
        })
    }
}

pub(crate) async fn register_broker(
    cluster_id: &Uuid,
    broker_id: i32,
    sc: &mut StorageContainer,
) -> Result<()> {
    let incarnation_id = Uuid::now_v7();

    debug!(?cluster_id, ?broker_id, ?incarnation_id);

    let broker_registration = BrokerRegistrationRequest {
        broker_id,
        cluster_id: cluster_id.to_owned().into(),
        incarnation_id,
        rack: None,
    };

    sc.register_broker(broker_registration)
        .await
        .map_err(Into::into)
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct FetchResponse {
    error_code: ErrorCode,
    session_id: Option<i32>,
    responses: Vec<FetchableTopicResponse>,
    node_endpoints: Vec<NodeEndpoint>,
}

impl FetchResponse {
    pub(crate) fn error_code(&self) -> ErrorCode {
        self.error_code
    }

    pub(crate) fn responses(&self) -> &[FetchableTopicResponse] {
        &self.responses
    }
}

impl TryFrom<Body> for FetchResponse {
    type Error = Error;

    fn try_from(value: Body) -> Result<Self, Self::Error> {
        match value {
            Body::FetchResponse {
                error_code,
                session_id,
                responses,
                node_endpoints,
                ..
            } => Ok(FetchResponse {
                error_code: error_code.map_or(Ok(ErrorCode::None), TryInto::try_into)?,
                session_id,
                responses: responses.unwrap_or_default(),
                node_endpoints: node_endpoints.unwrap_or_default(),
            }),

            otherwise => panic!("{otherwise:?}"),
        }
    }
}
