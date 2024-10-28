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

use bytes::Bytes;
use rand::{distributions::Alphanumeric, prelude::*, thread_rng};
use tansu_kafka_sans_io::{
    join_group_request::JoinGroupRequestProtocol, join_group_response::JoinGroupResponseMember,
    offset_fetch_request::OffsetFetchRequestTopic, offset_fetch_response::OffsetFetchResponseTopic,
    sync_group_request::SyncGroupRequestAssignment, Body, ErrorCode,
};
use tansu_server::{
    coordinator::group::{administrator::Controller, Coordinator},
    Error, Result,
};
use tansu_storage::{pg::Postgres, StorageContainer};

pub(crate) fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
    Postgres::builder("postgres://postgres:postgres@localhost")
        .map(|builder| builder.cluster(cluster))
        .map(|builder| builder.node(node))
        .map(|builder| builder.build())
        .map(StorageContainer::Postgres)
        .map_err(Into::into)
}

pub(crate) fn alphanumeric_string(length: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct JoinGroupResponse {
    pub error_code: ErrorCode,
    pub generation_id: i32,
    pub protocol_type: String,
    pub protocol_name: String,
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
                protocol_type: Some(protocol_type),
                protocol_name: Some(protocol_name),
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
        .map_err(Into::into)
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
        .map_err(Into::into)
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
        .map_err(Into::into)
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
        .map_err(Into::into)
        .and_then(TryInto::try_into)
}
