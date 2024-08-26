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

use std::{collections::BTreeMap, fmt::Debug, time::Instant};

use bytes::Bytes;
use tansu_kafka_sans_io::{
    join_group_request::JoinGroupRequestProtocol,
    join_group_response::JoinGroupResponseMember,
    leave_group_request::MemberIdentity,
    leave_group_response::MemberResponse,
    offset_commit_request::OffsetCommitRequestTopic,
    offset_commit_response::{OffsetCommitResponsePartition, OffsetCommitResponseTopic},
    offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopic},
    offset_fetch_response::{
        OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
        OffsetFetchResponseTopic, OffsetFetchResponseTopics,
    },
    sync_group_request::SyncGroupRequestAssignment,
    Body, ErrorCode,
};
use tracing::{debug, instrument};
use uuid::Uuid;

use crate::{Error, Result};

use super::Coordinator;

pub trait Group: Debug + Send {
    type JoinState;
    type SyncState;
    type HeartbeatState;
    type LeaveState;
    type OffsetCommitState;
    type OffsetFetchState;

    #[allow(clippy::too_many_arguments)]
    fn join(
        self,
        now: Instant,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> (Self::JoinState, Body);

    #[allow(clippy::too_many_arguments)]
    fn sync(
        self,
        now: Instant,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Self::SyncState, Body);

    fn heartbeat(
        self,
        now: Instant,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Self::HeartbeatState, Body);

    fn leave(
        self,
        now: Instant,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Self::LeaveState, Body);

    #[allow(clippy::too_many_arguments)]
    fn offset_commit(
        self,
        now: Instant,
        group_id: &str,
        generation_id_or_member_epoch: Option<i32>,
        member_id: Option<&str>,
        group_instance_id: Option<&str>,
        retention_time_ms: Option<i64>,
        topics: Option<&[OffsetCommitRequestTopic]>,
    ) -> (Self::OffsetCommitState, Body);

    fn offset_fetch(
        self,
        now: Instant,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Self::OffsetFetchState, Body);
}

#[derive(Debug)]
pub enum Wrapper {
    Fresh(Inner<Fresh>),
    Forming(Inner<Forming>),
    Syncing(Inner<Syncing>),
    Formed(Inner<Formed>),
}

impl From<Inner<Fresh>> for Wrapper {
    fn from(value: Inner<Fresh>) -> Self {
        Self::Fresh(value)
    }
}

impl From<Inner<Forming>> for Wrapper {
    fn from(value: Inner<Forming>) -> Self {
        Self::Forming(value)
    }
}

impl From<Inner<Syncing>> for Wrapper {
    fn from(value: Inner<Syncing>) -> Self {
        Self::Syncing(value)
    }
}

impl From<Inner<Formed>> for Wrapper {
    fn from(value: Inner<Formed>) -> Self {
        Self::Formed(value)
    }
}

impl Wrapper {
    pub fn generation_id(&self) -> i32 {
        match self {
            Self::Fresh(inner) => inner.generation_id,
            Self::Forming(inner) => inner.generation_id,
            Self::Syncing(inner) => inner.generation_id,
            Self::Formed(inner) => inner.generation_id,
        }
    }

    pub fn session_timeout_ms(&self) -> i32 {
        match self {
            Self::Fresh(inner) => inner.session_timeout_ms,
            Self::Forming(inner) => inner.session_timeout_ms,
            Self::Syncing(inner) => inner.session_timeout_ms,
            Self::Formed(inner) => inner.session_timeout_ms,
        }
    }

    pub fn rebalance_timeout_ms(&self) -> Option<i32> {
        match self {
            Self::Fresh(inner) => inner.rebalance_timeout_ms,
            Self::Forming(inner) => inner.rebalance_timeout_ms,
            Self::Syncing(inner) => inner.rebalance_timeout_ms,
            Self::Formed(inner) => inner.rebalance_timeout_ms,
        }
    }

    pub fn protocol_type(&self) -> Option<&str> {
        match self {
            Self::Fresh(_inner) => None,
            Self::Forming(inner) => Some(inner.state.protocol_type.as_str()),
            Self::Syncing(inner) => Some(inner.state.protocol_type.as_str()),
            Self::Formed(inner) => Some(inner.state.protocol_type.as_str()),
        }
    }

    pub fn protocol_name(&self) -> Option<&str> {
        match self {
            Self::Fresh(_inner) => None,
            Self::Forming(inner) => Some(inner.state.protocol_name.as_str()),
            Self::Syncing(inner) => Some(inner.state.protocol_name.as_str()),
            Self::Formed(inner) => Some(inner.state.protocol_name.as_str()),
        }
    }

    pub fn leader(&self) -> Option<&str> {
        match self {
            Self::Fresh(_inner) => None,
            Self::Forming(inner) => inner.state.leader.as_deref(),
            Self::Syncing(inner) => Some(inner.state.leader.as_str()),
            Self::Formed(inner) => Some(inner.state.leader.as_str()),
        }
    }

    fn members(&self) -> Vec<JoinGroupResponseMember> {
        match self {
            Wrapper::Fresh(_) => vec![],

            Wrapper::Forming(inner) => inner
                .members
                .values()
                .cloned()
                .map(|member| member.join_response)
                .collect(),

            Wrapper::Syncing(inner) => inner
                .members
                .values()
                .cloned()
                .map(|member| member.join_response)
                .collect(),

            Wrapper::Formed(inner) => inner
                .members
                .values()
                .cloned()
                .map(|member| member.join_response)
                .collect(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument]
    fn join(
        self,
        now: Instant,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> (Wrapper, Body) {
        match self {
            Self::Fresh(inner) => {
                let (state, body) = inner.join(
                    now,
                    client_id,
                    group_id,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    member_id,
                    group_instance_id,
                    protocol_type,
                    protocols,
                    reason,
                );
                (state, body)
            }

            Self::Forming(inner) => {
                let (state, body) = inner.join(
                    now,
                    client_id,
                    group_id,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    member_id,
                    group_instance_id,
                    protocol_type,
                    protocols,
                    reason,
                );
                (state.into(), body)
            }

            Self::Syncing(inner) => {
                let (state, body) = inner.join(
                    now,
                    client_id,
                    group_id,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    member_id,
                    group_instance_id,
                    protocol_type,
                    protocols,
                    reason,
                );
                (state, body)
            }

            Self::Formed(inner) => {
                let (state, body) = inner.join(
                    now,
                    client_id,
                    group_id,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    member_id,
                    group_instance_id,
                    protocol_type,
                    protocols,
                    reason,
                );
                (state, body)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument]
    fn sync(
        self,
        now: Instant,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Wrapper, Body) {
        match self {
            Wrapper::Fresh(inner) => {
                let (state, body) = inner.sync(
                    now,
                    group_id,
                    generation_id,
                    member_id,
                    group_instance_id,
                    protocol_name,
                    assignments,
                );
                (state.into(), body)
            }

            Wrapper::Forming(inner) => {
                let (state, body) = inner.sync(
                    now,
                    group_id,
                    generation_id,
                    member_id,
                    group_instance_id,
                    protocol_name,
                    assignments,
                );
                (state, body)
            }

            Wrapper::Syncing(inner) => {
                let (state, body) = inner.sync(
                    now,
                    group_id,
                    generation_id,
                    member_id,
                    group_instance_id,
                    protocol_name,
                    assignments,
                );
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner.sync(
                    now,
                    group_id,
                    generation_id,
                    member_id,
                    group_instance_id,
                    protocol_name,
                    assignments,
                );
                (state.into(), body)
            }
        }
    }

    #[instrument]
    fn leave(
        self,
        now: Instant,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Wrapper, Body) {
        match self {
            Wrapper::Fresh(inner) => {
                let (state, body) = inner.leave(now, group_id, member_id, members);
                (state.into(), body)
            }

            Wrapper::Forming(inner) => {
                let (state, body) = inner.leave(now, group_id, member_id, members);
                (state.into(), body)
            }

            Wrapper::Syncing(inner) => {
                let (state, body) = inner.leave(now, group_id, member_id, members);
                (state, body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner.leave(now, group_id, member_id, members);
                (state, body)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument]
    fn offset_commit(
        self,
        now: Instant,
        group_id: &str,
        generation_id_or_member_epoch: Option<i32>,
        member_id: Option<&str>,
        group_instance_id: Option<&str>,
        retention_time_ms: Option<i64>,
        topics: Option<&[OffsetCommitRequestTopic]>,
    ) -> (Wrapper, Body) {
        match self {
            Wrapper::Fresh(inner) => {
                let (state, body) = inner.offset_commit(
                    now,
                    group_id,
                    generation_id_or_member_epoch,
                    member_id,
                    group_instance_id,
                    retention_time_ms,
                    topics,
                );
                (state.into(), body)
            }

            Wrapper::Forming(inner) => {
                let (state, body) = inner.offset_commit(
                    now,
                    group_id,
                    generation_id_or_member_epoch,
                    member_id,
                    group_instance_id,
                    retention_time_ms,
                    topics,
                );
                (state.into(), body)
            }

            Wrapper::Syncing(inner) => {
                let (state, body) = inner.offset_commit(
                    now,
                    group_id,
                    generation_id_or_member_epoch,
                    member_id,
                    group_instance_id,
                    retention_time_ms,
                    topics,
                );
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner.offset_commit(
                    now,
                    group_id,
                    generation_id_or_member_epoch,
                    member_id,
                    group_instance_id,
                    retention_time_ms,
                    topics,
                );
                (state.into(), body)
            }
        }
    }

    #[instrument]
    fn offset_fetch(
        self,
        now: Instant,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Wrapper, Body) {
        match self {
            Wrapper::Fresh(inner) => {
                let (state, body) =
                    inner.offset_fetch(now, group_id, topics, groups, require_stable);
                (state.into(), body)
            }

            Wrapper::Forming(inner) => {
                let (state, body) =
                    inner.offset_fetch(now, group_id, topics, groups, require_stable);
                (state.into(), body)
            }

            Wrapper::Syncing(inner) => {
                let (state, body) =
                    inner.offset_fetch(now, group_id, topics, groups, require_stable);
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) =
                    inner.offset_fetch(now, group_id, topics, groups, require_stable);
                (state.into(), body)
            }
        }
    }

    #[instrument]
    fn heartbeat(
        self,
        now: Instant,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Wrapper, Body) {
        match self {
            Wrapper::Fresh(inner) => {
                let (state, body) =
                    inner.heartbeat(now, group_id, generation_id, member_id, group_instance_id);
                (state.into(), body)
            }

            Wrapper::Forming(inner) => {
                let (state, body) =
                    inner.heartbeat(now, group_id, generation_id, member_id, group_instance_id);
                (state.into(), body)
            }

            Wrapper::Syncing(inner) => {
                let (state, body) =
                    inner.heartbeat(now, group_id, generation_id, member_id, group_instance_id);
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) =
                    inner.heartbeat(now, group_id, generation_id, member_id, group_instance_id);
                (state.into(), body)
            }
        }
    }
}

#[derive(Debug)]
pub struct Controller {
    wrapper: Option<Wrapper>,
}

impl Controller {
    pub fn new(wrapper: Wrapper) -> Self {
        Self {
            wrapper: Some(wrapper),
        }
    }
}

impl Coordinator for Controller {
    fn join(
        &mut self,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> Result<Body> {
        let now = Instant::now();

        self.wrapper
            .take()
            .ok_or(Error::EmptyCoordinatorWrapper)
            .map(|wrapper| {
                wrapper.join(
                    now,
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
            })
            .map(|(wrapper, body)| {
                _ = self.wrapper.replace(wrapper);
                body
            })
    }

    fn sync(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> Result<Body> {
        let now = Instant::now();

        self.wrapper
            .take()
            .ok_or(Error::EmptyCoordinatorWrapper)
            .map(|wrapper| {
                wrapper.sync(
                    now,
                    group_id,
                    generation_id,
                    member_id,
                    group_instance_id,
                    protocol_type,
                    protocol_name,
                    assignments,
                )
            })
            .map(|(wrapper, body)| {
                _ = self.wrapper.replace(wrapper);
                body
            })
    }

    fn leave(
        &mut self,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> Result<Body> {
        let now = Instant::now();

        self.wrapper
            .take()
            .ok_or(Error::EmptyCoordinatorWrapper)
            .map(|wrapper| wrapper.leave(now, group_id, member_id, members))
            .map(|(wrapper, body)| {
                _ = self.wrapper.replace(wrapper);
                body
            })
    }

    fn offset_commit(
        &mut self,
        group_id: &str,
        generation_id_or_member_epoch: Option<i32>,
        member_id: Option<&str>,
        group_instance_id: Option<&str>,
        retention_time_ms: Option<i64>,
        topics: Option<&[OffsetCommitRequestTopic]>,
    ) -> Result<Body> {
        let now = Instant::now();

        self.wrapper
            .take()
            .ok_or(Error::EmptyCoordinatorWrapper)
            .map(|wrapper| {
                wrapper.offset_commit(
                    now,
                    group_id,
                    generation_id_or_member_epoch,
                    member_id,
                    group_instance_id,
                    retention_time_ms,
                    topics,
                )
            })
            .map(|(wrapper, body)| {
                _ = self.wrapper.replace(wrapper);
                body
            })
    }

    fn offset_fetch(
        &mut self,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> Result<Body> {
        let now = Instant::now();

        self.wrapper
            .take()
            .ok_or(Error::EmptyCoordinatorWrapper)
            .map(|wrapper| wrapper.offset_fetch(now, group_id, topics, groups, require_stable))
            .map(|(wrapper, body)| {
                _ = self.wrapper.replace(wrapper);
                body
            })
    }

    fn heartbeat(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<Body> {
        let now = Instant::now();

        self.wrapper
            .take()
            .ok_or(Error::EmptyCoordinatorWrapper)
            .map(|wrapper| {
                wrapper.heartbeat(now, group_id, generation_id, member_id, group_instance_id)
            })
            .map(|(wrapper, body)| {
                _ = self.wrapper.replace(wrapper);
                body
            })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Fresh;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Forming {
    protocol_type: String,
    protocol_name: String,
    leader: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Syncing {
    protocol_type: String,
    protocol_name: String,
    leader: String,
    assignments: BTreeMap<String, Bytes>,
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Formed {
    protocol_type: String,
    protocol_name: String,
    leader: String,
    assignments: BTreeMap<String, Bytes>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Inner<S: Debug> {
    session_timeout_ms: i32,
    rebalance_timeout_ms: Option<i32>,
    group_instance_id: Option<String>,
    members: BTreeMap<String, Member>,
    member_id: i32,
    generation_id: i32,
    state: S,
}

impl Default for Inner<Fresh> {
    fn default() -> Self {
        Self {
            session_timeout_ms: 0,
            rebalance_timeout_ms: None,
            group_instance_id: None,
            members: BTreeMap::new(),
            member_id: 1_000,
            generation_id: 0,
            state: Fresh,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Member {
    join_response: JoinGroupResponseMember,
    last_contact: Option<Instant>,
}

impl Group for Inner<Fresh> {
    type JoinState = Wrapper;
    type SyncState = Inner<Fresh>;
    type HeartbeatState = Inner<Fresh>;
    type LeaveState = Inner<Fresh>;
    type OffsetCommitState = Inner<Fresh>;
    type OffsetFetchState = Inner<Fresh>;

    #[instrument]
    fn join(
        self,
        now: Instant,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> (Self::JoinState, Body) {
        if let Some(client_id) = client_id {
            debug!(?client_id);

            if member_id.is_empty() {
                let member_id = format!("{client_id}-{}", Uuid::new_v4());

                let body = Body::JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::MemberIdRequired.into(),
                    generation_id: -1,
                    protocol_type: None,
                    protocol_name: None,
                    leader: String::from(""),
                    skip_assignment: Some(false),
                    member_id,
                    members: Some([].into()),
                };

                return (self.into(), body);
            }
        }

        let generation_id = 0;

        let Some(protocols) = protocols else {
            let body = Body::JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::InvalidRequest.into(),
                generation_id,
                protocol_type: Some(String::from(protocol_type)),
                protocol_name: None,
                leader: String::from(""),
                skip_assignment: None,
                member_id: String::from(""),
                members: Some([].into()),
            };

            return (self.into(), body);
        };

        let protocol = &protocols[0];
        debug!(?protocol);

        let join_response = JoinGroupResponseMember {
            member_id: member_id.to_string(),
            group_instance_id: group_instance_id
                .map(|group_instance_id| group_instance_id.to_owned()),
            metadata: protocol.metadata.clone(),
        };

        let state = {
            let mut members = BTreeMap::new();
            _ = members.insert(
                member_id.to_owned(),
                Member {
                    join_response: join_response.clone(),
                    last_contact: Some(now),
                },
            );

            debug!(?members);

            Inner {
                generation_id: 0,
                session_timeout_ms,
                rebalance_timeout_ms,
                group_instance_id: None,
                members,
                member_id: self.member_id,
                state: Forming {
                    protocol_name: String::from(protocol.name.as_str()),
                    protocol_type: String::from(protocol_type),
                    leader: Some(member_id.to_owned()),
                },
            }
        };

        let body = {
            let members = Some(vec![join_response]);

            Body::JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::None.into(),
                generation_id,
                protocol_name: Some(protocol.name.clone()),
                protocol_type: Some(String::from(protocol_type)),
                leader: member_id.to_owned(),
                skip_assignment: Some(false),
                member_id: member_id.to_owned(),
                members,
            }
        };

        (state.into(), body)
    }

    #[instrument]
    fn sync(
        self,
        now: Instant,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Self::SyncState, Body) {
        let body = Body::SyncGroupResponse {
            throttle_time_ms: Some(0),
            error_code: ErrorCode::UnknownMemberId.into(),
            protocol_name: protocol_name.map(|protocol_name| protocol_name.to_owned()),
            protocol_type: None,
            assignment: Bytes::new(),
        };

        (self, body)
    }

    #[instrument]
    fn heartbeat(
        self,
        now: Instant,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Self::HeartbeatState, Body) {
        let body = Body::HeartbeatResponse {
            throttle_time_ms: Some(0),
            error_code: ErrorCode::UnknownMemberId.into(),
        };

        (self, body)
    }

    #[instrument]
    fn leave(
        self,
        now: Instant,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Self::LeaveState, Body) {
        let body = Body::LeaveGroupResponse {
            throttle_time_ms: Some(0),
            error_code: ErrorCode::UnknownMemberId.into(),
            members: Some([].into()),
        };

        (self, body)
    }

    #[instrument]
    fn offset_commit(
        self,
        now: Instant,
        group_id: &str,
        generation_id_or_member_epoch: Option<i32>,
        member_id: Option<&str>,
        group_instance_id: Option<&str>,
        retention_time_ms: Option<i64>,
        topics: Option<&[OffsetCommitRequestTopic]>,
    ) -> (Self::OffsetCommitState, Body) {
        let body = Body::OffsetCommitResponse {
            throttle_time_ms: Some(0),
            topics: topics.map(|topics| {
                topics
                    .iter()
                    .map(|topic| OffsetCommitResponseTopic {
                        name: topic.name.clone(),
                        partitions: topic.partitions.as_ref().map(|partitions| {
                            partitions
                                .iter()
                                .map(|partition| OffsetCommitResponsePartition {
                                    partition_index: partition.partition_index,
                                    error_code: ErrorCode::UnknownMemberId.into(),
                                })
                                .collect()
                        }),
                    })
                    .collect()
            }),
        };

        (self, body)
    }

    #[instrument]
    fn offset_fetch(
        self,
        now: Instant,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Self::OffsetFetchState, Body) {
        let body = Body::OffsetFetchResponse {
            throttle_time_ms: Some(0),
            topics: topics.map(|topics| {
                topics
                    .iter()
                    .map(|topic| OffsetFetchResponseTopic {
                        name: topic.name.clone(),
                        partitions: topic.partition_indexes.as_ref().map(|partition_indexes| {
                            partition_indexes
                                .iter()
                                .map(|partition_index| OffsetFetchResponsePartition {
                                    partition_index: *partition_index,
                                    committed_offset: -1,
                                    committed_leader_epoch: None,
                                    metadata: None,
                                    error_code: ErrorCode::UnknownMemberId.into(),
                                })
                                .collect()
                        }),
                    })
                    .collect()
            }),
            error_code: Some(ErrorCode::UnknownMemberId.into()),
            groups: None,
        };

        (self, body)
    }
}

impl Group for Inner<Forming> {
    type JoinState = Inner<Forming>;
    type SyncState = Wrapper;
    type HeartbeatState = Inner<Forming>;
    type LeaveState = Inner<Forming>;
    type OffsetCommitState = Inner<Forming>;
    type OffsetFetchState = Inner<Forming>;

    #[instrument]
    fn join(
        mut self,
        now: Instant,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> (Self::JoinState, Body) {
        if let Some(client_id) = client_id {
            if member_id.is_empty() {
                let member_id = format!("{client_id}-{}", Uuid::new_v4());
                debug!(?member_id);

                let body = Body::JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::MemberIdRequired.into(),
                    generation_id: -1,
                    protocol_type: None,
                    protocol_name: None,
                    leader: String::from(""),
                    skip_assignment: Some(false),
                    member_id,
                    members: Some([].into()),
                };

                return (self, body);
            }
        }

        let Some(protocols) = protocols else {
            let body = Body::JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::InvalidRequest.into(),
                generation_id: self.generation_id,
                protocol_type: Some(String::from(protocol_type)),
                protocol_name: None,
                leader: String::from(""),
                skip_assignment: None,
                member_id: String::from(""),
                members: Some([].into()),
            };

            return (self, body);
        };

        let Some(protocol) = protocols
            .iter()
            .find(|protocol| protocol.name == self.state.protocol_name)
        else {
            let body = Body::JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::InconsistentGroupProtocol.into(),
                generation_id: self.generation_id,
                protocol_type: Some(String::from(protocol_type)),
                protocol_name: Some(self.state.protocol_name.clone()),
                leader: String::from(""),
                skip_assignment: None,
                member_id: String::from(""),
                members: Some([].into()),
            };

            return (self, body);
        };

        debug!(?protocol);
        debug!(?member_id, ?self.members);

        match self.members.insert(
            member_id.to_owned(),
            Member {
                join_response: JoinGroupResponseMember {
                    member_id: member_id.to_string(),
                    group_instance_id: group_instance_id.map(|s| s.to_owned()),
                    metadata: protocol.metadata.clone(),
                },
                last_contact: Some(now),
            },
        ) {
            Some(
                ref member @ Member {
                    join_response: JoinGroupResponseMember { ref metadata, .. },
                    ..
                },
            ) => {
                debug!(?member);

                if *metadata != protocol.metadata {
                    debug!(?metadata, ?protocol.metadata);
                    self.generation_id += 1;
                }
            }

            None => self.generation_id += 1,
        }

        debug!(?member_id, ?self.members);

        let body = {
            Body::JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::None.into(),
                generation_id: self.generation_id,
                protocol_type: Some(self.state.protocol_type.clone()),
                protocol_name: Some(self.state.protocol_name.clone()),
                leader: self
                    .state
                    .leader
                    .as_ref()
                    .map_or(String::from(""), |leader| leader.clone()),
                skip_assignment: Some(false),
                member_id: member_id.to_string(),
                members: Some(
                    if self
                        .state
                        .leader
                        .as_ref()
                        .is_some_and(|leader| leader == member_id)
                    {
                        self.members
                            .values()
                            .cloned()
                            .map(|member| member.join_response)
                            .collect()
                    } else {
                        [].into()
                    },
                ),
            }
        };

        (self, body)
    }

    #[instrument]
    fn sync(
        mut self,
        now: Instant,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Self::SyncState, Body) {
        if !self.members.contains_key(member_id) {
            let body = Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::UnknownMemberId.into(),
                protocol_type: Some(self.state.protocol_type.clone()),
                protocol_name: Some(self.state.protocol_name.clone()),
                assignment: Bytes::from_static(b""),
            };

            return (self.into(), body);
        }

        debug!(?member_id);

        if generation_id > self.generation_id {
            let body = Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::IllegalGeneration.into(),
                protocol_type: Some(self.state.protocol_type.clone()),
                protocol_name: Some(self.state.protocol_name.clone()),
                assignment: Bytes::from_static(b""),
            };

            return (self.into(), body);
        }

        if generation_id < self.generation_id {
            let body = Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::RebalanceInProgress.into(),
                protocol_type: Some(self.state.protocol_type.clone()),
                protocol_name: Some(self.state.protocol_name.clone()),
                assignment: Bytes::from_static(b""),
            };

            return (self.into(), body);
        }

        _ = self
            .members
            .entry(member_id.to_owned())
            .and_modify(|member| _ = member.last_contact.replace(now));

        if self
            .state
            .leader
            .as_ref()
            .is_some_and(|leader_id| member_id != leader_id.as_str())
        {
            let body = Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::RebalanceInProgress.into(),
                protocol_type: Some(self.state.protocol_type.clone()),
                protocol_name: Some(self.state.protocol_name.clone()),
                assignment: Bytes::from_static(b""),
            };

            return (self.into(), body);
        }

        let Some(assignments) = assignments else {
            let body = Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::RebalanceInProgress.into(),
                protocol_type: Some(self.state.protocol_type.clone()),
                protocol_name: Some(self.state.protocol_name.clone()),
                assignment: Bytes::from_static(b""),
            };

            return (self.into(), body);
        };

        debug!(?assignments);

        let assignments = assignments
            .iter()
            .fold(BTreeMap::new(), |mut acc, assignment| {
                _ = acc.insert(assignment.member_id.clone(), assignment.assignment.clone());
                acc
            });

        debug!(?assignments);

        let body = Body::SyncGroupResponse {
            throttle_time_ms: Some(0),
            error_code: ErrorCode::None.into(),
            protocol_type: Some(self.state.protocol_type.clone()),
            protocol_name: Some(self.state.protocol_name.clone()),
            assignment: assignments
                .get(member_id)
                .cloned()
                .unwrap_or(Bytes::from_static(b"")),
        };

        debug!(?body);

        let state = Inner {
            session_timeout_ms: self.session_timeout_ms,
            rebalance_timeout_ms: self.rebalance_timeout_ms,
            group_instance_id: self.group_instance_id,
            members: self.members,
            generation_id: self.generation_id,
            member_id: self.member_id,
            state: Formed {
                protocol_name: self.state.protocol_name,
                protocol_type: self.state.protocol_type,
                leader: member_id.to_owned(),
                assignments,
            },
        };

        debug!(?state);

        (state.into(), body)
    }

    #[instrument]
    fn heartbeat(
        mut self,
        now: Instant,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Self::HeartbeatState, Body) {
        if !self.members.contains_key(member_id) {
            return (
                self,
                Body::HeartbeatResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::UnknownMemberId.into(),
                },
            );
        }

        if generation_id > self.generation_id {
            return (
                self,
                Body::HeartbeatResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::IllegalGeneration.into(),
                },
            );
        }

        if generation_id < self.generation_id {
            return (
                self,
                Body::HeartbeatResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::RebalanceInProgress.into(),
                },
            );
        }

        _ = self
            .members
            .entry(member_id.to_owned())
            .and_modify(|member| _ = member.last_contact.replace(now));

        let body = Body::HeartbeatResponse {
            throttle_time_ms: Some(0),
            error_code: ErrorCode::None.into(),
        };

        (self, body)
    }

    #[instrument]
    fn leave(
        mut self,
        now: Instant,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Self::LeaveState, Body) {
        let _ = group_id;
        let _ = now;

        let members = if let Some(member_id) = member_id {
            vec![MemberResponse {
                member_id: member_id.to_owned(),
                group_instance_id: None,
                error_code: {
                    if self.members.remove(member_id).is_some() {
                        ErrorCode::None.into()
                    } else {
                        ErrorCode::UnknownMemberId.into()
                    }
                },
            }]
        } else {
            members.map_or(vec![], |members| {
                members
                    .iter()
                    .map(|member| MemberResponse {
                        member_id: member.member_id.clone(),
                        group_instance_id: member.group_instance_id.clone(),
                        error_code: {
                            if self.members.remove(&member.member_id).is_some() {
                                ErrorCode::None.into()
                            } else {
                                ErrorCode::UnknownMemberId.into()
                            }
                        },
                    })
                    .collect::<Vec<MemberResponse>>()
            })
        };

        if members.iter().any(|member| {
            let error_code = i16::from(ErrorCode::None);

            member.error_code == error_code
        }) {
            self.generation_id += 1;
        }

        let body = {
            Body::LeaveGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::None.into(),
                members: Some(members),
            }
        };

        (self, body)
    }

    #[instrument]
    fn offset_commit(
        self,
        now: Instant,
        group_id: &str,
        generation_id_or_member_epoch: Option<i32>,
        member_id: Option<&str>,
        group_instance_id: Option<&str>,
        retention_time_ms: Option<i64>,
        topics: Option<&[OffsetCommitRequestTopic]>,
    ) -> (Self::OffsetCommitState, Body) {
        let error_code = member_id.map_or(ErrorCode::UnknownMemberId, |member_id| {
            if self.members.contains_key(member_id) {
                ErrorCode::None
            } else {
                ErrorCode::UnknownMemberId
            }
        });

        let body = Body::OffsetCommitResponse {
            throttle_time_ms: Some(0),
            topics: topics.map(|topics| {
                topics
                    .as_ref()
                    .iter()
                    .map(|topic| OffsetCommitResponseTopic {
                        name: topic.name.clone(),
                        partitions: topic.partitions.as_ref().map(|partitions| {
                            partitions
                                .iter()
                                .map(|partition| OffsetCommitResponsePartition {
                                    partition_index: partition.partition_index,
                                    error_code: error_code.into(),
                                })
                                .collect()
                        }),
                    })
                    .collect()
            }),
        };
        (self, body)
    }

    #[instrument]
    fn offset_fetch(
        self,
        now: Instant,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Self::OffsetFetchState, Body) {
        todo!()
    }
}

impl Group for Inner<Syncing> {
    type JoinState = Wrapper;
    type SyncState = Inner<Syncing>;
    type HeartbeatState = Inner<Formed>;
    type LeaveState = Wrapper;
    type OffsetCommitState = Inner<Syncing>;
    type OffsetFetchState = Inner<Syncing>;

    #[instrument]
    fn join(
        mut self,
        now: Instant,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> (Self::JoinState, Body) {
        if let Some(client_id) = client_id {
            if member_id.is_empty() {
                let member_id = format!("{client_id}-{}", Uuid::new_v4());

                let body = Body::JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::MemberIdRequired.into(),
                    generation_id: -1,
                    protocol_type: None,
                    protocol_name: None,
                    leader: String::from(""),
                    skip_assignment: Some(false),
                    member_id,
                    members: Some([].into()),
                };

                return (self.into(), body);
            }
        }

        let Some(protocols) = protocols else {
            let body = Body::JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::InvalidRequest.into(),
                generation_id: self.generation_id,
                protocol_type: Some(String::from(protocol_type)),
                protocol_name: None,
                leader: String::from(""),
                skip_assignment: None,
                member_id: String::from(""),
                members: Some([].into()),
            };

            return (self.into(), body);
        };

        let Some(protocol) = protocols
            .iter()
            .find(|protocol| protocol.name == self.state.protocol_name)
        else {
            let body = Body::JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::InconsistentGroupProtocol.into(),
                generation_id: self.generation_id,
                protocol_type: Some(String::from(protocol_type)),
                protocol_name: Some(self.state.protocol_name.clone()),
                leader: String::from(""),
                skip_assignment: None,
                member_id: String::from(""),
                members: Some([].into()),
            };

            return (self.into(), body);
        };

        let protocol_type = self.state.protocol_type.clone();
        let protocol_name = self.state.protocol_name.clone();

        let state: Wrapper = match self.members.insert(
            member_id.to_owned(),
            Member {
                join_response: JoinGroupResponseMember {
                    member_id: member_id.to_string(),
                    group_instance_id: group_instance_id.map(|s| s.to_owned()),
                    metadata: protocol.metadata.clone(),
                },
                last_contact: Some(now),
            },
        ) {
            Some(Member {
                join_response: JoinGroupResponseMember { metadata, .. },
                ..
            }) => {
                if metadata == protocol.metadata {
                    self.into()
                } else {
                    Inner {
                        generation_id: self.generation_id + 1,
                        session_timeout_ms: self.session_timeout_ms,
                        rebalance_timeout_ms: self.rebalance_timeout_ms,
                        group_instance_id: self.group_instance_id,
                        members: self.members,
                        member_id: self.member_id,
                        state: Forming {
                            protocol_type: self.state.protocol_type,
                            protocol_name: self.state.protocol_name,
                            leader: Some(self.state.leader),
                        },
                    }
                    .into()
                }
            }

            None => Inner {
                generation_id: self.generation_id + 1,
                session_timeout_ms: self.session_timeout_ms,
                rebalance_timeout_ms: self.rebalance_timeout_ms,
                group_instance_id: self.group_instance_id,
                members: self.members,
                member_id: self.member_id,
                state: Forming {
                    protocol_type: self.state.protocol_type,
                    protocol_name: self.state.protocol_name,
                    leader: Some(self.state.leader),
                },
            }
            .into(),
        };

        let body = {
            let members = state.members();

            Body::JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::None.into(),
                generation_id: state.generation_id(),
                protocol_type: Some(protocol_type),
                protocol_name: Some(protocol_name),
                leader: String::from(""),
                skip_assignment: None,
                member_id: member_id.to_string(),
                members: Some(members),
            }
        };

        (state, body)
    }

    #[instrument]
    fn sync(
        self,
        now: Instant,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Self::SyncState, Body) {
        todo!()
    }

    #[instrument]
    fn heartbeat(
        self,
        now: Instant,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Self::HeartbeatState, Body) {
        todo!()
    }

    #[instrument]
    fn leave(
        mut self,
        now: Instant,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Self::LeaveState, Body) {
        let members = if let Some(member_id) = member_id {
            vec![MemberResponse {
                member_id: member_id.to_owned(),
                group_instance_id: None,
                error_code: {
                    if self.members.remove(member_id).is_some() {
                        ErrorCode::None.into()
                    } else {
                        ErrorCode::UnknownMemberId.into()
                    }
                },
            }]
        } else {
            members.map_or(vec![], |members| {
                members
                    .iter()
                    .map(|member| MemberResponse {
                        member_id: member.member_id.clone(),
                        group_instance_id: member.group_instance_id.clone(),
                        error_code: {
                            if self.members.remove(&member.member_id).is_some() {
                                ErrorCode::None.into()
                            } else {
                                ErrorCode::UnknownMemberId.into()
                            }
                        },
                    })
                    .collect::<Vec<MemberResponse>>()
            })
        };

        let state: Wrapper = if members.iter().any(|member| {
            let error_code = i16::from(ErrorCode::None);

            member.error_code == error_code
        }) {
            let leader = if self.members.contains_key(&self.state.leader) {
                Some(self.state.leader)
            } else {
                None
            };

            Inner {
                generation_id: self.generation_id + 1,
                session_timeout_ms: self.session_timeout_ms,
                rebalance_timeout_ms: self.rebalance_timeout_ms,
                group_instance_id: self.group_instance_id,
                members: self.members,
                member_id: self.member_id,
                state: Forming {
                    protocol_type: self.state.protocol_type,
                    protocol_name: self.state.protocol_name,
                    leader,
                },
            }
            .into()
        } else {
            self.into()
        };

        let body = {
            Body::LeaveGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::None.into(),
                members: Some(members),
            }
        };

        (state, body)
    }

    #[instrument]
    fn offset_commit(
        self,
        now: Instant,
        group_id: &str,
        generation_id_or_member_epoch: Option<i32>,
        member_id: Option<&str>,
        group_instance_id: Option<&str>,
        retention_time_ms: Option<i64>,
        topics: Option<&[OffsetCommitRequestTopic]>,
    ) -> (Self::OffsetCommitState, Body) {
        todo!()
    }

    #[instrument]
    fn offset_fetch(
        self,
        now: Instant,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Self::OffsetFetchState, Body) {
        todo!()
    }
}

impl Group for Inner<Formed> {
    type JoinState = Wrapper;
    type SyncState = Inner<Formed>;
    type HeartbeatState = Inner<Formed>;
    type LeaveState = Wrapper;
    type OffsetCommitState = Inner<Formed>;
    type OffsetFetchState = Inner<Formed>;

    #[instrument]
    fn join(
        mut self,
        now: Instant,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> (Self::JoinState, Body) {
        if let Some(client_id) = client_id {
            if member_id.is_empty() {
                let member_id = format!("{client_id}-{}", Uuid::new_v4());

                let body = Body::JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::MemberIdRequired.into(),
                    generation_id: -1,
                    protocol_type: None,
                    protocol_name: None,
                    leader: String::from(""),
                    skip_assignment: Some(false),
                    member_id,
                    members: Some([].into()),
                };

                return (self.into(), body);
            }
        }

        let Some(protocols) = protocols else {
            let body = Body::JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::InvalidRequest.into(),
                generation_id: self.generation_id,
                protocol_type: Some(String::from(protocol_type)),
                protocol_name: None,
                leader: String::from(""),
                skip_assignment: None,
                member_id: String::from(""),
                members: Some([].into()),
            };

            return (self.into(), body);
        };

        let Some(protocol) = protocols
            .iter()
            .find(|protocol| protocol.name == self.state.protocol_name)
        else {
            let body = Body::JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::InconsistentGroupProtocol.into(),
                generation_id: self.generation_id,
                protocol_type: Some(String::from(protocol_type)),
                protocol_name: Some(self.state.protocol_name.clone()),
                leader: String::from(""),
                skip_assignment: None,
                member_id: String::from(""),
                members: Some([].into()),
            };

            return (self.into(), body);
        };

        match self.members.insert(
            member_id.to_owned(),
            Member {
                join_response: JoinGroupResponseMember {
                    member_id: member_id.to_string(),
                    group_instance_id: group_instance_id.map(|s| s.to_owned()),
                    metadata: protocol.metadata.clone(),
                },
                last_contact: Some(now),
            },
        ) {
            Some(Member {
                join_response: JoinGroupResponseMember { metadata, .. },
                ..
            }) => {
                if metadata == protocol.metadata {
                    let state: Wrapper = self.into();

                    let body = {
                        let members = state.members();
                        let protocol_type = state.protocol_type().map(|s| s.to_owned());
                        let protocol_name = state.protocol_name().map(|s| s.to_owned());

                        Body::JoinGroupResponse {
                            throttle_time_ms: Some(0),
                            error_code: ErrorCode::None.into(),
                            generation_id: state.generation_id(),
                            protocol_type,
                            protocol_name,
                            leader: state
                                .leader()
                                .map(|s| s.to_owned())
                                .unwrap_or(String::from("")),
                            skip_assignment: Some(false),
                            member_id: member_id.to_string(),
                            members: Some(members),
                        }
                    };

                    (state, body)
                } else {
                    let state: Wrapper = Inner {
                        generation_id: self.generation_id + 1,
                        session_timeout_ms: self.session_timeout_ms,
                        rebalance_timeout_ms: self.rebalance_timeout_ms,
                        group_instance_id: self.group_instance_id,
                        members: self.members,
                        member_id: self.member_id,
                        state: Forming {
                            protocol_type: self.state.protocol_type,
                            protocol_name: self.state.protocol_name,
                            leader: Some(self.state.leader),
                        },
                    }
                    .into();

                    let body = {
                        let members = state.members();
                        let protocol_type = state.protocol_type().map(|s| s.to_owned());
                        let protocol_name = state.protocol_name().map(|s| s.to_owned());

                        Body::JoinGroupResponse {
                            throttle_time_ms: Some(0),
                            error_code: ErrorCode::None.into(),
                            generation_id: state.generation_id(),
                            protocol_type,
                            protocol_name,
                            leader: state
                                .leader()
                                .map(|s| s.to_owned())
                                .unwrap_or(String::from("")),
                            skip_assignment: Some(false),
                            member_id: member_id.to_string(),
                            members: Some(members),
                        }
                    };

                    (state, body)
                }
            }

            None => {
                let state: Wrapper = Inner {
                    generation_id: self.generation_id + 1,
                    session_timeout_ms: self.session_timeout_ms,
                    rebalance_timeout_ms: self.rebalance_timeout_ms,
                    group_instance_id: self.group_instance_id,
                    members: self.members,
                    member_id: self.member_id,
                    state: Forming {
                        protocol_type: self.state.protocol_type,
                        protocol_name: self.state.protocol_name,
                        leader: Some(self.state.leader),
                    },
                }
                .into();

                let body = {
                    let protocol_type = state.protocol_type().map(|s| s.to_owned());
                    let protocol_name = state.protocol_name().map(|s| s.to_owned());

                    Body::JoinGroupResponse {
                        throttle_time_ms: Some(0),
                        error_code: ErrorCode::None.into(),
                        generation_id: state.generation_id(),
                        protocol_type,
                        protocol_name,
                        leader: state
                            .leader()
                            .map(|s| s.to_owned())
                            .unwrap_or(String::from("")),
                        skip_assignment: Some(false),
                        member_id: member_id.to_string(),
                        members: Some([].into()),
                    }
                };

                (state, body)
            }
        }
    }

    #[instrument]
    fn sync(
        mut self,
        now: Instant,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Self::SyncState, Body) {
        if !self.members.contains_key(member_id) {
            let body = Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::UnknownMemberId.into(),
                protocol_type: Some(self.state.protocol_type.clone()),
                protocol_name: Some(self.state.protocol_name.clone()),
                assignment: Bytes::from_static(b""),
            };

            return (self, body);
        }

        debug!(?member_id);

        if generation_id > self.generation_id {
            let body = Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::IllegalGeneration.into(),
                protocol_type: Some(self.state.protocol_type.clone()),
                protocol_name: Some(self.state.protocol_name.clone()),
                assignment: Bytes::from_static(b""),
            };

            return (self, body);
        }

        if generation_id < self.generation_id {
            let body = Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::RebalanceInProgress.into(),
                protocol_type: Some(self.state.protocol_type.clone()),
                protocol_name: Some(self.state.protocol_name.clone()),
                assignment: Bytes::from_static(b""),
            };

            return (self, body);
        }

        _ = self
            .members
            .entry(member_id.to_owned())
            .and_modify(|member| _ = member.last_contact.replace(now));

        let body = Body::SyncGroupResponse {
            throttle_time_ms: Some(0),
            error_code: ErrorCode::None.into(),
            protocol_type: Some(self.state.protocol_type.clone()),
            protocol_name: Some(self.state.protocol_name.clone()),
            assignment: self
                .state
                .assignments
                .get(member_id)
                .cloned()
                .unwrap_or(Bytes::from_static(b"")),
        };

        debug!(?body);

        (self, body)
    }

    #[instrument]
    fn heartbeat(
        mut self,
        now: Instant,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Self::HeartbeatState, Body) {
        if !self.members.contains_key(member_id) {
            return (
                self,
                Body::HeartbeatResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::UnknownMemberId.into(),
                },
            );
        }

        if generation_id > self.generation_id {
            return (
                self,
                Body::HeartbeatResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::IllegalGeneration.into(),
                },
            );
        }

        if generation_id < self.generation_id {
            return (
                self,
                Body::HeartbeatResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::RebalanceInProgress.into(),
                },
            );
        }

        _ = self
            .members
            .entry(member_id.to_owned())
            .and_modify(|member| _ = member.last_contact.replace(now));

        let body = Body::HeartbeatResponse {
            throttle_time_ms: Some(0),
            error_code: ErrorCode::None.into(),
        };

        (self, body)
    }

    #[instrument]
    fn leave(
        mut self,
        now: Instant,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Self::LeaveState, Body) {
        let members = if let Some(member_id) = member_id {
            vec![MemberResponse {
                member_id: member_id.to_owned(),
                group_instance_id: None,
                error_code: {
                    if self.members.remove(member_id).is_some() {
                        ErrorCode::None.into()
                    } else {
                        ErrorCode::UnknownMemberId.into()
                    }
                },
            }]
        } else {
            members.map_or(vec![], |members| {
                members
                    .iter()
                    .map(|member| MemberResponse {
                        member_id: member.member_id.clone(),
                        group_instance_id: member.group_instance_id.clone(),
                        error_code: {
                            if self.members.remove(&member.member_id).is_some() {
                                ErrorCode::None.into()
                            } else {
                                ErrorCode::UnknownMemberId.into()
                            }
                        },
                    })
                    .collect::<Vec<MemberResponse>>()
            })
        };

        let state: Wrapper = if members.iter().any(|member| {
            let error_code = i16::from(ErrorCode::None);

            member.error_code == error_code
        }) {
            let leader = if self.members.contains_key(&self.state.leader) {
                Some(self.state.leader)
            } else {
                None
            };

            Inner {
                generation_id: self.generation_id + 1,
                session_timeout_ms: self.session_timeout_ms,
                rebalance_timeout_ms: self.rebalance_timeout_ms,
                group_instance_id: self.group_instance_id,
                members: self.members,
                member_id: self.member_id,
                state: Forming {
                    protocol_type: self.state.protocol_type,
                    protocol_name: self.state.protocol_name,
                    leader,
                },
            }
            .into()
        } else {
            self.into()
        };

        let body = {
            Body::LeaveGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::None.into(),
                members: Some(members),
            }
        };

        (state, body)
    }

    #[instrument]
    fn offset_commit(
        self,
        now: Instant,
        group_id: &str,
        generation_id_or_member_epoch: Option<i32>,
        member_id: Option<&str>,
        group_instance_id: Option<&str>,
        retention_time_ms: Option<i64>,
        topics: Option<&[OffsetCommitRequestTopic]>,
    ) -> (Self::OffsetCommitState, Body) {
        let body = Body::OffsetCommitResponse {
            throttle_time_ms: Some(0),
            topics: topics.map(|topics| {
                topics
                    .as_ref()
                    .iter()
                    .map(|topic| OffsetCommitResponseTopic {
                        name: topic.name.clone(),
                        partitions: topic.partitions.as_ref().map(|partitions| {
                            partitions
                                .iter()
                                .map(|partition| OffsetCommitResponsePartition {
                                    partition_index: partition.partition_index,
                                    error_code: ErrorCode::None.into(),
                                })
                                .collect()
                        }),
                    })
                    .collect()
            }),
        };

        (self, body)
    }

    #[instrument]
    fn offset_fetch(
        self,
        now: Instant,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Self::OffsetFetchState, Body) {
        let groups = groups.map(|groups| {
            groups
                .as_ref()
                .iter()
                .map(|group| OffsetFetchResponseGroup {
                    group_id: group.group_id.clone(),
                    topics: group.topics.as_ref().map(|topics| {
                        topics
                            .iter()
                            .map(|topic| OffsetFetchResponseTopics {
                                name: topic.name.clone(),
                                partitions: topic.partition_indexes.as_ref().map(
                                    |partition_indexes| {
                                        partition_indexes
                                            .iter()
                                            .map(|partition_index| OffsetFetchResponsePartitions {
                                                partition_index: *partition_index,
                                                committed_offset: 0,
                                                committed_leader_epoch: 0,
                                                metadata: None,
                                                error_code: 0,
                                            })
                                            .collect()
                                    },
                                ),
                            })
                            .collect()
                    }),
                    error_code: ErrorCode::None.into(),
                })
                .collect()
        });

        let body = Body::OffsetFetchResponse {
            throttle_time_ms: Some(0),
            topics: None,
            error_code: None,
            groups,
        };

        (self, body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use tansu_kafka_sans_io::{
        offset_commit_request::OffsetCommitRequestPartition,
        offset_fetch_request::OffsetFetchRequestTopics,
        offset_fetch_response::{
            OffsetFetchResponseGroup, OffsetFetchResponsePartitions, OffsetFetchResponseTopics,
        },
    };
    use tracing::subscriber::DefaultGuard;

    #[cfg(miri)]
    fn init_tracing() -> Result<()> {
        Ok(())
    }

    #[cfg(not(miri))]
    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        use tracing_subscriber::fmt::format::FmtSpan;

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_max_level(tracing::Level::DEBUG)
                .with_span_events(FmtSpan::ACTIVE)
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME")))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    #[test]
    fn join_requires_member_id() -> Result<()> {
        let _guard = init_tracing()?;

        let s: Wrapper = Inner::<Fresh>::default().into();

        let client_id = "console-consumer";
        let group_id = "test-consumer-group";
        let _topic = "test";
        let session_timeout_ms = 45_000;
        let rebalance_timeout_ms = Some(300_000);
        let member_id = "";
        let group_instance_id = None;
        let reason = None;

        let range_meta = Bytes::from_static(
            b"\0\x03\0\0\0\x01\0\x04test\xff\xff\xff\xff\0\0\0\0\xff\xff\xff\xff\xff\xff",
        );

        let sticky_meta = Bytes::from_static(
            b"\0\x03\0\0\0\x01\0\x04test\0\0\0\x04\xff\xff\xff\xff\0\0\0\0\xff\xff\xff\xff\xff\xff",
        );

        let protocol_type = "consumer";
        let protocols = [
            JoinGroupRequestProtocol {
                name: "range".into(),
                metadata: range_meta.clone(),
            },
            JoinGroupRequestProtocol {
                name: "cooperative-sticky".into(),
                metadata: sticky_meta,
            },
        ];

        let now = Instant::now();

        let (s, join_response) = s.join(
            now,
            Some(client_id),
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            Some(&protocols[..]),
            reason,
        );

        match join_response {
            Body::JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code,
                generation_id: -1,
                protocol_type: None,
                protocol_name: None,
                leader,
                skip_assignment: Some(false),
                members,
                member_id,
            } => {
                assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
                assert!(leader.is_empty());
                assert!(member_id.starts_with(client_id));
                assert_eq!(Some(0), members.map(|members| members.len()));

                _ = Uuid::parse_str(&member_id[client_id.len() + 1..])?;

                let (_, join_response) = s.join(
                    now,
                    Some(client_id),
                    group_id,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    member_id.as_str(),
                    group_instance_id,
                    protocol_type,
                    Some(&protocols[..]),
                    reason,
                );

                let join_response_expected = Body::JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::None.into(),
                    generation_id: 0,
                    protocol_type: Some(protocol_type.to_owned()),
                    protocol_name: Some(String::from("range")),
                    leader: member_id.clone(),
                    skip_assignment: Some(false),
                    member_id: member_id.clone(),
                    members: Some(
                        [JoinGroupResponseMember {
                            member_id: member_id.clone(),
                            group_instance_id: None,
                            metadata: range_meta,
                        }]
                        .into(),
                    ),
                };

                assert_eq!(join_response_expected, join_response);
            }

            otherwise => panic!("{otherwise:?}"),
        };

        Ok(())
    }

    #[test]
    fn fresh_join() -> Result<()> {
        let _guard = init_tracing()?;

        let s: Wrapper = Inner::<Fresh>::default().into();

        let client_id = "console-consumer";
        let group_id = "test-consumer-group";
        let topic = "test";
        let session_timeout_ms = 45_000;
        let rebalance_timeout_ms = Some(300_000);
        let member_id = "";
        let group_instance_id = None;
        let reason = None;

        let range_meta = Bytes::from_static(
            b"\0\x03\0\0\0\x01\0\x04test\xff\xff\xff\xff\0\0\0\0\xff\xff\xff\xff\xff\xff",
        );

        let sticky_meta = Bytes::from_static(
            b"\0\x03\0\0\0\x01\0\x04test\0\0\0\x04\xff\xff\xff\xff\0\0\0\0\xff\xff\xff\xff\xff\xff",
        );

        let protocol_type = "consumer";
        let protocols = [
            JoinGroupRequestProtocol {
                name: "range".into(),
                metadata: range_meta.clone(),
            },
            JoinGroupRequestProtocol {
                name: "cooperative-sticky".into(),
                metadata: sticky_meta,
            },
        ];

        let now = Instant::now();

        let (s, member_id) = match s.join(
            now,
            Some(client_id),
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            Some(&protocols[..]),
            reason,
        ) {
            (
                s,
                Body::JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code,
                    generation_id: -1,
                    protocol_type: None,
                    protocol_name: None,
                    leader,
                    skip_assignment: Some(false),
                    members,
                    member_id,
                },
            ) => {
                assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
                assert!(leader.is_empty());
                assert!(member_id.starts_with(client_id));
                assert_eq!(Some(0), members.map(|members| members.len()));

                let (s, join_response) = s.join(
                    now,
                    Some(client_id),
                    group_id,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    &member_id,
                    group_instance_id,
                    protocol_type,
                    Some(&protocols[..]),
                    reason,
                );

                assert_eq!(session_timeout_ms, s.session_timeout_ms());
                assert_eq!(rebalance_timeout_ms, s.rebalance_timeout_ms());
                assert_eq!(Some("consumer"), s.protocol_type());
                assert_eq!(Some("range"), s.protocol_name());

                let join_response_expected = Body::JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::None.into(),
                    generation_id: 0,
                    protocol_type: Some(protocol_type.to_owned()),
                    protocol_name: Some(String::from("range")),
                    leader: member_id.clone(),
                    skip_assignment: Some(false),
                    member_id: member_id.clone(),
                    members: Some(
                        [JoinGroupResponseMember {
                            member_id: member_id.clone(),
                            group_instance_id: None,
                            metadata: range_meta.clone(),
                        }]
                        .into(),
                    ),
                };

                assert_eq!(join_response_expected, join_response);

                (s, member_id)
            }

            otherwise => panic!("{otherwise:?}"),
        };

        let generation_id = s.generation_id();
        let protocol_type = s.protocol_type().map(|s| s.to_owned());
        let protocol_name = s.protocol_name().map(|s| s.to_owned());

        let assignment = Bytes::from_static(
            b"\0\x03\0\0\0\x01\0\x04test\0\0\0\x03\0\0\0\0\0\0\0\x01\0\0\0\x02\xff\xff\xff\xff",
        );

        let assignments = [SyncGroupRequestAssignment {
            member_id: member_id.clone(),
            assignment: assignment.clone(),
        }];

        let (s, sync_response) = s.sync(
            now,
            group_id,
            generation_id,
            &member_id,
            group_instance_id,
            protocol_type.as_deref(),
            protocol_name.as_deref(),
            Some(&assignments),
        );

        let sync_response_expected = Body::SyncGroupResponse {
            throttle_time_ms: Some(0),
            error_code: 0,
            protocol_type: protocol_type.clone(),
            protocol_name: protocol_name.clone(),
            assignment,
        };

        assert_eq!(sync_response_expected, sync_response);

        let groups = [OffsetFetchRequestGroup {
            group_id: group_id.into(),
            member_id: None,
            member_epoch: Some(-1),
            topics: Some(
                [OffsetFetchRequestTopics {
                    name: topic.into(),
                    partition_indexes: Some([0].into()),
                }]
                .into(),
            ),
        }];

        let (s, offset_fetch_response) = s.offset_fetch(now, None, None, Some(&groups), Some(true));

        let offset_fetch_response_expected = Body::OffsetFetchResponse {
            throttle_time_ms: Some(0),
            topics: None,
            error_code: None,
            groups: Some(
                [OffsetFetchResponseGroup {
                    group_id: group_id.into(),
                    topics: Some(
                        [OffsetFetchResponseTopics {
                            name: topic.into(),
                            partitions: Some(
                                [OffsetFetchResponsePartitions {
                                    partition_index: 0,
                                    committed_offset: 0,
                                    committed_leader_epoch: 0,
                                    metadata: None,
                                    error_code: 0,
                                }]
                                .into(),
                            ),
                        }]
                        .into(),
                    ),
                    error_code: 0,
                }]
                .into(),
            ),
        };

        assert_eq!(offset_fetch_response_expected, offset_fetch_response);

        let (s, heartbeat_response) =
            s.heartbeat(now, group_id, generation_id, &member_id, group_instance_id);

        let heartbeat_response_expected = Body::HeartbeatResponse {
            throttle_time_ms: Some(0),
            error_code: ErrorCode::None.into(),
        };

        assert_eq!(heartbeat_response_expected, heartbeat_response);

        let generation_id_or_member_epoch = Some(generation_id);
        let retention_time_ms = None;

        let topics = [OffsetCommitRequestTopic {
            name: topic.into(),
            partitions: Some(
                [OffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: 1,
                    committed_leader_epoch: Some(-1),
                    commit_timestamp: None,
                    committed_metadata: Some("".into()),
                }]
                .into(),
            ),
        }];

        let (_s, offset_commit_response) = s.offset_commit(
            now,
            group_id,
            generation_id_or_member_epoch,
            Some(&member_id),
            group_instance_id,
            retention_time_ms,
            Some(&topics[..]),
        );

        let offset_commit_response_expected = Body::OffsetCommitResponse {
            throttle_time_ms: Some(0),
            topics: Some(
                [OffsetCommitResponseTopic {
                    name: topic.into(),
                    partitions: Some(
                        [OffsetCommitResponsePartition {
                            partition_index: 0,
                            error_code: 0,
                        }]
                        .into(),
                    ),
                }]
                .into(),
            ),
        };

        assert_eq!(offset_commit_response_expected, offset_commit_response);

        Ok(())
    }

    #[test]
    fn formed_join() -> Result<()> {
        let _guard = init_tracing()?;

        let s: Wrapper = Inner::<Fresh>::default().into();

        let session_timeout_ms = 45_000;
        let rebalance_timeout_ms = Some(300_000);
        let group_instance_id = None;
        let reason = None;

        const CLIENT_ID: &str = "console-consumer";
        const GROUP_ID: &str = "test-consumer-group";
        const TOPIC: &str = "test";
        const RANGE: &str = "range";
        const COOPERATIVE_STICKY: &str = "cooperative-sticky";

        const PROTOCOL_TYPE: &str = "consumer";

        let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
        let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

        let protocols = [
            JoinGroupRequestProtocol {
                name: RANGE.into(),
                metadata: first_member_range_meta.clone(),
            },
            JoinGroupRequestProtocol {
                name: COOPERATIVE_STICKY.into(),
                metadata: first_member_sticky_meta,
            },
        ];

        let now = Instant::now();

        let (s, first_member_id) = match s.join(
            now,
            Some(CLIENT_ID),
            GROUP_ID,
            session_timeout_ms,
            rebalance_timeout_ms,
            "",
            group_instance_id,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            reason,
        ) {
            (
                s,
                Body::JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code,
                    generation_id: -1,
                    protocol_type: None,
                    protocol_name: None,
                    leader,
                    skip_assignment: Some(false),
                    members,
                    member_id,
                },
            ) => {
                assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
                assert!(leader.is_empty());
                assert!(member_id.starts_with(CLIENT_ID));
                assert_eq!(Some(0), members.map(|members| members.len()));

                let (s, join_response) = s.join(
                    now,
                    Some(CLIENT_ID),
                    GROUP_ID,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    &member_id,
                    group_instance_id,
                    PROTOCOL_TYPE,
                    Some(&protocols[..]),
                    reason,
                );

                assert_eq!(session_timeout_ms, s.session_timeout_ms());
                assert_eq!(rebalance_timeout_ms, s.rebalance_timeout_ms());
                assert_eq!(Some(PROTOCOL_TYPE), s.protocol_type());
                assert_eq!(Some(RANGE), s.protocol_name());

                let join_response_expected = Body::JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::None.into(),
                    generation_id: 0,
                    protocol_type: Some(PROTOCOL_TYPE.into()),
                    protocol_name: Some(RANGE.into()),
                    leader: member_id.clone(),
                    skip_assignment: Some(false),
                    member_id: member_id.clone(),
                    members: Some(
                        [JoinGroupResponseMember {
                            member_id: member_id.clone(),
                            group_instance_id: None,
                            metadata: first_member_range_meta.clone(),
                        }]
                        .into(),
                    ),
                };

                assert_eq!(join_response_expected, join_response);

                (s, member_id)
            }

            otherwise => panic!("{otherwise:?}"),
        };

        assert_eq!(1, s.members().len());

        let first_member_assignment_01 = Bytes::from_static(b"assignment_01");

        let assignments = [SyncGroupRequestAssignment {
            member_id: first_member_id.clone(),
            assignment: first_member_assignment_01.clone(),
        }];

        let s = {
            let generation_id = s.generation_id();

            let (s, sync_response) = s.sync(
                now,
                GROUP_ID,
                generation_id,
                &first_member_id,
                group_instance_id,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(&assignments),
            );

            let sync_response_expected = Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code: 0,
                protocol_type: Some(PROTOCOL_TYPE.into()),
                protocol_name: Some(RANGE.into()),
                assignment: first_member_assignment_01,
            };

            assert_eq!(sync_response_expected, sync_response);

            s
        };

        let s = {
            let generation_id = s.generation_id();

            let (s, heartbeat_response) = s.heartbeat(
                now,
                GROUP_ID,
                generation_id,
                &first_member_id,
                group_instance_id,
            );

            let heartbeat_response_expected = Body::HeartbeatResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::None.into(),
            };

            assert_eq!(heartbeat_response_expected, heartbeat_response);

            s
        };

        let second_member_range_meta = Bytes::from_static(b"second_member_range_meta_01");
        let second_member_sticky_meta = Bytes::from_static(b"second_member_sticky_meta_01");

        let protocols = [
            JoinGroupRequestProtocol {
                name: RANGE.into(),
                metadata: second_member_range_meta.clone(),
            },
            JoinGroupRequestProtocol {
                name: COOPERATIVE_STICKY.into(),
                metadata: second_member_sticky_meta.clone(),
            },
        ];

        let (s, second_member_id, previous_generation) = match s.join(
            now,
            Some(CLIENT_ID),
            GROUP_ID,
            session_timeout_ms,
            rebalance_timeout_ms,
            "",
            group_instance_id,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            reason,
        ) {
            (
                s,
                Body::JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code,
                    generation_id: -1,
                    protocol_type: None,
                    protocol_name: None,
                    leader,
                    skip_assignment: Some(false),
                    members,
                    member_id,
                },
            ) => {
                assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
                assert!(leader.is_empty());
                assert!(member_id.starts_with(CLIENT_ID));
                assert_eq!(Some(0), members.map(|members| members.len()));

                let previous_generation = s.generation_id();

                let (s, join_response) = s.join(
                    now,
                    Some(CLIENT_ID),
                    GROUP_ID,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    &member_id,
                    group_instance_id,
                    PROTOCOL_TYPE,
                    Some(&protocols[..]),
                    reason,
                );

                assert_eq!(s.generation_id(), previous_generation + 1);

                assert_eq!(session_timeout_ms, s.session_timeout_ms());
                assert_eq!(rebalance_timeout_ms, s.rebalance_timeout_ms());
                assert_eq!(Some(PROTOCOL_TYPE), s.protocol_type());
                assert_eq!(Some(RANGE), s.protocol_name());

                let join_response_expected = Body::JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::None.into(),
                    generation_id: s.generation_id(),
                    protocol_type: Some(PROTOCOL_TYPE.into()),
                    protocol_name: Some(RANGE.into()),
                    leader: first_member_id.clone(),
                    skip_assignment: Some(false),
                    member_id: member_id.clone(),
                    members: Some([].into()),
                };

                assert_eq!(join_response_expected, join_response);

                (s, member_id, previous_generation)
            }

            otherwise => panic!("{otherwise:?}"),
        };

        assert_eq!(2, s.members().len());

        let s = match s.heartbeat(
            now,
            GROUP_ID,
            previous_generation,
            &first_member_id,
            group_instance_id,
        ) {
            (
                s,
                Body::HeartbeatResponse {
                    throttle_time_ms: Some(0),
                    error_code,
                },
            ) if error_code == i16::from(ErrorCode::RebalanceInProgress) => s,

            otherwise => panic!("{otherwise:?}"),
        };

        let s = match s.offset_commit(
            now,
            GROUP_ID,
            Some(previous_generation),
            Some(&first_member_id),
            group_instance_id,
            None,
            Some(&[OffsetCommitRequestTopic {
                name: TOPIC.into(),
                partitions: Some(
                    (0..=2)
                        .map(|partition_index| OffsetCommitRequestPartition {
                            partition_index,
                            committed_offset: 1,
                            committed_leader_epoch: Some(0),
                            commit_timestamp: None,
                            committed_metadata: Some("".into()),
                        })
                        .collect(),
                ),
            }]),
        ) {
            (
                s,
                Body::OffsetCommitResponse {
                    throttle_time_ms: Some(0),
                    topics: Some(topics),
                },
            ) if topics
                == [OffsetCommitResponseTopic {
                    name: TOPIC.into(),
                    partitions: Some(
                        (0..=2)
                            .map(|partition_index| OffsetCommitResponsePartition {
                                partition_index,
                                error_code: ErrorCode::None.into(),
                            })
                            .collect(),
                    ),
                }] =>
            {
                s
            }

            otherwise => panic!("{otherwise:?}"),
        };

        let s = {
            let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_02");
            let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_02");

            let protocols = [
                JoinGroupRequestProtocol {
                    name: RANGE.into(),
                    metadata: first_member_range_meta.clone(),
                },
                JoinGroupRequestProtocol {
                    name: COOPERATIVE_STICKY.into(),
                    metadata: first_member_sticky_meta,
                },
            ];

            let previous_generation = s.generation_id();

            match s.join(
                now,
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                &first_member_id,
                group_instance_id,
                PROTOCOL_TYPE,
                Some(&protocols),
                reason,
            ) {
                (
                    s,
                    Body::JoinGroupResponse {
                        throttle_time_ms: Some(0),
                        error_code,
                        generation_id,
                        protocol_type,
                        protocol_name,
                        leader,
                        skip_assignment: Some(false),
                        member_id,
                        members: Some(members),
                    },
                ) => {
                    assert_eq!(i16::from(ErrorCode::None), error_code);
                    assert_eq!(previous_generation + 1, generation_id);
                    assert_eq!(Some(PROTOCOL_TYPE.into()), protocol_type);
                    assert_eq!(Some(RANGE.into()), protocol_name);
                    assert_eq!(first_member_id, leader);
                    assert_eq!(first_member_id, member_id);

                    assert_eq!(
                        Some(first_member_range_meta),
                        members
                            .iter()
                            .find(|member| member.member_id == first_member_id)
                            .map(|member| member.metadata.clone())
                    );

                    assert_eq!(
                        Some(second_member_range_meta.clone()),
                        members
                            .iter()
                            .find(|member| member.member_id == second_member_id)
                            .map(|member| member.metadata.clone())
                    );

                    s
                }

                otherwise => panic!("{otherwise:?}"),
            }
        };

        let s = {
            let protocols = [
                JoinGroupRequestProtocol {
                    name: RANGE.into(),
                    metadata: second_member_range_meta.clone(),
                },
                JoinGroupRequestProtocol {
                    name: COOPERATIVE_STICKY.into(),
                    metadata: second_member_sticky_meta.clone(),
                },
            ];

            let join_response_expected = Body::JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::None.into(),
                generation_id: s.generation_id(),
                protocol_type: Some(PROTOCOL_TYPE.into()),
                protocol_name: Some(RANGE.into()),
                leader: first_member_id.clone(),
                skip_assignment: Some(false),
                member_id: second_member_id.clone(),
                members: Some([].into()),
            };

            let (s, join_group_response) = s.join(
                now,
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                &second_member_id,
                group_instance_id,
                PROTOCOL_TYPE,
                Some(&protocols),
                reason,
            );

            assert_eq!(join_response_expected, join_group_response);

            s
        };

        let second_member_assignment_02 = Bytes::from_static(b"second_member_assignment_02");

        let s = {
            let first_member_assignment_02 = Bytes::from_static(b"first_member_assignment_02");

            let assignments = [
                SyncGroupRequestAssignment {
                    member_id: first_member_id.clone(),
                    assignment: first_member_assignment_02.clone(),
                },
                SyncGroupRequestAssignment {
                    member_id: second_member_id.clone(),
                    assignment: second_member_assignment_02.clone(),
                },
            ];

            let generation_id = s.generation_id();

            let (s, sync_response) = s.sync(
                now,
                GROUP_ID,
                generation_id,
                &first_member_id,
                group_instance_id,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(&assignments),
            );

            let sync_response_expected = Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code: 0,
                protocol_type: Some(PROTOCOL_TYPE.into()),
                protocol_name: Some(RANGE.into()),
                assignment: first_member_assignment_02,
            };

            assert_eq!(sync_response_expected, sync_response);

            s
        };

        let s = {
            let generation_id = s.generation_id();

            let (s, sync_response) = s.sync(
                now,
                GROUP_ID,
                generation_id,
                &second_member_id,
                group_instance_id,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(&assignments),
            );

            let sync_response_expected = Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code: 0,
                protocol_type: Some(PROTOCOL_TYPE.into()),
                protocol_name: Some(RANGE.into()),
                assignment: second_member_assignment_02,
            };

            assert_eq!(sync_response_expected, sync_response);

            s
        };

        let _ = s;

        Ok(())
    }
}
