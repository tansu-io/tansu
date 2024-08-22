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

use crate::{Error, Result};

use super::Coordinator;

pub trait Group: Debug + Send {
    type JoinState;
    type SyncState;
    type HeartbeatState;
    type LeaveState;
    type OffsetCommitState;
    type OffsetFetchState;

    fn join(
        self,
        now: Instant,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> (Self::JoinState, Body);

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

    fn join(
        self,
        now: Instant,
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

            Self::Forming(inner) => {
                let (state, body) = inner.join(
                    now,
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

            Self::Formed(inner) => {
                let (state, body) = inner.join(
                    now,
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
        }
    }

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
        let _ = protocol_type;
        let _ = group_id;

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
                (state.into(), body)
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
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner.leave(now, group_id, member_id, members);
                (state.into(), body)
            }
        }
    }

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
        let _ = group_id;
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

    fn offset_fetch(
        self,
        now: Instant,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Wrapper, Body) {
        let _ = group_id;
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

impl<S> Inner<S>
where
    S: Debug,
{
    fn member_id(&mut self, member_id: &str) -> String {
        str::parse::<i32>(member_id)
            .unwrap_or_else(|_| {
                let member_id = self.member_id;
                self.member_id += 1;
                member_id
            })
            .to_string()
    }
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
        mut self,
        now: Instant,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> (Self::JoinState, Body) {
        let _ = reason;
        let _ = group_id;

        let member_id = self.member_id(member_id);
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
                members: None,
            };

            return (self.into(), body);
        };

        let protocol = &protocols[0];

        let join_response = JoinGroupResponseMember {
            member_id: member_id.to_string(),
            group_instance_id: group_instance_id
                .map(|group_instance_id| group_instance_id.to_owned()),
            metadata: protocol.metadata.clone(),
        };

        let state = {
            let mut members = BTreeMap::new();
            _ = members.insert(
                member_id.clone(),
                Member {
                    join_response: join_response.clone(),
                    last_contact: Some(now),
                },
            );

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
                    leader: Some(member_id.to_string()),
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
                leader: member_id.to_string(),
                skip_assignment: Some(false),
                member_id: member_id.to_string(),
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
        let _ = group_id;
        let _ = now;
        let _ = generation_id;
        let _ = member_id;
        let _ = group_instance_id;
        let _ = protocol_name;
        let _ = assignments;

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
        let _ = group_id;
        let _ = now;
        let _ = generation_id;
        let _ = member_id;
        let _ = group_instance_id;

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
        let _ = member_id;
        let _ = group_id;
        let _ = now;
        let _ = members;

        let body = Body::LeaveGroupResponse {
            throttle_time_ms: Some(0),
            error_code: ErrorCode::UnknownMemberId.into(),
            members: None,
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
        let _ = group_id;
        let _ = now;
        let _ = generation_id_or_member_epoch;
        let _ = member_id;
        let _ = group_instance_id;
        let _ = retention_time_ms;
        let _ = topics;

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
        let _ = group_id;
        let _ = now;
        let _ = topics;
        let _ = groups;
        let _ = require_stable;

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
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> (Self::JoinState, Body) {
        let _ = group_id;
        let _ = session_timeout_ms;
        let _ = rebalance_timeout_ms;
        let _ = reason;

        let member_id = self.member_id(member_id);

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
                members: None,
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
                members: None,
            };

            return (self.into(), body);
        };

        match self.members.insert(
            member_id.clone(),
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
                if metadata != protocol.metadata {
                    self.generation_id += 1;
                }
            }

            None => self.generation_id += 1,
        }

        let body = {
            let members = self
                .members
                .values()
                .cloned()
                .map(|member| member.join_response)
                .collect();

            Body::JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::None.into(),
                generation_id: self.generation_id,
                protocol_type: Some(self.state.protocol_type.clone()),
                protocol_name: Some(self.state.protocol_name.clone()),
                leader: String::from(""),
                skip_assignment: Some(false),
                member_id: member_id.to_string(),
                members: Some(members),
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
        let _ = group_id;
        let _ = group_instance_id;
        let _ = protocol_name;

        let Ok(member_id) = str::parse::<i32>(member_id).map(|member_id| member_id.to_string())
        else {
            let body = Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::UnknownMemberId.into(),
                protocol_type: Some(self.state.protocol_type.clone()),
                protocol_name: Some(self.state.protocol_name.clone()),
                assignment: Bytes::from_static(b""),
            };

            return (self.into(), body);
        };

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
            .entry(member_id.clone())
            .and_modify(|member| _ = member.last_contact.replace(now));

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
                .get(&member_id)
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
                leader: member_id.clone(),
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
        let _ = group_id;
        let _ = group_instance_id;

        let Ok(member_id) = str::parse::<i32>(member_id).map(|member_id| member_id.to_string())
        else {
            return (
                self.into(),
                Body::HeartbeatResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::UnknownMemberId.into(),
                },
            );
        };

        if !self.members.contains_key(&member_id) {
            return (
                self.into(),
                Body::HeartbeatResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::UnknownMemberId.into(),
                },
            );
        }

        if generation_id > self.generation_id {
            return (
                self.into(),
                Body::HeartbeatResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::IllegalGeneration.into(),
                },
            );
        }

        if generation_id < self.generation_id {
            return (
                self.into(),
                Body::HeartbeatResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::RebalanceInProgress.into(),
                },
            );
        }

        _ = self
            .members
            .entry(member_id)
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
                    match str::parse::<i32>(member_id) {
                        Err(_) => ErrorCode::UnknownMemberId.into(),
                        Ok(_) => {
                            if self.members.remove(member_id).is_some() {
                                ErrorCode::None.into()
                            } else {
                                ErrorCode::UnknownMemberId.into()
                            }
                        }
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
                            match str::parse::<i32>(&member.member_id) {
                                Err(_) => ErrorCode::UnknownMemberId.into(),
                                Ok(_) => {
                                    if self.members.remove(&member.member_id).is_some() {
                                        ErrorCode::None.into()
                                    } else {
                                        ErrorCode::UnknownMemberId.into()
                                    }
                                }
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
        let _ = group_id;
        let _ = now;
        let _ = topics;
        let _ = retention_time_ms;
        let _ = group_instance_id;
        let _ = member_id;
        let _ = generation_id_or_member_epoch;
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
        let _ = group_id;
        let _ = now;
        let _ = require_stable;
        let _ = groups;
        let _ = topics;
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
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> (Self::JoinState, Body) {
        let _ = group_id;
        let _ = session_timeout_ms;
        let _ = rebalance_timeout_ms;
        let _ = reason;

        let member_id = self.member_id(member_id);

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
                members: None,
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
                members: None,
            };

            return (self.into(), body);
        };

        let protocol_type = self.state.protocol_type.clone();
        let protocol_name = self.state.protocol_name.clone();

        let state: Wrapper = match self.members.insert(
            member_id.clone(),
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
        let _ = group_id;
        let _ = now;
        let _ = assignments;
        let _ = protocol_name;
        let _ = group_instance_id;
        let _ = member_id;
        let _ = generation_id;
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
        let _ = group_id;
        let _ = now;
        let _ = group_instance_id;
        let _ = member_id;
        let _ = generation_id;
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
        let _ = group_id;
        let _ = now;

        let members = if let Some(member_id) = member_id {
            vec![MemberResponse {
                member_id: member_id.to_owned(),
                group_instance_id: None,
                error_code: {
                    match str::parse::<i32>(member_id) {
                        Err(_) => ErrorCode::UnknownMemberId.into(),
                        Ok(_) => {
                            if self.members.remove(member_id).is_some() {
                                ErrorCode::None.into()
                            } else {
                                ErrorCode::UnknownMemberId.into()
                            }
                        }
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
                            match str::parse::<i32>(&member.member_id) {
                                Err(_) => ErrorCode::UnknownMemberId.into(),
                                Ok(_) => {
                                    if self.members.remove(&member.member_id).is_some() {
                                        ErrorCode::None.into()
                                    } else {
                                        ErrorCode::UnknownMemberId.into()
                                    }
                                }
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
        let _ = group_id;
        let _ = now;
        let _ = topics;
        let _ = retention_time_ms;
        let _ = group_instance_id;
        let _ = member_id;
        let _ = generation_id_or_member_epoch;
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
        let _ = group_id;
        let _ = now;
        let _ = require_stable;
        let _ = groups;
        let _ = topics;
        todo!()
    }
}

impl Group for Inner<Formed> {
    type JoinState = Inner<Forming>;
    type SyncState = Inner<Syncing>;
    type HeartbeatState = Inner<Formed>;
    type LeaveState = Wrapper;
    type OffsetCommitState = Inner<Formed>;
    type OffsetFetchState = Inner<Formed>;

    #[instrument]
    fn join(
        self,
        now: Instant,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> (Self::JoinState, Body) {
        let _ = reason;
        let _ = protocols;
        let _ = protocol_type;
        let _ = group_instance_id;
        let _ = member_id;
        let _ = rebalance_timeout_ms;
        let _ = session_timeout_ms;
        let _ = group_id;
        let _ = now;
        todo!()
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
        let _ = group_id;
        let _ = assignments;
        let _ = protocol_name;
        let _ = group_instance_id;
        let _ = member_id;
        let _ = generation_id;
        let _ = now;
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
        let _ = group_id;
        let _ = group_instance_id;
        let _ = member_id;
        let _ = generation_id;
        let _ = now;

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
                    match str::parse::<i32>(member_id) {
                        Err(_) => ErrorCode::UnknownMemberId.into(),
                        Ok(_) => {
                            if self.members.remove(member_id).is_some() {
                                ErrorCode::None.into()
                            } else {
                                ErrorCode::UnknownMemberId.into()
                            }
                        }
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
                            match str::parse::<i32>(&member.member_id) {
                                Err(_) => ErrorCode::UnknownMemberId.into(),
                                Ok(_) => {
                                    if self.members.remove(&member.member_id).is_some() {
                                        ErrorCode::None.into()
                                    } else {
                                        ErrorCode::UnknownMemberId.into()
                                    }
                                }
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
        let _ = group_id;
        let _ = topics;
        let _ = retention_time_ms;
        let _ = group_instance_id;
        let _ = member_id;
        let _ = generation_id_or_member_epoch;
        let _ = now;

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
        let _ = group_id;
        let _ = require_stable;
        let _ = groups;
        let _ = topics;
        let _ = now;

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
    use tansu_kafka_sans_io::{
        offset_commit_request::OffsetCommitRequestPartition,
        offset_fetch_request::OffsetFetchRequestTopics,
        offset_fetch_response::{
            OffsetFetchResponseGroup, OffsetFetchResponsePartitions, OffsetFetchResponseTopics,
        },
    };

    use super::*;

    #[test]
    fn fresh_join() -> Result<()> {
        let s: Wrapper = Inner::<Fresh>::default().into();

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

        let (s, join_response) = s.join(
            now,
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            Some(&protocols[..]),
            reason,
        );

        assert_eq!(session_timeout_ms, s.session_timeout_ms());
        assert_eq!(rebalance_timeout_ms, s.rebalance_timeout_ms());
        assert_eq!(Some("consumer"), s.protocol_type());
        assert_eq!(Some("range"), s.protocol_name());

        let member_id = s.members()[0].member_id.clone();

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

        let generation_id = s.generation_id();
        let protocol_type = s.protocol_type().map(|s| s.to_owned());
        let protocol_name = s.protocol_name().map(|s| s.to_owned());

        let assignment =
            Bytes::from_static(b"\0\x03\0\0\0\x01\0\x04test\0\0\0\x01\0\0\0\0\xff\xff\xff\xff");

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
            protocol_type.as_ref().map(|s| s.as_str()),
            protocol_name.as_ref().map(|s| s.as_str()),
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
}
