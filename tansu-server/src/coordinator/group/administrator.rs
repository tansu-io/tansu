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

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    ops::Deref,
    time::SystemTime,
};

use async_trait::async_trait;
use bytes::Bytes;
use tansu_kafka_sans_io::{
    join_group_request::JoinGroupRequestProtocol,
    join_group_response::JoinGroupResponseMember,
    leave_group_request::MemberIdentity,
    leave_group_response::MemberResponse,
    offset_commit_response::{OffsetCommitResponsePartition, OffsetCommitResponseTopic},
    offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopic},
    offset_fetch_response::{
        OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
        OffsetFetchResponseTopic, OffsetFetchResponseTopics,
    },
    sync_group_request::SyncGroupRequestAssignment,
    Body, ErrorCode,
};
use tansu_storage::{
    GroupDetail, GroupMember, GroupState, OffsetCommitRequest, Storage, Topition, UpdateError,
    Version,
};
use tokio::time::{sleep, Duration};
use tracing::{debug, info};
use uuid::Uuid;

use crate::{Error, Result};

use super::{Coordinator, OffsetCommit};

const PAUSE_MS: u64 = 100;

#[async_trait]
pub trait Group: Debug + Send {
    type JoinState;
    type SyncState;
    type HeartbeatState;
    type LeaveState;
    type OffsetCommitState;
    type OffsetFetchState;

    #[allow(clippy::too_many_arguments)]
    async fn join(
        self,
        now: SystemTime,
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
    async fn sync(
        self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Self::SyncState, Body);

    async fn heartbeat(
        self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Self::HeartbeatState, Body);

    async fn leave(
        self,
        now: SystemTime,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Self::LeaveState, Body);

    #[allow(clippy::too_many_arguments)]
    async fn offset_commit(
        self,
        now: SystemTime,
        detail: &OffsetCommit<'_>,
    ) -> (Self::OffsetCommitState, Body);

    async fn offset_fetch(
        self,
        now: SystemTime,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Self::OffsetFetchState, Body);
}

#[derive(Clone, Debug)]
pub enum Wrapper<O: Storage> {
    Forming(Inner<O, Forming>),
    Formed(Inner<O, Formed>),
}

impl<O> From<Inner<O, Forming>> for Wrapper<O>
where
    O: Storage,
{
    fn from(value: Inner<O, Forming>) -> Self {
        Self::Forming(value)
    }
}

impl<O> From<Inner<O, Formed>> for Wrapper<O>
where
    O: Storage,
{
    fn from(value: Inner<O, Formed>) -> Self {
        Self::Formed(value)
    }
}

impl<O> From<&Wrapper<O>> for GroupDetail
where
    O: Storage,
{
    fn from(value: &Wrapper<O>) -> Self {
        match value {
            Wrapper::Forming(Inner {
                session_timeout_ms,
                rebalance_timeout_ms,
                group_instance_id,
                members,
                generation_id,
                state,
                skip_assignment,
                ..
            }) => GroupDetail {
                session_timeout_ms: *session_timeout_ms,
                rebalance_timeout_ms: *rebalance_timeout_ms,
                group_instance_id: group_instance_id.clone(),
                members: members
                    .iter()
                    .map(|(id, member)| {
                        (
                            id.to_owned(),
                            GroupMember {
                                join_response: member.join_response.clone(),
                                last_contact: member.last_contact,
                            },
                        )
                    })
                    .collect(),
                generation_id: *generation_id,
                skip_assignment: *skip_assignment,
                state: GroupState::Forming {
                    protocol_type: state.protocol_type.clone(),
                    protocol_name: state.protocol_name.clone(),
                    leader: state.leader.clone(),
                },
            },
            Wrapper::Formed(Inner {
                session_timeout_ms,
                rebalance_timeout_ms,
                group_instance_id,
                members,
                generation_id,
                state,
                skip_assignment,
                ..
            }) => GroupDetail {
                session_timeout_ms: *session_timeout_ms,
                rebalance_timeout_ms: *rebalance_timeout_ms,
                group_instance_id: group_instance_id.clone(),
                members: members
                    .iter()
                    .map(|(id, member)| {
                        (
                            id.to_owned(),
                            GroupMember {
                                join_response: member.join_response.clone(),
                                last_contact: member.last_contact,
                            },
                        )
                    })
                    .collect(),
                generation_id: *generation_id,
                skip_assignment: *skip_assignment,
                state: GroupState::Formed {
                    protocol_type: state.protocol_type.clone(),
                    protocol_name: state.protocol_name.clone(),
                    leader: state.leader.clone(),
                    assignments: state.assignments.clone(),
                },
            },
        }
    }
}

impl<O> Wrapper<O>
where
    O: Storage,
{
    pub fn with_storage_group_detail(storage: O, gd: GroupDetail) -> Self {
        match gd.state {
            GroupState::Forming {
                protocol_type,
                protocol_name,
                leader,
            } => Self::Forming(Inner {
                session_timeout_ms: gd.session_timeout_ms,
                rebalance_timeout_ms: gd.rebalance_timeout_ms,
                group_instance_id: gd.group_instance_id,
                members: gd
                    .members
                    .iter()
                    .map(|(id, member)| {
                        (
                            id.to_owned(),
                            Member {
                                join_response: member.join_response.clone(),
                                last_contact: member.last_contact,
                            },
                        )
                    })
                    .collect(),
                generation_id: gd.generation_id,
                state: Forming {
                    protocol_type,
                    protocol_name,
                    leader,
                },
                storage,
                skip_assignment: gd.skip_assignment,
            }),
            GroupState::Formed {
                protocol_type,
                protocol_name,
                leader,
                assignments,
            } => Self::Formed(Inner {
                session_timeout_ms: gd.session_timeout_ms,
                rebalance_timeout_ms: gd.rebalance_timeout_ms,
                group_instance_id: gd.group_instance_id,
                members: gd
                    .members
                    .iter()
                    .map(|(id, member)| {
                        (
                            id.to_owned(),
                            Member {
                                join_response: member.join_response.clone(),
                                last_contact: member.last_contact,
                            },
                        )
                    })
                    .collect(),
                generation_id: gd.generation_id,
                state: Formed {
                    protocol_type,
                    protocol_name,
                    leader,
                    assignments,
                },
                storage,
                skip_assignment: gd.skip_assignment,
            }),
        }
    }

    pub fn generation_id(&self) -> i32 {
        match self {
            Self::Forming(inner) => inner.generation_id,
            Self::Formed(inner) => inner.generation_id,
        }
    }

    pub fn session_timeout_ms(&self) -> i32 {
        match self {
            Self::Forming(inner) => inner.session_timeout_ms,
            Self::Formed(inner) => inner.session_timeout_ms,
        }
    }

    pub fn rebalance_timeout_ms(&self) -> Option<i32> {
        match self {
            Self::Forming(inner) => inner.rebalance_timeout_ms,
            Self::Formed(inner) => inner.rebalance_timeout_ms,
        }
    }

    pub fn protocol_type(&self) -> Option<&str> {
        match self {
            Self::Forming(inner) => inner.state.protocol_type.as_deref(),
            Self::Formed(inner) => Some(inner.state.protocol_type.as_str()),
        }
    }

    pub fn protocol_name(&self) -> Option<&str> {
        match self {
            Self::Forming(inner) => inner.state.protocol_name.as_deref(),
            Self::Formed(inner) => Some(inner.state.protocol_name.as_str()),
        }
    }

    pub fn leader(&self) -> Option<&str> {
        match self {
            Self::Forming(inner) => inner.state.leader.as_deref(),
            Self::Formed(inner) => Some(inner.state.leader.as_str()),
        }
    }

    pub fn skip_assignment(&self) -> Option<&bool> {
        match self {
            Self::Forming(inner) => inner.skip_assignment.as_ref(),
            Self::Formed(inner) => inner.skip_assignment.as_ref(),
        }
    }

    fn members(&self) -> Vec<JoinGroupResponseMember> {
        match self {
            Wrapper::Forming(inner) => inner
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

    fn missed_heartbeat(self, group_id: &str, now: SystemTime) -> Self {
        debug!(?group_id, ?now);

        match self {
            Wrapper::Forming(mut inner) => {
                _ = inner.missed_heartbeat(group_id, now);
                Wrapper::Forming(inner)
            }
            Wrapper::Formed(mut inner) => {
                if inner.missed_heartbeat(group_id, now) {
                    info!("missed heartbeat for {group_id} in {}", inner.generation_id);

                    Wrapper::Forming(Inner {
                        session_timeout_ms: inner.session_timeout_ms,
                        rebalance_timeout_ms: inner.rebalance_timeout_ms,
                        group_instance_id: inner.group_instance_id,
                        members: inner.members,
                        generation_id: inner.generation_id,
                        state: Forming {
                            protocol_type: Some(inner.state.protocol_type),
                            protocol_name: Some(inner.state.protocol_name),
                            leader: None,
                        },
                        storage: inner.storage,
                        skip_assignment: inner.skip_assignment,
                    })
                } else {
                    Wrapper::Formed(inner)
                }
            }
        }
    }
}

#[async_trait]
impl<O> Group for Wrapper<O>
where
    O: Storage,
{
    type JoinState = Wrapper<O>;
    type SyncState = Wrapper<O>;
    type HeartbeatState = Wrapper<O>;
    type LeaveState = Wrapper<O>;
    type OffsetCommitState = Wrapper<O>;
    type OffsetFetchState = Wrapper<O>;

    #[allow(clippy::too_many_arguments)]
    async fn join(
        self,
        now: SystemTime,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> (Wrapper<O>, Body) {
        match self {
            Self::Forming(inner) => {
                let (state, body) = inner
                    .join(
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
                    .await;
                (state.into(), body)
            }

            Self::Formed(inner) => {
                let (state, body) = inner
                    .join(
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
                    .await;
                (state, body)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn sync(
        self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Wrapper<O>, Body) {
        match self {
            Wrapper::Forming(inner) => {
                let (state, body) = inner
                    .sync(
                        now,
                        group_id,
                        generation_id,
                        member_id,
                        group_instance_id,
                        protocol_type,
                        protocol_name,
                        assignments,
                    )
                    .await;
                (state, body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner
                    .sync(
                        now,
                        group_id,
                        generation_id,
                        member_id,
                        group_instance_id,
                        protocol_type,
                        protocol_name,
                        assignments,
                    )
                    .await;
                (state.into(), body)
            }
        }
    }

    async fn leave(
        self,
        now: SystemTime,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Wrapper<O>, Body) {
        match self {
            Wrapper::Forming(inner) => {
                let (state, body) = inner.leave(now, group_id, member_id, members).await;
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner.leave(now, group_id, member_id, members).await;
                (state, body)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn offset_commit(self, now: SystemTime, detail: &OffsetCommit<'_>) -> (Wrapper<O>, Body) {
        match self {
            Wrapper::Forming(inner) => {
                let (state, body) = inner.offset_commit(now, detail).await;
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner.offset_commit(now, detail).await;
                (state.into(), body)
            }
        }
    }

    async fn offset_fetch(
        self,
        now: SystemTime,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Wrapper<O>, Body) {
        match self {
            Wrapper::Forming(inner) => {
                let (state, body) = inner
                    .offset_fetch(now, group_id, topics, groups, require_stable)
                    .await;
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner
                    .offset_fetch(now, group_id, topics, groups, require_stable)
                    .await;
                (state.into(), body)
            }
        }
    }

    async fn heartbeat(
        self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Wrapper<O>, Body) {
        debug!(
            ?now,
            ?group_id,
            ?generation_id,
            ?member_id,
            ?group_instance_id
        );

        match self {
            Wrapper::Forming(inner) => {
                let (state, body) = inner
                    .heartbeat(now, group_id, generation_id, member_id, group_instance_id)
                    .await;
                debug!(?state, ?body);

                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner
                    .heartbeat(now, group_id, generation_id, member_id, group_instance_id)
                    .await;
                debug!(?state, ?body);

                (state.into(), body)
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct Controller<O: Storage> {
    storage: O,
    wrappers: BTreeMap<String, (Wrapper<O>, Option<Version>)>,
}

impl<O> Controller<O>
where
    O: Storage,
{
    pub fn with_storage(storage: O) -> Result<Self> {
        Ok(Self {
            storage,
            wrappers: BTreeMap::new(),
        })
    }
}

#[async_trait]
impl<O> Coordinator for Controller<O>
where
    O: Storage + Clone,
{
    async fn join(
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
        debug!(
            ?client_id,
            ?group_id,
            ?session_timeout_ms,
            ?rebalance_timeout_ms,
            ?member_id,
            ?group_instance_id,
            ?protocol_type,
            ?protocols,
            ?reason,
        );

        let mut iteration = -1;

        loop {
            iteration += 1;

            let (wrapper, version) = self.wrappers.remove(group_id).unwrap_or_else(|| {
                debug!(?iteration, ?group_id);

                let inner = Inner {
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    group_instance_id: group_instance_id.map(ToOwned::to_owned),
                    members: Default::default(),
                    generation_id: -1,
                    state: Forming::default(),
                    skip_assignment: Some(false),
                    storage: self.storage.clone(),
                };

                (Wrapper::Forming(inner), None)
            });

            debug!(?wrapper, ?version, ?iteration);

            let now = SystemTime::now();
            let wrapper = wrapper.missed_heartbeat(group_id, now);

            let (wrapper, body) = wrapper
                .join(
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
                .await;

            match self
                .storage
                .update_group(group_id, GroupDetail::from(&wrapper), version)
                .await
            {
                Ok(version) => {
                    _ = self
                        .wrappers
                        .insert(group_id.to_owned(), (wrapper, Some(version)));

                    return Ok(body);
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(?current, ?version, ?iteration);

                    _ = self.wrappers.insert(
                        group_id.to_owned(),
                        (
                            Wrapper::with_storage_group_detail(self.storage.clone(), current),
                            Some(version),
                        ),
                    );

                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::ObjectStore(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),
            }
        }
    }

    async fn sync(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> Result<Body> {
        debug!(
            ?group_id,
            ?generation_id,
            ?member_id,
            ?group_instance_id,
            ?protocol_type,
            ?protocol_name,
            ?assignments
        );

        let mut iteration = -1;

        loop {
            iteration += 1;

            let (wrapper, version) = self.wrappers.remove(group_id).unwrap_or_else(|| {
                debug!(?iteration, ?group_id);

                let inner = Inner {
                    session_timeout_ms: Default::default(),
                    rebalance_timeout_ms: Default::default(),
                    group_instance_id: group_instance_id.map(ToOwned::to_owned),
                    members: Default::default(),
                    generation_id: -1,
                    state: Forming::default(),
                    skip_assignment: Some(false),
                    storage: self.storage.clone(),
                };

                (Wrapper::Forming(inner), None)
            });

            debug!(?wrapper, ?version, ?iteration);

            let now = SystemTime::now();
            let wrapper = wrapper.missed_heartbeat(group_id, now);

            let (wrapper, body) = wrapper
                .sync(
                    now,
                    group_id,
                    generation_id,
                    member_id,
                    group_instance_id,
                    protocol_type,
                    protocol_name,
                    assignments,
                )
                .await;

            match self
                .storage
                .update_group(group_id, GroupDetail::from(&wrapper), version)
                .await
            {
                Ok(version) => {
                    _ = self
                        .wrappers
                        .insert(group_id.to_owned(), (wrapper, Some(version)));
                    return Ok(body);
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(?current, ?version);

                    _ = self.wrappers.insert(
                        group_id.to_owned(),
                        (
                            Wrapper::with_storage_group_detail(self.storage.clone(), current),
                            Some(version),
                        ),
                    );

                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::ObjectStore(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),
            }
        }
    }

    async fn leave(
        &mut self,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> Result<Body> {
        debug!(?group_id, ?member_id, ?members);

        let mut iteration = -1;

        loop {
            iteration += 1;

            let (wrapper, version) = self.wrappers.remove(group_id).unwrap_or_else(|| {
                debug!(?iteration, ?group_id);

                let inner = Inner {
                    session_timeout_ms: Default::default(),
                    rebalance_timeout_ms: Default::default(),
                    group_instance_id: Default::default(),
                    members: Default::default(),
                    generation_id: -1,
                    state: Forming::default(),
                    skip_assignment: Some(false),
                    storage: self.storage.clone(),
                };

                (Wrapper::Forming(inner), None)
            });

            debug!(?wrapper, ?version, ?iteration);

            let now = SystemTime::now();
            let wrapper = wrapper.missed_heartbeat(group_id, now);

            let (wrapper, body) = wrapper.leave(now, group_id, member_id, members).await;

            match self
                .storage
                .update_group(group_id, GroupDetail::from(&wrapper), version)
                .await
            {
                Ok(version) => {
                    _ = self
                        .wrappers
                        .insert(group_id.to_owned(), (wrapper, Some(version)));
                    return Ok(body);
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(?current, ?version);

                    _ = self.wrappers.insert(
                        group_id.to_owned(),
                        (
                            Wrapper::with_storage_group_detail(self.storage.clone(), current),
                            Some(version),
                        ),
                    );

                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::ObjectStore(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),
            }
        }
    }

    async fn offset_commit(&mut self, offset_commit: OffsetCommit<'_>) -> Result<Body> {
        let group_id = offset_commit.group_id;
        let mut iteration = -1;

        loop {
            iteration += 1;

            let (wrapper, version) = self.wrappers.remove(group_id).unwrap_or_else(|| {
                debug!(?iteration, ?group_id);

                let inner = Inner {
                    session_timeout_ms: Default::default(),
                    rebalance_timeout_ms: Default::default(),
                    group_instance_id: Default::default(),
                    members: Default::default(),
                    generation_id: -1,
                    state: Forming::default(),
                    skip_assignment: Some(false),
                    storage: self.storage.clone(),
                };

                (Wrapper::Forming(inner), None)
            });

            let now = SystemTime::now();
            let wrapper = wrapper.missed_heartbeat(group_id, now);

            let (wrapper, body) = wrapper.offset_commit(now, &offset_commit).await;

            match self
                .storage
                .update_group(group_id, GroupDetail::from(&wrapper), version)
                .await
            {
                Ok(version) => {
                    _ = self
                        .wrappers
                        .insert(group_id.to_owned(), (wrapper, Some(version)));
                    return Ok(body);
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(?current, ?version);

                    _ = self.wrappers.insert(
                        group_id.to_owned(),
                        (
                            Wrapper::with_storage_group_detail(self.storage.clone(), current),
                            Some(version),
                        ),
                    );

                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::ObjectStore(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),
            }
        }
    }

    async fn offset_fetch(
        &mut self,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> Result<Body> {
        let wrapper = Wrapper::Forming(Inner {
            session_timeout_ms: Default::default(),
            rebalance_timeout_ms: Default::default(),
            group_instance_id: Default::default(),
            members: Default::default(),
            generation_id: -1,
            state: Forming::default(),
            skip_assignment: Some(false),
            storage: self.storage.clone(),
        });

        let now = SystemTime::now();
        let (_wrapper, body) = wrapper
            .offset_fetch(now, group_id, topics, groups, require_stable)
            .await;
        Ok(body)
    }

    async fn heartbeat(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<Body> {
        debug!(?group_id, ?generation_id, ?member_id, ?group_instance_id);

        let mut iteration = -1;

        loop {
            iteration += 1;

            let (wrapper, version) = self.wrappers.remove(group_id).unwrap_or_else(|| {
                debug!(?iteration, ?group_id);

                let inner = Inner {
                    session_timeout_ms: Default::default(),
                    rebalance_timeout_ms: Default::default(),
                    group_instance_id: Default::default(),
                    members: Default::default(),
                    generation_id: -1,
                    state: Forming::default(),
                    skip_assignment: Some(false),
                    storage: self.storage.clone(),
                };

                (Wrapper::Forming(inner), None)
            });

            let now = SystemTime::now();
            // let wrapper = wrapper.missed_heartbeat(group_id, now);

            let (wrapper, body) = wrapper
                .heartbeat(now, group_id, generation_id, member_id, group_instance_id)
                .await;

            match self
                .storage
                .update_group(group_id, GroupDetail::from(&wrapper), version)
                .await
            {
                Ok(version) => {
                    _ = self
                        .wrappers
                        .insert(group_id.to_owned(), (wrapper, Some(version)));
                    return Ok(body);
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(?current, ?version);

                    _ = self.wrappers.insert(
                        group_id.to_owned(),
                        (
                            Wrapper::with_storage_group_detail(self.storage.clone(), current),
                            Some(version),
                        ),
                    );

                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::ObjectStore(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),
            }
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Forming {
    protocol_type: Option<String>,
    protocol_name: Option<String>,
    leader: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Formed {
    protocol_type: String,
    protocol_name: String,
    leader: String,
    assignments: BTreeMap<String, Bytes>,
}

#[derive(Clone, Debug)]
pub struct Inner<O, S> {
    session_timeout_ms: i32,
    rebalance_timeout_ms: Option<i32>,
    group_instance_id: Option<String>,
    members: BTreeMap<String, Member>,
    generation_id: i32,
    state: S,
    storage: O,
    skip_assignment: Option<bool>,
}

impl<O> Inner<O, Forming>
where
    O: Storage,
{
    fn missed_heartbeat(&mut self, group_id: &str, now: SystemTime) -> bool {
        let original = self.members.len();

        self.members.retain(|member_id, member| {
            member
                .last_contact
                .map(|last_contact| now.duration_since(last_contact).unwrap_or_default())
                .inspect(|duration| {
                    debug!(
                        "{member_id}, since last contact: {}ms",
                        duration.as_millis()
                    )
                })
                .is_some_and(|duration| {
                    if duration.as_millis()
                        > u128::try_from(self.session_timeout_ms).unwrap_or(45_000)
                    {

                        if self.state.leader.as_ref().is_some_and(|leader|leader == member_id){
                            info!(
                                "missed heartbeat for leader {member_id} for {group_id} in generation: {}, after {}ms",
                                self.generation_id, duration.as_millis()
                            );

                            _ = self.state.leader.take();
                        } else {
                            info!(
                                "missed heartbeat for {member_id} for {group_id} in generation: {}, after {}ms",
                                self.generation_id, duration.as_millis()
                            );
                        }

                        false
                    } else {
                        true
                    }
                })
        });

        original > self.members.len()
    }
}

impl<O> Inner<O, Formed>
where
    O: Storage,
{
    fn missed_heartbeat(&mut self, group_id: &str, now: SystemTime) -> bool {
        debug!(?group_id, ?now);

        let original = self.members.len();

        self.members.retain(|member_id, member| {
            debug!(?member_id, ?member);

            member
                .last_contact
                .map(|last_contact| now.duration_since(last_contact).unwrap_or_default())
                .inspect(|duration| {
                    debug!(
                        "{member_id}, since last contact: {}ms",
                        duration.as_millis()
                    )
                })
                .is_some_and(|duration| {
                    if duration.as_millis()
                        > u128::try_from(self.session_timeout_ms).unwrap_or(45_000)
                    {
                        info!(
                            "missed heartbeat for {member_id} for {group_id} in generation: {}, after {}ms",
                            self.generation_id, duration.as_millis()
                        );


                        false
                    } else {
                        true
                    }
                })
        });

        original > self.members.len()
    }
}

impl<O, S> Inner<O, S>
where
    O: Storage,
    S: Debug,
{
    async fn fetch_offset(
        &mut self,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> Result<Body> {
        let topics = if let Some(topics) = topics {
            let topics: Vec<Topition> = topics
                .iter()
                .flat_map(|topic| {
                    topic
                        .partition_indexes
                        .as_ref()
                        .map(|partition_indexes| {
                            partition_indexes
                                .iter()
                                .map(|partition_index| {
                                    Topition::new(topic.name.clone(), *partition_index)
                                })
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                })
                .collect();

            self.storage
                .offset_fetch(group_id, topics.deref(), require_stable)
                .await
                .map(|offsets| {
                    offsets
                        .iter()
                        .fold(BTreeSet::new(), |mut topics, (topition, _)| {
                            _ = topics.insert(topition.topic());
                            topics
                        })
                        .iter()
                        .map(|topic_name| OffsetFetchResponseTopic {
                            name: (*topic_name).into(),
                            partitions: Some(
                                offsets
                                    .iter()
                                    .filter_map(|(topition, offset)| {
                                        if topition.topic() == *topic_name {
                                            Some(OffsetFetchResponsePartition {
                                                partition_index: topition.partition(),
                                                committed_offset: *offset,
                                                committed_leader_epoch: None,
                                                metadata: None,
                                                error_code: ErrorCode::None.into(),
                                            })
                                        } else {
                                            None
                                        }
                                    })
                                    .collect(),
                            ),
                        })
                        .collect()
                })
                .map(Some)?
        } else {
            None
        };

        let groups = if let Some(groups) = groups {
            let mut responses = vec![];

            for group in groups {
                if let Some(topics) = group.topics.as_ref().map(|topics| {
                    topics
                        .iter()
                        .flat_map(|topic| {
                            topic
                                .partition_indexes
                                .as_ref()
                                .map(|partition_indexes| {
                                    partition_indexes
                                        .iter()
                                        .map(|partition_index| {
                                            Topition::new(topic.name.clone(), *partition_index)
                                        })
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default()
                        })
                        .collect::<Vec<_>>()
                }) {
                    let response = self
                        .storage
                        .offset_fetch(
                            Some(group.group_id.as_str()),
                            topics.deref(),
                            require_stable,
                        )
                        .await
                        .map(|offsets| OffsetFetchResponseGroup {
                            group_id: group.group_id.clone(),
                            topics: Some(
                                offsets
                                    .iter()
                                    .fold(BTreeSet::new(), |mut topics, (topition, _)| {
                                        _ = topics.insert(topition.topic());
                                        topics
                                    })
                                    .iter()
                                    .map(|topic_name| OffsetFetchResponseTopics {
                                        name: (*topic_name).into(),
                                        partitions: Some(
                                            offsets
                                                .iter()
                                                .filter_map(|(topition, offset)| {
                                                    if topition.topic() == *topic_name {
                                                        Some(OffsetFetchResponsePartitions {
                                                            partition_index: topition.partition(),
                                                            committed_offset: *offset,
                                                            committed_leader_epoch: -1,
                                                            metadata: None,
                                                            error_code: ErrorCode::None.into(),
                                                        })
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect(),
                                        ),
                                    })
                                    .collect(),
                            ),
                            error_code: ErrorCode::None.into(),
                        })?;

                    responses.push(response);
                }
            }

            Some(responses)
        } else {
            None
        };

        let body = Body::OffsetFetchResponse {
            throttle_time_ms: Some(0),
            topics,
            error_code: Some(ErrorCode::None.into()),
            groups,
        };

        Ok(body)
    }

    async fn commit_offset(&mut self, detail: &OffsetCommit<'_>) -> Result<Body> {
        let retention_time_ms = detail
            .retention_time_ms
            .map_or(Ok(None), |ms| {
                u64::try_from(ms).map_err(Error::from).map(Some)
            })?
            .map(Duration::from_millis);

        if let Some(topics) = detail.topics {
            let mut offsets = vec![];

            for topic in topics {
                if let Some(ref partitions) = topic.partitions {
                    for partition in partitions {
                        let topition = Topition::new(topic.name.clone(), partition.partition_index);
                        let offset = OffsetCommitRequest::try_from(partition)?;

                        offsets.push((topition, offset));
                    }
                }
            }

            self.storage
                .offset_commit(detail.group_id, retention_time_ms, offsets.deref())
                .await
                .map(|value| {
                    let topics = value
                        .iter()
                        .fold(BTreeSet::new(), |mut topics, (topition, _)| {
                            _ = topics.insert(topition.topic());
                            topics
                        })
                        .iter()
                        .map(|topic_name| OffsetCommitResponseTopic {
                            name: (*topic_name).into(),
                            partitions: Some(
                                value
                                    .iter()
                                    .filter_map(|(topition, error_code)| {
                                        if topition.topic() == *topic_name {
                                            Some(OffsetCommitResponsePartition {
                                                partition_index: topition.partition(),
                                                error_code: i16::from(*error_code),
                                            })
                                        } else {
                                            None
                                        }
                                    })
                                    .collect(),
                            ),
                        })
                        .collect();

                    Body::OffsetCommitResponse {
                        throttle_time_ms: Some(0),
                        topics: Some(topics),
                    }
                })
                .map_err(Into::into)
        } else {
            Ok(Body::OffsetCommitResponse {
                throttle_time_ms: Some(0),
                topics: detail.topics.map(|topics| {
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
                                        error_code: ErrorCode::UnknownMemberId.into(),
                                    })
                                    .collect()
                            }),
                        })
                        .collect()
                }),
            })
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Member {
    join_response: JoinGroupResponseMember,
    last_contact: Option<SystemTime>,
}

#[async_trait::async_trait]
impl<O> Group for Inner<O, Forming>
where
    O: Storage,
{
    type JoinState = Inner<O, Forming>;
    type SyncState = Wrapper<O>;
    type HeartbeatState = Inner<O, Forming>;
    type LeaveState = Inner<O, Forming>;
    type OffsetCommitState = Inner<O, Forming>;
    type OffsetFetchState = Inner<O, Forming>;

    async fn join(
        mut self,
        now: SystemTime,
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
        let _ = reason;

        if let Some(client_id) = client_id {
            if member_id.is_empty() {
                let member_id = format!("{client_id}-{}", Uuid::new_v4());
                debug!(?member_id);

                let body = Body::JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::MemberIdRequired.into(),
                    generation_id: -1,
                    protocol_type: self.state.protocol_type.clone(),
                    protocol_name: Some("".into()),
                    leader: "".into(),
                    skip_assignment: self.skip_assignment,
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
                protocol_type: self.state.protocol_type.clone(),
                protocol_name: Some("".into()),
                leader: "".into(),
                skip_assignment: self.skip_assignment,
                member_id: "".into(),
                members: Some([].into()),
            };

            return (self, body);
        };

        let protocol = if let Some(protocol_name) = self.state.protocol_name.as_deref() {
            if let Some(protocol) = protocols
                .iter()
                .find(|protocol| protocol.name == protocol_name)
            {
                protocol
            } else {
                let body = Body::JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::InconsistentGroupProtocol.into(),
                    generation_id: self.generation_id,
                    protocol_type: Some(protocol_type.into()),
                    protocol_name: self.state.protocol_name.clone(),
                    leader: "".into(),
                    skip_assignment: self.skip_assignment,
                    member_id: "".into(),
                    members: Some([].into()),
                };

                return (self, body);
            }
        } else {
            self.state.protocol_type = Some(protocol_type.to_owned());
            self.state.protocol_name = Some(protocols[0].name.as_str().to_owned());

            self.session_timeout_ms = session_timeout_ms;
            self.rebalance_timeout_ms = rebalance_timeout_ms;

            &protocols[0]
        };

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
                    debug!( ?protocol.metadata);
                    self.generation_id += 1;
                }
            }

            None => self.generation_id += 1,
        }

        debug!(?member_id, ?self.members);

        if self.state.leader.is_none() {
            info!(
                "{member_id} is now leader of: {group_id} in generation: {}",
                self.generation_id
            );

            _ = self.state.leader.replace(member_id.to_owned());
        }

        let body = {
            Body::JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::None.into(),
                generation_id: self.generation_id,
                protocol_type: self.state.protocol_type.clone(),
                protocol_name: self.state.protocol_name.clone(),
                leader: self
                    .state
                    .leader
                    .as_ref()
                    .map_or(String::from(""), |leader| leader.clone()),
                skip_assignment: self.skip_assignment,
                member_id: member_id.into(),
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

    async fn sync(
        mut self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Self::SyncState, Body) {
        let _ = group_id;
        let _ = group_instance_id;
        let _ = protocol_type;
        let _ = protocol_name;

        if !self.members.contains_key(member_id) {
            let body = Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::UnknownMemberId.into(),
                protocol_type: self.state.protocol_type.clone(),
                protocol_name: self.state.protocol_name.clone(),
                assignment: Bytes::from_static(b""),
            };

            return (self.into(), body);
        }

        debug!(?member_id);

        if generation_id > self.generation_id {
            let body = Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::IllegalGeneration.into(),
                protocol_type: self.state.protocol_type.clone(),
                protocol_name: self.state.protocol_name.clone(),
                assignment: Bytes::from_static(b""),
            };

            debug!("about to sleep");
            sleep(Duration::from_millis(PAUSE_MS)).await;
            debug!("done sleeping");

            return (self.into(), body);
        }

        if generation_id < self.generation_id {
            let body = Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::RebalanceInProgress.into(),
                protocol_type: self.state.protocol_type.clone(),
                protocol_name: self.state.protocol_name.clone(),
                assignment: Bytes::from_static(b""),
            };

            debug!("about to sleep");
            sleep(Duration::from_millis(PAUSE_MS)).await;
            debug!("done sleeping");

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
                protocol_type: self.state.protocol_type.clone(),
                protocol_name: self.state.protocol_name.clone(),
                assignment: Bytes::from_static(b""),
            };

            debug!("about to sleep");
            sleep(Duration::from_millis(PAUSE_MS)).await;
            debug!("done sleeping");

            return (self.into(), body);
        }

        let Some(assignments) = assignments else {
            let body = Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::RebalanceInProgress.into(),
                protocol_type: self.state.protocol_type.clone(),
                protocol_name: self.state.protocol_name.clone(),
                assignment: Bytes::from_static(b""),
            };

            debug!("about to sleep");
            sleep(Duration::from_millis(PAUSE_MS)).await;
            debug!("done sleeping");

            return (self.into(), body);
        };

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
            protocol_type: self.state.protocol_type.clone(),
            protocol_name: self.state.protocol_name.clone(),
            assignment: assignments
                .get(member_id)
                .cloned()
                .unwrap_or(Bytes::from_static(b"")),
        };

        let state = Inner {
            session_timeout_ms: self.session_timeout_ms,
            rebalance_timeout_ms: self.rebalance_timeout_ms,
            group_instance_id: self.group_instance_id,
            members: self.members,
            generation_id: self.generation_id,
            state: Formed {
                protocol_name: self.state.protocol_name.expect("protocol_name"),
                protocol_type: self.state.protocol_type.expect("protocol_type"),
                leader: member_id.to_owned(),
                assignments,
            },
            storage: self.storage,
            skip_assignment: self.skip_assignment,
        };

        debug!(?state);

        (state.into(), body)
    }

    async fn heartbeat(
        mut self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Self::HeartbeatState, Body) {
        debug!(
            ?now,
            ?group_id,
            ?generation_id,
            ?member_id,
            ?group_instance_id
        );

        let _ = group_instance_id;

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

        _ = self
            .members
            .entry(member_id.to_owned())
            .and_modify(|member| _ = member.last_contact.replace(now));

        if self.missed_heartbeat(group_id, now) || (generation_id < self.generation_id) {
            return (
                self,
                Body::HeartbeatResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::RebalanceInProgress.into(),
                },
            );
        }

        let body = Body::HeartbeatResponse {
            throttle_time_ms: Some(0),
            error_code: ErrorCode::None.into(),
        };

        (self, body)
    }

    async fn leave(
        mut self,
        now: SystemTime,
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

    async fn offset_commit(
        mut self,
        now: SystemTime,
        detail: &OffsetCommit<'_>,
    ) -> (Self::OffsetCommitState, Body) {
        let _ = now;

        match self.commit_offset(detail).await {
            Ok(body) => (self, body),
            Err(reason) => {
                debug!(?reason);
                (
                    self,
                    Body::OffsetCommitResponse {
                        throttle_time_ms: Some(0),
                        topics: detail.topics.map(|topics| {
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
                                                error_code: ErrorCode::UnknownMemberId.into(),
                                            })
                                            .collect()
                                    }),
                                })
                                .collect()
                        }),
                    },
                )
            }
        }
    }

    async fn offset_fetch(
        mut self,
        now: SystemTime,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Self::OffsetFetchState, Body) {
        let _ = now;
        match self
            .fetch_offset(group_id, topics, groups, require_stable)
            .await
        {
            Ok(body) => (self, body),
            Err(error) => {
                debug!(?error);
                todo!()
            }
        }
    }
}

#[async_trait::async_trait]
impl<O> Group for Inner<O, Formed>
where
    O: Storage,
{
    type JoinState = Wrapper<O>;
    type SyncState = Inner<O, Formed>;
    type HeartbeatState = Inner<O, Formed>;
    type LeaveState = Wrapper<O>;
    type OffsetCommitState = Inner<O, Formed>;
    type OffsetFetchState = Inner<O, Formed>;

    async fn join(
        mut self,
        now: SystemTime,
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
        let _ = group_id;
        let _ = session_timeout_ms;
        let _ = rebalance_timeout_ms;
        let _ = reason;

        if let Some(client_id) = client_id {
            if member_id.is_empty() {
                let member_id = format!("{client_id}-{}", Uuid::new_v4());

                let body = Body::JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::MemberIdRequired.into(),
                    generation_id: -1,
                    protocol_type: None,
                    protocol_name: Some("".into()),
                    leader: "".into(),
                    skip_assignment: self.skip_assignment,
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
                protocol_type: Some(protocol_type.into()),
                protocol_name: Some("".into()),
                leader: "".into(),
                skip_assignment: self.skip_assignment,
                member_id: "".into(),
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
                protocol_type: Some(protocol_type.into()),
                protocol_name: Some(self.state.protocol_name.clone()),
                leader: "".into(),
                skip_assignment: self.skip_assignment,
                member_id: "".into(),
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
                    let state: Wrapper<O> = self.into();

                    let body = {
                        let members = state.members();
                        let protocol_type = state.protocol_type().map(ToOwned::to_owned);
                        let protocol_name = state.protocol_name().map(ToOwned::to_owned);

                        Body::JoinGroupResponse {
                            throttle_time_ms: Some(0),
                            error_code: ErrorCode::None.into(),
                            generation_id: state.generation_id(),
                            protocol_type,
                            protocol_name,
                            leader: state
                                .leader()
                                .map(|s| s.to_owned())
                                .unwrap_or("".to_owned()),
                            skip_assignment: state.skip_assignment().map(ToOwned::to_owned),
                            member_id: member_id.into(),
                            members: Some(members),
                        }
                    };

                    (state, body)
                } else {
                    let state: Wrapper<O> = Inner {
                        generation_id: self.generation_id + 1,
                        session_timeout_ms: self.session_timeout_ms,
                        rebalance_timeout_ms: self.rebalance_timeout_ms,
                        group_instance_id: self.group_instance_id,
                        members: self.members,
                        state: Forming {
                            protocol_type: Some(self.state.protocol_type),
                            protocol_name: Some(self.state.protocol_name),
                            leader: Some(self.state.leader),
                        },
                        storage: self.storage,
                        skip_assignment: self.skip_assignment,
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
                                .unwrap_or("".to_owned()),
                            skip_assignment: self.skip_assignment,
                            member_id: member_id.into(),
                            members: Some(members),
                        }
                    };

                    (state, body)
                }
            }

            None => {
                let state: Wrapper<O> = Inner {
                    generation_id: self.generation_id + 1,
                    session_timeout_ms: self.session_timeout_ms,
                    rebalance_timeout_ms: self.rebalance_timeout_ms,
                    group_instance_id: self.group_instance_id,
                    members: self.members,
                    state: Forming {
                        protocol_type: Some(self.state.protocol_type),
                        protocol_name: Some(self.state.protocol_name),
                        leader: Some(self.state.leader),
                    },
                    storage: self.storage,
                    skip_assignment: self.skip_assignment,
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
                            .unwrap_or("".to_owned()),
                        skip_assignment: self.skip_assignment,
                        member_id: member_id.into(),
                        members: Some([].into()),
                    }
                };

                (state, body)
            }
        }
    }

    async fn sync(
        mut self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Self::SyncState, Body) {
        let _ = group_id;
        let _ = group_instance_id;
        let _ = protocol_type;
        let _ = protocol_name;
        let _ = assignments;

        if !self.members.contains_key(member_id) {
            let body = Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::UnknownMemberId.into(),
                protocol_type: Some(self.state.protocol_type.clone()),
                protocol_name: Some(self.state.protocol_name.clone()),
                assignment: Bytes::from_static(b""),
            };

            debug!("about to sleep");
            sleep(Duration::from_millis(PAUSE_MS)).await;
            debug!("done sleeping");

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

            debug!("about to sleep");
            sleep(Duration::from_millis(PAUSE_MS)).await;
            debug!("done sleeping");

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

            debug!("about to sleep");
            sleep(Duration::from_millis(PAUSE_MS)).await;
            debug!("done sleeping");

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

        (self, body)
    }

    async fn heartbeat(
        mut self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Self::HeartbeatState, Body) {
        debug!(?group_id, ?generation_id, ?member_id, ?group_instance_id);

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

        if self.missed_heartbeat(group_id, now) || (generation_id < self.generation_id) {
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

    async fn leave(
        mut self,
        now: SystemTime,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Self::LeaveState, Body) {
        let _ = now;
        let _ = group_id;

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

        let state: Wrapper<O> = if members
            .iter()
            .any(|member| member.error_code == i16::from(ErrorCode::None))
        {
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
                state: Forming {
                    protocol_type: Some(self.state.protocol_type),
                    protocol_name: Some(self.state.protocol_name),
                    leader,
                },
                storage: self.storage,
                skip_assignment: self.skip_assignment,
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

    async fn offset_commit(
        mut self,
        now: SystemTime,
        detail: &OffsetCommit<'_>,
    ) -> (Self::OffsetCommitState, Body) {
        let _ = now;

        match self.commit_offset(detail).await {
            Ok(body) => (self, body),
            Err(reason) => {
                debug!(?reason);
                (
                    self,
                    Body::OffsetCommitResponse {
                        throttle_time_ms: Some(0),
                        topics: detail.topics.map(|topics| {
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
                                                error_code: ErrorCode::UnknownMemberId.into(),
                                            })
                                            .collect()
                                    }),
                                })
                                .collect()
                        }),
                    },
                )
            }
        }
    }

    async fn offset_fetch(
        mut self,
        now: SystemTime,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Self::OffsetFetchState, Body) {
        let _ = now;

        match self
            .fetch_offset(group_id, topics, groups, require_stable)
            .await
        {
            Ok(body) => (self, body),
            Err(error) => {
                debug!(?error);
                todo!()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use pretty_assertions::assert_eq;
    use tansu_kafka_sans_io::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use tansu_storage::dynostore::DynoStore;
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

    #[tokio::test]
    async fn lifecycle() -> Result<()> {
        let _guard = init_tracing()?;

        let session_timeout_ms = 45_000;
        let rebalance_timeout_ms = Some(300_000);
        let group_instance_id = None;
        let reason = None;

        let cluster = "abc";
        let node = 12321;

        const CLIENT_ID: &str = "console-consumer";
        const GROUP_ID: &str = "test-consumer-group";
        const TOPIC: &str = "test";
        const RANGE: &str = "range";
        const COOPERATIVE_STICKY: &str = "cooperative-sticky";

        const PROTOCOL_TYPE: &str = "consumer";

        let mut s = Controller::with_storage(DynoStore::new(cluster, node, InMemory::new()))?;

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

        let first_member_id = match s
            .join(
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                "",
                group_instance_id,
                PROTOCOL_TYPE,
                Some(&protocols[..]),
                reason,
            )
            .await?
        {
            Body::JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code,
                generation_id: -1,
                protocol_type: None,
                protocol_name: Some(protocol_name),
                leader,
                skip_assignment: Some(false),
                members: Some(members),
                member_id,
            } => {
                assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
                assert_eq!("", protocol_name);
                assert!(leader.is_empty());
                assert!(member_id.starts_with(CLIENT_ID));
                assert_eq!(0, members.len());

                let join_response = s
                    .join(
                        Some(CLIENT_ID),
                        GROUP_ID,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        &member_id,
                        group_instance_id,
                        PROTOCOL_TYPE,
                        Some(&protocols[..]),
                        reason,
                    )
                    .await?;

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

                member_id
            }

            otherwise => panic!("{otherwise:?}"),
        };

        let first_member_assignment_01 = Bytes::from_static(b"assignment_01");

        let assignments = [SyncGroupRequestAssignment {
            member_id: first_member_id.clone(),
            assignment: first_member_assignment_01.clone(),
        }];

        assert_eq!(
            Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code: 0,
                protocol_type: Some(PROTOCOL_TYPE.into()),
                protocol_name: Some(RANGE.into()),
                assignment: first_member_assignment_01,
            },
            s.sync(
                GROUP_ID,
                0,
                &first_member_id,
                group_instance_id,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(&assignments),
            )
            .await?
        );

        assert_eq!(
            Body::HeartbeatResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::None.into(),
            },
            s.heartbeat(GROUP_ID, 0, &first_member_id, group_instance_id)
                .await?
        );

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

        let second_member_id = match s
            .join(
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                "",
                group_instance_id,
                PROTOCOL_TYPE,
                Some(&protocols[..]),
                reason,
            )
            .await?
        {
            Body::JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code,
                generation_id: -1,
                protocol_type: None,
                protocol_name: Some(protocol_name),
                leader,
                skip_assignment: Some(false),
                members: Some(members),
                member_id,
            } => {
                assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
                assert_eq!("", protocol_name);
                assert!(leader.is_empty());
                assert!(member_id.starts_with(CLIENT_ID));
                assert_eq!(0, members.len());

                let join_response = s
                    .join(
                        Some(CLIENT_ID),
                        GROUP_ID,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        &member_id,
                        group_instance_id,
                        PROTOCOL_TYPE,
                        Some(&protocols[..]),
                        reason,
                    )
                    .await?;

                let join_response_expected = Body::JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::None.into(),
                    generation_id: 1,
                    protocol_type: Some(PROTOCOL_TYPE.into()),
                    protocol_name: Some(RANGE.into()),
                    leader: first_member_id.clone(),
                    skip_assignment: Some(false),
                    member_id: member_id.clone(),
                    members: Some([].into()),
                };

                assert_eq!(join_response_expected, join_response);

                member_id
            }

            otherwise => panic!("{otherwise:?}"),
        };

        assert_eq!(
            Body::HeartbeatResponse {
                throttle_time_ms: Some(0),
                error_code: i16::from(ErrorCode::RebalanceInProgress),
            },
            s.heartbeat(GROUP_ID, 0, &first_member_id, group_instance_id,)
                .await?
        );

        assert_eq!(
            Body::OffsetCommitResponse {
                throttle_time_ms: Some(0),
                topics: Some(
                    [OffsetCommitResponseTopic {
                        name: TOPIC.into(),
                        partitions: Some(
                            (0..=2)
                                .map(|partition_index| OffsetCommitResponsePartition {
                                    partition_index,
                                    error_code: ErrorCode::None.into(),
                                })
                                .collect(),
                        ),
                    }]
                    .into()
                ),
            },
            s.offset_commit(OffsetCommit {
                group_id: GROUP_ID,
                generation_id_or_member_epoch: Some(0),
                member_id: Some(&first_member_id),
                group_instance_id,
                retention_time_ms: None,
                topics: Some(&[OffsetCommitRequestTopic {
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
            })
            .await?
        );

        {
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

            match s
                .join(
                    Some(CLIENT_ID),
                    GROUP_ID,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    &first_member_id,
                    group_instance_id,
                    PROTOCOL_TYPE,
                    Some(&protocols),
                    reason,
                )
                .await?
            {
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
                } => {
                    assert_eq!(i16::from(ErrorCode::None), error_code);
                    assert_eq!(2, generation_id);
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
                }

                otherwise => panic!("{otherwise:?}"),
            }
        }

        {
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

            match s
                .join(
                    Some(CLIENT_ID),
                    GROUP_ID,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    &second_member_id,
                    group_instance_id,
                    PROTOCOL_TYPE,
                    Some(&protocols),
                    reason,
                )
                .await?
            {
                Body::JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code,
                    generation_id,
                    protocol_type: Some(protocol_type),
                    protocol_name: Some(protocol_name),
                    leader,
                    skip_assignment: Some(false),
                    member_id,
                    members: Some(members),
                } => {
                    assert_eq!(i16::from(ErrorCode::None), error_code);
                    assert_eq!(2, generation_id);
                    assert_eq!(PROTOCOL_TYPE, protocol_type);
                    assert_eq!(RANGE, protocol_name);
                    assert_eq!(first_member_id, leader);
                    assert_eq!(second_member_id, member_id);
                    assert_eq!(0, members.len());
                }

                otherwise => panic!("{otherwise:?}"),
            }
        }

        let second_member_assignment_02 = Bytes::from_static(b"second_member_assignment_02");

        {
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

            assert_eq!(
                Body::SyncGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::None.into(),
                    protocol_type: Some(PROTOCOL_TYPE.into()),
                    protocol_name: Some(RANGE.into()),
                    assignment: first_member_assignment_02,
                },
                s.sync(
                    GROUP_ID,
                    2,
                    &first_member_id,
                    group_instance_id,
                    Some(PROTOCOL_TYPE),
                    Some(RANGE),
                    Some(&assignments),
                )
                .await?
            );
        }

        assert_eq!(
            Body::SyncGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::None.into(),
                protocol_type: Some(PROTOCOL_TYPE.into()),
                protocol_name: Some(RANGE.into()),
                assignment: second_member_assignment_02,
            },
            s.sync(
                GROUP_ID,
                2,
                &second_member_id,
                group_instance_id,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(&[]),
            )
            .await?
        );

        assert_eq!(
            Body::HeartbeatResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::None.into(),
            },
            s.heartbeat(GROUP_ID, 2, &first_member_id, group_instance_id,)
                .await?
        );

        assert_eq!(
            Body::HeartbeatResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::None.into(),
            },
            s.heartbeat(GROUP_ID, 2, &second_member_id, group_instance_id,)
                .await?
        );

        assert_eq!(
            Body::LeaveGroupResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::None.into(),
                members: Some(
                    [MemberResponse {
                        member_id: first_member_id.clone(),
                        group_instance_id: None,
                        error_code: ErrorCode::None.into(),
                    }]
                    .into()
                ),
            },
            s.leave(
                GROUP_ID,
                None,
                Some(&[MemberIdentity {
                    member_id: first_member_id.clone(),
                    group_instance_id: None,
                    reason: Some("the consumer is being closed".into()),
                }]),
            )
            .await?
        );

        assert_eq!(
            Body::HeartbeatResponse {
                throttle_time_ms: Some(0),
                error_code: ErrorCode::RebalanceInProgress.into(),
            },
            s.heartbeat(GROUP_ID, 2, &second_member_id, group_instance_id,)
                .await?
        );

        {
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

            assert_eq!(
                Body::JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::None.into(),
                    generation_id: 3,
                    protocol_type: Some(PROTOCOL_TYPE.into()),
                    protocol_name: Some(RANGE.into()),
                    leader: second_member_id.clone(),
                    skip_assignment: Some(false),
                    member_id: second_member_id.clone(),
                    members: Some(
                        [JoinGroupResponseMember {
                            member_id: second_member_id.clone(),
                            group_instance_id: None,
                            metadata: second_member_range_meta.clone(),
                        },]
                        .into()
                    ),
                },
                s.join(
                    Some(CLIENT_ID),
                    GROUP_ID,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    &second_member_id,
                    group_instance_id,
                    PROTOCOL_TYPE,
                    Some(&protocols),
                    reason,
                )
                .await?
            );
        }

        {
            let second_member_assignment_03 = Bytes::from_static(b"second_member_assignment_03");

            let assignments = [SyncGroupRequestAssignment {
                member_id: second_member_id.clone(),
                assignment: second_member_assignment_03.clone(),
            }];

            assert_eq!(
                Body::SyncGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code: ErrorCode::None.into(),
                    protocol_type: Some(PROTOCOL_TYPE.into()),
                    protocol_name: Some(RANGE.into()),
                    assignment: second_member_assignment_03,
                },
                s.sync(
                    GROUP_ID,
                    3,
                    &second_member_id,
                    group_instance_id,
                    Some(PROTOCOL_TYPE),
                    Some(RANGE),
                    Some(&assignments),
                )
                .await?
            );
        }

        Ok(())
    }
}
