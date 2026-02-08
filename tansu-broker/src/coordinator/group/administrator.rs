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

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    hash::{Hash, Hasher},
    marker::PhantomData,
    ops::Deref,
    sync::LazyLock,
    time::SystemTime,
};

use async_trait::async_trait;
use bytes::Bytes;
use opentelemetry::{KeyValue, metrics::Counter};
use tansu_sans_io::{
    Body, ErrorCode,
    heartbeat_response::HeartbeatResponse,
    join_group_request::JoinGroupRequestProtocol,
    join_group_response::{JoinGroupResponse, JoinGroupResponseMember},
    leave_group_request::MemberIdentity,
    leave_group_response::{LeaveGroupResponse, MemberResponse},
    offset_commit_response::{
        OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    },
    offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopic},
    offset_fetch_response::{
        OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
        OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
    },
    sync_group_request::SyncGroupRequestAssignment,
    sync_group_response::SyncGroupResponse,
};
use tansu_storage::{
    GroupDetail, GroupMember, GroupState, OffsetCommitRequest, Storage, Topition, UpdateError,
    Version,
};
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::{Error, METER, Result};

use super::{Coordinator, OffsetCommit};

const PAUSE_MS: u128 = 3_000;

static COORDINATOR_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_group_coordinator_requests")
        .with_description("consumer group coordinator requests")
        .build()
});

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
pub enum Wrapper<O> {
    Forming(Inner<O, Forming>),
    Formed(Inner<O, Formed>),
}

impl<O> PartialEq for Wrapper<O> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Forming(sinner), Self::Forming(oinner)) => sinner == oinner,
            (Self::Formed(sinner), Self::Formed(oinner)) => sinner == oinner,
            _ => false,
        }
    }
}

impl<O> Hash for Wrapper<O>
where
    O: Storage,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Forming(inner) => {
                state.write_u8(3);
                inner.hash(state)
            }
            Self::Formed(inner) => {
                state.write_u8(5);
                inner.hash(state)
            }
        }
    }
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
                members,
                generation_id,
                state,
                skip_assignment,
                inception,
                ..
            }) => GroupDetail {
                session_timeout_ms: *session_timeout_ms,
                rebalance_timeout_ms: *rebalance_timeout_ms,
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
                inception: *inception,
                state: GroupState::Forming {
                    protocol_type: state.protocol_type.clone(),
                    protocol_name: state.protocol_name.clone(),
                    leader: state.leader.clone(),
                },
            },
            Wrapper::Formed(Inner {
                session_timeout_ms,
                rebalance_timeout_ms,
                members,
                generation_id,
                state,
                skip_assignment,
                inception,
                ..
            }) => GroupDetail {
                session_timeout_ms: *session_timeout_ms,
                rebalance_timeout_ms: *rebalance_timeout_ms,
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
                inception: *inception,
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
                mut leader,
            } => {
                if let Some(ref leader_id) = leader
                    && !gd
                        .members
                        .iter()
                        .any(|(member_id, _)| member_id == leader_id)
                {
                    _ = leader.take();
                }

                Self::Forming(Inner {
                    session_timeout_ms: gd.session_timeout_ms,
                    rebalance_timeout_ms: gd.rebalance_timeout_ms,
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
                    inception: gd.inception,
                })
            }
            GroupState::Formed {
                protocol_type,
                protocol_name,
                leader,
                assignments,
            } => Self::Formed(Inner {
                session_timeout_ms: gd.session_timeout_ms,
                rebalance_timeout_ms: gd.rebalance_timeout_ms,
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
                inception: gd.inception,
            }),
        }
    }

    pub fn generation_id(&self) -> i32 {
        match self {
            Self::Forming(inner) => inner.generation_id,
            Self::Formed(inner) => inner.generation_id,
        }
    }

    pub fn inception(&self) -> SystemTime {
        match self {
            Self::Forming(inner) => inner.inception,
            Self::Formed(inner) => inner.inception,
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
            Self::Forming(inner) => inner
                .members
                .values()
                .cloned()
                .map(|member| member.join_response)
                .collect(),

            Self::Formed(inner) => inner
                .members
                .values()
                .cloned()
                .map(|member| member.join_response)
                .collect(),
        }
    }

    fn is_forming(&self) -> bool {
        matches!(self, Self::Forming(..))
    }

    #[cfg(test)]
    fn assignments(&self) -> Option<BTreeMap<String, Bytes>> {
        match self {
            Self::Forming(..) => None,
            Self::Formed(inner) => Some(inner.state.assignments.clone()),
        }
    }

    fn missed_heartbeat(self, group_id: &str, now: SystemTime) -> Self {
        debug!(?group_id, ?now);

        match self {
            Self::Forming(mut inner) => {
                _ = inner.missed_heartbeat(group_id, now);
                Self::Forming(inner)
            }
            Self::Formed(mut inner) => {
                if inner.missed_heartbeat(group_id, now) {
                    info!("missed heartbeat for {group_id} in {}", inner.generation_id);

                    Self::Forming(Inner {
                        session_timeout_ms: inner.session_timeout_ms,
                        rebalance_timeout_ms: inner.rebalance_timeout_ms,
                        members: inner.members,
                        generation_id: inner.generation_id,
                        state: Forming {
                            protocol_type: Some(inner.state.protocol_type),
                            protocol_name: Some(inner.state.protocol_name),
                            leader: None,
                        },
                        storage: inner.storage,
                        skip_assignment: inner.skip_assignment,
                        inception: inner.inception,
                    })
                } else {
                    Self::Formed(inner)
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
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner
                    .heartbeat(now, group_id, generation_id, member_id, group_instance_id)
                    .await;
                (state.into(), body)
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct Controller<O> {
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
    O: Storage,
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

        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "join")]);

        let started_at = SystemTime::now();

        let mut iteration = 0;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "join_loop")]);

            let now = SystemTime::now();

            let (mut original, version) = self.wrappers.remove(group_id).unwrap_or_else(|| {
                debug!(?iteration, ?group_id);

                let inner = Inner {
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    members: Default::default(),
                    generation_id: -1,
                    state: Forming::default(),
                    skip_assignment: Some(false),
                    storage: self.storage.clone(),
                    inception: SystemTime::now(),
                };

                (Wrapper::Forming(inner), None)
            });

            if group_instance_id.is_none() {
                original = original.missed_heartbeat(group_id, now);
            }

            if iteration == 0
                && !member_id.is_empty()
                && original.leader().is_some_and(|leader| leader != member_id)
                && group_instance_id.is_none()
            {
                debug!(?member_id);
                COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "join_follower_pause")]);
                sleep(Duration::from_millis(PAUSE_MS as u64)).await;
            }

            debug!(?group_id, ?original, ?version, ?iteration);

            let (updated, body) = original
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

            debug!(group_id, ?updated, ?version, iteration,);

            match self
                .storage
                .update_group(group_id, GroupDetail::from(&updated), version)
                .await
            {
                Ok(version) => {
                    let elapsed = SystemTime::now()
                        .duration_since(started_at)
                        .map(|duration| duration.as_millis())
                        .unwrap_or(0);

                    debug!(
                        group_id,
                        ?version,
                        iteration,
                        elapsed,
                        is_forming = updated.is_forming()
                    );

                    _ = self
                        .wrappers
                        .insert(group_id.to_owned(), (updated, Some(version)));

                    if group_instance_id.is_some() && elapsed < PAUSE_MS {
                        let pause = PAUSE_MS.saturating_sub(elapsed);
                        debug!(pause);

                        COORDINATOR_REQUESTS
                            .add(1, &[KeyValue::new("method", "join_group_instance_pause")]);
                        sleep(Duration::from_millis(pause as u64)).await;

                        iteration += 1;
                        continue;
                    } else {
                        return Ok(body);
                    }
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(group_id, ?current, ?version, iteration);

                    COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "join_outdated")]);

                    _ = self.wrappers.insert(
                        group_id.to_owned(),
                        (
                            Wrapper::with_storage_group_detail(self.storage.clone(), current),
                            Some(version),
                        ),
                    );

                    iteration += 1;
                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),

                Err(UpdateError::MissingEtag) => {
                    return Err(Error::Message(String::from("missing e-tag")));
                }

                Err(UpdateError::Uuid(uuid)) => {
                    return Err(Error::Message(format!("uuid: {uuid}")));
                }
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

        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "sync")]);

        let started_at = SystemTime::now();

        let mut iteration = 0;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "sync_loop")]);

            let now = SystemTime::now();

            let (mut original, version) = self
                .wrappers
                .remove(group_id)
                .unwrap_or((Wrapper::Forming(Inner::new(self.storage.clone())), None));

            debug!(?group_id, ?original, ?version, ?iteration);

            if group_instance_id.is_none() {
                original = original.missed_heartbeat(group_id, now);
            }

            let (updated, body) = original
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

            debug!(group_id, ?updated, ?version, iteration,);
            match self
                .storage
                .update_group(group_id, GroupDetail::from(&updated), version)
                .await
            {
                Ok(version) => {
                    let elapsed = SystemTime::now()
                        .duration_since(started_at)
                        .map(|duration| duration.as_millis())
                        .unwrap_or(0);

                    debug!(
                        group_id,
                        ?version,
                        iteration,
                        elapsed,
                        is_forming = updated.is_forming()
                    );

                    _ = self
                        .wrappers
                        .insert(group_id.to_owned(), (updated, Some(version)));

                    if group_instance_id.is_some() && elapsed < PAUSE_MS {
                        let pause = PAUSE_MS.saturating_sub(elapsed);
                        debug!(pause);

                        COORDINATOR_REQUESTS
                            .add(1, &[KeyValue::new("method", "sync_group_instance_pause")]);
                        sleep(Duration::from_millis(pause as u64)).await;

                        iteration += 1;
                        continue;
                    } else {
                        return Ok(body);
                    }
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(?group_id, ?current, ?version);
                    COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "sync_outdated")]);

                    _ = self.wrappers.insert(
                        group_id.to_owned(),
                        (
                            Wrapper::with_storage_group_detail(self.storage.clone(), current),
                            Some(version),
                        ),
                    );

                    iteration += 1;
                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),

                Err(UpdateError::MissingEtag) => {
                    return Err(Error::Message(String::from("missing e-tag")));
                }

                Err(UpdateError::Uuid(uuid)) => {
                    return Err(Error::Message(format!("uuid: {uuid}")));
                }
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

        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "leave")]);

        let mut iteration = 0;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "leave_loop")]);

            let (wrapper, version) = self
                .wrappers
                .remove(group_id)
                .unwrap_or((Wrapper::Forming(Inner::new(self.storage.clone())), None));

            debug!(?group_id, ?wrapper, ?version, ?iteration);

            let now = SystemTime::now();
            let wrapper = wrapper.missed_heartbeat(group_id, now);

            let (wrapper, body) = wrapper.leave(now, group_id, member_id, members).await;
            debug!(group_id, ?wrapper, ?version, iteration,);

            match self
                .storage
                .update_group(group_id, GroupDetail::from(&wrapper), version)
                .await
            {
                Ok(version) => {
                    debug!(?group_id, ?version);

                    _ = self
                        .wrappers
                        .insert(group_id.to_owned(), (wrapper, Some(version)));

                    return Ok(body);
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(?group_id, ?current, ?version);
                    COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "leave_outdated")]);

                    _ = self.wrappers.insert(
                        group_id.to_owned(),
                        (
                            Wrapper::with_storage_group_detail(self.storage.clone(), current),
                            Some(version),
                        ),
                    );

                    iteration += 1;
                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),

                Err(UpdateError::MissingEtag) => {
                    return Err(Error::Message(String::from("missing e-tag")));
                }

                Err(UpdateError::Uuid(uuid)) => {
                    return Err(Error::Message(format!("uuid: {uuid}")));
                }
            }
        }
    }

    async fn offset_commit(&mut self, offset_commit: OffsetCommit<'_>) -> Result<Body> {
        debug!(?offset_commit);
        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "offset_commit")]);

        let group_id = offset_commit.group_id;
        let mut iteration = 0;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "offset_commit_loop")]);

            let (wrapper, version) = self
                .wrappers
                .remove(group_id)
                .unwrap_or((Wrapper::Forming(Inner::new(self.storage.clone())), None));

            debug!(?group_id, ?wrapper, ?version, ?iteration);

            let now = SystemTime::now();

            let (wrapper, body) = wrapper.offset_commit(now, &offset_commit).await;
            debug!(group_id, ?wrapper, ?version, iteration,);

            match self
                .storage
                .update_group(group_id, GroupDetail::from(&wrapper), version)
                .await
            {
                Ok(version) => {
                    debug!(?group_id, ?version);

                    _ = self
                        .wrappers
                        .insert(group_id.to_owned(), (wrapper, Some(version)));

                    return Ok(body);
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(?group_id, ?current, ?version);
                    COORDINATOR_REQUESTS
                        .add(1, &[KeyValue::new("method", "offset_commit_outdated")]);

                    _ = self.wrappers.insert(
                        group_id.to_owned(),
                        (
                            Wrapper::with_storage_group_detail(self.storage.clone(), current),
                            Some(version),
                        ),
                    );

                    iteration += 1;
                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),

                Err(UpdateError::MissingEtag) => {
                    return Err(Error::Message(String::from("missing e-tag")));
                }

                Err(UpdateError::Uuid(uuid)) => {
                    return Err(Error::Message(format!("uuid: {uuid}")));
                }
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
        debug!(?group_id, ?topics, ?groups, ?require_stable);
        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "offset_fetch")]);

        let wrapper = Wrapper::Forming(Inner::new(self.storage.clone()));

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
        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "heartbeat")]);

        let mut iteration = 0;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "heartbeat_loop")]);

            let (wrapper, version) = self
                .wrappers
                .remove(group_id)
                .unwrap_or((Wrapper::Forming(Inner::new(self.storage.clone())), None));

            debug!(?group_id, ?wrapper, ?version, ?iteration);

            let now = SystemTime::now();

            let (mut wrapper, body) = wrapper
                .heartbeat(now, group_id, generation_id, member_id, group_instance_id)
                .await;

            if group_instance_id.is_none() {
                wrapper = wrapper.missed_heartbeat(group_id, now);
            }

            debug!(group_id, ?wrapper, ?version, iteration,);

            match self
                .storage
                .update_group(group_id, GroupDetail::from(&wrapper), version)
                .await
            {
                Ok(version) => {
                    debug!(?group_id, ?version);

                    _ = self
                        .wrappers
                        .insert(group_id.to_owned(), (wrapper, Some(version)));

                    return Ok(body);
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(?group_id, ?current, ?version);
                    COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "heartbeat_outdated")]);

                    _ = self.wrappers.insert(
                        group_id.to_owned(),
                        (
                            Wrapper::with_storage_group_detail(self.storage.clone(), current),
                            Some(version),
                        ),
                    );

                    iteration += 1;
                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),

                Err(UpdateError::MissingEtag) => {
                    return Err(Error::Message(String::from("missing e-tag")));
                }

                Err(UpdateError::Uuid(uuid)) => {
                    return Err(Error::Message(format!("uuid: {uuid}")));
                }
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
    members: BTreeMap<String, Member>,
    generation_id: i32,
    state: S,
    storage: O,
    skip_assignment: Option<bool>,
    inception: SystemTime,
}

impl<O, S> PartialEq for Inner<O, S>
where
    S: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.session_timeout_ms == other.session_timeout_ms
            && self.rebalance_timeout_ms == other.rebalance_timeout_ms
            && self.members == other.members
            && self.generation_id == other.generation_id
            && self.state == other.state
            && self.skip_assignment == other.skip_assignment
            && self.inception == other.inception
    }
}

impl<O, S> Hash for Inner<O, S>
where
    S: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.session_timeout_ms.hash(state);
        self.rebalance_timeout_ms.hash(state);
        self.members.hash(state);
        self.generation_id.hash(state);
        self.state.hash(state);
        self.skip_assignment.hash(state);
        self.inception.hash(state);
    }
}

impl<O> Inner<O, PhantomData<Forming>>
where
    O: Storage,
{
    pub fn new(storage: O) -> Inner<O, Forming> {
        Inner {
            session_timeout_ms: Default::default(),
            rebalance_timeout_ms: Default::default(),
            members: Default::default(),
            generation_id: -1,
            state: Forming::default(),
            skip_assignment: Some(false),
            storage,
            inception: SystemTime::now(),
        }
    }
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
        debug!(?group_id, ?topics, ?groups, ?require_stable);

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
                        .map(|topic_name| {
                            OffsetFetchResponseTopic::default()
                                .name((*topic_name).into())
                                .partitions(Some(
                                    offsets
                                        .iter()
                                        .filter_map(|(topition, offset)| {
                                            if topition.topic() == *topic_name {
                                                Some(
                                                    OffsetFetchResponsePartition::default()
                                                        .partition_index(topition.partition())
                                                        .committed_offset(*offset)
                                                        .committed_leader_epoch(Some(-1))
                                                        .metadata(Some("".into()))
                                                        .error_code(ErrorCode::None.into()),
                                                )
                                            } else {
                                                None
                                            }
                                        })
                                        .collect(),
                                ))
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
                debug!(?group);

                let response = if let Some(topics) = group.topics.as_ref().map(|topics| {
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
                    self.storage
                        .offset_fetch(
                            Some(group.group_id.as_str()),
                            topics.deref(),
                            require_stable,
                        )
                        .await
                        .inspect(|offsets| debug!(?offsets))
                        .inspect_err(|err| error!(?err, ?group))
                } else {
                    self.storage
                        .committed_offset_topitions(&group.group_id)
                        .await
                        .inspect(|offsets| debug!(?offsets))
                        .inspect_err(|err| error!(?err, ?group))
                }
                .map(|offsets| {
                    OffsetFetchResponseGroup::default()
                        .group_id(group.group_id.clone())
                        .topics(Some(
                            offsets
                                .iter()
                                .fold(BTreeSet::new(), |mut topics, (topition, _)| {
                                    _ = topics.insert(topition.topic());
                                    topics
                                })
                                .iter()
                                .map(|topic_name| {
                                    OffsetFetchResponseTopics::default()
                                        .name((*topic_name).into())
                                        .partitions(Some(
                                            offsets
                                                .iter()
                                                .filter_map(|(topition, offset)| {
                                                    if topition.topic() == *topic_name {
                                                        Some(
                                                            OffsetFetchResponsePartitions::default(
                                                            )
                                                            .partition_index(topition.partition())
                                                            .committed_offset(*offset)
                                                            .committed_leader_epoch(-1)
                                                            .metadata(None)
                                                            .error_code(ErrorCode::None.into()),
                                                        )
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect(),
                                        ))
                                })
                                .collect(),
                        ))
                        .error_code(ErrorCode::None.into())
                })?;

                responses.push(response);
            }

            Some(responses)
        } else {
            None
        };

        Ok(OffsetFetchResponse::default()
            .throttle_time_ms(Some(0))
            .topics(topics)
            .error_code(Some(ErrorCode::None.into()))
            .groups(groups)
            .into())
    }

    async fn commit_offset(&mut self, detail: &OffsetCommit<'_>) -> Result<Body> {
        let retention_time_ms = detail.retention_time_ms.map_or(Ok(None), |ms| {
            u64::try_from(ms)
                .map(Duration::from_millis)
                .map_err(Error::from)
                .map(Some)
        })?;

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
                        .map(|topic_name| {
                            OffsetCommitResponseTopic::default()
                                .name((*topic_name).into())
                                .partitions(Some(
                                    value
                                        .iter()
                                        .filter_map(|(topition, error_code)| {
                                            if topition.topic() == *topic_name {
                                                Some(
                                                    OffsetCommitResponsePartition::default()
                                                        .partition_index(topition.partition())
                                                        .error_code(i16::from(*error_code)),
                                                )
                                            } else {
                                                None
                                            }
                                        })
                                        .collect(),
                                ))
                        })
                        .collect();

                    OffsetCommitResponse::default()
                        .throttle_time_ms(Some(0))
                        .topics(Some(topics))
                        .into()
                })
                .inspect_err(|err| error!(?err))
                .map_err(Into::into)
        } else {
            Ok(OffsetCommitResponse::default()
                .throttle_time_ms(Some(0))
                .topics(detail.topics.map(|topics| {
                    topics
                        .as_ref()
                        .iter()
                        .map(|topic| {
                            OffsetCommitResponseTopic::default()
                                .name(topic.name.clone())
                                .partitions(topic.partitions.as_ref().map(|partitions| {
                                    partitions
                                        .iter()
                                        .map(|partition| {
                                            OffsetCommitResponsePartition::default()
                                                .partition_index(partition.partition_index)
                                                .error_code(ErrorCode::UnknownMemberId.into())
                                        })
                                        .collect()
                                }))
                        })
                        .collect()
                }))
                .into())
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
        debug!(
            client_id,
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            ?protocols,
            reason
        );

        let Some(protocols) = protocols else {
            debug!(join_outcome = ?ErrorCode::InvalidRequest);

            let join_group_response = JoinGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::InvalidRequest.into())
                .generation_id(self.generation_id)
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(Some("".into()))
                .leader("".into())
                .skip_assignment(self.skip_assignment)
                .member_id("".into())
                .members(Some([].into()));

            return (self, join_group_response.into());
        };

        let protocol = if let Some(protocol_name) = self.state.protocol_name.as_deref() {
            debug!(protocol_name);

            if let Some(protocol) = protocols
                .iter()
                .find(|protocol| protocol.name == protocol_name)
            {
                debug!(?protocol);

                protocol
            } else {
                debug!(join_outcome = ?ErrorCode::InconsistentGroupProtocol);

                let join_group_response = JoinGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::InconsistentGroupProtocol.into())
                    .generation_id(self.generation_id)
                    .protocol_type(Some(protocol_type.into()))
                    .protocol_name(self.state.protocol_name.clone())
                    .leader("".into())
                    .skip_assignment(self.skip_assignment)
                    .member_id("".into())
                    .members(Some([].into()));

                return (self, join_group_response.into());
            }
        } else {
            self.state.protocol_type = Some(protocol_type.to_owned());
            self.state.protocol_name = Some(protocols[0].name.as_str().to_owned());

            self.session_timeout_ms = session_timeout_ms;
            self.rebalance_timeout_ms = rebalance_timeout_ms;

            &protocols[0]
        };

        if member_id.is_empty() && group_instance_id.is_none() {
            let member_id = if let Some(client_id) = client_id {
                format!("{client_id}-{}", Uuid::new_v4())
            } else {
                format!("{}", Uuid::new_v4())
            };
            debug!(?member_id, join_outcome = ?ErrorCode::MemberIdRequired);

            let join_group_response = JoinGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::MemberIdRequired.into())
                .generation_id(-1)
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(Some("".into()))
                .leader("".into())
                .skip_assignment(self.skip_assignment)
                .member_id(member_id.clone())
                .members(Some([].into()));

            _ = self.members.insert(
                member_id.clone(),
                Member {
                    join_response: JoinGroupResponseMember::default()
                        .member_id(member_id)
                        .group_instance_id(group_instance_id.map(|s| s.to_owned()))
                        .metadata(protocol.metadata.clone()),
                    last_contact: Some(now),
                },
            );

            self.generation_id += 1;

            return (self, join_group_response.into());
        }

        let member_id = group_instance_id.map_or(member_id.to_owned(), |group_instance_id| {
            if member_id.is_empty() {
                if let Some((member_id, _)) = self.members.iter().find(|(_, member)| {
                    member.join_response.group_instance_id.as_deref() == Some(group_instance_id)
                }) {
                    member_id.into()
                } else {
                    format!("{group_instance_id}-{}", Uuid::new_v4())
                }
            } else {
                member_id.into()
            }
        });

        debug!(?member_id, ?self.members);

        if let Some(member) = self.members.get_mut(&member_id) {
            if member.join_response.metadata == protocol.metadata {
                debug!(
                    member_metadata = "existing",
                    member_id,
                    generation_id = self.generation_id
                );
            } else if group_instance_id.is_some() {
                debug!(
                    member_metadata = "soft_update",
                    member_id,
                    group_instance_id,
                    updated = ?protocol.metadata,
                    existing = ?member.join_response.metadata,
                    generation_id = self.generation_id
                );

                member.join_response.metadata = protocol.metadata.clone();
            } else {
                self.generation_id += 1;

                debug!(
                    member_metadata = "update",
                    member_id,
                    updated = ?protocol.metadata,
                    existing = ?member.join_response.metadata,
                    generation_id = self.generation_id
                );

                member.join_response.metadata = protocol.metadata.clone();
            }
        } else {
            self.generation_id += 1;

            debug!(
                member_metadata = "new",
                member_id,
                generation_id = self.generation_id
            );

            _ = self.members.insert(
                member_id.clone(),
                Member {
                    join_response: JoinGroupResponseMember::default()
                        .member_id(member_id.to_string())
                        .group_instance_id(group_instance_id.map(|s| s.to_owned()))
                        .metadata(protocol.metadata.clone()),
                    last_contact: Some(now),
                },
            );
        }

        debug!(?member_id, ?self.members);

        if self.state.leader.is_none() {
            info!(member_id, group_id, self.generation_id);

            _ = self.state.leader.replace(member_id.clone());
        }

        let join_group_response = JoinGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .generation_id(self.generation_id)
            .protocol_type(self.state.protocol_type.clone())
            .protocol_name(self.state.protocol_name.clone())
            .leader(
                self.state
                    .leader
                    .as_ref()
                    .map_or(String::from(""), |leader| leader.clone()),
            )
            .skip_assignment(self.skip_assignment)
            .members(Some(
                if self
                    .state
                    .leader
                    .as_ref()
                    .is_some_and(|leader| leader == member_id.as_str())
                {
                    self.members
                        .values()
                        .cloned()
                        .map(|member| member.join_response)
                        .collect()
                } else {
                    [].into()
                },
            ))
            .member_id(member_id);

        debug!(join_outcome = ?ErrorCode::None);

        (self, join_group_response.into())
    }

    async fn sync(
        mut self,
        _now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Self::SyncState, Body) {
        debug!(
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            protocol_type,
            protocol_name,
            ?assignments
        );

        if !self.members.contains_key(member_id) {
            debug!(?self.members, sync_outcome = ?ErrorCode::UnknownMemberId);

            let sync_group_response = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::UnknownMemberId.into())
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(self.state.protocol_name.clone())
                .assignment(Bytes::from_static(b""));

            return (self.into(), sync_group_response.into());
        }

        debug!(?member_id);

        if generation_id > self.generation_id {
            debug!(self.generation_id, sync_outcome = ?ErrorCode::IllegalGeneration);

            let sync_group_response = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::IllegalGeneration.into())
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(self.state.protocol_name.clone())
                .assignment(Bytes::from_static(b""));

            return (self.into(), sync_group_response.into());
        }

        if generation_id < self.generation_id {
            debug!(self.generation_id, sync_outcome = ?ErrorCode::RebalanceInProgress);

            let sync_group_response = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::RebalanceInProgress.into())
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(self.state.protocol_name.clone())
                .assignment(Bytes::from_static(b""));

            return (self.into(), sync_group_response.into());
        }

        if self
            .state
            .leader
            .as_ref()
            .is_some_and(|leader_id| member_id != leader_id.as_str())
        {
            debug!(?self.state.leader, sync_outcome = ?ErrorCode::RebalanceInProgress);

            let sync_group_response = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::RebalanceInProgress.into())
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(self.state.protocol_name.clone())
                .assignment(Bytes::from_static(b""));

            return (self.into(), sync_group_response.into());
        }

        let Some(assignments) = assignments else {
            debug!(sync_outcome = ?ErrorCode::RebalanceInProgress);

            let sync_group_response = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::RebalanceInProgress.into())
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(self.state.protocol_name.clone())
                .assignment(Bytes::from_static(b""));

            return (self.into(), sync_group_response.into());
        };

        let assignments = assignments
            .iter()
            .fold(BTreeMap::new(), |mut acc, assignment| {
                _ = acc.insert(assignment.member_id.clone(), assignment.assignment.clone());
                acc
            });

        debug!(?assignments);

        let sync_group_response = SyncGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .protocol_type(self.state.protocol_type.clone())
            .protocol_name(self.state.protocol_name.clone())
            .assignment(
                assignments
                    .get(member_id)
                    .cloned()
                    .unwrap_or(Bytes::from_static(b"")),
            );

        debug!(sync_outcome = ?ErrorCode::None, sync_assignment = assignments.contains_key(member_id));

        let state = Inner {
            session_timeout_ms: self.session_timeout_ms,
            rebalance_timeout_ms: self.rebalance_timeout_ms,

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
            inception: self.inception,
        };

        (state.into(), sync_group_response.into())
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
            debug!(?self.members);

            return (
                self,
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::UnknownMemberId.into())
                    .into(),
            );
        }

        if generation_id > self.generation_id {
            debug!(?self.generation_id);

            return (
                self,
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::IllegalGeneration.into())
                    .into(),
            );
        }

        _ = self
            .members
            .entry(member_id.to_owned())
            .and_modify(|member| _ = member.last_contact.replace(now));

        if self.missed_heartbeat(group_id, now) || (generation_id < self.generation_id) {
            debug!(self.generation_id);

            return (
                self,
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::RebalanceInProgress.into())
                    .into(),
            );
        }

        let body = HeartbeatResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .into();

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
        debug!(?group_id, member_id, ?members);

        let members = if let Some(member_id) = member_id {
            debug!(member_id);

            vec![
                MemberResponse::default()
                    .member_id(member_id.to_owned())
                    .group_instance_id(None)
                    .error_code({
                        if self.members.remove(member_id).is_some() {
                            ErrorCode::None.into()
                        } else {
                            ErrorCode::UnknownMemberId.into()
                        }
                    }),
            ]
        } else {
            members.map_or(vec![], |members| {
                members
                    .iter()
                    .map(|member| {
                        MemberResponse::default()
                            .member_id(member.member_id.clone())
                            .group_instance_id(member.group_instance_id.clone())
                            .error_code({
                                if self.members.remove(&member.member_id).is_some() {
                                    ErrorCode::None.into()
                                } else {
                                    ErrorCode::UnknownMemberId.into()
                                }
                            })
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

        let body = LeaveGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .members(Some(members))
            .into();

        (self, body)
    }

    async fn offset_commit(
        mut self,
        now: SystemTime,
        detail: &OffsetCommit<'_>,
    ) -> (Self::OffsetCommitState, Body) {
        let _ = now;
        debug!(?detail);

        match self.commit_offset(detail).await {
            Ok(body) => (self, body),
            Err(reason) => {
                debug!(?reason);
                (
                    self,
                    OffsetCommitResponse::default()
                        .throttle_time_ms(Some(0))
                        .topics(detail.topics.map(|topics| {
                            topics
                                .as_ref()
                                .iter()
                                .map(|topic| {
                                    OffsetCommitResponseTopic::default()
                                        .name(topic.name.clone())
                                        .partitions(topic.partitions.as_ref().map(|partitions| {
                                            partitions
                                                .iter()
                                                .map(|partition| {
                                                    OffsetCommitResponsePartition::default()
                                                        .partition_index(partition.partition_index)
                                                        .error_code(
                                                            ErrorCode::UnknownMemberId.into(),
                                                        )
                                                })
                                                .collect()
                                        }))
                                })
                                .collect()
                        }))
                        .into(),
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
        debug!(group_id, ?topics, ?groups, ?require_stable);
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
        debug!(
            client_id,
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            ?protocols,
            reason
        );

        let Some(protocols) = protocols else {
            debug!(join_outcome = ?ErrorCode::InvalidRequest);

            let join_group_response = JoinGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::InvalidRequest.into())
                .generation_id(self.generation_id)
                .protocol_type(Some(protocol_type.into()))
                .protocol_name(Some("".into()))
                .leader("".into())
                .skip_assignment(self.skip_assignment)
                .member_id("".into())
                .members(Some([].into()));

            return (self.into(), join_group_response.into());
        };

        let Some(protocol) = protocols
            .iter()
            .find(|protocol| protocol.name == self.state.protocol_name)
        else {
            debug!(join_outcome = ?ErrorCode::InconsistentGroupProtocol);

            let join_group_response = JoinGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::InconsistentGroupProtocol.into())
                .generation_id(self.generation_id)
                .protocol_type(Some(protocol_type.into()))
                .protocol_name(Some(self.state.protocol_name.clone()))
                .leader("".into())
                .skip_assignment(self.skip_assignment)
                .member_id("".into())
                .members(Some([].into()));

            return (self.into(), join_group_response.into());
        };

        if member_id.is_empty() && group_instance_id.is_none() {
            let member_id = if let Some(client_id) = client_id {
                format!("{client_id}-{}", Uuid::new_v4())
            } else {
                format!("{}", Uuid::new_v4())
            };
            debug!(?member_id, join_outcome = ?ErrorCode::MemberIdRequired);

            let join_group_response = JoinGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::MemberIdRequired.into())
                .generation_id(-1)
                .protocol_type(None)
                .protocol_name(Some("".into()))
                .leader("".into())
                .skip_assignment(self.skip_assignment)
                .member_id(member_id.clone())
                .members(Some([].into()));

            _ = self.members.insert(
                member_id.clone(),
                Member {
                    join_response: JoinGroupResponseMember::default()
                        .member_id(member_id)
                        .group_instance_id(group_instance_id.map(|s| s.to_owned()))
                        .metadata(protocol.metadata.clone()),
                    last_contact: Some(now),
                },
            );

            return (
                Inner {
                    generation_id: self.generation_id + 1,
                    session_timeout_ms: self.session_timeout_ms,
                    rebalance_timeout_ms: self.rebalance_timeout_ms,

                    members: self.members,
                    state: Forming {
                        protocol_type: Some(self.state.protocol_type),
                        protocol_name: Some(self.state.protocol_name),
                        leader: Some(self.state.leader),
                    },
                    storage: self.storage,
                    skip_assignment: self.skip_assignment,
                    inception: self.inception,
                }
                .into(),
                join_group_response.into(),
            );
        }

        let member_id = group_instance_id.map_or(member_id.to_owned(), |group_instance_id| {
            if member_id.is_empty() {
                if let Some((member_id, _)) = self.members.iter().find(|(_, member)| {
                    member.join_response.group_instance_id.as_deref() == Some(group_instance_id)
                }) {
                    member_id.into()
                } else {
                    format!("{group_instance_id}-{}", Uuid::new_v4())
                }
            } else {
                member_id.into()
            }
        });

        debug!(?member_id, ?self.members);

        match self.members.get_mut(&member_id) {
            Some(Member {
                join_response: JoinGroupResponseMember { metadata, .. },
                ..
            }) if *metadata == protocol.metadata => {
                debug!(
                    member_metadata = "existing",
                    member_id,
                    generation_id = self.generation_id
                );

                let state: Wrapper<O> = self.into();

                let body = {
                    let members = Some(
                        if state.leader().is_some_and(|leader| leader == member_id) {
                            state.members()
                        } else {
                            [].into()
                        },
                    );
                    let protocol_type = state.protocol_type().map(ToOwned::to_owned);
                    let protocol_name = state.protocol_name().map(ToOwned::to_owned);

                    JoinGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .generation_id(state.generation_id())
                        .protocol_type(protocol_type)
                        .protocol_name(protocol_name)
                        .leader(
                            state
                                .leader()
                                .map(|s| s.to_owned())
                                .unwrap_or("".to_owned()),
                        )
                        .skip_assignment(state.skip_assignment().map(ToOwned::to_owned))
                        .member_id(member_id)
                        .members(members)
                        .into()
                };

                debug!(join_outcome = ?ErrorCode::None);

                (state, body)
            }

            Some(Member {
                join_response: JoinGroupResponseMember { metadata, .. },
                ..
            }) => {
                debug!(
                    member_metadata = if group_instance_id.is_none() {"update"} else { "soft_update"},
                    member_id,
                    updated = ?protocol.metadata,
                    existing = ?metadata,
                );

                *metadata = protocol.metadata.clone();

                let state: Wrapper<O> = Inner {
                    generation_id: if group_instance_id.is_none() {
                        self.generation_id + 1
                    } else {
                        self.generation_id
                    },
                    session_timeout_ms: self.session_timeout_ms,
                    rebalance_timeout_ms: self.rebalance_timeout_ms,

                    members: self.members,
                    state: Forming {
                        protocol_type: Some(self.state.protocol_type),
                        protocol_name: Some(self.state.protocol_name),
                        leader: Some(self.state.leader),
                    },
                    storage: self.storage,
                    skip_assignment: self.skip_assignment,
                    inception: self.inception,
                }
                .into();

                let body = {
                    let members = Some(
                        if state.leader().is_some_and(|leader| leader == member_id) {
                            state.members()
                        } else {
                            [].into()
                        },
                    );
                    let protocol_type = state.protocol_type().map(|s| s.to_owned());
                    let protocol_name = state.protocol_name().map(|s| s.to_owned());

                    JoinGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .generation_id(state.generation_id())
                        .protocol_type(protocol_type)
                        .protocol_name(protocol_name)
                        .leader(
                            state
                                .leader()
                                .map(|s| s.to_owned())
                                .unwrap_or("".to_owned()),
                        )
                        .skip_assignment(self.skip_assignment)
                        .member_id(member_id)
                        .members(members)
                        .into()
                };

                debug!(join_outcome = ?ErrorCode::None);

                (state, body)
            }

            None => {
                debug!(
                    member_metadata = "new",
                    member_id,
                    generation_id = self.generation_id + 1
                );

                _ = self.members.insert(
                    member_id.clone(),
                    Member {
                        join_response: JoinGroupResponseMember::default()
                            .member_id(member_id.to_string())
                            .group_instance_id(group_instance_id.map(|s| s.to_owned()))
                            .metadata(protocol.metadata.clone()),
                        last_contact: Some(now),
                    },
                );

                let state: Wrapper<O> = Inner {
                    generation_id: self.generation_id + 1,
                    session_timeout_ms: self.session_timeout_ms,
                    rebalance_timeout_ms: self.rebalance_timeout_ms,

                    members: self.members,
                    state: Forming {
                        protocol_type: Some(self.state.protocol_type),
                        protocol_name: Some(self.state.protocol_name),
                        leader: Some(self.state.leader),
                    },
                    storage: self.storage,
                    skip_assignment: self.skip_assignment,
                    inception: self.inception,
                }
                .into();

                let body = {
                    let members = Some(
                        if state.leader().is_some_and(|leader| leader == member_id) {
                            state.members()
                        } else {
                            [].into()
                        },
                    );

                    let protocol_type = state.protocol_type().map(|s| s.to_owned());
                    let protocol_name = state.protocol_name().map(|s| s.to_owned());

                    JoinGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .generation_id(state.generation_id())
                        .protocol_type(protocol_type)
                        .protocol_name(protocol_name)
                        .leader(
                            state
                                .leader()
                                .map(|s| s.to_owned())
                                .unwrap_or("".to_owned()),
                        )
                        .skip_assignment(self.skip_assignment)
                        .member_id(member_id)
                        .members(members)
                        .into()
                };

                debug!(join_outcome = ?ErrorCode::None);

                (state, body)
            }
        }
    }

    async fn sync(
        mut self,
        _now: SystemTime,
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
            debug!(sync_outcome = ?ErrorCode::UnknownMemberId);

            let body = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::UnknownMemberId.into())
                .protocol_type(Some(self.state.protocol_type.clone()))
                .protocol_name(Some(self.state.protocol_name.clone()))
                .assignment(Bytes::from_static(b""))
                .into();

            return (self, body);
        }

        debug!(?member_id);

        if generation_id > self.generation_id {
            debug!(sync_outcome = ?ErrorCode::IllegalGeneration);

            let body = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::IllegalGeneration.into())
                .protocol_type(Some(self.state.protocol_type.clone()))
                .protocol_name(Some(self.state.protocol_name.clone()))
                .assignment(Bytes::from_static(b""))
                .into();

            return (self, body);
        }

        if generation_id < self.generation_id {
            debug!(sync_outcome = ?ErrorCode::RebalanceInProgress);

            let body = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::RebalanceInProgress.into())
                .protocol_type(Some(self.state.protocol_type.clone()))
                .protocol_name(Some(self.state.protocol_name.clone()))
                .assignment(Bytes::from_static(b""))
                .into();

            return (self, body);
        }

        let body = SyncGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .protocol_type(Some(self.state.protocol_type.clone()))
            .protocol_name(Some(self.state.protocol_name.clone()))
            .assignment(
                self.state
                    .assignments
                    .get(member_id)
                    .cloned()
                    .unwrap_or(Bytes::from_static(b"")),
            )
            .into();

        debug!(sync_outcome = ?ErrorCode::None, sync_assignment = self.state.assignments.contains_key(member_id));

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
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::UnknownMemberId.into())
                    .into(),
            );
        }

        if generation_id > self.generation_id {
            return (
                self,
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::IllegalGeneration.into())
                    .into(),
            );
        }

        if self.missed_heartbeat(group_id, now) || (generation_id < self.generation_id) {
            return (
                self,
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::RebalanceInProgress.into())
                    .into(),
            );
        }

        _ = self
            .members
            .entry(member_id.to_owned())
            .and_modify(|member| _ = member.last_contact.replace(now));

        let body = HeartbeatResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .into();

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
            vec![
                MemberResponse::default()
                    .member_id(member_id.to_owned())
                    .group_instance_id(None)
                    .error_code({
                        if self.members.remove(member_id).is_some() {
                            ErrorCode::None.into()
                        } else {
                            ErrorCode::UnknownMemberId.into()
                        }
                    }),
            ]
        } else {
            members.map_or(vec![], |members| {
                members
                    .iter()
                    .map(|member| {
                        MemberResponse::default()
                            .member_id(member.member_id.clone())
                            .group_instance_id(member.group_instance_id.clone())
                            .error_code({
                                if self.members.remove(&member.member_id).is_some() {
                                    ErrorCode::None.into()
                                } else {
                                    ErrorCode::UnknownMemberId.into()
                                }
                            })
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

                members: self.members,
                state: Forming {
                    protocol_type: Some(self.state.protocol_type),
                    protocol_name: Some(self.state.protocol_name),
                    leader,
                },
                storage: self.storage,
                skip_assignment: self.skip_assignment,
                inception: self.inception,
            }
            .into()
        } else {
            self.into()
        };

        let body = LeaveGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .members(Some(members))
            .into();

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
                    OffsetCommitResponse::default()
                        .throttle_time_ms(Some(0))
                        .topics(detail.topics.map(|topics| {
                            topics
                                .as_ref()
                                .iter()
                                .map(|topic| {
                                    OffsetCommitResponseTopic::default()
                                        .name(topic.name.clone())
                                        .partitions(topic.partitions.as_ref().map(|partitions| {
                                            partitions
                                                .iter()
                                                .map(|partition| {
                                                    OffsetCommitResponsePartition::default()
                                                        .partition_index(partition.partition_index)
                                                        .error_code(
                                                            ErrorCode::UnknownMemberId.into(),
                                                        )
                                                })
                                                .collect()
                                        }))
                                })
                                .collect()
                        }))
                        .into(),
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
    use pretty_assertions::assert_eq;
    use tansu_sans_io::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use tansu_storage::StorageContainer;
    use tracing::subscriber::DefaultGuard;
    use url::Url;

    #[cfg(miri)]
    fn init_tracing() -> Result<()> {
        Ok(())
    }

    #[cfg(not(miri))]
    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        use tracing::Level;
        use tracing_subscriber::fmt::format::FmtSpan;

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_max_level(Level::DEBUG)
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

        let storage = StorageContainer::builder()
            .cluster_id(cluster)
            .node_id(node)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .schema_registry(None)
            .storage(Url::parse("memory://")?)
            .build()
            .await?;

        let mut s = Controller::with_storage(storage)?;

        let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
        let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

        let protocols = [
            JoinGroupRequestProtocol::default()
                .name(RANGE.into())
                .metadata(first_member_range_meta.clone()),
            JoinGroupRequestProtocol::default()
                .name(COOPERATIVE_STICKY.into())
                .metadata(first_member_sticky_meta),
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
            Body::JoinGroupResponse(JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code,
                generation_id: -1,
                protocol_type: Some(protocol_type),
                protocol_name: Some(protocol_name),
                leader,
                skip_assignment: Some(false),
                members: Some(members),
                member_id,
                ..
            }) => {
                assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
                assert_eq!("consumer", protocol_type);
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

                let join_response_expected = Body::from(
                    JoinGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .generation_id(0)
                        .protocol_type(Some(PROTOCOL_TYPE.into()))
                        .protocol_name(Some(RANGE.into()))
                        .leader(member_id.clone())
                        .skip_assignment(Some(false))
                        .member_id(member_id.clone())
                        .members(Some(
                            [JoinGroupResponseMember::default()
                                .member_id(member_id.clone())
                                .group_instance_id(None)
                                .metadata(first_member_range_meta.clone())]
                            .into(),
                        )),
                );

                assert_eq!(join_response_expected, join_response);

                member_id
            }

            otherwise => panic!("{otherwise:?}"),
        };

        let first_member_assignment_01 = Bytes::from_static(b"assignment_01");

        let assignments = [SyncGroupRequestAssignment::default()
            .member_id(first_member_id.clone())
            .assignment(first_member_assignment_01.clone())];

        assert_eq!(
            Body::from(
                SyncGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(0)
                    .protocol_type(Some(PROTOCOL_TYPE.into()))
                    .protocol_name(Some(RANGE.into()))
                    .assignment(first_member_assignment_01)
            ),
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
            Body::from(
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
            ),
            s.heartbeat(GROUP_ID, 0, &first_member_id, group_instance_id)
                .await?
        );

        let second_member_range_meta = Bytes::from_static(b"second_member_range_meta_01");
        let second_member_sticky_meta = Bytes::from_static(b"second_member_sticky_meta_01");

        let protocols = [
            JoinGroupRequestProtocol::default()
                .name(RANGE.into())
                .metadata(second_member_range_meta.clone()),
            JoinGroupRequestProtocol::default()
                .name(COOPERATIVE_STICKY.into())
                .metadata(second_member_sticky_meta.clone()),
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
            Body::JoinGroupResponse(JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code,
                generation_id: -1,
                protocol_type: None,
                protocol_name: Some(protocol_name),
                leader,
                skip_assignment: Some(false),
                members: Some(members),
                member_id,
                ..
            }) => {
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

                let join_response_expected = Body::from(
                    JoinGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .generation_id(1)
                        .protocol_type(Some(PROTOCOL_TYPE.into()))
                        .protocol_name(Some(RANGE.into()))
                        .leader(first_member_id.clone())
                        .skip_assignment(Some(false))
                        .member_id(member_id.clone())
                        .members(Some([].into())),
                );

                assert_eq!(join_response_expected, join_response);

                member_id
            }

            otherwise => panic!("{otherwise:?}"),
        };

        assert_eq!(
            Body::from(
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(i16::from(ErrorCode::RebalanceInProgress))
            ),
            s.heartbeat(GROUP_ID, 0, &first_member_id, group_instance_id,)
                .await?
        );

        assert_eq!(
            Body::from(
                OffsetCommitResponse::default()
                    .throttle_time_ms(Some(0))
                    .topics(Some(
                        [OffsetCommitResponseTopic::default()
                            .name(TOPIC.into())
                            .partitions(Some(
                                (0..=2)
                                    .map(|partition_index| OffsetCommitResponsePartition::default()
                                        .partition_index(partition_index)
                                        .error_code(ErrorCode::UnknownTopicOrPartition.into()))
                                    .collect(),
                            )),]
                        .into()
                    ))
            ),
            s.offset_commit(OffsetCommit {
                group_id: GROUP_ID,
                generation_id_or_member_epoch: Some(0),
                member_id: Some(&first_member_id),
                group_instance_id,
                retention_time_ms: None,
                topics: Some(&[OffsetCommitRequestTopic::default()
                    .name(TOPIC.into())
                    .partitions(Some(
                        (0..=2)
                            .map(|partition_index| OffsetCommitRequestPartition::default()
                                .partition_index(partition_index)
                                .committed_offset(1)
                                .committed_leader_epoch(Some(0))
                                .commit_timestamp(None)
                                .committed_metadata(Some("".into())))
                            .collect(),
                    )),]),
            })
            .await?
        );

        {
            let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_02");
            let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_02");

            let protocols = [
                JoinGroupRequestProtocol::default()
                    .name(RANGE.into())
                    .metadata(first_member_range_meta.clone()),
                JoinGroupRequestProtocol::default()
                    .name(COOPERATIVE_STICKY.into())
                    .metadata(first_member_sticky_meta),
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
                Body::JoinGroupResponse(JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code,
                    generation_id,
                    protocol_type,
                    protocol_name,
                    leader,
                    skip_assignment: Some(false),
                    member_id,
                    members: Some(members),
                    ..
                }) => {
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
                JoinGroupRequestProtocol::default()
                    .name(RANGE.into())
                    .metadata(second_member_range_meta.clone()),
                JoinGroupRequestProtocol::default()
                    .name(COOPERATIVE_STICKY.into())
                    .metadata(second_member_sticky_meta.clone()),
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
                Body::JoinGroupResponse(JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code,
                    generation_id,
                    protocol_type: Some(protocol_type),
                    protocol_name: Some(protocol_name),
                    leader,
                    skip_assignment: Some(false),
                    member_id,
                    members: Some(members),
                    ..
                }) => {
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
                SyncGroupRequestAssignment::default()
                    .member_id(first_member_id.clone())
                    .assignment(first_member_assignment_02.clone()),
                SyncGroupRequestAssignment::default()
                    .member_id(second_member_id.clone())
                    .assignment(second_member_assignment_02.clone()),
            ];

            assert_eq!(
                Body::from(
                    SyncGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .protocol_type(Some(PROTOCOL_TYPE.into()))
                        .protocol_name(Some(RANGE.into()))
                        .assignment(first_member_assignment_02)
                ),
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
            Body::from(
                SyncGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
                    .protocol_type(Some(PROTOCOL_TYPE.into()))
                    .protocol_name(Some(RANGE.into()))
                    .assignment(second_member_assignment_02)
            ),
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
            Body::from(
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
            ),
            s.heartbeat(GROUP_ID, 2, &first_member_id, group_instance_id,)
                .await?
        );

        assert_eq!(
            Body::from(
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
            ),
            s.heartbeat(GROUP_ID, 2, &second_member_id, group_instance_id,)
                .await?
        );

        assert_eq!(
            Body::from(
                LeaveGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
                    .members(Some(
                        [MemberResponse::default()
                            .member_id(first_member_id.clone())
                            .group_instance_id(None)
                            .error_code(ErrorCode::None.into())]
                        .into()
                    ))
            ),
            s.leave(
                GROUP_ID,
                None,
                Some(&[MemberIdentity::default()
                    .member_id(first_member_id.clone())
                    .group_instance_id(None)
                    .reason(Some("the consumer is being closed".into()))]),
            )
            .await?
        );

        assert_eq!(
            Body::from(
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::RebalanceInProgress.into())
            ),
            s.heartbeat(GROUP_ID, 2, &second_member_id, group_instance_id,)
                .await?
        );

        {
            let protocols = [
                JoinGroupRequestProtocol::default()
                    .name(RANGE.into())
                    .metadata(second_member_range_meta.clone()),
                JoinGroupRequestProtocol::default()
                    .name(COOPERATIVE_STICKY.into())
                    .metadata(second_member_sticky_meta.clone()),
            ];

            assert_eq!(
                Body::from(
                    JoinGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .generation_id(3)
                        .protocol_type(Some(PROTOCOL_TYPE.into()))
                        .protocol_name(Some(RANGE.into()))
                        .leader(second_member_id.clone())
                        .skip_assignment(Some(false))
                        .member_id(second_member_id.clone())
                        .members(Some(
                            [JoinGroupResponseMember::default()
                                .member_id(second_member_id.clone())
                                .group_instance_id(None)
                                .metadata(second_member_range_meta.clone())]
                            .into()
                        ))
                ),
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

            let assignments = [SyncGroupRequestAssignment::default()
                .member_id(second_member_id.clone())
                .assignment(second_member_assignment_03.clone())];

            assert_eq!(
                Body::from(
                    SyncGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .protocol_type(Some(PROTOCOL_TYPE.into()))
                        .protocol_name(Some(RANGE.into()))
                        .assignment(second_member_assignment_03)
                ),
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

    #[tokio::test]
    async fn rejoin() -> Result<()> {
        let _guard = init_tracing()?;

        let session_timeout_ms = 45_000;
        let rebalance_timeout_ms = Some(300_000);
        let group_instance_id = None;
        let reason = None;

        let cluster = "abc";
        let node = 12321;

        const CLIENT_ID: &str = "console-consumer";
        const GROUP_ID: &str = "test-consumer-group";
        const RANGE: &str = "range";
        const COOPERATIVE_STICKY: &str = "cooperative-sticky";

        const PROTOCOL_TYPE: &str = "consumer";

        let storage = StorageContainer::builder()
            .cluster_id(cluster)
            .node_id(node)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .schema_registry(None)
            .storage(Url::parse("memory://")?)
            .build()
            .await?;

        let mut s = Controller::with_storage(storage)?;

        let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
        let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

        let first_member_protocols = [
            JoinGroupRequestProtocol::default()
                .name(RANGE.into())
                .metadata(first_member_range_meta.clone()),
            JoinGroupRequestProtocol::default()
                .name(COOPERATIVE_STICKY.into())
                .metadata(first_member_sticky_meta),
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
                Some(&first_member_protocols[..]),
                reason,
            )
            .await?
        {
            Body::JoinGroupResponse(JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code,
                generation_id: -1,
                leader,
                skip_assignment: Some(false),
                members: Some(members),
                member_id,
                ..
            }) => {
                assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
                assert!(leader.is_empty());
                assert!(member_id.starts_with(CLIENT_ID));
                assert_eq!(0, members.len());

                assert_eq!(
                    Body::from(
                        JoinGroupResponse::default()
                            .throttle_time_ms(Some(0))
                            .error_code(ErrorCode::None.into())
                            .generation_id(0)
                            .protocol_type(Some(PROTOCOL_TYPE.into()))
                            .protocol_name(Some(RANGE.into()))
                            .leader(member_id.clone())
                            .skip_assignment(Some(false))
                            .member_id(member_id.clone())
                            .members(Some(
                                [JoinGroupResponseMember::default()
                                    .member_id(member_id.clone())
                                    .group_instance_id(None)
                                    .metadata(first_member_range_meta.clone())]
                                .into()
                            ))
                    ),
                    s.join(
                        Some(CLIENT_ID),
                        GROUP_ID,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        &member_id,
                        group_instance_id,
                        PROTOCOL_TYPE,
                        Some(&first_member_protocols[..]),
                        reason,
                    )
                    .await?
                );

                member_id
            }

            otherwise => panic!("{otherwise:?}"),
        };

        let second_member_range_meta = Bytes::from_static(b"second_member_range_meta_01");
        let second_member_sticky_meta = Bytes::from_static(b"second_member_sticky_meta_01");

        let second_member_protocols = [
            JoinGroupRequestProtocol::default()
                .name(RANGE.into())
                .metadata(second_member_range_meta.clone()),
            JoinGroupRequestProtocol::default()
                .name(COOPERATIVE_STICKY.into())
                .metadata(second_member_sticky_meta),
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
                Some(&second_member_protocols[..]),
                reason,
            )
            .await?
        {
            Body::JoinGroupResponse(JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code,
                generation_id: -1,
                leader,
                skip_assignment: Some(false),
                members: Some(members),
                member_id,
                ..
            }) => {
                assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
                assert!(leader.is_empty());
                assert!(member_id.starts_with(CLIENT_ID));
                assert_eq!(0, members.len());

                assert_eq!(
                    Body::from(
                        JoinGroupResponse::default()
                            .throttle_time_ms(Some(0))
                            .error_code(ErrorCode::None.into())
                            .generation_id(1)
                            .protocol_type(Some(PROTOCOL_TYPE.into()))
                            .protocol_name(Some(RANGE.into()))
                            .leader(first_member_id.clone())
                            .skip_assignment(Some(false))
                            .member_id(member_id.clone())
                            .members(Some([].into()))
                    ),
                    s.join(
                        Some(CLIENT_ID),
                        GROUP_ID,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        &member_id,
                        group_instance_id,
                        PROTOCOL_TYPE,
                        Some(&second_member_protocols[..]),
                        reason,
                    )
                    .await?
                );

                member_id
            }

            otherwise => panic!("{otherwise:?}"),
        };

        match s
            .join(
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                &first_member_id,
                group_instance_id,
                PROTOCOL_TYPE,
                Some(&first_member_protocols[..]),
                reason,
            )
            .await?
        {
            Body::JoinGroupResponse(JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code,
                generation_id: 1,
                protocol_type,
                protocol_name,
                leader,
                skip_assignment: Some(false),
                member_id,
                members: Some(members),
                ..
            }) => {
                assert_eq!(i16::from(ErrorCode::None), error_code);
                assert_eq!(Some(PROTOCOL_TYPE.into()), protocol_type);
                assert_eq!(Some(RANGE.into()), protocol_name);
                assert_eq!(first_member_id.clone(), leader);
                assert_eq!(first_member_id.clone(), member_id);
                assert_eq!(2, members.len());
                assert!(
                    members.contains(
                        &JoinGroupResponseMember::default()
                            .member_id(second_member_id.clone())
                            .group_instance_id(None)
                            .metadata(second_member_range_meta.clone())
                    )
                );
                assert!(
                    members.contains(
                        &JoinGroupResponseMember::default()
                            .member_id(first_member_id.clone())
                            .group_instance_id(None)
                            .metadata(first_member_range_meta.clone())
                    )
                );
            }

            otherwise => panic!("{otherwise:?}"),
        }

        assert_eq!(
            Body::from(
                JoinGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
                    .generation_id(1)
                    .protocol_type(Some(PROTOCOL_TYPE.into()))
                    .protocol_name(Some(RANGE.into()))
                    .leader(first_member_id.clone())
                    .skip_assignment(Some(false))
                    .member_id(second_member_id.clone())
                    .members(Some([].into(),))
            ),
            s.join(
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                &second_member_id,
                group_instance_id,
                PROTOCOL_TYPE,
                Some(&second_member_protocols[..]),
                reason,
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn member_id_required_error_code_joins_group() -> Result<()> {
        let _guard = init_tracing()?;

        let session_timeout_ms = 45_000;
        let rebalance_timeout_ms = Some(300_000);
        let group_instance_id = None;
        let reason = None;

        let cluster = "abc";
        let node = 12321;

        const CLIENT_ID: &str = "console-consumer";
        const GROUP_ID: &str = "test-consumer-group";
        const RANGE: &str = "range";
        const COOPERATIVE_STICKY: &str = "cooperative-sticky";

        const PROTOCOL_TYPE: &str = "consumer";

        let storage = StorageContainer::builder()
            .cluster_id(cluster)
            .node_id(node)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .schema_registry(None)
            .storage(Url::parse("memory://")?)
            .build()
            .await?;

        let s = Wrapper::with_storage_group_detail(
            storage,
            GroupDetail {
                session_timeout_ms,
                rebalance_timeout_ms,
                state: GroupState::Forming {
                    protocol_type: Some(PROTOCOL_TYPE.into()),
                    protocol_name: Some(RANGE.into()),
                    leader: None,
                },
                ..Default::default()
            },
        );

        let now = SystemTime::now();

        let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
        let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

        let first_member_protocols = [
            JoinGroupRequestProtocol::default()
                .name(RANGE.into())
                .metadata(first_member_range_meta.clone()),
            JoinGroupRequestProtocol::default()
                .name(COOPERATIVE_STICKY.into())
                .metadata(first_member_sticky_meta),
        ];

        assert!(s.members().is_empty());

        match s
            .join(
                now,
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                "",
                group_instance_id,
                PROTOCOL_TYPE,
                Some(&first_member_protocols[..]),
                reason,
            )
            .await
        {
            (
                s,
                Body::JoinGroupResponse(JoinGroupResponse {
                    error_code,
                    generation_id,
                    leader,
                    member_id,
                    members,
                    ..
                }),
            ) => {
                assert_eq!(-1, generation_id);
                assert_eq!(i16::from(ErrorCode::MemberIdRequired), error_code);
                assert_eq!("", leader);
                assert_eq!(Some([].into()), members);
                assert!(member_id.starts_with(CLIENT_ID));
                assert_eq!(1, s.members().len());
            }

            otherwise => panic!("{otherwise:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn forming_leader_leaves_group() -> Result<()> {
        let _guard = init_tracing()?;

        let session_timeout_ms = 45_000;
        let rebalance_timeout_ms = Some(300_000);
        let group_instance_id = None;
        let reason = None;

        let cluster = "abc";
        let node = 12321;

        const CLIENT_ID: &str = "console-consumer";
        const GROUP_ID: &str = "test-consumer-group";
        const RANGE: &str = "range";
        const COOPERATIVE_STICKY: &str = "cooperative-sticky";

        const PROTOCOL_TYPE: &str = "consumer";

        let storage = StorageContainer::builder()
            .cluster_id(cluster)
            .node_id(node)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .schema_registry(None)
            .storage(Url::parse("memory://")?)
            .build()
            .await?;

        let s = Wrapper::with_storage_group_detail(
            storage,
            GroupDetail {
                session_timeout_ms,
                rebalance_timeout_ms,
                state: GroupState::Forming {
                    protocol_type: Some(PROTOCOL_TYPE.into()),
                    protocol_name: Some(RANGE.into()),
                    leader: None,
                },
                ..Default::default()
            },
        );

        let now = SystemTime::now();

        let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
        let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

        let first_member_protocols = [
            JoinGroupRequestProtocol::default()
                .name(RANGE.into())
                .metadata(first_member_range_meta.clone()),
            JoinGroupRequestProtocol::default()
                .name(COOPERATIVE_STICKY.into())
                .metadata(first_member_sticky_meta),
        ];

        assert!(s.members().is_empty());

        let (s, member_id) = match s
            .join(
                now,
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                "",
                group_instance_id,
                PROTOCOL_TYPE,
                Some(&first_member_protocols[..]),
                reason,
            )
            .await
        {
            (
                s,
                Body::JoinGroupResponse(JoinGroupResponse {
                    error_code,
                    generation_id,
                    leader,
                    member_id,
                    members,
                    ..
                }),
            ) => {
                assert_eq!(-1, generation_id);
                assert_eq!(i16::from(ErrorCode::MemberIdRequired), error_code);
                assert_eq!("", leader);
                assert_eq!(Some([].into()), members);
                assert!(member_id.starts_with(CLIENT_ID));
                assert_eq!(1, s.members().len());

                (s, member_id)
            }

            otherwise => panic!("{otherwise:?}"),
        };

        let assignments = [SyncGroupRequestAssignment::default()
            .member_id(member_id.clone())
            .assignment(Bytes::from_static(b"assignment_01"))];

        let (s, _) = s
            .sync(
                now,
                GROUP_ID,
                0,
                member_id.as_str(),
                group_instance_id,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(&assignments),
            )
            .await;

        assert_eq!(Some(member_id.as_str()), s.leader());

        let (s, _) = s.leave(now, GROUP_ID, Some(member_id.as_str()), None).await;

        assert_eq!(None, s.leader());

        Ok(())
    }

    #[tokio::test]
    async fn sync_from_member_while_forming() -> Result<()> {
        let _guard = init_tracing()?;

        let session_timeout_ms = 45_000;
        let rebalance_timeout_ms = Some(300_000);
        let group_instance_id = None;
        let reason = None;

        let cluster = "abc";
        let node = 12321;

        const CLIENT_ID: &str = "console-consumer";
        const GROUP_ID: &str = "test-consumer-group";
        const RANGE: &str = "range";
        const COOPERATIVE_STICKY: &str = "cooperative-sticky";

        const PROTOCOL_TYPE: &str = "consumer";

        let storage = StorageContainer::builder()
            .cluster_id(cluster)
            .node_id(node)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .schema_registry(None)
            .storage(Url::parse("memory://")?)
            .build()
            .await?;

        let s = Wrapper::with_storage_group_detail(
            storage,
            GroupDetail {
                session_timeout_ms,
                rebalance_timeout_ms,
                state: GroupState::Forming {
                    protocol_type: Some(PROTOCOL_TYPE.into()),
                    protocol_name: Some(RANGE.into()),
                    leader: None,
                },
                ..Default::default()
            },
        );

        let now = SystemTime::now();

        let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
        let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

        let second_member_range_meta = Bytes::from_static(b"second_member_range_meta_01");
        let second_member_sticky_meta = Bytes::from_static(b"second_member_sticky_meta_01");

        assert!(s.members().is_empty());
        assert_eq!(None, s.leader());
        assert_eq!(None, s.assignments());

        let first_member_id = format!("{}-{}", CLIENT_ID, Uuid::new_v4());

        let (s, _) = s
            .join(
                now,
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                first_member_id.as_str(),
                group_instance_id,
                PROTOCOL_TYPE,
                Some(
                    &[
                        JoinGroupRequestProtocol::default()
                            .name(RANGE.into())
                            .metadata(first_member_range_meta.clone()),
                        JoinGroupRequestProtocol::default()
                            .name(COOPERATIVE_STICKY.into())
                            .metadata(first_member_sticky_meta),
                    ][..],
                ),
                reason,
            )
            .await;

        assert_eq!(0, s.generation_id());
        assert_eq!(1, s.members().len());
        assert!(
            s.members().contains(
                &JoinGroupResponseMember::default()
                    .member_id(first_member_id.clone())
                    .group_instance_id(None)
                    .metadata(first_member_range_meta.clone())
            )
        );
        assert_eq!(Some(first_member_id.as_str()), s.leader());

        let second_member_id = format!("{}-{}", CLIENT_ID, Uuid::new_v4());

        let (s, _) = s
            .join(
                now,
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                second_member_id.as_str(),
                group_instance_id,
                PROTOCOL_TYPE,
                Some(
                    &[
                        JoinGroupRequestProtocol::default()
                            .name(RANGE.into())
                            .metadata(second_member_range_meta.clone()),
                        JoinGroupRequestProtocol::default()
                            .name(COOPERATIVE_STICKY.into())
                            .metadata(second_member_sticky_meta),
                    ][..],
                ),
                reason,
            )
            .await;

        assert_eq!(1, s.generation_id());
        assert_eq!(2, s.members().len());
        assert_eq!(None, s.assignments());

        assert!(
            s.members().contains(
                &JoinGroupResponseMember::default()
                    .member_id(first_member_id.clone())
                    .group_instance_id(None)
                    .metadata(first_member_range_meta.clone())
            )
        );

        assert!(
            s.members().contains(
                &JoinGroupResponseMember::default()
                    .member_id(second_member_id.clone())
                    .group_instance_id(None)
                    .metadata(second_member_range_meta.clone())
            )
        );

        assert_eq!(Some(first_member_id.as_str()), s.leader());

        let (s, _) = s
            .sync(
                now,
                GROUP_ID,
                1,
                second_member_id.as_str(),
                group_instance_id,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(&[]),
            )
            .await;

        assert_eq!(1, s.generation_id());
        assert_eq!(Some(first_member_id.as_str()), s.leader());
        assert_eq!(Some(first_member_id.as_str()), s.leader());
        assert_eq!(None, s.assignments());

        let assignments = [SyncGroupRequestAssignment::default()
            .member_id(first_member_id.clone())
            .assignment(Bytes::from_static(b"first_assignment_01"))];

        let (s, _) = s
            .sync(
                now,
                GROUP_ID,
                0,
                first_member_id.as_str(),
                group_instance_id,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(&assignments[..]),
            )
            .await;

        assert_eq!(1, s.generation_id());
        assert_eq!(Some(first_member_id.as_str()), s.leader());
        assert_eq!(None, s.assignments());

        let first_member_assignment = Bytes::from_static(b"first_assignment_02");
        let second_member_assignment = Bytes::from_static(b"second_assignment_02");

        let mut assignments = BTreeMap::new();
        _ = assignments.insert(first_member_id.clone(), first_member_assignment.clone());
        _ = assignments.insert(second_member_id.clone(), second_member_assignment.clone());

        let (s, _) = s
            .sync(
                now,
                GROUP_ID,
                1,
                first_member_id.as_str(),
                group_instance_id,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(
                    &assignments
                        .iter()
                        .map(|(member_id, assignment)| {
                            SyncGroupRequestAssignment::default()
                                .member_id(member_id.to_owned())
                                .assignment(assignment.to_owned())
                        })
                        .collect::<Vec<_>>()[..],
                ),
            )
            .await;

        assert_eq!(1, s.generation_id());
        assert_eq!(Some(first_member_id.as_str()), s.leader());
        assert_eq!(Some(first_member_id.as_str()), s.leader());

        assert_eq!(
            Some(first_member_assignment),
            s.assignments()
                .map(|assignments| assignments.get(first_member_id.as_str()).cloned())
                .unwrap()
        );

        assert_eq!(
            Some(second_member_assignment),
            s.assignments()
                .map(|assignments| assignments.get(second_member_id.as_str()).cloned())
                .unwrap()
        );

        Ok(())
    }
}
