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
    collections::BTreeMap,
    fmt::Debug,
    hash::{DefaultHasher, Hash, Hasher},
    sync::{Arc, Mutex, MutexGuard},
    time::{Instant, SystemTime},
};

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
    record::{inflated, Record},
    sync_group_request::SyncGroupRequestAssignment,
    to_timestamp, Body, ErrorCode,
};
use tansu_storage::{segment::Storage, Topition};
use tracing::{debug, info, instrument};
use uuid::Uuid;

use crate::{Error, Result, CONSUMER_OFFSETS};

use super::{Coordinator, GroupTopition, OffsetCommit, OffsetCommitKey, OffsetCommitValue};

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
        detail: OffsetCommit<'_>,
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
    Forming(Inner<Forming>),
    Formed(Inner<Formed>),
}

impl From<Inner<Forming>> for Wrapper {
    fn from(value: Inner<Forming>) -> Self {
        Self::Forming(value)
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
            Wrapper::Forming(inner) => {
                let (state, body) = inner.leave(now, group_id, member_id, members);
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner.leave(now, group_id, member_id, members);
                (state, body)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument]
    fn offset_commit(self, now: Instant, detail: OffsetCommit<'_>) -> (Wrapper, Body) {
        match self {
            Wrapper::Forming(inner) => {
                let (state, body) = inner.offset_commit(now, detail);
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner.offset_commit(now, detail);
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
            Wrapper::Forming(inner) => {
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
            Wrapper::Forming(inner) => {
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

    fn offset_commit(&mut self, detail: OffsetCommit<'_>) -> Result<Body> {
        let now = Instant::now();

        self.wrapper
            .take()
            .ok_or(Error::EmptyCoordinatorWrapper)
            .map(|wrapper| wrapper.offset_commit(now, detail))
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
pub struct Inner<S: Debug> {
    session_timeout_ms: i32,
    rebalance_timeout_ms: Option<i32>,
    group_instance_id: Option<String>,
    members: BTreeMap<String, Member>,
    generation_id: i32,
    state: S,
    storage: Arc<Mutex<Storage>>,
    offsets: Arc<Mutex<BTreeMap<GroupTopition, OffsetCommitValue>>>,
    partitions: i32,
    skip_assignment: Option<bool>,
}

impl<S> Inner<S>
where
    S: Debug,
{
    fn storage_lock(&self) -> Result<MutexGuard<'_, Storage>> {
        self.storage.lock().map_err(Into::into)
    }

    fn offsets_lock(&self) -> Result<MutexGuard<'_, BTreeMap<GroupTopition, OffsetCommitValue>>> {
        self.offsets.lock().map_err(Into::into)
    }

    fn missed_heartbeat(&mut self, group_id: &str, now: Instant) -> bool {
        let original = self.members.len();

        self.members.retain(|member_id, member| {
            member
                .last_contact
                .and_then(|last_contact| now.checked_duration_since(last_contact))
                .inspect(|duration| {
                    debug!(
                        "{member_id}, since last contact: {}ms",
                        duration.as_millis()
                    )
                })
                .is_some_and(|duration| {
                    if duration.as_millis()
                        > u128::try_from(self.session_timeout_ms).unwrap_or(30_000)
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

    fn partition_for(&self, key: Bytes) -> Result<i32> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        u64::try_from(self.partitions)
            .map_err(Into::into)
            .and_then(|partitions| i32::try_from(hasher.finish() % partitions).map_err(Into::into))
    }

    fn commit_offset(&mut self, detail: OffsetCommit<'_>) -> Body {
        if detail
            .member_id
            .is_some_and(|member_id| self.members.contains_key(member_id))
        {
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
                                    .map(|partition| {
                                        let Ok(key) = Bytes::try_from(OffsetCommitKey {
                                            group: detail.group_id.to_owned(),
                                            topic: topic.name.clone(),
                                            partition: partition.partition_index,
                                        }) else {
                                            return OffsetCommitResponsePartition {
                                                partition_index: partition.partition_index,
                                                error_code: ErrorCode::UnknownServerError.into(),
                                            };
                                        };

                                        let Ok(commit_timestamp) = to_timestamp(SystemTime::now())
                                        else {
                                            return OffsetCommitResponsePartition {
                                                partition_index: partition.partition_index,
                                                error_code: ErrorCode::UnknownServerError.into(),
                                            };
                                        };

                                        let expire_timestamp = detail
                                            .retention_time_ms
                                            .map(|retention_time_ms| {
                                                commit_timestamp + retention_time_ms
                                            })
                                            .unwrap_or(-1);

                                        let offset_commit_value = OffsetCommitValue {
                                            offset: partition.committed_offset,
                                            leader_epoch: -1,
                                            metadata: "".to_owned(),
                                            commit_timestamp,
                                            expire_timestamp,
                                        };

                                        let Ok(value) =
                                            Bytes::try_from(offset_commit_value.clone())
                                        else {
                                            return OffsetCommitResponsePartition {
                                                partition_index: partition.partition_index,
                                                error_code: ErrorCode::UnknownServerError.into(),
                                            };
                                        };

                                        let Ok(consumer_offsets_partition) =
                                            self.partition_for(key.clone())
                                        else {
                                            return OffsetCommitResponsePartition {
                                                partition_index: partition.partition_index,
                                                error_code: ErrorCode::UnknownServerError.into(),
                                            };
                                        };

                                        let topition = Topition::new(
                                            CONSUMER_OFFSETS,
                                            consumer_offsets_partition,
                                        );

                                        if let Ok(_offset) = inflated::Batch::builder()
                                            .record(
                                                Record::builder()
                                                    .key(key.into())
                                                    .value(value.into()),
                                            )
                                            .build()
                                            .and_then(TryInto::try_into)
                                            .map_err(Into::into)
                                            .and_then(|batch| {
                                                self.storage_lock().and_then(|mut storage| {
                                                    storage
                                                        .produce(&topition, batch)
                                                        .map_err(Into::into)
                                                })
                                            })
                                        {
                                            let gtp = GroupTopition {
                                                group: detail.group_id.to_owned(),
                                                topition: Topition::new(
                                                    topic.name.as_str(),
                                                    partition.partition_index,
                                                ),
                                            };

                                            if let Ok(mut offsets) = self.offsets_lock() {
                                                _ = offsets.insert(gtp, offset_commit_value);

                                                OffsetCommitResponsePartition {
                                                    partition_index: partition.partition_index,
                                                    error_code: ErrorCode::None.into(),
                                                }
                                            } else {
                                                OffsetCommitResponsePartition {
                                                    partition_index: partition.partition_index,
                                                    error_code: ErrorCode::UnknownServerError
                                                        .into(),
                                                }
                                            }
                                        } else {
                                            OffsetCommitResponsePartition {
                                                partition_index: partition.partition_index,
                                                error_code: ErrorCode::UnknownServerError.into(),
                                            }
                                        }
                                    })
                                    .collect()
                            }),
                        })
                        .collect()
                }),
            }
        } else {
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
            }
        }
    }
}

impl Inner<Forming> {
    pub fn with_storage(
        storage: Arc<Mutex<Storage>>,
        partitions: i32,
        offsets: Arc<Mutex<BTreeMap<GroupTopition, OffsetCommitValue>>>,
    ) -> Result<Self> {
        Ok(Self {
            session_timeout_ms: 0,
            rebalance_timeout_ms: None,
            group_instance_id: None,
            members: BTreeMap::new(),
            generation_id: -1,
            state: Forming::default(),
            storage,
            offsets,
            partitions,
            skip_assignment: Some(false),
        })
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Member {
    join_response: JoinGroupResponseMember,
    last_contact: Option<Instant>,
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
                    debug!(?metadata, ?protocol.metadata);
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
            protocol_type: self.state.protocol_type.clone(),
            protocol_name: self.state.protocol_name.clone(),
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
            state: Formed {
                protocol_name: self.state.protocol_name.expect("protocol_name"),
                protocol_type: self.state.protocol_type.expect("protocol_type"),
                leader: member_id.to_owned(),
                assignments,
            },
            storage: self.storage,
            offsets: self.offsets,
            partitions: self.partitions,
            skip_assignment: self.skip_assignment,
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
        mut self,
        now: Instant,
        detail: OffsetCommit<'_>,
    ) -> (Self::OffsetCommitState, Body) {
        let body = self.commit_offset(detail);
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
                    let state: Wrapper = self.into();

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
                    let state: Wrapper = Inner {
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
                        offsets: self.offsets,
                        partitions: self.partitions,
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
                let state: Wrapper = Inner {
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
                    offsets: self.offsets,
                    partitions: self.partitions,
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

        let state: Wrapper = if members
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
                offsets: self.offsets,
                partitions: self.partitions,
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

    #[instrument]
    fn offset_commit(
        mut self,
        now: Instant,
        detail: OffsetCommit<'_>,
    ) -> (Self::OffsetCommitState, Body) {
        let body = self.commit_offset(detail);
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
        let topics = if let Some(group_id) = group_id {
            topics.map(|topics| {
                topics
                    .as_ref()
                    .iter()
                    .map(|topic| OffsetFetchResponseTopic {
                        name: topic.name.clone(),
                        partitions: topic.partition_indexes.as_ref().map(|partition_indexes| {
                            partition_indexes
                                .iter()
                                .map(|partition_index| {
                                    let gtp = GroupTopition {
                                        group: group_id.into(),
                                        topition: Topition::new(
                                            topic.name.as_str(),
                                            *partition_index,
                                        ),
                                    };

                                    if let Ok(offsets) = self.offsets_lock() {
                                        debug!(?offsets, ?gtp);

                                        offsets.get(&gtp).map_or(
                                            OffsetFetchResponsePartition {
                                                partition_index: *partition_index,
                                                committed_offset: -1,
                                                committed_leader_epoch: Some(-1),
                                                metadata: Some("".into()),
                                                error_code: ErrorCode::None.into(),
                                            },
                                            |offset_commit| {
                                                debug!(?gtp, ?offset_commit);

                                                OffsetFetchResponsePartition {
                                                    partition_index: *partition_index,
                                                    committed_offset: offset_commit.offset,
                                                    committed_leader_epoch: Some(
                                                        offset_commit.leader_epoch,
                                                    ),
                                                    metadata: Some(offset_commit.metadata.clone()),
                                                    error_code: ErrorCode::None.into(),
                                                }
                                            },
                                        )
                                    } else {
                                        OffsetFetchResponsePartition {
                                            partition_index: *partition_index,
                                            committed_offset: -1,
                                            committed_leader_epoch: Some(-1),
                                            metadata: Some("".into()),
                                            error_code: ErrorCode::UnknownServerError.into(),
                                        }
                                    }
                                })
                                .collect()
                        }),
                    })
                    .collect()
            })
        } else {
            None
        };

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
                                            .map(|partition_index| {
                                                let gtp = GroupTopition {
                                                    group: group.group_id.clone(),
                                                    topition: Topition::new(
                                                        topic.name.as_str(),
                                                        *partition_index,
                                                    ),
                                                };

                                                if let Ok(offsets) = self.offsets_lock() {
                                                    debug!(?offsets, ?gtp);

                                                    offsets.get(&gtp).map_or(
                                                        OffsetFetchResponsePartitions {
                                                            partition_index: *partition_index,
                                                            committed_offset: -1,
                                                            committed_leader_epoch: -1,
                                                            metadata: Some("".into()),
                                                            error_code: ErrorCode::None.into(),
                                                        },
                                                        |offset_commit| {
                                                            debug!(?gtp, ?offset_commit);

                                                            OffsetFetchResponsePartitions {
                                                                partition_index: *partition_index,
                                                                committed_offset: offset_commit
                                                                    .offset,
                                                                committed_leader_epoch:
                                                                    offset_commit.leader_epoch,
                                                                metadata: Some(
                                                                    offset_commit.metadata.clone(),
                                                                ),
                                                                error_code: ErrorCode::None.into(),
                                                            }
                                                        },
                                                    )
                                                } else {
                                                    OffsetFetchResponsePartitions {
                                                        partition_index: *partition_index,
                                                        committed_offset: -1,
                                                        committed_leader_epoch: -1,
                                                        metadata: Some("".into()),
                                                        error_code: ErrorCode::UnknownServerError
                                                            .into(),
                                                    }
                                                }
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
            topics,
            error_code: Some(ErrorCode::None.into()),
            groups,
        };

        (self, body)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::NUM_CONSUMER_OFFSETS_PARTITIONS;

    use super::*;
    use pretty_assertions::assert_eq;
    use tansu_kafka_sans_io::{
        offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
        offset_fetch_request::OffsetFetchRequestTopics,
        offset_fetch_response::{
            OffsetFetchResponseGroup, OffsetFetchResponsePartitions, OffsetFetchResponseTopics,
        },
    };
    use tansu_storage::segment::MemorySegmentProvider;
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

        let storage = Storage::with_segment_provider(Box::new(MemorySegmentProvider::default()))
            .map(Mutex::new)
            .map(Arc::new)?;

        let offsets = Arc::new(Mutex::new(BTreeMap::new()));

        let s: Wrapper =
            Inner::<Forming>::with_storage(storage, NUM_CONSUMER_OFFSETS_PARTITIONS, offsets)?
                .into();

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
                protocol_name: Some(protocol_name),
                leader,
                skip_assignment: Some(false),
                members,
                member_id,
            } => {
                assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
                assert_eq!("", protocol_name);
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
                    protocol_type: Some("consumer".into()),
                    protocol_name: Some("range".into()),
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

        let storage = Storage::with_segment_provider(Box::new(MemorySegmentProvider::default()))
            .map(Mutex::new)
            .map(Arc::new)?;

        let offsets = Arc::new(Mutex::new(BTreeMap::new()));

        let s: Wrapper =
            Inner::<Forming>::with_storage(storage, NUM_CONSUMER_OFFSETS_PARTITIONS, offsets)?
                .into();

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
                    protocol_name: Some(protocol_name),
                    leader,
                    skip_assignment: Some(false),
                    members,
                    member_id,
                },
            ) => {
                assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
                assert_eq!("", protocol_name);
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
            error_code: Some(0),
            groups: Some(
                [OffsetFetchResponseGroup {
                    group_id: group_id.into(),
                    topics: Some(
                        [OffsetFetchResponseTopics {
                            name: topic.into(),
                            partitions: Some(
                                [OffsetFetchResponsePartitions {
                                    partition_index: 0,
                                    committed_offset: -1,
                                    committed_leader_epoch: -1,
                                    metadata: Some("".into()),
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
            OffsetCommit {
                group_id,
                generation_id_or_member_epoch,
                member_id: Some(&member_id),
                group_instance_id,
                retention_time_ms,
                topics: Some(&topics[..]),
            },
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
    fn lifecycle() -> Result<()> {
        let _guard = init_tracing()?;

        let storage = Storage::with_segment_provider(Box::new(MemorySegmentProvider::default()))
            .map(Mutex::new)
            .map(Arc::new)?;

        let offsets = Arc::new(Mutex::new(BTreeMap::new()));

        let s: Wrapper =
            Inner::<Forming>::with_storage(storage, NUM_CONSUMER_OFFSETS_PARTITIONS, offsets)?
                .into();

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
                    protocol_name: Some(protocol_name),
                    leader,
                    skip_assignment: Some(false),
                    members: Some(members),
                    member_id,
                },
            ) => {
                assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
                assert_eq!("", protocol_name);
                assert!(leader.is_empty());
                assert!(member_id.starts_with(CLIENT_ID));
                assert_eq!(0, members.len());

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

        let s = {
            let first_member_assignment_01 = Bytes::from_static(b"assignment_01");

            let assignments = [SyncGroupRequestAssignment {
                member_id: first_member_id.clone(),
                assignment: first_member_assignment_01.clone(),
            }];

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
                    protocol_name: Some(protocol_name),
                    leader,
                    skip_assignment: Some(false),
                    members: Some(members),
                    member_id,
                },
            ) => {
                assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
                assert_eq!("", protocol_name);
                assert!(leader.is_empty());
                assert!(member_id.starts_with(CLIENT_ID));
                assert_eq!(0, members.len());

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
            OffsetCommit {
                group_id: GROUP_ID,
                generation_id_or_member_epoch: Some(previous_generation),
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
            },
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
            let unchanged_generation_id = s.generation_id();

            match s.join(
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
            ) {
                (
                    s,
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
                    },
                ) => {
                    assert_eq!(i16::from(ErrorCode::None), error_code);
                    assert_eq!(unchanged_generation_id, generation_id);
                    assert_eq!(PROTOCOL_TYPE, protocol_type);
                    assert_eq!(RANGE, protocol_name);
                    assert_eq!(first_member_id, leader);
                    assert_eq!(second_member_id, member_id);
                    assert_eq!(0, members.len());

                    s
                }

                otherwise => panic!("{otherwise:?}"),
            }
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

            let assignments = [];

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

        let s = {
            let generation_id = s.generation_id();

            match s.heartbeat(
                now,
                GROUP_ID,
                generation_id,
                &first_member_id,
                group_instance_id,
            ) {
                (
                    s,
                    Body::HeartbeatResponse {
                        throttle_time_ms: Some(0),
                        error_code,
                    },
                ) => {
                    assert_eq!(i16::from(ErrorCode::None), error_code);
                    s
                }

                otherwise => panic!("{otherwise:?}"),
            }
        };

        let s = {
            let generation_id = s.generation_id();

            match s.heartbeat(
                now,
                GROUP_ID,
                generation_id,
                &second_member_id,
                group_instance_id,
            ) {
                (
                    s,
                    Body::HeartbeatResponse {
                        throttle_time_ms: Some(0),
                        error_code,
                    },
                ) => {
                    assert_eq!(i16::from(ErrorCode::None), error_code);
                    s
                }

                otherwise => panic!("{otherwise:?}"),
            }
        };

        let generation_prior_to_leader_leaving = s.generation_id();

        let s = {
            match s.leave(
                now,
                GROUP_ID,
                None,
                Some(&[MemberIdentity {
                    member_id: first_member_id.clone(),
                    group_instance_id: None,
                    reason: Some("the consumer is being closed".into()),
                }]),
            ) {
                (
                    s,
                    Body::LeaveGroupResponse {
                        throttle_time_ms: Some(0),
                        error_code,
                        members: Some(members),
                    },
                ) => {
                    assert_eq!(i16::from(ErrorCode::None), error_code);

                    assert_eq!(
                        vec![MemberResponse {
                            member_id: first_member_id.clone(),
                            group_instance_id: None,
                            error_code: 0,
                        }],
                        members
                    );

                    s
                }
                otherwise => panic!("{otherwise:?}"),
            }
        };

        let s = {
            match s.heartbeat(
                now,
                GROUP_ID,
                generation_prior_to_leader_leaving,
                &second_member_id,
                group_instance_id,
            ) {
                (
                    s,
                    Body::HeartbeatResponse {
                        throttle_time_ms: Some(0),
                        error_code,
                    },
                ) => {
                    assert_eq!(i16::from(ErrorCode::RebalanceInProgress), error_code);
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
            let unchanged_generation_id = s.generation_id();

            match s.join(
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
            ) {
                (
                    s,
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
                    },
                ) => {
                    assert_eq!(i16::from(ErrorCode::None), error_code);
                    assert_eq!(unchanged_generation_id, generation_id);
                    assert_eq!(PROTOCOL_TYPE, protocol_type);
                    assert_eq!(RANGE, protocol_name);
                    assert_eq!(second_member_id, leader);
                    assert_eq!(second_member_id, member_id);
                    assert_eq!(1, members.len());

                    s
                }

                otherwise => panic!("{otherwise:?}"),
            }
        };

        let s = {
            let second_member_assignment_03 = Bytes::from_static(b"second_member_assignment_03");

            let assignments = [SyncGroupRequestAssignment {
                member_id: second_member_id.clone(),
                assignment: second_member_assignment_03.clone(),
            }];

            let generation_id = s.generation_id();

            match s.sync(
                now,
                GROUP_ID,
                generation_id,
                &second_member_id,
                group_instance_id,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(&assignments),
            ) {
                (
                    s,
                    Body::SyncGroupResponse {
                        throttle_time_ms: Some(0),
                        error_code,
                        protocol_type: Some(protocol_type),
                        protocol_name: Some(protocol_name),
                        assignment,
                    },
                ) => {
                    assert_eq!(i16::from(ErrorCode::None), error_code);
                    assert_eq!(PROTOCOL_TYPE, protocol_type);
                    assert_eq!(RANGE, protocol_name);
                    assert_eq!(second_member_assignment_03, assignment);

                    s
                }

                otherwise => panic!("{otherwise:?}"),
            }
        };

        let _ = s;

        Ok(())
    }

    #[test]
    fn missed_heartbeat() -> Result<()> {
        let _guard = init_tracing()?;

        let storage = Storage::with_segment_provider(Box::new(MemorySegmentProvider::default()))
            .map(Mutex::new)
            .map(Arc::new)?;

        let offsets = Arc::new(Mutex::new(BTreeMap::new()));

        let s: Wrapper =
            Inner::<Forming>::with_storage(storage, NUM_CONSUMER_OFFSETS_PARTITIONS, offsets)?
                .into();

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
                    protocol_name: Some(protocol_name),
                    leader,
                    skip_assignment: Some(false),
                    members: Some(members),
                    member_id,
                },
            ) => {
                assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
                assert_eq!("", protocol_name);
                assert!(leader.is_empty());
                assert!(member_id.starts_with(CLIENT_ID));
                assert_eq!(0, members.len());

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

        let s = {
            let first_member_assignment_01 = Bytes::from_static(b"assignment_01");

            let assignments = [SyncGroupRequestAssignment {
                member_id: first_member_id.clone(),
                assignment: first_member_assignment_01.clone(),
            }];

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
                    protocol_name: Some(protocol_name),
                    leader,
                    skip_assignment: Some(false),
                    members: Some(members),
                    member_id,
                },
            ) => {
                assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
                assert_eq!("", protocol_name);
                assert!(leader.is_empty());
                assert!(member_id.starts_with(CLIENT_ID));
                assert_eq!(0, members.len());

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
            OffsetCommit {
                group_id: GROUP_ID,
                generation_id_or_member_epoch: Some(previous_generation),
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
            },
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
            let unchanged_generation_id = s.generation_id();

            match s.join(
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
            ) {
                (
                    s,
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
                    },
                ) => {
                    assert_eq!(i16::from(ErrorCode::None), error_code);
                    assert_eq!(unchanged_generation_id, generation_id);
                    assert_eq!(PROTOCOL_TYPE, protocol_type);
                    assert_eq!(RANGE, protocol_name);
                    assert_eq!(first_member_id, leader);
                    assert_eq!(second_member_id, member_id);
                    assert_eq!(0, members.len());

                    s
                }

                otherwise => panic!("{otherwise:?}"),
            }
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

            let assignments = [];

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

        let s = {
            let now = now
                .checked_add(Duration::from_millis(
                    u64::try_from(session_timeout_ms).expect("session_time_ms") - 1,
                ))
                .expect("now");

            let generation_id = s.generation_id();

            match s.heartbeat(
                now,
                GROUP_ID,
                generation_id,
                &first_member_id,
                group_instance_id,
            ) {
                (
                    s,
                    Body::HeartbeatResponse {
                        throttle_time_ms: Some(0),
                        error_code,
                    },
                ) => {
                    assert_eq!(i16::from(ErrorCode::None), error_code);
                    s
                }

                otherwise => panic!("{otherwise:?}"),
            }
        };

        let s = {
            let now = now
                .checked_add(Duration::from_millis(
                    u64::try_from(session_timeout_ms).expect("session_time_ms") + 1,
                ))
                .expect("now");

            let generation_id = s.generation_id();

            match s.heartbeat(
                now,
                GROUP_ID,
                generation_id,
                &second_member_id,
                group_instance_id,
            ) {
                (
                    s,
                    Body::HeartbeatResponse {
                        throttle_time_ms: Some(0),
                        error_code,
                    },
                ) => {
                    assert_eq!(i16::from(ErrorCode::RebalanceInProgress), error_code);
                    s
                }

                otherwise => panic!("{otherwise:?}"),
            }
        };

        let _ = s;

        Ok(())
    }
}
