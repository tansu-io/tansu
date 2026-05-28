// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::{fmt, str::FromStr, time::Duration};

use bytes::{Buf as _, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::{
    Body, Decoder, Encoder, Error, ErrorCode, HeartbeatRequest, HeartbeatResponse,
    JoinGroupRequest, JoinGroupResponse, MetadataResponse, SyncGroupRequest, SyncGroupResponse,
    join_group_request::JoinGroupRequestProtocol, join_group_response::JoinGroupResponseMember,
    sync_group_request::SyncGroupRequestAssignment,
};

mod assignor;
mod codec;

pub use assignor::RangeAssignor;

pub const CONSUMER: &str = "consumer";

pub trait ConsumerAssignor {
    fn assign(
        &self,
        members: &[JoinGroupResponseMember],
        metadata: &MetadataResponse,
    ) -> Result<Vec<SyncGroupRequestAssignment>, Error>;
}

pub enum Assignor {
    Uniform,
    Range,
    RoundRobin,
    CooperativeSticky,
}

impl Assignor {
    pub const COOPERATIVE_STICKY: &str = "cooperative-sticky";
    pub const RANGE: &str = "range";
    pub const ROUND_ROBIN: &str = "roundrobin";
    pub const UNIFORM: &str = "uniform";
}

impl AsRef<str> for Assignor {
    fn as_ref(&self) -> &str {
        match self {
            Self::CooperativeSticky => Self::COOPERATIVE_STICKY,
            Self::Range => Self::RANGE,
            Self::RoundRobin => Self::ROUND_ROBIN,
            Self::Uniform => Self::UNIFORM,
        }
    }
}

impl FromStr for Assignor {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            Self::COOPERATIVE_STICKY => Ok(Self::CooperativeSticky),
            Self::RANGE => Ok(Self::Range),
            Self::ROUND_ROBIN => Ok(Self::RoundRobin),
            Self::UNIFORM => Ok(Self::Uniform),
            otherwise => Err(Error::UnknownAssignor(otherwise.into())),
        }
    }
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "codec::MemberMetadata")]
#[serde(into = "codec::MemberMetadata")]
pub struct MemberMetadata {
    pub version: i16,
    pub subscription: ConsumerProtocolSubscription,
}

impl fmt::Display for MemberMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v: {}, sub: {}", self.version, self.subscription)
    }
}

impl MemberMetadata {
    pub fn version(self, version: i16) -> Self {
        Self { version, ..self }
    }

    pub fn subscription(self, subscription: ConsumerProtocolSubscription) -> Self {
        Self {
            subscription,
            ..self
        }
    }
}

impl TryFrom<Bytes> for MemberMetadata {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let mut reader = value.reader();
        let mut decoder = Decoder::new(&mut reader);
        Self::deserialize(&mut decoder)
    }
}

impl TryFrom<&MemberMetadata> for Bytes {
    type Error = Error;

    fn try_from(value: &MemberMetadata) -> Result<Self, Self::Error> {
        let mut encoder = Encoder::new(BytesMut::new());

        value.serialize(&mut encoder).and(Ok(Bytes::from(encoder)))
    }
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ConsumerProtocolSubscription {
    pub topics: Vec<String>,
    pub user_data: Option<Bytes>,
    pub owned_partitions: Option<Vec<TopicPartition>>,
    pub generation_id: Option<i32>,
    pub rack_id: Option<String>,
}

impl fmt::Display for ConsumerProtocolSubscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "owned: {}",
            self.owned_partitions
                .as_deref()
                .map(|partitions| {
                    partitions
                        .iter()
                        .map(|partition| partition.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default()
        )
    }
}

impl ConsumerProtocolSubscription {
    pub fn topics(self, topics: impl IntoIterator<Item = String>) -> Self {
        Self {
            topics: topics.into_iter().collect(),
            ..self
        }
    }

    pub fn user_data(self, user_data: Option<Bytes>) -> Self {
        Self { user_data, ..self }
    }

    pub fn owned_partitions(self, owned_partitions: impl Iterator<Item = TopicPartition>) -> Self {
        Self {
            owned_partitions: Some(owned_partitions.collect()),
            ..self
        }
    }

    pub fn generation_id(self, generation_id: Option<i32>) -> Self {
        Self {
            generation_id,
            ..self
        }
    }

    pub fn rack_id(self, rack_id: Option<String>) -> Self {
        Self { rack_id, ..self }
    }
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TopicPartition {
    pub topic: String,
    pub partitions: Vec<i32>,
}

impl fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: [{}]",
            self.topic,
            self.partitions
                .iter()
                .map(|partition| partition.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl TopicPartition {
    pub fn topic(self, topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            ..self
        }
    }

    pub fn partitions(self, partitions: impl Iterator<Item = i32>) -> Self {
        Self {
            partitions: partitions.collect(),
            ..self
        }
    }
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "codec::MemberAssignment")]
#[serde(into = "codec::MemberAssignment")]
pub struct MemberAssignment {
    pub version: i16,
    pub assignment: ConsumerProtocolAssignment,
}

impl fmt::Display for MemberAssignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.assignment.to_string().as_str())
    }
}

impl MemberAssignment {
    pub fn version(self, version: i16) -> Self {
        Self { version, ..self }
    }

    pub fn assignment(self, assignment: ConsumerProtocolAssignment) -> Self {
        Self { assignment, ..self }
    }
}

impl TryFrom<Bytes> for MemberAssignment {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let mut reader = value.reader();
        let mut codec = Decoder::new(&mut reader);
        Self::deserialize(&mut codec)
    }
}

impl TryFrom<&MemberAssignment> for Bytes {
    type Error = Error;

    fn try_from(value: &MemberAssignment) -> Result<Self, Self::Error> {
        let mut encoder = Encoder::new(BytesMut::new());

        value.serialize(&mut encoder).and(Ok(Bytes::from(encoder)))
    }
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ConsumerProtocolAssignment {
    pub assigned_partitions: Vec<TopicPartition>,
    pub user_data: Option<Bytes>,
}

impl fmt::Display for ConsumerProtocolAssignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(
            self.assigned_partitions
                .iter()
                .map(|assigned| assigned.to_string())
                .collect::<Vec<_>>()
                .join(", ")
                .as_str(),
        )
    }
}

impl ConsumerProtocolAssignment {
    pub fn assigned_partitions(
        self,
        assigned_partitions: impl Iterator<Item = TopicPartition>,
    ) -> Self {
        Self {
            assigned_partitions: assigned_partitions.collect(),
            ..self
        }
    }

    pub fn user_data(self, mut user_data: impl Iterator<Item = Bytes>) -> Self {
        Self {
            user_data: user_data.next(),
            ..self
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GroupConsumer<A> {
    group_id: String,
    topics: Vec<String>,
    rebalance_timeout: Duration,
    session_timeout: Duration,
    heartbeat_interval: Duration,
    member_state: MemberState,
    metadata: MetadataResponse,
    assignor: A,
}

impl<A> GroupConsumer<A>
where
    A: ConsumerAssignor,
{
    pub fn new(
        group_id: impl Into<String>,
        topics: Vec<String>,
        metadata: MetadataResponse,
        assignor: A,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            topics,
            rebalance_timeout: Duration::from_secs(60),
            session_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(3),
            member_state: MemberState::Outsider {
                generation_id: None,
                member_id: None,
                member_assignment: None,
            },
            metadata,
            assignor,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum MemberState {
    Outsider {
        generation_id: Option<i32>,
        member_id: Option<String>,
        member_assignment: Option<MemberAssignment>,
    },
    Follower {
        generation_id: i32,
        member_id: String,
        member_assignment: Option<MemberAssignment>,
    },
    Leader {
        generation_id: i32,
        member_id: String,
        member_assignment: Option<MemberAssignment>,
    },
}

impl<A> GroupConsumer<A>
where
    A: ConsumerAssignor,
{
    const ASSIGNORS: &[Assignor] = &[Assignor::Range];

    fn join_protocol(
        &self,
        assignor: &Assignor,
        generation_id: Option<i32>,
        member_assignment: Option<&MemberAssignment>,
    ) -> Result<JoinGroupRequestProtocol, Error> {
        Bytes::try_from(
            &MemberMetadata::default()
                .version(
                    member_assignment
                        .as_ref()
                        .map_or(3, |member_assignment| member_assignment.version),
                )
                .subscription(
                    ConsumerProtocolSubscription::default()
                        .topics(self.topics.clone().into_iter())
                        .owned_partitions(
                            member_assignment
                                .as_ref()
                                .map(|member_assignment| {
                                    member_assignment.assignment.assigned_partitions.clone()
                                })
                                .unwrap_or_default()
                                .into_iter(),
                        )
                        .generation_id(generation_id.or(Some(-1))),
                ),
        )
        .map(|metadata| {
            JoinGroupRequestProtocol::default()
                .name(assignor.as_ref().into())
                .metadata(metadata)
        })
        .map_err(Into::into)
    }

    fn join_protocols(
        &self,
        generation_id: Option<i32>,
        member_assignment: Option<&MemberAssignment>,
    ) -> Result<Vec<JoinGroupRequestProtocol>, Error> {
        Self::ASSIGNORS
            .iter()
            .map(|assignor| self.join_protocol(assignor, generation_id, member_assignment))
            .collect::<Result<Vec<_>, Error>>()
    }

    fn join_request(
        &self,
        generation_id: Option<i32>,
        member_id: Option<String>,
        member_assignment: Option<MemberAssignment>,
        reason: &str,
    ) -> Result<JoinGroupRequest, Error> {
        self.join_protocols(generation_id, member_assignment.as_ref())
            .map(Some)
            .map(|protocols| {
                JoinGroupRequest::default()
                    .group_id(self.group_id.clone())
                    .protocol_type(CONSUMER.into())
                    .rebalance_timeout_ms(Some(self.rebalance_timeout.as_millis() as i32))
                    .session_timeout_ms(self.session_timeout.as_millis() as i32)
                    .member_id(member_id.to_owned().unwrap_or_default())
                    .reason(Some(reason.into()))
                    .protocols(protocols)
            })
    }

    #[instrument(skip(self, input), fields(group_id = self.group_id, member_state = ?self.member_state), ret)]
    pub fn next_action(&mut self, input: Option<Body>) -> Result<Body, Error> {
        match (&mut self.member_state, input) {
            (
                MemberState::Outsider {
                    generation_id,
                    member_id,
                    member_assignment,
                },
                None,
            ) => {
                let generation_id = *generation_id;
                let member_id = member_id.clone();
                let member_assignment = member_assignment.clone();

                self.join_request(generation_id, member_id, member_assignment, "join")
                    .map(Into::into)
            }

            (
                MemberState::Follower {
                    generation_id,
                    member_id,
                    ..
                },
                Some(Body::HeartbeatResponse(HeartbeatResponse { error_code, .. })),
            ) if error_code == i16::from(ErrorCode::RebalanceInProgress) => {
                Ok(HeartbeatRequest::default()
                    .generation_id(*generation_id)
                    .member_id(member_id.clone())
                    .into())
            }

            (
                MemberState::Outsider {
                    generation_id,
                    member_id,
                    member_assignment,
                },
                Some(Body::JoinGroupResponse(JoinGroupResponse {
                    error_code,
                    generation_id: current_generation_id,
                    member_id: required_member_id,
                    ..
                })),
            ) if error_code == i16::from(ErrorCode::MemberIdRequired) => {
                _ = generation_id.replace(current_generation_id);
                _ = member_id.replace(required_member_id);

                let generation_id = *generation_id;
                let member_id = member_id.clone();
                let member_assignment = member_assignment.clone();

                self.join_request(
                    generation_id,
                    member_id,
                    member_assignment,
                    "join member id required",
                )
                .map(Into::into)
            }

            (
                MemberState::Outsider { .. },
                Some(Body::JoinGroupResponse(JoinGroupResponse {
                    error_code,
                    generation_id,
                    protocol_type,
                    protocol_name,
                    leader,
                    member_id,
                    members,
                    ..
                })),
            ) if error_code == i16::from(ErrorCode::None) && leader == member_id => {
                self.member_state = MemberState::Leader {
                    generation_id,
                    member_id,
                    member_assignment: None,
                };

                debug!(?self.member_state);

                self.assignor
                    .assign(members.as_deref().unwrap_or_default(), &self.metadata)
                    .inspect(|assignments| {
                        for assignment in assignments {
                            debug!(
                                member_id = assignment.member_id,
                                assignment =
                                    ?MemberAssignment::try_from(assignment.assignment.clone()).ok()
                            );
                        }
                    })
                    .map(Some)
                    .map(|assignments| {
                        SyncGroupRequest::default()
                            .generation_id(generation_id)
                            .group_id(self.group_id.clone())
                            .member_id(leader)
                            .assignments(assignments)
                            .protocol_name(protocol_name)
                            .protocol_type(protocol_type)
                    })
                    .map(Into::into)
                    .map_err(Into::into)
            }

            (
                MemberState::Outsider { .. },
                Some(Body::JoinGroupResponse(JoinGroupResponse {
                    error_code,
                    generation_id,
                    protocol_type,
                    protocol_name,
                    member_id,
                    ..
                })),
            ) if i16::from(ErrorCode::None) == error_code => {
                self.member_state = MemberState::Follower {
                    generation_id,
                    member_id: member_id.clone(),
                    member_assignment: None,
                };

                debug!(?self.member_state);

                Ok(SyncGroupRequest::default()
                    .generation_id(generation_id)
                    .group_id(self.group_id.clone())
                    .member_id(member_id)
                    .assignments(Some([].into()))
                    .protocol_name(protocol_name)
                    .protocol_type(protocol_type)
                    .into())
            }

            (
                MemberState::Leader {
                    member_assignment,
                    generation_id,
                    member_id,
                },
                Some(Body::SyncGroupResponse(SyncGroupResponse { assignment, .. })),
            ) => {
                _ = member_assignment.replace(MemberAssignment::try_from(assignment)?);

                Ok(HeartbeatRequest::default()
                    .generation_id(*generation_id)
                    .member_id(member_id.clone())
                    .into())
            }

            (
                MemberState::Follower {
                    generation_id,
                    member_id,
                    ..
                },
                Some(Body::HeartbeatResponse(HeartbeatResponse { error_code, .. })),
            ) if error_code == i16::from(ErrorCode::None) => Ok(HeartbeatRequest::default()
                .generation_id(*generation_id)
                .member_id(member_id.clone())
                .into()),

            (state, input) => todo!("state: {state:?}, input: {input:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_cpa() -> Result<(), Error> {
        let user_data = Bytes::from_static(b"abc");

        let abc = TopicPartition::default().topic("pqr").partitions(0..3);

        let cpa =
            ConsumerProtocolAssignment::default().user_data(Some(user_data.clone()).into_iter());

        assert_eq!(Some(user_data), cpa.user_data);

        Ok(())
    }

    #[test]
    fn metadata_from_bytes_001() -> Result<(), Error> {
        let expected = MemberMetadata {
            version: 3,
            subscription: ConsumerProtocolSubscription {
                topics: ["test-topic".into()].into(),
                user_data: Some(Bytes::from_static(
                    b"\0\0\0\x01\0\ntest-topic\0\0\0\x01\0\0\0\x02\0\0\0\x11",
                )),
                owned_partitions: Some(
                    [TopicPartition {
                        topic: "test-topic".into(),
                        partitions: [2].into(),
                    }]
                    .into(),
                ),
                generation_id: Some(17),
                rack_id: None,
            },
        };

        let decoded = MemberMetadata::try_from(Bytes::from_static(b"\0\x03\0\0\0\x01\0\ntest-topic\0\0\0\x1c\0\0\0\x01\0\ntest-topic\0\0\0\x01\0\0\0\x02\0\0\0\x11\0\0\0\x01\0\ntest-topic\0\0\0\x01\0\0\0\x02\0\0\0\x11\xff\xff"))?;

        assert_eq!(expected, decoded);
        Ok(())
    }

    #[test]
    fn metadata_from_bytes_002() -> Result<(), Error> {
        let expected = MemberMetadata {
            version: 3,
            subscription: ConsumerProtocolSubscription {
                topics: ["test-topic".into()].into(),
                user_data: Some(Bytes::from_static(
                    b"\0\0\0\x01\0\ntest-topic\0\0\0\x01\0\0\0\x02\0\0\0\x0e",
                )),
                owned_partitions: Some(
                    [TopicPartition {
                        topic: "test-topic".into(),
                        partitions: [2].into(),
                    }]
                    .into(),
                ),
                generation_id: Some(14),
                rack_id: None,
            },
        };

        let decoded = MemberMetadata::try_from(Bytes::from_static(b"\0\x03\0\0\0\x01\0\ntest-topic\0\0\0\x1c\0\0\0\x01\0\ntest-topic\0\0\0\x01\0\0\0\x02\0\0\0\x0e\0\0\0\x01\0\ntest-topic\0\0\0\x01\0\0\0\x02\0\0\0\x0e\xff\xff"))?;

        assert_eq!(expected, decoded);
        Ok(())
    }
}
