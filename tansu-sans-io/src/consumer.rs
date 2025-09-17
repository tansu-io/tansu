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

use std::str::FromStr;

use bytes::{Buf as _, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::{
    Decoder, Encoder, Error, MetadataResponse, join_group_response::JoinGroupResponseMember,
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
    const COOPERATIVE_STICKY: &str = "cooperative-sticky";
    const RANGE: &str = "range";
    const ROUND_ROBIN: &str = "roundrobin";
    const UNIFORM: &str = "uniform";
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
        let mut writer = BytesMut::new().writer();
        let mut encoder = Encoder::new(&mut writer);

        value
            .serialize(&mut encoder)
            .and(Ok(Bytes::from(writer.into_inner())))
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
        let mut writer = BytesMut::new().writer();
        let mut encoder = Encoder::new(&mut writer);

        value
            .serialize(&mut encoder)
            .and(Ok(Bytes::from(writer.into_inner())))
    }
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ConsumerProtocolAssignment {
    pub assigned_partitions: Vec<TopicPartition>,
    pub user_data: Option<Bytes>,
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
}
