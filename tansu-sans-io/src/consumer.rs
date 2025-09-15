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

use bytes::{Buf as _, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::{Decoder, Encoder, Error};

mod codec;

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "codec::MemberMetadata")]
#[serde(into = "codec::MemberMetadata")]
pub struct MemberMetadata {
    pub version: i16,
    pub subscription: ConsumerProtocolSubscription,
}

impl TryFrom<Bytes> for MemberMetadata {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let mut reader = value.reader();
        let mut decoder = Decoder::new(&mut reader);
        Self::deserialize(&mut decoder)
    }
}

impl TryFrom<MemberMetadata> for Bytes {
    type Error = Error;

    fn try_from(value: MemberMetadata) -> Result<Self, Self::Error> {
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

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TopicPartition {
    pub topic: String,
    pub partitions: Vec<i32>,
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "codec::MemberAssignment")]
#[serde(into = "codec::MemberAssignment")]
pub struct MemberAssignment {
    pub version: i16,
    pub assignment: ConsumerProtocolAssignment,
}

impl TryFrom<Bytes> for MemberAssignment {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let mut reader = value.reader();
        let mut codec = Decoder::new(&mut reader);
        Self::deserialize(&mut codec)
    }
}

impl TryFrom<MemberAssignment> for Bytes {
    type Error = Error;

    fn try_from(value: MemberAssignment) -> Result<Self, Self::Error> {
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
