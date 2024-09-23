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

#[cfg_attr(feature = "nightly-features", feature(error_generic_member_access))]
#[cfg(feature = "nightly-features")]
use std::backtrace::Backtrace;
use std::{
    collections::BTreeMap,
    fmt, io,
    num::TryFromIntError,
    result,
    str::{FromStr, Utf8Error},
    string::FromUtf8Error,
    sync::{Arc, PoisonError},
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tansu_kafka_sans_io::{
    broker_registration_request::Listener,
    create_topics_request::{CreatableReplicaAssignment, CreatableTopic},
    fetch_request::FetchTopic,
    list_partition_reassignments_request::ListPartitionReassignmentsTopics,
    produce_request::TopicProduceData,
    ErrorCode,
};
use tansu_raft::Index;
use tansu_storage::Topition;
use thiserror::Error;
use uuid::Uuid;

pub mod broker;
pub mod command;
pub mod coordinator;
pub mod raft;

static RAFT_LOG: &str = "raft_log";
static RAFT_STATE: &str = "raft_state";
static CONSUMER_OFFSETS: &str = "consumer_offsets";
pub static NUM_CONSUMER_OFFSETS_PARTITIONS: i32 = 3;

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct State {
    applied: Index,
    cluster_id: Option<String>,
    brokers: BTreeMap<i32, BrokerDetail>,
    topics: BTreeMap<String, TopicDetail>,
    topic_uuid_to_name: BTreeMap<Uuid, String>,
}

impl State {
    fn new() -> Self {
        let mut topics = BTreeMap::new();

        _ = topics.insert(
            RAFT_LOG.to_owned(),
            TopicDetail {
                id: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                creatable_topic: CreatableTopic {
                    name: RAFT_LOG.to_owned(),
                    num_partitions: 1,
                    replication_factor: 0,
                    assignments: Some([].into()),
                    configs: Some([].into()),
                },
            },
        );

        _ = topics.insert(
            RAFT_STATE.to_owned(),
            TopicDetail {
                id: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                creatable_topic: CreatableTopic {
                    name: RAFT_STATE.to_owned(),
                    num_partitions: 1,
                    replication_factor: 0,
                    assignments: Some([].into()),
                    configs: Some([].into()),
                },
            },
        );

        _ = topics.insert(
            CONSUMER_OFFSETS.to_owned(),
            TopicDetail {
                id: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                creatable_topic: CreatableTopic {
                    name: CONSUMER_OFFSETS.to_owned(),
                    num_partitions: 1,
                    replication_factor: 0,
                    assignments: Some([].into()),
                    configs: Some([].into()),
                },
            },
        );

        Self {
            topics,
            ..Default::default()
        }
    }

    #[allow(dead_code)]
    fn topics(&self) -> &BTreeMap<String, TopicDetail> {
        &self.topics
    }

    fn topic(&self, id: TopicId) -> Option<&TopicDetail> {
        match id {
            TopicId::Name(ref name) => self.topics.get(name),

            TopicId::Id(ref id) => self
                .topic_uuid_to_name
                .get(id)
                .and_then(|name| self.topics.get(name)),
        }
    }

    #[allow(dead_code)]
    fn topition(&self, id: TopicId, partition: i32) -> Option<Topition> {
        match id {
            TopicId::Name(ref name) => Some(Topition::new(name, partition)),

            TopicId::Id(ref id) => self
                .topic_uuid_to_name
                .get(id)
                .map(|name| Topition::new(name, partition)),
        }
    }
}

impl TryFrom<&State> for Bytes {
    type Error = Error;

    fn try_from(state: &State) -> Result<Self, Self::Error> {
        serde_json::to_vec(state)
            .map(Bytes::from)
            .map_err(|error| error.into())
    }
}

impl TryFrom<State> for Bytes {
    type Error = Error;

    fn try_from(state: State) -> Result<Self, Self::Error> {
        serde_json::to_vec(&state)
            .map(Bytes::from)
            .map_err(|error| error.into())
    }
}

impl TryFrom<&Bytes> for State {
    type Error = Error;

    fn try_from(value: &Bytes) -> Result<Self, Self::Error> {
        serde_json::from_slice::<State>(&value.slice(..)).map_err(Into::into)
    }
}

impl TryFrom<Bytes> for State {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        serde_json::from_slice::<State>(&value.slice(..)).map_err(Into::into)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TopicId {
    Name(String),
    Id(Uuid),
}

impl From<&FetchTopic> for TopicId {
    fn from(value: &FetchTopic) -> Self {
        if let Some(ref topic) = value.topic {
            Self::Name(topic.to_string())
        } else if let Some(topic_id) = value.topic_id {
            Self::Id(Uuid::from_bytes(topic_id))
        } else {
            todo!()
        }
    }
}

impl From<&ListPartitionReassignmentsTopics> for TopicId {
    fn from(value: &ListPartitionReassignmentsTopics) -> Self {
        Self::Name(value.name.to_owned())
    }
}

impl From<&TopicProduceData> for TopicId {
    fn from(value: &TopicProduceData) -> Self {
        Self::Name(value.name.to_owned())
    }
}

impl From<&CreatableTopic> for TopicId {
    fn from(value: &CreatableTopic) -> Self {
        Self::Name(value.name.to_owned())
    }
}

impl From<&str> for TopicId {
    fn from(value: &str) -> Self {
        Self::Name(value.to_string())
    }
}

impl From<String> for TopicId {
    fn from(value: String) -> Self {
        Self::Name(value)
    }
}

impl FromStr for TopicId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::Name(s.to_string()))
    }
}

impl From<[u8; 16]> for TopicId {
    fn from(value: [u8; 16]) -> Self {
        Self::Id(Uuid::from_bytes(value))
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TopicDetail {
    id: [u8; 16],
    creatable_topic: CreatableTopic,
}

impl TopicDetail {
    pub fn id(&self) -> [u8; 16] {
        self.id
    }

    pub fn name(&self) -> &str {
        self.creatable_topic.name.as_str()
    }

    pub fn replica_assignments(&self) -> Option<&[CreatableReplicaAssignment]> {
        self.creatable_topic.assignments.as_deref()
    }
}

#[derive(
    Copy, Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct NodeDetail {
    port: u16,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct BrokerDetail {
    incarnation_id: Uuid,
    listeners: Option<Vec<Listener>>,
    rack: Option<String>,
}

#[derive(Error, Debug)]
pub enum Error {
    Api(ErrorCode),
    ClientRpc(#[from] tarpc::client::RpcError),
    Custom(String),
    EmptyCoordinatorWrapper,
    EmptyJoinGroupRequestProtocol,
    ExpectedJoinGroupRequestProtocol(&'static str),
    Io(Arc<io::Error>),
    Json(#[from] serde_json::Error),
    KafkaProtocol {
        #[from]
        source: tansu_kafka_sans_io::Error,
        #[cfg(feature = "nightly-features")]
        backtrace: Backtrace,
    },
    Message(String),
    Model(#[from] tansu_kafka_model::Error),
    ParseInt(#[from] std::num::ParseIntError),
    Poison,
    Pool(#[from] deadpool_postgres::PoolError),
    Raft(#[from] tansu_raft::Error),
    Sasl(#[from] rsasl::prelude::SASLError),
    SaslSession(#[from] rsasl::prelude::SessionError),
    Storage(#[from] tansu_storage::Error),
    StringUtf8(#[from] FromUtf8Error),
    TokioPostgres(#[from] tokio_postgres::error::Error),
    TryFromInt(#[from] TryFromIntError),
    Url(#[from] url::ParseError),
    Utf8(#[from] Utf8Error),
    Uuid(#[from] uuid::Error),
}

impl From<Error> for tansu_raft::Error {
    fn from(error: Error) -> Self {
        tansu_raft::Error::Custom(format!("{error:?}"))
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(msg) => write!(f, "{}", msg),
            error => write!(f, "{:?}", error),
        }
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;
