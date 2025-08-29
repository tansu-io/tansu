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
//
//! Tansu Storage Abstraction
//!
//! Provides an abstraction over Storage. Currently implemented as:
//! - [`StorageContainer`], which implements [`Storage`] and can be configured to either use [`Postgres`] or [`DynoStore`].
//!
//! The underlying storage implementations of [`Storage`]:
//! - [`Postgres`]
//! - [`DynoStore`]
//!

use async_trait::async_trait;
use bytes::Bytes;
use dynostore::DynoStore;
use glob::{GlobError, PatternError};
use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    metrics::{Counter, Meter},
};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use pg::Postgres;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    array::TryFromSliceError,
    collections::BTreeMap,
    ffi::OsString,
    fmt::{self, Debug, Display, Formatter},
    fs::DirEntry,
    io,
    num::{ParseIntError, TryFromIntError},
    path::PathBuf,
    result,
    str::FromStr,
    sync::{Arc, LazyLock, PoisonError},
    time::{Duration, SystemTime, SystemTimeError},
};
use tansu_sans_io::{
    Body, ConfigResource, ErrorCode, IsolationLevel, NULL_TOPIC_ID,
    add_partitions_to_txn_request::{
        AddPartitionsToTxnRequest, AddPartitionsToTxnTopic, AddPartitionsToTxnTransaction,
    },
    add_partitions_to_txn_response::{AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult},
    consumer_group_describe_response,
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    delete_topics_request::DeleteTopicState,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_groups_response,
    describe_topic_partitions_request::{Cursor, TopicRequest},
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    fetch_request::FetchTopic,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    join_group_response::JoinGroupResponseMember,
    list_groups_response::ListedGroup,
    metadata_request::MetadataRequestTopic,
    metadata_response::{MetadataResponseBroker, MetadataResponseTopic},
    offset_commit_request::OffsetCommitRequestPartition,
    record::deflated,
    to_system_time, to_timestamp,
    txn_offset_commit_request::TxnOffsetCommitRequestTopic,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use tracing::{Instrument, debug, debug_span};
use tracing_subscriber::filter::ParseError;
use url::Url;
use uuid::Uuid;

pub mod dynostore;
pub mod pg;
pub mod service;

mod os;

/// Storage Errors
#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    Api(ErrorCode),
    DeadPoolBuild(#[from] deadpool::managed::BuildError),
    Glob(Arc<GlobError>),
    Io(Arc<io::Error>),
    KafkaSansIo(#[from] tansu_sans_io::Error),
    LessThanBaseOffset {
        offset: i64,
        base_offset: i64,
    },
    LessThanLastOffset {
        offset: i64,
        last_offset: Option<i64>,
    },

    LessThanMaxTime {
        time: i64,
        max_time: Option<i64>,
    },
    LessThanMinTime {
        time: i64,
        min_time: Option<i64>,
    },
    Message(String),
    NoSuchEntry {
        nth: u32,
    },
    NoSuchOffset(i64),
    OsString(OsString),
    ObjectStore(Arc<object_store::Error>),
    ParseFilter(Arc<ParseError>),
    Pattern(Arc<PatternError>),
    ParseInt(#[from] ParseIntError),
    PhantomCached(),
    Poison,
    Pool(Arc<deadpool_postgres::PoolError>),
    Regex(#[from] regex::Error),
    Schema(Arc<tansu_schema::Error>),

    SegmentEmpty(Topition),

    SegmentMissing {
        topition: Topition,
        offset: Option<i64>,
    },

    SerdeJson(Arc<serde_json::Error>),
    SystemTime(#[from] SystemTimeError),
    TokioPostgres(Arc<tokio_postgres::error::Error>),
    TryFromInt(#[from] TryFromIntError),
    TryFromSlice(#[from] TryFromSliceError),
    UnexpectedBody(Box<Body>),
    UnexpectedAddPartitionsToTxnRequest(Box<AddPartitionsToTxnRequest>),
    Url(#[from] url::ParseError),
    UnknownTxnState(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl From<GlobError> for Error {
    fn from(value: GlobError) -> Self {
        Self::Glob(Arc::new(value))
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl From<object_store::Error> for Error {
    fn from(value: object_store::Error) -> Self {
        Self::ObjectStore(Arc::new(value))
    }
}

impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        Self::ParseFilter(Arc::new(value))
    }
}

impl From<PatternError> for Error {
    fn from(value: PatternError) -> Self {
        Self::Pattern(Arc::new(value))
    }
}

impl From<deadpool_postgres::PoolError> for Error {
    fn from(value: deadpool_postgres::PoolError) -> Self {
        Self::Pool(Arc::new(value))
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::SerdeJson(Arc::new(value))
    }
}

impl From<tokio_postgres::error::Error> for Error {
    fn from(value: tokio_postgres::error::Error) -> Self {
        Self::TokioPostgres(Arc::new(value))
    }
}

impl From<tansu_schema::Error> for Error {
    fn from(value: tansu_schema::Error) -> Self {
        if let tansu_schema::Error::Api(error_code) = value {
            Self::Api(error_code)
        } else {
            Self::Schema(Arc::new(value))
        }
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;

/// Topic Partition (topition)
///
/// A topic partition pair.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Topition {
    topic: String,
    partition: i32,
}

impl Topition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        let topic = topic.into();
        Self { topic, partition }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }
}

impl From<Cursor> for Topition {
    fn from(value: Cursor) -> Self {
        Self {
            topic: value.topic_name,
            partition: value.partition_index,
        }
    }
}

impl TryFrom<&DirEntry> for Topition {
    type Error = Error;

    fn try_from(value: &DirEntry) -> result::Result<Self, Self::Error> {
        Regex::new(r"^(?<topic>.+)-(?<partition>\d{10})$")
            .map_err(Into::into)
            .and_then(|re| {
                value
                    .file_name()
                    .into_string()
                    .map_err(Error::OsString)
                    .and_then(|ref file_name| {
                        re.captures(file_name)
                            .ok_or(Error::Message(format!("no captures for {file_name}")))
                            .and_then(|ref captures| {
                                let topic = captures
                                    .name("topic")
                                    .ok_or(Error::Message(format!("missing topic for {file_name}")))
                                    .map(|s| s.as_str().to_owned())?;

                                let partition = captures
                                    .name("partition")
                                    .ok_or(Error::Message(format!(
                                        "missing partition for: {file_name}"
                                    )))
                                    .map(|s| s.as_str())
                                    .and_then(|s| str::parse(s).map_err(Into::into))?;

                                Ok(Self { topic, partition })
                            })
                    })
            })
    }
}

impl FromStr for Topition {
    type Err = Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        i32::from_str(&s[s.len() - 10..])
            .map(|partition| {
                let topic = String::from(&s[..s.len() - 11]);

                Self { topic, partition }
            })
            .map_err(Into::into)
    }
}

impl From<&Topition> for PathBuf {
    fn from(value: &Topition) -> Self {
        let topic = value.topic.as_str();
        let partition = value.partition;
        PathBuf::from(format!("{topic}-{partition:0>10}"))
    }
}

/// Topic Partition Offset
///
/// A topic partition with an offset.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TopitionOffset {
    topition: Topition,
    offset: i64,
}

impl TopitionOffset {
    pub fn new(topition: Topition, offset: i64) -> Self {
        Self { topition, offset }
    }

    pub fn topition(&self) -> &Topition {
        &self.topition
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }
}

impl From<&TopitionOffset> for PathBuf {
    fn from(value: &TopitionOffset) -> Self {
        let offset = value.offset;
        PathBuf::from(value.topition()).join(format!("{offset:0>20}"))
    }
}

/// List Offset Request
///
/// An enumeration of offset request types, with conversion from/to an i64 protocol representation.
///
#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ListOffsetRequest {
    #[default]
    Earliest,
    Latest,
    Timestamp(SystemTime),
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ListOffsetResponse {
    pub error_code: ErrorCode,
    pub timestamp: Option<SystemTime>,
    pub offset: Option<i64>,
}

impl Default for ListOffsetResponse {
    fn default() -> Self {
        Self {
            error_code: ErrorCode::None,
            timestamp: None,
            offset: None,
        }
    }
}

impl ListOffsetResponse {
    pub fn offset(&self) -> Option<i64> {
        self.offset
    }

    pub fn timestamp(&self) -> Result<Option<i64>> {
        self.timestamp.map_or(Ok(None), |system_time| {
            to_timestamp(system_time).map(Some).map_err(Into::into)
        })
    }

    pub fn error_code(&self) -> ErrorCode {
        self.error_code
    }
}

impl TryFrom<ListOffsetRequest> for i64 {
    type Error = Error;

    fn try_from(value: ListOffsetRequest) -> Result<Self, Self::Error> {
        match value {
            ListOffsetRequest::Earliest => Ok(-2),
            ListOffsetRequest::Latest => Ok(-1),
            ListOffsetRequest::Timestamp(timestamp) => to_timestamp(timestamp).map_err(Into::into),
        }
    }
}

impl TryFrom<i64> for ListOffsetRequest {
    type Error = Error;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        match value {
            -2 => Ok(ListOffsetRequest::Earliest),
            -1 => Ok(ListOffsetRequest::Latest),
            timestamp => to_system_time(timestamp)
                .map(ListOffsetRequest::Timestamp)
                .map_err(Into::into),
        }
    }
}

/// Offset Commit Request
///
/// A structure representing an [`tansu_sans_io::OffsetCommitRequestPartition](OffsetCommitRequestPartition).
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct OffsetCommitRequest {
    offset: i64,
    leader_epoch: Option<i32>,
    timestamp: Option<SystemTime>,
    metadata: Option<String>,
}

impl OffsetCommitRequest {
    pub fn offset(self, offset: i64) -> Self {
        Self { offset, ..self }
    }
}

impl TryFrom<&OffsetCommitRequestPartition> for OffsetCommitRequest {
    type Error = Error;

    fn try_from(value: &OffsetCommitRequestPartition) -> Result<Self, Self::Error> {
        value
            .commit_timestamp
            .map_or(Ok(None), |commit_timestamp| {
                to_system_time(commit_timestamp)
                    .map(Some)
                    .map_err(Into::into)
            })
            .map(|timestamp| Self {
                offset: value.committed_offset,
                leader_epoch: value.committed_leader_epoch,
                timestamp,
                metadata: value.committed_metadata.clone(),
            })
    }
}

/// Topic Id
///
/// An enumeration of either the name or UUID of a topic.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TopicId {
    Name(String),
    Id(Uuid),
}

impl FromStr for TopicId {
    type Err = Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        Ok(Self::Name(s.into()))
    }
}

impl From<&str> for TopicId {
    fn from(value: &str) -> Self {
        Self::Name(value.to_owned())
    }
}

impl From<String> for TopicId {
    fn from(value: String) -> Self {
        Self::Name(value)
    }
}

impl From<Uuid> for TopicId {
    fn from(value: Uuid) -> Self {
        Self::Id(value)
    }
}

impl From<[u8; 16]> for TopicId {
    fn from(value: [u8; 16]) -> Self {
        Self::Id(Uuid::from_bytes(value))
    }
}

impl From<&TopicId> for [u8; 16] {
    fn from(value: &TopicId) -> Self {
        match value {
            TopicId::Id(id) => id.into_bytes(),
            TopicId::Name(_) => NULL_TOPIC_ID,
        }
    }
}

impl From<&FetchTopic> for TopicId {
    fn from(value: &FetchTopic) -> Self {
        if let Some(ref name) = value.topic {
            Self::Name(name.into())
        } else if let Some(ref id) = value.topic_id {
            Self::Id(Uuid::from_bytes(*id))
        } else {
            panic!("neither name nor uuid")
        }
    }
}

impl From<&MetadataRequestTopic> for TopicId {
    fn from(value: &MetadataRequestTopic) -> Self {
        if let Some(ref name) = value.name {
            Self::Name(name.into())
        } else if let Some(ref id) = value.topic_id {
            Self::Id(Uuid::from_bytes(*id))
        } else {
            panic!("neither name nor uuid")
        }
    }
}

impl From<DeleteTopicState> for TopicId {
    fn from(value: DeleteTopicState) -> Self {
        match value {
            DeleteTopicState {
                name: Some(name),
                topic_id,
                ..
            } if topic_id == NULL_TOPIC_ID => name.into(),

            DeleteTopicState { topic_id, .. } => topic_id.into(),
        }
    }
}

impl From<&TopicRequest> for TopicId {
    fn from(value: &TopicRequest) -> Self {
        value.name.to_owned().into()
    }
}

impl From<&Topition> for TopicId {
    fn from(value: &Topition) -> Self {
        value.topic.to_owned().into()
    }
}

/// Broker Registration Request
///
/// A broker will register with storage using this structure.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct BrokerRegistrationRequest {
    pub broker_id: i32,
    pub cluster_id: String,
    pub incarnation_id: Uuid,
    pub rack: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct MetadataResponse {
    cluster: Option<String>,
    controller: Option<i32>,
    brokers: Vec<MetadataResponseBroker>,
    topics: Vec<MetadataResponseTopic>,
}

impl MetadataResponse {
    pub fn cluster(&self) -> Option<&str> {
        self.cluster.as_deref()
    }

    pub fn controller(&self) -> Option<i32> {
        self.controller
    }

    pub fn brokers(&self) -> &[MetadataResponseBroker] {
        self.brokers.as_ref()
    }

    pub fn topics(&self) -> &[MetadataResponseTopic] {
        self.topics.as_ref()
    }
}

/// Offset Stage
///
/// An offset stage structure represents the `last_stable`, `high_watermark` and `log_start` offsets.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct OffsetStage {
    last_stable: i64,
    high_watermark: i64,
    log_start: i64,
}

impl OffsetStage {
    pub fn last_stable(&self) -> i64 {
        self.last_stable
    }

    pub fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    pub fn log_start(&self) -> i64 {
        self.log_start
    }
}

/// Group Member
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct GroupMember {
    pub join_response: JoinGroupResponseMember,
    pub last_contact: Option<SystemTime>,
}

/// Group State
///
/// A group is either in the process of [`GroupState::Forming`] or has [`GroupState::Formed`].
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum GroupState {
    Forming {
        protocol_type: Option<String>,
        protocol_name: Option<String>,
        leader: Option<String>,
    },

    Formed {
        protocol_type: String,
        protocol_name: String,
        leader: String,
        assignments: BTreeMap<String, Bytes>,
    },
}

impl GroupState {
    pub fn protocol_type(&self) -> Option<String> {
        match self {
            Self::Forming { protocol_type, .. } => protocol_type.clone(),
            Self::Formed { protocol_type, .. } => Some(protocol_type.clone()),
        }
    }

    pub fn protocol_name(&self) -> Option<String> {
        match self {
            Self::Forming { protocol_name, .. } => protocol_name.clone(),
            Self::Formed { protocol_name, .. } => Some(protocol_name.clone()),
        }
    }

    pub fn leader(&self) -> Option<String> {
        match self {
            Self::Forming { leader, .. } => leader.clone(),
            Self::Formed { leader, .. } => Some(leader.clone()),
        }
    }

    pub fn assignments(&self) -> BTreeMap<String, Bytes> {
        match self {
            Self::Forming { .. } => BTreeMap::new(),
            Self::Formed { assignments, .. } => assignments.clone(),
        }
    }
}

impl Default for GroupState {
    fn default() -> Self {
        Self::Forming {
            protocol_type: None,
            protocol_name: Some("".into()),
            leader: None,
        }
    }
}

/// Consumer Group State
///
/// A helper type for conversion into [`consumer_group_describe_response::DescribedGroup`].
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum ConsumerGroupState {
    Unknown,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
    Empty,
    Assigning,
    Reconciling,
}

impl Display for ConsumerGroupState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => f.write_str("Unknown"),
            Self::PreparingRebalance => f.write_str("PreparingRebalance"),
            Self::CompletingRebalance => f.write_str("CompletingRebalance"),
            Self::Stable => f.write_str("Stable"),
            Self::Dead => f.write_str("Dead"),
            Self::Empty => f.write_str("Empty"),
            Self::Assigning => f.write_str("Assigning"),
            Self::Reconciling => f.write_str("Reconciling"),
        }
    }
}

/// Group Detail
///
/// A helper type that can be easily converted into [`consumer_group_describe_response::DescribedGroup`].
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct GroupDetail {
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: Option<i32>,
    pub members: BTreeMap<String, GroupMember>,
    pub generation_id: i32,
    pub skip_assignment: Option<bool>,
    pub inception: SystemTime,
    pub state: GroupState,
}

impl Default for GroupDetail {
    fn default() -> Self {
        Self {
            session_timeout_ms: 45_000,
            rebalance_timeout_ms: None,
            members: BTreeMap::new(),
            generation_id: -1,
            skip_assignment: Some(false),
            inception: SystemTime::now(),
            state: GroupState::default(),
        }
    }
}

impl From<&GroupDetail> for ConsumerGroupState {
    fn from(value: &GroupDetail) -> Self {
        match value {
            GroupDetail { members, .. } if members.is_empty() => Self::Empty,

            GroupDetail {
                state: GroupState::Forming { leader: None, .. },
                ..
            } => Self::Assigning,

            GroupDetail {
                state: GroupState::Formed { .. },
                ..
            } => Self::Stable,

            _ => {
                debug!(unknown = ?value);
                Self::Unknown
            }
        }
    }
}

impl From<&GroupDetail> for consumer_group_describe_response::DescribedGroup {
    fn from(value: &GroupDetail) -> Self {
        let assignor_name = match value.state {
            GroupState::Forming { ref leader, .. } => leader.clone().unwrap_or_default(),
            GroupState::Formed { ref leader, .. } => leader.clone(),
        };

        let group_state = ConsumerGroupState::from(value).to_string();

        Self::default()
            .error_code(ErrorCode::None.into())
            .error_message(Some(ErrorCode::None.to_string()))
            .group_id(Default::default())
            .group_state(group_state)
            .group_epoch(-1)
            .assignment_epoch(-1)
            .assignor_name(assignor_name)
            .members(Some([].into()))
            .authorized_operations(-1)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum GroupDetailResponse {
    ErrorCode(ErrorCode),
    Found(GroupDetail),
}

/// NamedGroupDetail
///
/// A helper type that can be easily converted into [`consumer_group_describe_response::DescribedGroup`]
/// or [`describe_groups_response::DescribedGroup`].
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct NamedGroupDetail {
    name: String,
    response: GroupDetailResponse,
}

impl NamedGroupDetail {
    pub fn error_code(name: String, error_code: ErrorCode) -> Self {
        Self {
            name,
            response: GroupDetailResponse::ErrorCode(error_code),
        }
    }

    pub fn found(name: String, found: GroupDetail) -> Self {
        Self {
            name,
            response: GroupDetailResponse::Found(found),
        }
    }
}

impl From<&NamedGroupDetail> for consumer_group_describe_response::DescribedGroup {
    fn from(value: &NamedGroupDetail) -> Self {
        match value {
            NamedGroupDetail {
                name,
                response: GroupDetailResponse::Found(group_detail),
            } => {
                let assignor_name = match group_detail.state {
                    GroupState::Forming { ref leader, .. } => leader.clone().unwrap_or_default(),
                    GroupState::Formed { ref leader, .. } => leader.clone(),
                };

                let group_state = ConsumerGroupState::from(group_detail).to_string();

                Self::default()
                    .error_code(ErrorCode::None.into())
                    .error_message(Some(ErrorCode::None.to_string()))
                    .group_id(name.into())
                    .group_state(group_state)
                    .group_epoch(-1)
                    .assignment_epoch(-1)
                    .assignor_name(assignor_name)
                    .members(Some([].into()))
                    .authorized_operations(-1)
            }

            NamedGroupDetail {
                name,
                response: GroupDetailResponse::ErrorCode(error_code),
            } => Self::default()
                .error_code((*error_code).into())
                .error_message(Some(error_code.to_string()))
                .group_id(name.into())
                .group_state("Unknown".into())
                .group_epoch(-1)
                .assignment_epoch(-1)
                .assignor_name("".into())
                .members(Some([].into()))
                .authorized_operations(-1),
        }
    }
}

impl From<&NamedGroupDetail> for describe_groups_response::DescribedGroup {
    fn from(value: &NamedGroupDetail) -> Self {
        match value {
            NamedGroupDetail {
                name,
                response: GroupDetailResponse::Found(group_detail),
            } => {
                let group_state = ConsumerGroupState::from(group_detail).to_string();

                let members = group_detail
                    .members
                    .keys()
                    .map(|member_id| {
                        describe_groups_response::DescribedGroupMember::default()
                            .member_id(member_id.into())
                            .group_instance_id(None)
                            .client_id("".into())
                            .client_host("".into())
                            .member_metadata(Bytes::new())
                            .member_assignment(Bytes::new())
                    })
                    .collect::<Vec<_>>();

                Self::default()
                    .error_code(ErrorCode::None.into())
                    .group_id(name.clone())
                    .group_state(group_state)
                    .protocol_type(group_detail.state.protocol_type().unwrap_or_default())
                    .protocol_data("".into())
                    .members(Some(members))
                    .authorized_operations(Some(-1))
            }

            NamedGroupDetail {
                name,
                response: GroupDetailResponse::ErrorCode(error_code),
            } => Self::default()
                .error_code((*error_code).into())
                .group_id(name.clone())
                .group_state("Unknown".into())
                .protocol_type("".into())
                .protocol_data("".into())
                .members(Some(vec![]))
                .authorized_operations(Some(-1)),
        }
    }
}

/// Topition (topic partition) Detail
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TopitionDetail {
    error: ErrorCode,
    topic: TopicId,
    partitions: Option<Vec<PartitionDetail>>,
}

/// Partition Detail
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PartitionDetail {
    error: ErrorCode,
    partition_index: i32,
}

/// Version
///
/// Representing an `e_tag` and `version` used in conditional writes to an object store.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Version {
    e_tag: Option<String>,
    version: Option<String>,
}

/// Producer Id Response
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ProducerIdResponse {
    pub error: ErrorCode,
    pub id: i64,
    pub epoch: i16,
}

impl Default for ProducerIdResponse {
    fn default() -> Self {
        Self {
            error: ErrorCode::None,
            id: 1,
            epoch: 0,
        }
    }
}

/// Transaction Add Partitions Request
///
/// For protocol versions 0..=3 using `AddPartitionsToTxnTopic`, thereafter using `AddPartitionsToTxnTransaction`.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TxnAddPartitionsRequest {
    VersionZeroToThree {
        transaction_id: String,
        producer_id: i64,
        producer_epoch: i16,
        topics: Vec<AddPartitionsToTxnTopic>,
    },

    VersionFourPlus {
        transactions: Vec<AddPartitionsToTxnTransaction>,
    },
}

impl TryFrom<AddPartitionsToTxnRequest> for TxnAddPartitionsRequest {
    type Error = Error;

    fn try_from(value: AddPartitionsToTxnRequest) -> result::Result<Self, Self::Error> {
        match value {
            AddPartitionsToTxnRequest {
                transactions: None,
                v_3_and_below_transactional_id: Some(transactional_id),
                v_3_and_below_producer_id: Some(producer_id),
                v_3_and_below_producer_epoch: Some(producer_epoch),
                v_3_and_below_topics: Some(topics),
                ..
            } => Ok(Self::VersionZeroToThree {
                transaction_id: transactional_id,
                producer_id,
                producer_epoch,
                topics,
            }),

            AddPartitionsToTxnRequest {
                transactions: Some(transactions),
                v_3_and_below_transactional_id: None,
                v_3_and_below_producer_id: None,
                v_3_and_below_producer_epoch: None,
                v_3_and_below_topics: None,
                ..
            } => Ok(Self::VersionFourPlus { transactions }),

            unexpected => Err(Error::UnexpectedAddPartitionsToTxnRequest(Box::new(
                unexpected,
            ))),
        }
    }
}

impl TryFrom<Body> for TxnAddPartitionsRequest {
    type Error = Error;

    fn try_from(value: Body) -> result::Result<Self, Self::Error> {
        match value {
            Body::AddPartitionsToTxnRequest(add_partitions_to_txn) => {
                TxnAddPartitionsRequest::try_from(add_partitions_to_txn)
            }

            unexpected => Err(Error::UnexpectedBody(Box::new(unexpected))),
        }
    }
}

/// Transaction Add Partitions Response
///
/// For protocol versions 0..=3 using `AddPartitionsToTxnTopic`, thereafter using `AddPartitionsToTxnTransaction`.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TxnAddPartitionsResponse {
    VersionZeroToThree(Vec<AddPartitionsToTxnTopicResult>),
    VersionFourPlus(Vec<AddPartitionsToTxnResult>),
}

impl TxnAddPartitionsResponse {
    pub fn zero_to_three(&self) -> &[AddPartitionsToTxnTopicResult] {
        match self {
            Self::VersionZeroToThree(result) => result.as_slice(),
            Self::VersionFourPlus(_) => &[][..],
        }
    }

    pub fn four_plus(&self) -> &[AddPartitionsToTxnResult] {
        match self {
            Self::VersionZeroToThree(_) => &[][..],
            Self::VersionFourPlus(result) => result.as_slice(),
        }
    }
}

/// Transaction Offset Commit Request
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TxnOffsetCommitRequest {
    pub transaction_id: String,
    pub group_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub generation_id: Option<i32>,
    pub member_id: Option<String>,
    pub group_instance_id: Option<String>,
    pub topics: Vec<TxnOffsetCommitRequestTopic>,
}

/// Transaction State
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TxnState {
    Begin,
    PrepareCommit,
    PrepareAbort,
    Committed,
    Aborted,
}

impl TxnState {
    pub fn is_prepared(&self) -> bool {
        match self {
            Self::PrepareAbort | Self::PrepareCommit => true,
            _otherwise => false,
        }
    }
}

impl FromStr for TxnState {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ABORTED" => Ok(TxnState::Aborted),
            "BEGIN" => Ok(TxnState::Begin),
            "COMMITTED" => Ok(TxnState::Committed),
            "PREPARE_ABORT" => Ok(TxnState::PrepareAbort),
            "PREPARE_COMMIT" => Ok(TxnState::PrepareCommit),
            otherwise => Err(Error::UnknownTxnState(otherwise.to_owned())),
        }
    }
}

impl TryFrom<String> for TxnState {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<TxnState> for String {
    fn from(value: TxnState) -> Self {
        match value {
            TxnState::Begin => "BEGIN".into(),
            TxnState::PrepareCommit => "PREPARE_COMMIT".into(),
            TxnState::PrepareAbort => "PREPARE_ABORT".into(),
            TxnState::Committed => "COMMITTED".into(),
            TxnState::Aborted => "ABORTED".into(),
        }
    }
}

/// Storage
///
/// The Core storage abstraction. All storage engines implement this type.
#[async_trait]
pub trait Storage: Clone + Debug + Send + Sync + 'static {
    /// On startup a broker will register with storage.
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()>;

    /// Create a topic on this storage.
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid>;

    /// Incrementally alter a resource on this storage.
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse>;

    /// Delete records on this storage.
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>>;

    /// Delete a topic from this storage.
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode>;

    /// Query the brokers registered with this storage.
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>>;

    /// Produce a deflated batch to this storage.
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64>;

    /// Fetch deflated batches from storage.
    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>>;

    /// Query the offset stage for a topic partition.
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage>;

    /// Query the offsets for one or more topic partitions.
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffsetRequest)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>>;

    /// Commit offsets for one or more topic partitions in a consumer group.
    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>>;

    /// Fetch committed offsets for one or more topic partitions in a consumer group.
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>>;

    /// Fetch all committed offsets in a consumer group.
    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>>;

    /// Query broker and topic metadata.
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse>;

    /// Query the configuration of a resource in this storage.
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult>;

    /// Query available groups optionally with a state filter.
    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>>;

    /// Delete one or more groups from storage.
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>>;

    /// Describe the groups found in this storage.
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>>;

    /// Describe the topic partitions found in this storage.
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>>;

    /// Conditionally update the state of a group in this storage.
    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>>;

    /// Initialise a transactional or idempotent producer in this storage.
    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse>;

    /// Add offsets to a transaction for a producer.
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode>;

    /// Add partitions to a transaction.
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse>;

    /// Commit an offset within a transaction.
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>>;

    /// Commit or abort a running transaction.
    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode>;

    /// Run periodic maintenance on this storage.
    async fn maintain(&self) -> Result<()> {
        Ok(())
    }

    fn cluster_id(&self) -> Result<&str>;

    fn node(&self) -> Result<i32>;

    fn advertised_listener(&self) -> Result<&Url>;
}

/// Conditional Update Errors
#[derive(Debug, thiserror::Error)]
pub enum UpdateError<T> {
    Error(#[from] Error),
    ObjectStore(#[from] object_store::Error),
    Outdated { current: T, version: Version },
    SerdeJson(#[from] serde_json::Error),
    TokioPostgres(#[from] tokio_postgres::error::Error),
    MissingEtag,
    Uuid(#[from] uuid::Error),
}

/// Storage Container
///
/// An enumeration of available storage implementations.
/// - [`Postgres`]
/// - [`DynoStore`]
#[derive(Clone, Debug)]
pub enum StorageContainer {
    Postgres(Postgres),
    DynoStore(DynoStore),
}

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

static STORAGE_CONTAINER_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_storage_container_requests")
        .with_description("tansu storage container requests")
        .build()
});

static STORAGE_CONTAINER_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_storage_container_errors")
        .with_description("tansu storage container errors")
        .build()
});

#[async_trait]
impl Storage for StorageContainer {
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        let attributes = [KeyValue::new("method", "register_broker")];

        match self {
            Self::Postgres(pg) => pg.register_broker(broker_registration).await,
            Self::DynoStore(dyn_store) => dyn_store.register_broker(broker_registration).await,
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let attributes = [KeyValue::new("method", "incremental_alter_resource")];

        match self {
            Self::Postgres(pg) => pg.incremental_alter_resource(resource).await,
            Self::DynoStore(dyn_store) => dyn_store.incremental_alter_resource(resource).await,
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        let attributes = [KeyValue::new("method", "create_topic")];
        let span = debug_span!("create_topic", ?topic, validate_only);

        async move {
            match self {
                Self::Postgres(pg) => pg.create_topic(topic, validate_only),
                Self::DynoStore(dyn_store) => dyn_store.create_topic(topic, validate_only),
            }
            .await
        }
        .instrument(span)
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        let attributes = [KeyValue::new("method", "delete_records")];

        match self {
            Self::Postgres(pg) => pg.delete_records(topics).await,
            Self::DynoStore(dyn_store) => dyn_store.delete_records(topics).await,
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        let attributes = [KeyValue::new("method", "delete_topic")];

        match self {
            Self::Postgres(pg) => pg.delete_topic(topic).await,
            Self::DynoStore(dyn_store) => dyn_store.delete_topic(topic).await,
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let attributes = [KeyValue::new("method", "brokers")];

        match self {
            Self::Postgres(pg) => pg.brokers().await,
            Self::DynoStore(dyn_store) => dyn_store.brokers().await,
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        let attributes = [KeyValue::new("method", "produce")];

        match self {
            Self::Postgres(pg) => pg.produce(transaction_id, topition, batch).await,
            Self::DynoStore(dyn_store) => dyn_store.produce(transaction_id, topition, batch).await,
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        let attributes = [KeyValue::new("method", "fetch")];

        match self {
            Self::Postgres(pg) => {
                pg.fetch(topition, offset, min_bytes, max_bytes, isolation)
                    .await
            }
            Self::DynoStore(dyn_store) => {
                dyn_store
                    .fetch(topition, offset, min_bytes, max_bytes, isolation)
                    .await
            }
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let attributes = [KeyValue::new("method", "offset_stage")];

        match self {
            Self::Postgres(pg) => pg.offset_stage(topition).await,
            Self::DynoStore(dyn_store) => dyn_store.offset_stage(topition).await,
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffsetRequest)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let attributes = [KeyValue::new("method", "list_offsets")];

        match self {
            Self::Postgres(pg) => pg.list_offsets(isolation_level, offsets).await,
            Self::DynoStore(dyn_store) => dyn_store.list_offsets(isolation_level, offsets).await,
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let attributes = [KeyValue::new("method", "offset_commit")];

        match self {
            Self::Postgres(pg) => pg.offset_commit(group_id, retention_time_ms, offsets).await,
            Self::DynoStore(dyn_store) => {
                dyn_store
                    .offset_commit(group_id, retention_time_ms, offsets)
                    .await
            }
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        let attributes = [KeyValue::new("method", "committed_offset_topitions")];

        match self {
            Self::Postgres(inner) => inner.committed_offset_topitions(group_id).await,
            Self::DynoStore(inner) => inner.committed_offset_topitions(group_id).await,
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        let attributes = [KeyValue::new("method", "offset_fetch")];

        match self {
            Self::Postgres(pg) => pg.offset_fetch(group_id, topics, require_stable).await,
            Self::DynoStore(dyn_store) => {
                dyn_store
                    .offset_fetch(group_id, topics, require_stable)
                    .await
            }
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let attributes = [KeyValue::new("method", "metadata")];

        match self {
            Self::Postgres(pg) => pg.metadata(topics).await,
            Self::DynoStore(dyn_store) => dyn_store.metadata(topics).await,
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let attributes = [KeyValue::new("method", "describe_config")];

        match self {
            Self::Postgres(pg) => pg.describe_config(name, resource, keys).await,
            Self::DynoStore(dyn_store) => dyn_store.describe_config(name, resource, keys).await,
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let attributes = [KeyValue::new("method", "describe_topic_partitions")];

        match self {
            Self::Postgres(pg) => {
                pg.describe_topic_partitions(topics, partition_limit, cursor)
                    .await
            }
            Self::DynoStore(dyn_store) => {
                dyn_store
                    .describe_topic_partitions(topics, partition_limit, cursor)
                    .await
            }
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        let attributes = [KeyValue::new("method", "list_groups")];

        match self {
            Self::Postgres(pg) => pg.list_groups(states_filter).await,
            Self::DynoStore(dyn_store) => dyn_store.list_groups(states_filter).await,
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let attributes = [KeyValue::new("method", "delete_groups")];

        match self {
            Self::Postgres(inner) => inner.delete_groups(group_ids).await,
            Self::DynoStore(inner) => inner.delete_groups(group_ids).await,
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let attributes = [KeyValue::new("method", "describe_groups")];

        match self {
            Self::Postgres(inner) => {
                inner
                    .describe_groups(group_ids, include_authorized_operations)
                    .await
            }
            Self::DynoStore(inner) => {
                inner
                    .describe_groups(group_ids, include_authorized_operations)
                    .await
            }
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let attributes = [KeyValue::new("method", "update_group")];

        match self {
            Self::Postgres(pg) => pg.update_group(group_id, detail, version).await,
            Self::DynoStore(dyn_store) => dyn_store.update_group(group_id, detail, version).await,
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        debug!(
            ?transaction_id,
            ?transaction_timeout_ms,
            ?producer_id,
            ?producer_epoch
        );

        let attributes = [KeyValue::new("method", "init_producer")];

        match self {
            Self::Postgres(pg) => {
                pg.init_producer(
                    transaction_id,
                    transaction_timeout_ms,
                    producer_id,
                    producer_epoch,
                )
                .await
            }
            Self::DynoStore(dyn_store) => {
                dyn_store
                    .init_producer(
                        transaction_id,
                        transaction_timeout_ms,
                        producer_id,
                        producer_epoch,
                    )
                    .await
            }
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        let attributes = [KeyValue::new("method", "txn_add_offsets")];

        match self {
            Self::Postgres(pg) => {
                pg.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
                    .await
            }
            Self::DynoStore(dyn_store) => {
                dyn_store
                    .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
                    .await
            }
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        let attributes = [KeyValue::new("method", "txn_add_partitions")];

        match self {
            Self::Postgres(pg) => pg.txn_add_partitions(partitions).await,
            Self::DynoStore(dyn_store) => dyn_store.txn_add_partitions(partitions).await,
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let attributes = [KeyValue::new("method", "txn_offset_commit")];

        match self {
            Self::Postgres(pg) => pg.txn_offset_commit(offsets).await,
            Self::DynoStore(dyn_store) => dyn_store.txn_offset_commit(offsets).await,
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let attributes = [KeyValue::new("method", "txn_end")];

        match self {
            Self::Postgres(pg) => {
                pg.txn_end(transaction_id, producer_id, producer_epoch, committed)
                    .await
            }
            Self::DynoStore(dyn_store) => {
                dyn_store
                    .txn_end(transaction_id, producer_id, producer_epoch, committed)
                    .await
            }
        }
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    async fn maintain(&self) -> Result<()> {
        let attributes = [KeyValue::new("method", "maintain")];

        match self {
            Self::Postgres(pg) => pg.maintain().await,
            Self::DynoStore(dyn_store) => dyn_store.maintain().await,
        }
        .inspect(|maintain| {
            debug!(?maintain);
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|err| {
            debug!(?err);
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    fn cluster_id(&self) -> Result<&str> {
        match self {
            Self::Postgres(inner) => inner.cluster_id(),
            Self::DynoStore(inner) => inner.cluster_id(),
        }
    }

    fn node(&self) -> Result<i32> {
        match self {
            Self::Postgres(inner) => inner.node(),
            Self::DynoStore(inner) => inner.node(),
        }
    }

    fn advertised_listener(&self) -> Result<&Url> {
        match self {
            Self::Postgres(inner) => inner.advertised_listener(),
            Self::DynoStore(inner) => inner.advertised_listener(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topition_from_str() -> Result<()> {
        let topition = Topition::from_str("qwerty-2147483647")?;
        assert_eq!("qwerty", topition.topic());
        assert_eq!(i32::MAX, topition.partition());
        Ok(())
    }

    #[test]
    fn topic_with_dashes_in_name() -> Result<()> {
        let topition = Topition::from_str("test-topic-0000000-eFC79C8-2147483647")?;
        assert_eq!("test-topic-0000000-eFC79C8", topition.topic());
        assert_eq!(i32::MAX, topition.partition());
        Ok(())
    }
}
