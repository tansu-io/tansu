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

use async_trait::async_trait;
use bytes::Bytes;
use dynostore::DynoStore;
use glob::{GlobError, PatternError};
use pg::Postgres;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    array::TryFromSliceError,
    collections::BTreeMap,
    ffi::OsString,
    fmt::Debug,
    fs::DirEntry,
    io,
    num::{ParseIntError, TryFromIntError},
    path::PathBuf,
    result,
    str::FromStr,
    sync::PoisonError,
    time::{Duration, SystemTime, SystemTimeError},
};
use tansu_kafka_sans_io::{
    broker_registration_request::{Feature, Listener},
    create_topics_request::CreatableTopic,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    delete_topics_request::DeleteTopicState,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    fetch_request::FetchTopic,
    join_group_response::JoinGroupResponseMember,
    metadata_request::MetadataRequestTopic,
    metadata_response::{MetadataResponseBroker, MetadataResponseTopic},
    offset_commit_request::OffsetCommitRequestPartition,
    record::deflated,
    to_system_time, to_timestamp, ConfigResource, ErrorCode,
};
use tracing::debug;
use uuid::Uuid;

pub mod dynostore;
pub mod index;
pub mod os;
pub mod pg;
pub mod segment;

pub const NULL_TOPIC_ID: [u8; 16] = [0; 16];

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("api")]
    Api(ErrorCode),

    #[error("build")]
    DeadPoolBuild(#[from] deadpool::managed::BuildError),

    #[error("glob")]
    Glob(#[from] GlobError),

    #[error("io")]
    Io(#[from] io::Error),

    #[error("kafka sans io")]
    KafkaSansIo(#[from] tansu_kafka_sans_io::Error),

    #[error("offset: {offset}, is less than base offset: {base_offset}")]
    LessThanBaseOffset { offset: i64, base_offset: i64 },

    #[error("offset: {offset}, is less than last offset: {last_offset:?}")]
    LessThanLastOffset {
        offset: i64,
        last_offset: Option<i64>,
    },

    #[error("time: {time}, is less than max time: {max_time:?}")]
    LessThanMaxTime { time: i64, max_time: Option<i64> },

    #[error("time: {time}, is less than min time: {min_time:?}")]
    LessThanMinTime { time: i64, min_time: Option<i64> },

    #[error("message: {0}")]
    Message(String),

    #[error("no such entry nth: {nth}")]
    NoSuchEntry { nth: u32 },

    #[error("no such offset: {0}")]
    NoSuchOffset(i64),

    #[error("os string {0:?}")]
    OsString(OsString),

    #[error("object store: {0:?}")]
    ObjectStore(#[from] object_store::Error),

    #[error("pattern")]
    Pattern(#[from] PatternError),

    #[error("parse int: {0}")]
    ParseInt(#[from] ParseIntError),

    #[error("poision")]
    Poison,

    #[error("pool")]
    Pool(#[from] deadpool_postgres::PoolError),

    #[error("regex")]
    Regex(#[from] regex::Error),

    #[error("segment empty: {0:?}")]
    SegmentEmpty(Topition),

    #[error("segment missing: {topition:?}, at offset: {offset:?}")]
    SegmentMissing {
        topition: Topition,
        offset: Option<i64>,
    },

    #[error("json: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("system time: {0}")]
    SystemTime(#[from] SystemTimeError),

    #[error("postgres")]
    TokioPostgres(#[from] tokio_postgres::error::Error),

    #[error("try from int: {0}")]
    TryFromInt(#[from] TryFromIntError),

    #[error("try from slice: {0}")]
    TryFromSlice(#[from] TryFromSliceError),

    #[error("url: {0}")]
    Url(#[from] url::ParseError),
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;

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

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ListOffsetRequest {
    #[default]
    Earliest,
    Latest,
    Timestamp(SystemTime),
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ListOffsetResponse {
    error_code: ErrorCode,
    timestamp: Option<SystemTime>,
    offset: Option<i64>,
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

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct OffsetCommitRequest {
    offset: i64,
    leader_epoch: Option<i32>,
    timestamp: Option<SystemTime>,
    metadata: Option<String>,
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
            } if topic_id == NULL_TOPIC_ID => name.into(),

            DeleteTopicState { topic_id, .. } => topic_id.into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct BrokerRegistationRequest {
    pub broker_id: i32,
    pub cluster_id: String,
    pub incarnation_id: Uuid,
    pub listeners: Vec<Listener>,
    pub features: Vec<Feature>,
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

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct GroupMember {
    pub join_response: JoinGroupResponseMember,
    pub last_contact: Option<SystemTime>,
}

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

impl Default for GroupState {
    fn default() -> Self {
        Self::Forming {
            protocol_type: None,
            protocol_name: Some("".into()),
            leader: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct GroupDetail {
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: Option<i32>,
    pub group_instance_id: Option<String>,
    pub members: BTreeMap<String, GroupMember>,
    pub generation_id: i32,
    pub skip_assignment: Option<bool>,
    pub state: GroupState,
}

impl Default for GroupDetail {
    fn default() -> Self {
        Self {
            session_timeout_ms: 45_000,
            rebalance_timeout_ms: None,
            group_instance_id: None,
            members: BTreeMap::new(),
            generation_id: -1,
            skip_assignment: Some(false),
            state: GroupState::default(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Version {
    e_tag: Option<String>,
    version: Option<String>,
}

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

#[async_trait]
pub trait StorageProvider {
    async fn provide_storage(&mut self) -> impl Storage;
}

#[async_trait]
pub trait Storage: Clone + Debug + Send + Sync + 'static {
    async fn register_broker(
        &mut self,
        broker_registration: BrokerRegistationRequest,
    ) -> Result<()>;

    async fn create_topic(&mut self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid>;

    async fn delete_records(
        &mut self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>>;

    async fn delete_topic(&mut self, topic: &TopicId) -> Result<ErrorCode>;

    async fn brokers(&mut self) -> Result<Vec<DescribeClusterBroker>>;

    async fn produce(&mut self, topition: &Topition, batch: deflated::Batch) -> Result<i64>;

    async fn fetch(
        &mut self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
    ) -> Result<deflated::Batch>;

    async fn offset_stage(&mut self, topition: &Topition) -> Result<OffsetStage>;

    async fn list_offsets(
        &mut self,
        offsets: &[(Topition, ListOffsetRequest)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>>;

    async fn offset_commit(
        &mut self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>>;

    async fn offset_fetch(
        &mut self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>>;

    async fn metadata(&mut self, topics: Option<&[TopicId]>) -> Result<MetadataResponse>;

    async fn describe_config(
        &mut self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult>;

    async fn update_group(
        &mut self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>>;

    async fn init_producer(
        &mut self,
        transactional_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse>;
}

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

#[derive(Clone, Debug)]
pub enum StorageContainer {
    Postgres(Postgres),
    DynoStore(DynoStore),
}

#[async_trait]
impl Storage for StorageContainer {
    async fn register_broker(
        &mut self,
        broker_registration: BrokerRegistationRequest,
    ) -> Result<()> {
        match self {
            Self::Postgres(pg) => pg.register_broker(broker_registration).await,
            Self::DynoStore(dyn_store) => dyn_store.register_broker(broker_registration).await,
        }
    }

    async fn create_topic(&mut self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        match self {
            Self::Postgres(pg) => pg.create_topic(topic, validate_only).await,
            Self::DynoStore(dyn_store) => dyn_store.create_topic(topic, validate_only).await,
        }
    }

    async fn delete_records(
        &mut self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        match self {
            Self::Postgres(pg) => pg.delete_records(topics).await,
            Self::DynoStore(dyn_store) => dyn_store.delete_records(topics).await,
        }
    }

    async fn delete_topic(&mut self, topic: &TopicId) -> Result<ErrorCode> {
        match self {
            Self::Postgres(pg) => pg.delete_topic(topic).await,
            Self::DynoStore(dyn_store) => dyn_store.delete_topic(topic).await,
        }
    }

    async fn brokers(&mut self) -> Result<Vec<DescribeClusterBroker>> {
        match self {
            Self::Postgres(pg) => pg.brokers().await,
            Self::DynoStore(dyn_store) => dyn_store.brokers().await,
        }
    }

    async fn produce(&mut self, topition: &Topition, batch: deflated::Batch) -> Result<i64> {
        match self {
            Self::Postgres(pg) => pg.produce(topition, batch).await,
            Self::DynoStore(dyn_store) => dyn_store.produce(topition, batch).await,
        }
    }

    async fn fetch(
        &mut self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
    ) -> Result<deflated::Batch> {
        match self {
            Self::Postgres(pg) => pg.fetch(topition, offset, min_bytes, max_bytes).await,
            Self::DynoStore(dyn_store) => {
                dyn_store
                    .fetch(topition, offset, min_bytes, max_bytes)
                    .await
            }
        }
    }

    async fn offset_stage(&mut self, topition: &Topition) -> Result<OffsetStage> {
        match self {
            Self::Postgres(pg) => pg.offset_stage(topition).await,
            Self::DynoStore(dyn_store) => dyn_store.offset_stage(topition).await,
        }
    }

    async fn list_offsets(
        &mut self,
        offsets: &[(Topition, ListOffsetRequest)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        match self {
            Self::Postgres(pg) => pg.list_offsets(offsets).await,
            Self::DynoStore(dyn_store) => dyn_store.list_offsets(offsets).await,
        }
    }

    async fn offset_commit(
        &mut self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        match self {
            Self::Postgres(pg) => pg.offset_commit(group_id, retention_time_ms, offsets).await,
            Self::DynoStore(dyn_store) => {
                dyn_store
                    .offset_commit(group_id, retention_time_ms, offsets)
                    .await
            }
        }
    }

    async fn offset_fetch(
        &mut self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        match self {
            Self::Postgres(pg) => pg.offset_fetch(group_id, topics, require_stable).await,
            Self::DynoStore(dyn_store) => {
                dyn_store
                    .offset_fetch(group_id, topics, require_stable)
                    .await
            }
        }
    }

    async fn metadata(&mut self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        match self {
            Self::Postgres(pg) => pg.metadata(topics).await,
            Self::DynoStore(dyn_store) => dyn_store.metadata(topics).await,
        }
    }

    async fn describe_config(
        &mut self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        match self {
            Self::Postgres(pg) => pg.describe_config(name, resource, keys).await,
            Self::DynoStore(dyn_store) => dyn_store.describe_config(name, resource, keys).await,
        }
    }

    async fn update_group(
        &mut self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        match self {
            Self::Postgres(pg) => pg.update_group(group_id, detail, version).await,
            Self::DynoStore(dyn_store) => dyn_store.update_group(group_id, detail, version).await,
        }
    }

    async fn init_producer(
        &mut self,
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
