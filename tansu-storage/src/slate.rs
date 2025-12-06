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
    collections::BTreeMap,
    fmt,
    io::Cursor,
    iter,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use slatedb::Db;
use tansu_sans_io::{
    ConfigResource, Decoder, Encoder, ErrorCode, IsolationLevel, ListOffset,
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::deflated::{self, Batch},
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use tracing::debug;
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, Error, GroupDetail, ListOffsetResponse, MetadataResponse,
    NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result, Storage,
    TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest,
    TxnState, UpdateError, Version,
};

#[derive(Clone)]
pub struct Engine {
    cluster: String,
    node: i32,
    advertised_listener: Url,

    db: Arc<Db>,
}

impl fmt::Debug for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Engine)).finish()
    }
}

impl Engine {
    const PRODUCERS: &[u8] = b"producers.pc.bin";
    const TOPICS: &[u8] = b"topics.pc.bin";

    pub fn new(cluster: &str, node: i32, advertised_listener: Url, db: Arc<Db>) -> Self {
        Self {
            cluster: cluster.to_string(),
            node,
            advertised_listener,
            db,
        }
    }

    fn decode(&self, encoded: Bytes) -> Result<deflated::Batch> {
        let mut c = Cursor::new(encoded);
        let mut decoder = Decoder::new(&mut c);
        deflated::Batch::deserialize(&mut decoder).map_err(Into::into)
    }
}

type Group = String;
type Offset = i64;
type Partition = i32;
type ProducerEpoch = i16;
type ProducerId = i64;
type Sequence = i32;
type Topic = String;

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
struct TxnProduceOffset {
    offset_start: Offset,
    offset_end: Offset,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TxnCommitOffset {
    committed_offset: Offset,
    leader_epoch: Option<i32>,
    metadata: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TopicMetadata {
    id: Uuid,
    topic: CreatableTopic,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct Watermark {
    low: Option<i64>,
    high: Option<i64>,
    timestamps: Option<BTreeMap<i64, i64>>,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct WatermarkKey {
    topic_partition: TopicPartition,
    suffix: String,
}

impl Default for WatermarkKey {
    fn default() -> Self {
        Self {
            topic_partition: Default::default(),
            suffix: "watermark".into(),
        }
    }
}

impl WatermarkKey {
    fn topic_partition(self, topic_partition: TopicPartition) -> Self {
        Self {
            topic_partition,
            ..self
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct GroupDetailVersion {
    detail: GroupDetail,
    version: Version,
}

impl GroupDetailVersion {
    fn detail(self, detail: GroupDetail) -> Self {
        Self { detail, ..self }
    }

    fn version(self, version: Version) -> Self {
        Self { version, ..self }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct ProducerDetail {
    sequences: BTreeMap<ProducerEpoch, BTreeMap<String, BTreeMap<i32, Sequence>>>,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TxnId {
    transaction: String,
    producer_id: ProducerId,
    producer_epoch: ProducerEpoch,
    state: TxnState,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct Txn {
    producer: ProducerId,
    epochs: BTreeMap<ProducerEpoch, TxnDetail>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TxnDetail {
    transaction_timeout_ms: i32,
    started_at: Option<SystemTime>,
    state: Option<TxnState>,
    produces: BTreeMap<Topic, BTreeMap<Partition, Option<TxnProduceOffset>>>,
    offsets: BTreeMap<Group, BTreeMap<Topic, BTreeMap<Partition, TxnCommitOffset>>>,
}

type Topics = BTreeMap<Topic, TopicMetadata>;
type Producers = BTreeMap<ProducerId, ProducerDetail>;

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TopicPartition {
    topic: Uuid,
    #[serde(with = "postcard::fixint::le")]
    partition: Partition,
}

impl TopicPartition {
    fn topic(self, topic: Uuid) -> Self {
        Self { topic, ..self }
    }

    fn partition(self, partition: Partition) -> Self {
        Self { partition, ..self }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct BatchKey {
    topic_partition: TopicPartition,
    prefix: String,
    #[serde(with = "postcard::fixint::le")]
    offset: Offset,
}

impl Default for BatchKey {
    fn default() -> Self {
        Self {
            topic_partition: Default::default(),
            prefix: "batch".into(),
            offset: Default::default(),
        }
    }
}

impl BatchKey {
    fn topic_partition(self, topic_partition: TopicPartition) -> Self {
        Self {
            topic_partition,
            ..self
        }
    }

    fn offset(self, offset: Offset) -> Self {
        Self { offset, ..self }
    }
}

#[async_trait]
impl Storage for Engine {
    async fn register_broker(&self, _broker_registration: BrokerRegistrationRequest) -> Result<()> {
        Ok(())
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let broker_id = self.node;
        let host = self
            .advertised_listener
            .host_str()
            .unwrap_or("0.0.0.0")
            .into();
        let port = self.advertised_listener.port().unwrap_or(9092).into();
        let rack = None;

        Ok(vec![
            DescribeClusterBroker::default()
                .broker_id(broker_id)
                .host(host)
                .port(port)
                .rack(rack),
        ])
    }

    async fn create_topic(&self, topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut topics = tx
            .get(Self::TOPICS)
            .await
            .map_err(Error::from)
            .and_then(|topics| {
                topics.map_or(Ok(Topics::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })?;

        let name = topic.name.clone();

        if topics.contains_key(&name[..]) {
            return Err(Error::Api(ErrorCode::TopicAlreadyExists));
        }

        let id = Uuid::now_v7();
        let td = TopicMetadata { id, topic };

        assert_eq!(None, topics.insert(name, td));

        postcard::to_stdvec(&topics)
            .map_err(Error::from)
            .and_then(|encoded| tx.put(Self::TOPICS, encoded).map_err(Into::into))?;

        tx.commit().await.map_err(Error::from).and(Ok(id))
    }

    async fn delete_records(
        &self,
        _topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        todo!();
    }

    async fn delete_topic(&self, _topic: &TopicId) -> Result<ErrorCode> {
        todo!();
    }

    async fn incremental_alter_resource(
        &self,
        _resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        todo!();
    }

    async fn produce(
        &self,
        _transaction_id: Option<&str>,
        topition: &Topition,
        deflated: Batch,
    ) -> Result<i64> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let topics = tx
            .get(Self::TOPICS)
            .await
            .map_err(Error::from)
            .and_then(|topics| {
                topics.map_or(Ok(Topics::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })?;

        let Some(metadata) = topics.get(&topition.topic[..]) else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        if topition.partition < 0 || topition.partition >= metadata.topic.num_partitions {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        }

        let mut watermark = tx
            .get(postcard::to_stdvec(
                &WatermarkKey::default().topic_partition(
                    TopicPartition::default()
                        .topic(metadata.id)
                        .partition(topition.partition),
                ),
            )?)
            .await
            .map_err(Error::from)
            .and_then(|watermark| {
                watermark.map_or(Ok(Watermark::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })?;

        let offset = watermark.high.unwrap_or_default();

        watermark.high = watermark
            .high
            .map_or(Some(deflated.last_offset_delta as i64 + 1i64), |high| {
                Some(high + deflated.last_offset_delta as i64 + 1i64)
            });

        _ = watermark
            .timestamps
            .get_or_insert_default()
            .insert(deflated.base_timestamp, offset);

        debug!(?watermark);

        let encoded = {
            let mut writer = BytesMut::new().writer();
            let mut encoder = Encoder::new(&mut writer);
            deflated.serialize(&mut encoder)?;

            Bytes::from(writer.into_inner())
        };

        let key = postcard::to_stdvec(
            &BatchKey::default()
                .topic_partition(
                    TopicPartition::default()
                        .topic(metadata.id)
                        .partition(topition.partition),
                )
                .offset(offset),
        )?;

        tx.put(key, &encoded[..])?;

        tx.commit().await.map_err(Error::from).and(Ok(offset))
    }

    async fn fetch(
        &self,
        topition: &Topition,
        offset: i64,
        _min_bytes: u32,
        max_bytes: u32,
        _isolation_level: IsolationLevel,
    ) -> Result<Vec<Batch>> {
        let topics = self
            .db
            .get(Self::TOPICS)
            .await
            .map_err(Error::from)
            .and_then(|topics| {
                topics.map_or(Ok(Topics::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })?;

        let Some(metadata) = topics.get(&topition.topic[..]) else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        if topition.partition < 0 || topition.partition >= metadata.topic.num_partitions {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        }

        let mut i = {
            let from = postcard::to_stdvec(
                &BatchKey::default()
                    .topic_partition(
                        TopicPartition::default()
                            .topic(metadata.id)
                            .partition(topition.partition),
                    )
                    .offset(offset),
            )?;

            self.db.scan(from..).await?
        };

        let mut batches = vec![];
        let mut bytes = max_bytes as usize;

        while let Some(kv) = i.next().await? {
            let size = kv.value.len();

            let key: BatchKey = postcard::from_bytes(&kv.key)?;

            let mut batch = self.decode(kv.value)?;
            batch.base_offset = key.offset;
            batches.push(batch);

            if size > bytes {
                break;
            }

            bytes = bytes.saturating_sub(size);
        }

        Ok(batches)
    }

    async fn offset_stage(&self, _topition: &Topition) -> Result<OffsetStage> {
        todo!();
    }

    async fn offset_commit(
        &self,
        _group: &str,
        _retention: Option<Duration>,
        _offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        todo!();
    }

    async fn committed_offset_topitions(&self, _group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        todo!();
    }

    async fn offset_fetch(
        &self,
        _group_id: Option<&str>,
        _topics: &[Topition],
        _require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        todo!();
    }

    async fn list_offsets(
        &self,
        _isolation_level: IsolationLevel,
        _offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        todo!();
    }

    async fn metadata(&self, _topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let brokers = vec![
            MetadataResponseBroker::default()
                .node_id(self.node)
                .host(
                    self.advertised_listener
                        .host_str()
                        .unwrap_or("0.0.0.0")
                        .into(),
                )
                .port(self.advertised_listener.port().unwrap_or(9092).into())
                .rack(None),
        ];

        let existing = self
            .db
            .get(Self::TOPICS)
            .await
            .map_err(Error::from)
            .map_err(Error::from)
            .and_then(|existing| {
                existing.map_or(Ok(Topics::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })
            .map(|existing| {
                existing
                    .into_iter()
                    .map(|(_name, topic_metadata)| {
                        let name = Some(topic_metadata.topic.name.to_owned());
                        let error_code = ErrorCode::None.into();
                        let topic_id = Some(topic_metadata.id.into_bytes());
                        let is_internal = Some(false);
                        let partitions = topic_metadata.topic.num_partitions;
                        let replication_factor = topic_metadata.topic.replication_factor;

                        let partitions = Some(
                            (0..partitions)
                                .map(|partition_index| {
                                    let leader_id = self.node;
                                    let replica_nodes = Some(
                                        iter::repeat_n(self.node, replication_factor as usize)
                                            .collect(),
                                    );
                                    let isr_nodes = replica_nodes.clone();

                                    MetadataResponsePartition::default()
                                        .error_code(error_code)
                                        .partition_index(partition_index)
                                        .leader_id(leader_id)
                                        .leader_epoch(Some(-1))
                                        .replica_nodes(replica_nodes)
                                        .isr_nodes(isr_nodes)
                                        .offline_replicas(Some([].into()))
                                })
                                .collect(),
                        );

                        MetadataResponseTopic::default()
                            .error_code(error_code)
                            .name(name)
                            .topic_id(topic_id)
                            .is_internal(is_internal)
                            .partitions(partitions)
                            .topic_authorized_operations(Some(i32::MIN))
                    })
                    .collect()
            })?;

        Ok(MetadataResponse {
            cluster: Some(self.cluster.clone()),
            controller: Some(self.node),
            brokers,
            topics: existing,
        })
    }

    async fn describe_config(
        &self,
        _name: &str,
        _resource: ConfigResource,
        _keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        todo!();
    }

    async fn describe_topic_partitions(
        &self,
        _topics: Option<&[TopicId]>,
        _partition_limit: i32,
        _cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        todo!();
    }

    async fn list_groups(&self, _states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        todo!();
    }

    async fn delete_groups(
        &self,
        _group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        todo!();
    }

    async fn describe_groups(
        &self,
        _group_ids: Option<&[String]>,
        _include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        todo!();
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))
            .map_err(|err| UpdateError::Error(Error::Slate(Arc::new(err))))?;

        let key = group_id;

        if let Some(current) = tx.get(key).await.map_err(Error::from).and_then(|group| {
            group
                .map(|encoded| {
                    postcard::from_bytes::<GroupDetailVersion>(&encoded[..]).map_err(Into::into)
                })
                .transpose()
        })? && version.is_some_and(|version| version != current.version)
        {
            tx.rollback();

            Err(UpdateError::Outdated {
                current: current.detail,
                version: current.version,
            })
        } else {
            let updated = Version::from(&Uuid::now_v7());

            _ = tx
                .put(
                    key,
                    postcard::to_stdvec(
                        &GroupDetailVersion::default()
                            .detail(detail)
                            .version(updated.clone()),
                    )
                    .map_err(|err| UpdateError::Error(Error::Postcard(err)))?,
                )
                .map_err(|err| UpdateError::Error(Error::Slate(Arc::new(err))))?;

            tx.commit()
                .await
                .map_err(|err| UpdateError::Error(Error::Slate(Arc::new(err))))
                .and(Ok(updated))
        }
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        _transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        if let Some(_transaction_id) = transaction_id {
            todo!()
        } else if Some(-1) == producer_id && Some(-1) == producer_epoch {
            let tx = self
                .db
                .begin(slatedb::IsolationLevel::SerializableSnapshot)
                .await
                .inspect_err(|err| debug!(?err))?;

            let mut producers = tx
                .get(Self::PRODUCERS)
                .await
                .map_err(Error::from)
                .and_then(|topics| {
                    topics.map_or(Ok(Producers::default()), |encoded| {
                        postcard::from_bytes(&encoded[..]).map_err(Into::into)
                    })
                })?;

            let producer = producers.last_key_value().map_or(1.into(), |(k, _v)| k + 1);

            let epoch = 0;
            let mut pd = ProducerDetail::default();
            assert_eq!(None, pd.sequences.insert(epoch, BTreeMap::new()));
            debug!(?producer, ?pd);
            assert_eq!(None, producers.insert(producer, pd));

            postcard::to_stdvec(&producers)
                .map_err(Error::from)
                .and_then(|encoded| tx.put(Self::PRODUCERS, encoded).map_err(Into::into))?;

            tx.commit()
                .await
                .map_err(Error::from)
                .and(Ok(ProducerIdResponse {
                    id: producer,
                    epoch,
                    ..Default::default()
                }))
        } else {
            Ok(ProducerIdResponse {
                id: -1,
                epoch: -1,
                error: ErrorCode::UnknownServerError,
            })
        }
    }

    async fn txn_add_offsets(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _group_id: &str,
    ) -> Result<ErrorCode> {
        todo!();
    }

    async fn txn_add_partitions(
        &self,
        _partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        todo!();
    }

    async fn txn_offset_commit(
        &self,
        _offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        todo!();
    }

    async fn txn_end(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _committed: bool,
    ) -> Result<ErrorCode> {
        todo!();
    }

    async fn maintain(&self) -> Result<()> {
        Ok(())
    }

    async fn cluster_id(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    async fn node(&self) -> Result<i32> {
        Ok(self.node)
    }

    async fn advertised_listener(&self) -> Result<Url> {
        Ok(self.advertised_listener.clone())
    }
}
