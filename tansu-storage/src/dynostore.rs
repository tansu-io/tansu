// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    fmt::{Debug, Display},
    io::Cursor,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    stream::{BoxStream, TryStreamExt},
    StreamExt,
};
use object_store::{
    path::Path, Attribute, AttributeValue, Attributes, DynObjectStore, GetOptions, GetResult,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOpts, PutOptions,
    PutPayload, PutResult, TagSet, UpdateVersion,
};
use opentelemetry::{
    global,
    metrics::{Counter, Histogram},
    InstrumentationScope, KeyValue,
};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use rand::{prelude::*, thread_rng};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tansu_kafka_sans_io::{
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    list_groups_response::ListedGroup,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::{deflated, inflated, Record},
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
    BatchAttribute, ConfigResource, ConfigSource, ConfigType, ControlBatch, Decoder, Encoder,
    EndTransactionMarker, ErrorCode, IsolationLevel,
};
use tansu_schema_registry::Registry;
use tracing::{debug, error, warn};
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, Error, GroupDetail, ListOffsetRequest, ListOffsetResponse,
    MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse,
    Result, Storage, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, TxnState, UpdateError, Version, NULL_TOPIC_ID,
};

const APPLICATION_JSON: &str = "application/json";

#[derive(Clone, Debug)]
pub struct DynoStore {
    cluster: String,
    node: i32,
    advertised_listener: Url,
    schemas: Option<Registry>,
    watermarks: BTreeMap<Topition, ConditionData<Watermark>>,
    meta: ConditionData<Meta>,

    object_store: Arc<DynObjectStore>,
}

type ProducerId = i64;
type ProducerEpoch = i16;
type Offset = i64;
type Sequence = i32;

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct Meta {
    producers: BTreeMap<ProducerId, ProducerDetail>,
    transactions: BTreeMap<String, Txn>,
}

impl ConditionData<Meta> {
    fn new(cluster: &str) -> Self {
        Self {
            path: Path::from(format!("clusters/{}/meta.json", cluster)),
            data: Meta::default(),
            ..Default::default()
        }
    }
}

impl Meta {
    fn produced(
        &self,
        transaction_id: &str,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
    ) -> Result<BTreeMap<Topition, TxnProduceOffset>> {
        let Some(txn) = self.transactions.get(transaction_id) else {
            return Err(Error::Api(ErrorCode::TransactionalIdNotFound));
        };

        if txn.producer != producer_id {
            return Err(Error::Api(ErrorCode::UnknownProducerId));
        }

        let Some(txn_detail) = txn.epochs.get(&producer_epoch) else {
            return Err(Error::Api(ErrorCode::ProducerFenced));
        };

        let mut produced = BTreeMap::new();

        for (topic, partitions) in txn_detail.produces.iter() {
            for (partition, offset_range) in partitions.iter() {
                let Some(offset_range) = offset_range else {
                    continue;
                };

                let tp = Topition::new(topic.to_owned(), *partition);
                assert_eq!(None, produced.insert(tp, *offset_range));
            }
        }

        Ok(produced)
    }

    fn overlapping_transactions(
        &self,
        transaction_id: &str,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
    ) -> Result<Vec<TxnId>> {
        let candidates = self.produced(transaction_id, producer_id, producer_epoch)?;

        let mut overlapping = Vec::new();

        'candidates: for (candidate_id, txn) in self.transactions.iter() {
            for (epoch, txn_detail) in txn.epochs.iter() {
                if transaction_id == candidate_id
                    && producer_id == txn.producer
                    && producer_epoch == *epoch
                {
                    continue;
                }

                let Some(state) = txn_detail.state else {
                    continue;
                };

                for (topic, partitions) in txn_detail.produces.iter() {
                    for (partition, offset_range) in partitions.iter() {
                        let Some(offset_range) = offset_range else {
                            continue;
                        };

                        let tp = Topition::new(topic.to_owned(), *partition);

                        if let Some(candidate) = candidates.get(&tp) {
                            if offset_range.offset_start < candidate.offset_end {
                                overlapping.push(TxnId {
                                    transaction: candidate_id.to_owned(),
                                    producer_id: txn.producer,
                                    producer_epoch: *epoch,
                                    state,
                                });

                                continue 'candidates;
                            }
                        }
                    }
                }
            }
        }

        Ok(overlapping)
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

type Group = String;
type Topic = String;
type Partition = i32;

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TxnDetail {
    transaction_timeout_ms: i32,
    started_at: Option<SystemTime>,
    state: Option<TxnState>,
    produces: BTreeMap<Topic, BTreeMap<Partition, Option<TxnProduceOffset>>>,
    offsets: BTreeMap<Group, BTreeMap<Topic, BTreeMap<Partition, TxnCommitOffset>>>,
}

impl From<&TxnDetail> for BTreeMap<Topition, Offset> {
    fn from(value: &TxnDetail) -> Self {
        let mut result = BTreeMap::new();

        for (topic, partitions) in value.produces.iter() {
            for (partition, offset_range) in partitions.iter() {
                let Some(offset_range) = offset_range else {
                    continue;
                };

                let tp = Topition::new(topic.to_owned(), *partition);
                assert_eq!(None, result.insert(tp, offset_range.offset_start));
            }
        }

        result
    }
}

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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ConditionData<D> {
    path: Path,
    version: Option<UpdateVersion>,
    tags: TagSet,
    attributes: Attributes,
    data: D,
}

impl<D> ConditionData<D>
where
    D: Clone + Debug + DeserializeOwned + Send + Serialize + Sync,
{
    async fn get(&mut self, object_store: &impl ObjectStore) -> Result<()> {
        debug!(?self);

        let get_result = object_store
            .get(&self.path)
            .await
            .inspect_err(|error| error!(?error, ?self))
            .map_err(|_error| Error::Api(ErrorCode::UnknownServerError))?;

        self.version = Some(UpdateVersion {
            e_tag: get_result.meta.e_tag.clone(),
            version: get_result.meta.version.clone(),
        });

        let encoded = get_result
            .bytes()
            .await
            .inspect_err(|error| error!(?error, ?self))
            .map_err(|_error| Error::Api(ErrorCode::UnknownServerError))?;

        self.data = serde_json::from_slice::<D>(&encoded[..])
            .inspect_err(|error| error!(?error, ?self))
            .map_err(|_error| Error::Api(ErrorCode::UnknownServerError))?;

        debug!(?self);

        Ok(())
    }

    async fn with_mut<E, F>(&mut self, object_store: &impl ObjectStore, f: F) -> Result<E>
    where
        E: Debug,
        F: Fn(&mut D) -> Result<E>,
    {
        debug!(?self);

        loop {
            let outcome = f(&mut self.data);
            debug!(?self, ?outcome);

            let payload = serde_json::to_vec(&self.data)
                .map(Bytes::from)
                .map(PutPayload::from)
                .inspect_err(|err| error!(?err, data = ?self.data))?;

            match object_store
                .put_opts(&self.path, payload, PutOptions::from(&*self))
                .await
                .inspect_err(|error| match error {
                    object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. } => {
                        debug!(?error, ?self)
                    }

                    _ => error!(?error, ?self),
                })
                .inspect(|put_result| debug!(?self, ?put_result))
            {
                Ok(result) => {
                    debug!(?self, ?result);

                    self.version = Some(UpdateVersion {
                        e_tag: result.e_tag,
                        version: result.version,
                    });

                    return outcome;
                }

                Err(pre_condition @ object_store::Error::Precondition { .. }) => {
                    debug!(?self, ?pre_condition);
                    self.get(object_store).await?;
                    continue;
                }

                Err(already_exists @ object_store::Error::AlreadyExists { .. }) => {
                    debug!(?self, ?already_exists);
                    self.get(object_store).await?;
                    continue;
                }

                Err(error) => {
                    return {
                        error!(?self, ?error);
                        Err(Error::Api(ErrorCode::UnknownServerError))
                    }
                }
            }
        }
    }

    async fn with<E, F>(&mut self, object_store: &impl ObjectStore, f: F) -> Result<E>
    where
        F: Fn(&D) -> Result<E>,
    {
        debug!(?self);

        match object_store
            .get_opts(
                &self.path,
                GetOptions {
                    if_none_match: self
                        .version
                        .as_ref()
                        .and_then(|version| version.e_tag.clone()),
                    ..GetOptions::default()
                },
            )
            .await
        {
            Ok(get_result) => {
                self.version = Some(UpdateVersion {
                    e_tag: get_result.meta.e_tag.clone(),
                    version: get_result.meta.version.clone(),
                });

                let encoded = get_result
                    .bytes()
                    .await
                    .inspect_err(|error| error!(?error, ?self))
                    .map_err(|_error| Error::Api(ErrorCode::UnknownServerError))?;

                self.data = serde_json::from_slice::<D>(&encoded[..])
                    .inspect_err(|error| error!(?error, ?self))
                    .map_err(|_error| Error::Api(ErrorCode::UnknownServerError))?;

                Ok(())
            }

            Err(object_store::Error::NotModified { .. }) => Ok(()),

            Err(object_store::Error::NotFound { .. }) => {
                self.with_mut(object_store, |_| Ok(())).await
            }

            Err(error) => {
                error!(?self, ?error);
                Err(Error::Api(ErrorCode::UnknownServerError))
            }
        }
        .and(f(&self.data))
    }
}

impl<D> From<&ConditionData<D>> for PutOptions {
    fn from(value: &ConditionData<D>) -> Self {
        Self {
            mode: value.version.as_ref().map_or(PutMode::Create, |existing| {
                PutMode::Update(existing.to_owned())
            }),
            tags: value.tags.to_owned(),
            attributes: value.attributes.to_owned(),
        }
    }
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
}

impl ConditionData<Watermark> {
    fn new(cluster: &str, topition: &Topition) -> Self {
        Self {
            path: Path::from(format!(
                "clusters/{}/topics/{}/partitions/{:0>10}/watermark.json",
                cluster, topition.topic, topition.partition,
            )),
            data: Watermark::default(),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct WatermarkSequence {
    epoch: i16,
    sequence: i32,
    updated: SystemTime,
}

impl Default for WatermarkSequence {
    fn default() -> Self {
        Self {
            epoch: 0,
            sequence: 0,
            updated: SystemTime::now(),
        }
    }
}

fn json_content_type() -> Attributes {
    let mut attributes = Attributes::new();
    _ = attributes.insert(
        Attribute::ContentType,
        AttributeValue::from(APPLICATION_JSON),
    );
    attributes
}

impl DynoStore {
    pub fn new(cluster: &str, node: i32, object_store: impl ObjectStore) -> Self {
        Self {
            cluster: cluster.into(),
            node,
            advertised_listener: Url::parse("tcp://127.0.0.1/").unwrap(),
            schemas: None,
            watermarks: BTreeMap::new(),
            meta: ConditionData::<Meta>::new(cluster),
            object_store: Arc::new(Metron::new(object_store, cluster)),
        }
    }

    pub fn advertised_listener(self, advertised_listener: Url) -> Self {
        Self {
            advertised_listener,
            ..self
        }
    }

    pub fn schemas(self, schemas: Option<Registry>) -> Self {
        Self { schemas, ..self }
    }

    async fn topic_metadata(&self, topic: &TopicId) -> Result<TopicMetadata> {
        debug!(?topic);

        let location = match topic {
            TopicId::Name(name) => {
                Path::from(format!("clusters/{}/topics/{}.json", self.cluster, name))
            }

            TopicId::Id(id) => Path::from(format!(
                "clusters/{}/topics/uuids/{}.json",
                self.cluster, id
            )),
        };

        let get_result = self.object_store.get(&location).await?;

        let encoded = get_result.bytes().await?;

        serde_json::from_slice::<TopicMetadata>(&encoded[..])
            .inspect(|topic_metadata| debug!(?topic_metadata, ?topic))
            .map_err(Into::into)
    }

    fn encode(&self, deflated: deflated::Batch) -> Result<PutPayload> {
        let mut encoded = Cursor::new(vec![]);
        let mut encoder = Encoder::new(&mut encoded);
        deflated.serialize(&mut encoder)?;

        Ok(PutPayload::from(Bytes::from(encoded.into_inner())))
    }

    fn decode(&self, encoded: Bytes) -> Result<deflated::Batch> {
        let mut c = Cursor::new(encoded);

        let mut decoder = Decoder::new(&mut c);
        deflated::Batch::deserialize(&mut decoder).map_err(Into::into)
    }

    async fn get<P>(&self, location: &Path) -> Result<(P, Version)>
    where
        P: DeserializeOwned,
    {
        let get_result = self.object_store.get(location).await?;
        let meta = get_result.meta.clone();

        let payload = get_result
            .bytes()
            .await
            .map_err(Into::into)
            .and_then(|encoded| serde_json::from_reader(&encoded[..]).map_err(Error::from))?;

        Ok((payload, meta.into()))
    }

    async fn put<P>(
        &self,
        location: &Path,
        value: P,
        attributes: Attributes,
        update_version: Option<UpdateVersion>,
    ) -> Result<PutResult, UpdateError<P>>
    where
        P: PartialEq + Serialize + DeserializeOwned + Debug,
    {
        debug!(%location, ?attributes, ?update_version, ?value);

        let options = PutOptions {
            mode: update_version.map_or(PutMode::Create, PutMode::Update),
            tags: TagSet::default(),
            attributes,
        };

        let payload = serde_json::to_vec(&value)
            .map(Bytes::from)
            .map(PutPayload::from)?;

        match self
            .object_store
            .put_opts(location, payload, options)
            .await
            .inspect_err(|error| debug!(%location, ?error))
        {
            Ok(put_result) => Ok(put_result),

            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => {
                let (current, version) = self
                    .get(location)
                    .await
                    .inspect_err(|error| error!(%location, ?error))?;

                debug!(%location, ?value, ?current);

                Err(UpdateError::Outdated { current, version })
            }

            Err(otherwise) => Err(otherwise.into()),
        }
    }

    fn txn_offset_commit_response_error(
        offsets: &TxnOffsetCommitRequest,
        error_code: ErrorCode,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let mut responses = vec![];

        for topic in &offsets.topics {
            let mut partition_responses = vec![];

            if let Some(partitions) = topic.partitions.as_deref() {
                for partition in partitions {
                    partition_responses.push(TxnOffsetCommitResponsePartition {
                        partition_index: partition.partition_index,
                        error_code: error_code.into(),
                    });
                }
            }

            responses.push(TxnOffsetCommitResponseTopic {
                name: topic.name.to_string(),
                partitions: Some(partition_responses),
            });
        }

        Ok(responses)
    }
}

#[async_trait]
impl Storage for DynoStore {
    async fn register_broker(
        &mut self,
        broker_registration: BrokerRegistrationRequest,
    ) -> Result<()> {
        debug!(?broker_registration);
        Ok(())
    }

    async fn create_topic(&mut self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        debug!(?topic, ?validate_only);

        let id = Uuid::now_v7();

        let td = TopicMetadata { id, topic };

        let payload = serde_json::to_vec(&td)
            .map(Bytes::from)
            .map(PutPayload::from)?;

        let options = PutOptions {
            mode: PutMode::Create,
            tags: TagSet::default(),
            attributes: json_content_type(),
        };

        match (
            self.object_store
                .put_opts(
                    &Path::from(format!(
                        "clusters/{}/topics/{}.json",
                        self.cluster, td.topic.name,
                    )),
                    payload.clone(),
                    options.clone(),
                )
                .await
                .inspect(|put_result| debug!(?put_result))
                .inspect_err(|error| match error {
                    object_store::Error::AlreadyExists { .. } => {
                        debug!(?error, ?td, ?validate_only)
                    }

                    _ => error!(?error, ?td, ?validate_only),
                }),
            self.object_store
                .put_opts(
                    &Path::from(format!(
                        "clusters/{}/topics/uuids/{}.json",
                        self.cluster, id,
                    )),
                    payload,
                    options,
                )
                .await
                .inspect(|put_result| debug!(?put_result))
                .inspect_err(|error| match error {
                    object_store::Error::AlreadyExists { .. } => {
                        debug!(?error, ?td, ?validate_only)
                    }

                    _ => error!(?error, ?td, ?validate_only),
                }),
        ) {
            (Ok(_), Ok(_)) => {
                let payload = serde_json::to_vec(&Watermark::default())
                    .map(Bytes::from)
                    .map(PutPayload::from)?;

                for partition in 0..td.topic.num_partitions {
                    let location = Path::from(format!(
                        "clusters/{}/topics/{}/partitions/{:0>10}/watermark.json",
                        self.cluster, td.topic.name, partition,
                    ));

                    let options = PutOptions {
                        mode: PutMode::Create,
                        tags: TagSet::default(),
                        attributes: json_content_type(),
                    };

                    match self
                        .object_store
                        .put_opts(&location, payload.clone(), options)
                        .await
                        .inspect(|put_result| debug!(%location, ?put_result))
                        .inspect_err(|error| match error {
                            object_store::Error::AlreadyExists { .. } => {
                                debug!(?error, ?td, ?validate_only)
                            }

                            _ => error!(?error, ?td, ?validate_only),
                        }) {
                        Ok(_) => continue,

                        Err(object_store::Error::AlreadyExists { .. }) => {
                            return Err(Error::Api(ErrorCode::TopicAlreadyExists))
                        }

                        _ => return Err(Error::Api(ErrorCode::UnknownServerError)),
                    }
                }

                Ok(id)
            }

            (Err(object_store::Error::AlreadyExists { .. }), _) => {
                Err(Error::Api(ErrorCode::TopicAlreadyExists))
            }

            (_, _) => Err(Error::Api(ErrorCode::UnknownServerError)),
        }
    }

    async fn delete_records(
        &mut self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        debug!(?topics);
        todo!()
    }

    async fn delete_topic(&mut self, topic: &TopicId) -> Result<ErrorCode> {
        debug!(?topic);

        if let Ok(metadata) = self.topic_metadata(topic).await {
            let prefix = Path::from(format!(
                "clusters/{}/topics/{}/",
                self.cluster, metadata.topic.name,
            ));

            let locations = self
                .object_store
                .list(Some(&prefix))
                .map_ok(|m| m.location)
                .boxed();

            _ = self
                .object_store
                .delete_stream(locations)
                .try_collect::<Vec<Path>>()
                .await?;

            let prefix = Path::from(format!("clusters/{}/groups/consumers/", self.cluster));

            let locations = self
                .object_store
                .list(Some(&prefix))
                .filter_map(|m| async {
                    m.map_or(None, |m| {
                        debug!(?m.location);

                        m.location.prefix_match(&prefix).and_then(|mut i| {
                            // skip over the consumer group name
                            _ = i.next();

                            let sub = Path::from_iter(i);
                            debug!(?sub);

                            if sub.prefix_matches(&Path::from(format!(
                                "offsets/{}/partitions/",
                                metadata.topic.name
                            ))) {
                                Some(Ok(m.location.clone()))
                            } else {
                                None
                            }
                        })
                    })
                })
                .boxed();

            _ = self
                .object_store
                .delete_stream(locations)
                .try_collect::<Vec<Path>>()
                .await?;

            self.object_store
                .delete(&Path::from(format!(
                    "clusters/{}/topics/uuids/{}.json",
                    self.cluster, metadata.id,
                )))
                .await?;

            self.object_store
                .delete(&Path::from(format!(
                    "clusters/{}/topics/{}.json",
                    self.cluster, metadata.topic.name,
                )))
                .await?;

            Ok(ErrorCode::None)
        } else {
            Ok(ErrorCode::UnknownTopicOrPartition)
        }
    }

    async fn brokers(&mut self) -> Result<Vec<DescribeClusterBroker>> {
        debug!(cluster = self.cluster);

        let broker_id = self.node;
        let host = self
            .advertised_listener
            .host_str()
            .unwrap_or("0.0.0.0")
            .into();
        let port = self.advertised_listener.port().unwrap_or(9092).into();
        let rack = None;

        Ok(vec![DescribeClusterBroker {
            broker_id,
            host,
            port,
            rack,
        }])
    }

    async fn produce(
        &mut self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        debug!(?transaction_id, ?topition, ?deflated);

        if deflated.is_idempotent() {
            self.meta
                .with_mut(&self.object_store, |meta| {
                    let Some(pd) = meta.producers.get_mut(&deflated.producer_id) else {
                        debug!(producer_id = deflated.producer_id, ?meta.producers);
                        return Err(Error::Api(ErrorCode::UnknownProducerId));
                    };

                    let Some(mut current) = pd.sequences.last_entry() else {
                        debug!(last_entry = ?pd.sequences.last_entry());
                        return Err(Error::Api(ErrorCode::UnknownServerError));
                    };

                    if current.key() != &deflated.producer_epoch {
                        debug!(current = ?current.key(), producer_epoch = deflated.producer_epoch);
                        return Err(Error::Api(ErrorCode::ProducerFenced));
                    }

                    let sequences = current.get_mut();
                    debug!(?sequences);

                    match sequences
                        .entry(topition.topic.clone())
                        .or_default()
                        .entry(topition.partition)
                        .or_default()
                    {
                        sequence if *sequence < deflated.base_sequence => {
                            debug!(?sequence, base_sequence = deflated.base_sequence);

                            Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
                        }

                        sequence if *sequence > deflated.base_sequence => {
                            debug!(?sequence, base_sequence = deflated.base_sequence);

                            Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
                        }

                        sequence => {
                            debug!(?sequence, delta = deflated.last_offset_delta + 1);

                            *sequence += deflated.last_offset_delta + 1;
                            Ok(())
                        }
                    }
                })
                .await
                .inspect(|outcome| debug!(transaction_id, ?topition, ?outcome))
                .inspect_err(|err| error!(?err, transaction_id, ?topition))?;
        }

        if let Some(ref schemas) = self.schemas {
            let inflated = inflated::Batch::try_from(&deflated)?;

            schemas.validate(topition.topic(), &inflated).await?;
        }

        let offset = self
            .watermarks
            .entry(topition.to_owned())
            .or_insert(ConditionData::<Watermark>::new(
                self.cluster.as_str(),
                topition,
            ))
            .with_mut(&self.object_store, |watermark| {
                debug!(?watermark);

                let offset = watermark.high.unwrap_or_default();
                watermark.high = watermark
                    .high
                    .map_or(Some(deflated.last_offset_delta as i64 + 1i64), |high| {
                        Some(high + deflated.last_offset_delta as i64 + 1i64)
                    });

                debug!(?watermark);

                Ok(offset)
            })
            .await
            .inspect(|offset| debug!(offset, transaction_id, ?topition))
            .inspect_err(|err| error!(?err, transaction_id, ?topition))?;

        let attributes = BatchAttribute::try_from(deflated.attributes)?;

        if let Some(transaction_id) = transaction_id {
            if attributes.transaction {
                self.meta
                    .with_mut(&self.object_store, |meta| {
                        if let Some(transaction) = meta.transactions.get_mut(transaction_id) {
                            debug!(?transaction);

                            if let Some(txn_detail) =
                                transaction.epochs.get_mut(&deflated.producer_epoch)
                            {
                                debug!(?txn_detail);

                                let offset_end = offset + deflated.last_offset_delta as i64;

                                _ = txn_detail
                                    .produces
                                    .entry(topition.topic.clone())
                                    .or_default()
                                    .entry(topition.partition)
                                    .and_modify(|entry| {
                                        let range = entry.get_or_insert(TxnProduceOffset {
                                            offset_start: offset,
                                            offset_end,
                                        });

                                        if offset_end > range.offset_end {
                                            range.offset_end = offset_end;
                                        }
                                    })
                                    .or_insert(Some(TxnProduceOffset {
                                        offset_start: offset,
                                        offset_end,
                                    }));
                            }
                        }

                        Ok(())
                    })
                    .await
                    .inspect(|outcome| debug!(?outcome, transaction_id, ?topition))
                    .inspect_err(|err| error!(?err, transaction_id, ?topition))?;
            }
        }

        let location = Path::from(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/records/{:0>20}.batch",
            self.cluster, topition.topic, topition.partition, offset,
        ));

        let payload = self.encode(deflated)?;

        _ = self
            .object_store
            .put_opts(
                &location,
                payload,
                PutOptions {
                    mode: PutMode::Create,
                    tags: TagSet::default(),
                    attributes: Attributes::new(),
                },
            )
            .await
            .inspect(|outcome| debug!(?outcome, transaction_id, ?topition))
            .inspect_err(|error| error!(?error, transaction_id, ?topition))?;

        Ok(offset)
    }

    async fn fetch(
        &mut self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        let high_watermark = self.offset_stage(topition).await.map(|offset_stage| {
            if isolation_level == IsolationLevel::ReadCommitted {
                offset_stage.last_stable
            } else {
                offset_stage.high_watermark
            }
        })?;

        debug!(
            ?topition,
            ?offset,
            ?min_bytes,
            ?max_bytes,
            ?isolation_level,
            high_watermark
        );

        let mut offsets = BTreeSet::new();

        if offset < high_watermark {
            let location = Path::from(format!(
                "clusters/{}/topics/{}/partitions/{:0>10}/records/",
                self.cluster, topition.topic, topition.partition
            ));

            let mut list_stream = self.object_store.list(Some(&location));

            while let Some(meta) = list_stream
                .next()
                .await
                .inspect(|meta| debug!(?meta))
                .transpose()
                .inspect_err(|error| error!(?error, ?topition, ?offset, ?min_bytes, ?max_bytes))
                .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?
            {
                let Some(offset) = meta.location.parts().last() else {
                    continue;
                };

                let offset = i64::from_str(&offset.as_ref()[0..20])?;
                debug!(offset);

                if offset < high_watermark {
                    _ = offsets.insert(offset);
                }
            }
        }

        let mut batches = vec![];

        let mut bytes = max_bytes as usize;

        for offset in offsets.split_off(&offset) {
            debug!(?offset);

            let location = Path::from(format!(
                "clusters/{}/topics/{}/partitions/{:0>10}/records/{:0>20}.batch",
                self.cluster, topition.topic, topition.partition, offset,
            ));

            let get_result = self
                .object_store
                .get(&location)
                .await
                .inspect_err(|error| error!(?error, ?topition, ?offset, ?min_bytes, ?max_bytes))
                .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?;

            if get_result.meta.size > bytes {
                break;
            } else {
                bytes = bytes.saturating_sub(get_result.meta.size);
            }

            let mut batch = get_result
                .bytes()
                .await
                .inspect_err(|error| error!(?error, %location))
                .map_err(|_| Error::Api(ErrorCode::UnknownServerError))
                .and_then(|encoded| self.decode(encoded))?;
            batch.base_offset = offset;
            batches.push(batch);
        }

        Ok(batches)
    }

    async fn offset_stage(&mut self, topition: &Topition) -> Result<OffsetStage> {
        debug!(?topition);

        let stable = self
            .meta
            .with(&self.object_store, |meta| {
                Ok(meta
                    .transactions
                    .values()
                    .flat_map(|txn| {
                        debug!(?txn);

                        txn.epochs
                            .values()
                            .filter(|detail| {
                                detail.state.is_some_and(|state| {
                                    state != TxnState::Committed && state != TxnState::Aborted
                                })
                            })
                            .map(BTreeMap::<Topition, Offset>::from)
                            .collect::<Vec<_>>()
                    })
                    .reduce(|mut acc, e| {
                        debug!(?acc, ?e);

                        for (topition, offset_start) in e.iter() {
                            _ = acc
                                .entry(topition.to_owned())
                                .and_modify(|existing_offset_start| {
                                    if *existing_offset_start > *offset_start {
                                        *existing_offset_start = *offset_start
                                    }
                                })
                                .or_insert(*offset_start);
                        }

                        acc
                    })
                    .unwrap_or(BTreeMap::new()))
            })
            .await?;

        debug!(?stable);

        self.watermarks
            .entry(topition.to_owned())
            .or_insert(ConditionData::<Watermark>::new(
                self.cluster.as_str(),
                topition,
            ))
            .with(&self.object_store, |watermark| {
                debug!(?watermark);
                let high_watermark = watermark.high.unwrap_or(0);
                let log_start = watermark.low.unwrap_or(0);
                let last_stable = stable.get(topition).copied().unwrap_or(high_watermark);

                Ok(OffsetStage {
                    last_stable,
                    high_watermark,
                    log_start,
                })
            })
            .await
    }

    async fn list_offsets(
        &mut self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffsetRequest)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        debug!(?offsets, ?isolation_level);

        let stable = if isolation_level == IsolationLevel::ReadCommitted {
            self.meta
                .with(&self.object_store, |meta| {
                    Ok(meta
                        .transactions
                        .values()
                        .flat_map(|txn| {
                            txn.epochs
                                .values()
                                .filter(|detail| {
                                    detail.state.is_some_and(|state| {
                                        state != TxnState::Committed && state != TxnState::Aborted
                                    })
                                })
                                .map(BTreeMap::<Topition, Offset>::from)
                                .collect::<Vec<_>>()
                        })
                        .reduce(|mut acc, e| {
                            debug!(?acc, ?e);
                            for (topition, offset_start) in e.iter() {
                                _ = acc
                                    .entry(topition.to_owned())
                                    .and_modify(|existing_offset_start| {
                                        if *existing_offset_start > *offset_start {
                                            *existing_offset_start = *offset_start
                                        }
                                    })
                                    .or_insert(*offset_start);
                            }

                            acc
                        })
                        .unwrap_or(BTreeMap::new()))
                })
                .await?
        } else {
            BTreeMap::new()
        };

        let mut responses = vec![];

        for (topition, offset_request) in offsets {
            responses.push((
                topition.to_owned(),
                match offset_request {
                    ListOffsetRequest::Earliest => {
                        self.watermarks
                            .entry(topition.to_owned())
                            .or_insert(ConditionData::<Watermark>::new(
                                self.cluster.as_str(),
                                topition,
                            ))
                            .with(&self.object_store, |watermark| {
                                Ok(ListOffsetResponse {
                                    error_code: ErrorCode::None,
                                    timestamp: None,
                                    offset: Some(watermark.low.unwrap_or(0)),
                                })
                            })
                            .await?
                    }
                    ListOffsetRequest::Latest => {
                        if let Some(offset) = stable.get(topition) {
                            ListOffsetResponse {
                                error_code: ErrorCode::None,
                                timestamp: None,
                                offset: Some(*offset),
                            }
                        } else {
                            self.watermarks
                                .entry(topition.to_owned())
                                .or_insert(ConditionData::<Watermark>::new(
                                    self.cluster.as_str(),
                                    topition,
                                ))
                                .with(&self.object_store, |watermark| {
                                    Ok(ListOffsetResponse {
                                        error_code: ErrorCode::None,
                                        timestamp: None,
                                        offset: Some(watermark.high.unwrap_or(0)),
                                    })
                                })
                                .await?
                        }
                    }
                    ListOffsetRequest::Timestamp(..) => todo!(),
                },
            ));
        }

        Ok(responses)
    }

    async fn offset_commit(
        &mut self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        debug!(?retention_time_ms, ?group_id, ?offsets);

        let mut responses = vec![];

        for (topition, offset_commit) in offsets {
            if self
                .object_store
                .head(&Path::from(format!(
                    "clusters/{}/topics/{}.json",
                    self.cluster, topition.topic,
                )))
                .await
                .is_ok()
            {
                let location = Path::from(format!(
                    "clusters/{}/groups/consumers/{}/offsets/{}/partitions/{:0>10}.json",
                    self.cluster, group_id, topition.topic, topition.partition,
                ));

                let payload = serde_json::to_vec(&offset_commit)
                    .map(Bytes::from)
                    .map(PutPayload::from)?;

                let options = PutOptions {
                    mode: PutMode::Overwrite,
                    tags: TagSet::default(),
                    attributes: json_content_type(),
                };

                let error_code = self
                    .object_store
                    .put_opts(&location, payload, options)
                    .await
                    .inspect_err(|err| error!(?err))
                    .inspect(|outcome| debug!(?outcome))
                    .map_or(ErrorCode::UnknownServerError, |_| ErrorCode::None);

                responses.push((topition.to_owned(), error_code));
            } else {
                responses.push((topition.to_owned(), ErrorCode::UnknownTopicOrPartition));
            }
        }

        Ok(responses)
    }

    async fn committed_offset_topitions(
        &mut self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, i64>> {
        debug!(group_id);

        let mut topitions = vec![];

        {
            let location = Path::from(format!(
                "clusters/{}/groups/consumers/{}/offsets/",
                self.cluster, group_id,
            ));

            let mut list_stream = self.object_store.list(Some(&location));

            while let Some(meta) = list_stream
                .next()
                .await
                .inspect(|meta| debug!(?meta))
                .transpose()
                .inspect_err(|error| error!(?error))
                .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?
            {
                debug!(?meta);
                let Some(topic): Option<String> = meta
                    .location
                    .parts()
                    .nth(6)
                    .inspect(|topic| debug!(?topic))
                    .map(|topic| topic.as_ref().into())
                else {
                    continue;
                };

                let Some(partition) = meta
                    .location
                    .parts()
                    .nth(8)
                    .inspect(|partition| debug!(?partition))
                    .map(|partition| i32::from_str(&partition.as_ref()[0..10]))
                    .transpose()?
                else {
                    continue;
                };

                debug!(topic, partition);

                topitions.push(Topition::new(topic, partition));
            }
        }

        self.offset_fetch(Some(group_id), topitions.as_ref(), Some(false))
            .await
    }

    async fn offset_fetch(
        &mut self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        debug!(?group_id, ?topics, ?require_stable);
        let mut responses = BTreeMap::new();

        if let Some(group_id) = group_id {
            for topition in topics {
                let location = Path::from(format!(
                    "clusters/{}/groups/consumers/{}/offsets/{}/partitions/{:0>10}.json",
                    self.cluster, group_id, topition.topic, topition.partition,
                ));

                let offset = match self.object_store.get(&location).await {
                    Ok(get_result) => get_result
                        .bytes()
                        .await
                        .map_err(Error::from)
                        .and_then(|encoded| {
                            serde_json::from_slice::<OffsetCommitRequest>(&encoded[..])
                                .map_err(Error::from)
                        })
                        .map(|commit| commit.offset)
                        .inspect_err(|error| error!(?error, ?group_id, ?topition))
                        .map_err(|_| Error::Api(ErrorCode::UnknownServerError)),

                    Err(object_store::Error::NotFound { .. }) => Ok(-1),

                    Err(error) => {
                        error!(?error, ?group_id, ?topition);
                        Err(Error::Api(ErrorCode::UnknownServerError))
                    }
                }?;

                _ = responses.insert(topition.to_owned(), offset);
            }
        }

        Ok(responses)
    }

    async fn metadata(&mut self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        debug!(?topics);

        let brokers = vec![MetadataResponseBroker {
            node_id: self.node,
            host: self
                .advertised_listener
                .host_str()
                .unwrap_or("0.0.0.0")
                .into(),
            port: self.advertised_listener.port().unwrap_or(9092).into(),
            rack: None,
        }];

        let responses = match topics {
            Some(topics) if !topics.is_empty() => {
                let mut responses = vec![];

                for topic in topics {
                    let response = match self
                        .topic_metadata(topic)
                        .await
                        .inspect_err(|error| error!(?error))
                    {
                        Ok(topic_metadata) => {
                            let name = Some(topic_metadata.topic.name.to_owned());
                            let error_code = ErrorCode::None.into();
                            let topic_id = Some(topic_metadata.id.into_bytes());
                            let is_internal = Some(false);
                            let partitions = topic_metadata.topic.num_partitions;
                            let replication_factor = topic_metadata.topic.replication_factor;

                            debug!(
                                ?error_code,
                                ?topic_id,
                                ?name,
                                ?is_internal,
                                ?partitions,
                                ?replication_factor
                            );

                            let mut rng = thread_rng();
                            let mut broker_ids: Vec<_> =
                                brokers.iter().map(|broker| broker.node_id).collect();
                            broker_ids.shuffle(&mut rng);

                            let mut brokers = broker_ids.into_iter().cycle();

                            let partitions = Some(
                                (0..partitions)
                                    .map(|partition_index| {
                                        let leader_id = brokers.next().expect("cycling");

                                        let replica_nodes = Some(
                                            (0..replication_factor)
                                                .map(|_replica| brokers.next().expect("cycling"))
                                                .collect(),
                                        );
                                        let isr_nodes = replica_nodes.clone();

                                        MetadataResponsePartition {
                                            error_code,
                                            partition_index,
                                            leader_id,
                                            leader_epoch: Some(-1),
                                            replica_nodes,
                                            isr_nodes,
                                            offline_replicas: Some([].into()),
                                        }
                                    })
                                    .collect(),
                            );

                            MetadataResponseTopic {
                                error_code,
                                name,
                                topic_id,
                                is_internal,
                                partitions,
                                topic_authorized_operations: Some(-2147483648),
                            }
                        }

                        Err(Error::ObjectStore(object_store::Error::NotFound { .. })) => {
                            MetadataResponseTopic {
                                error_code: ErrorCode::UnknownTopicOrPartition.into(),
                                name: match topic {
                                    TopicId::Name(name) => Some(name.into()),
                                    TopicId::Id(_) => None,
                                },
                                topic_id: Some(match topic {
                                    TopicId::Name(_) => NULL_TOPIC_ID,
                                    TopicId::Id(id) => id.into_bytes(),
                                }),
                                is_internal: Some(false),
                                partitions: Some([].into()),
                                topic_authorized_operations: Some(-2147483648),
                            }
                        }

                        Err(_) => MetadataResponseTopic {
                            error_code: ErrorCode::UnknownServerError.into(),
                            name: match topic {
                                TopicId::Name(name) => Some(name.into()),
                                TopicId::Id(_) => Some("".into()),
                            },
                            topic_id: Some(match topic {
                                TopicId::Name(_) => NULL_TOPIC_ID,
                                TopicId::Id(id) => id.into_bytes(),
                            }),
                            is_internal: Some(false),
                            partitions: Some([].into()),
                            topic_authorized_operations: Some(-2147483648),
                        },
                    };

                    responses.push(response);
                }

                responses
            }

            _ => {
                let location = Path::from(format!("clusters/{}/topics/", self.cluster));
                debug!(%location);

                let mut responses = vec![];

                let Ok(list_result) = self
                    .object_store
                    .list_with_delimiter(Some(&location))
                    .await
                    .inspect_err(|error| error!(?error, %location))
                else {
                    return Ok(MetadataResponse {
                        cluster: Some(self.cluster.clone()),
                        controller: Some(self.node),
                        brokers,
                        topics: responses,
                    });
                };

                debug!(?list_result);

                for meta in list_result.objects {
                    debug!(?meta);

                    let Ok(payload) = self
                        .object_store
                        .get(&meta.location)
                        .await
                        .inspect_err(|error| error!(?error, %location))
                    else {
                        continue;
                    };

                    let Ok(encoded) = payload
                        .bytes()
                        .await
                        .inspect_err(|error| error!(?error, %location))
                    else {
                        continue;
                    };

                    let Ok(topic_metadata) = serde_json::from_slice::<TopicMetadata>(&encoded[..])
                        .inspect_err(|error| error!(?error, %location))
                    else {
                        continue;
                    };

                    let name = Some(topic_metadata.topic.name.to_owned());
                    let error_code = ErrorCode::None.into();
                    let topic_id = Some(topic_metadata.id.into_bytes());
                    let is_internal = Some(false);
                    let partitions = topic_metadata.topic.num_partitions;
                    let replication_factor = topic_metadata.topic.replication_factor;

                    debug!(
                        ?error_code,
                        ?topic_id,
                        ?name,
                        ?is_internal,
                        ?partitions,
                        ?replication_factor
                    );

                    let mut rng = thread_rng();
                    let mut broker_ids: Vec<_> =
                        brokers.iter().map(|broker| broker.node_id).collect();
                    broker_ids.shuffle(&mut rng);

                    let mut brokers = broker_ids.into_iter().cycle();

                    let partitions = Some(
                        (0..partitions)
                            .map(|partition_index| {
                                let leader_id = brokers.next().expect("cycling");

                                let replica_nodes = Some(
                                    (0..replication_factor)
                                        .map(|_replica| brokers.next().expect("cycling"))
                                        .collect(),
                                );
                                let isr_nodes = replica_nodes.clone();

                                MetadataResponsePartition {
                                    error_code,
                                    partition_index,
                                    leader_id,
                                    leader_epoch: Some(-1),
                                    replica_nodes,
                                    isr_nodes,
                                    offline_replicas: Some([].into()),
                                }
                            })
                            .collect(),
                    );

                    responses.push(MetadataResponseTopic {
                        error_code,
                        name,
                        topic_id,
                        is_internal,
                        partitions,
                        topic_authorized_operations: Some(-2147483648),
                    });
                }

                responses
            }
        };

        Ok(MetadataResponse {
            cluster: Some(self.cluster.clone()),
            controller: Some(self.node),
            brokers,
            topics: responses,
        })
    }

    async fn describe_config(
        &mut self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        debug!(?name, ?resource, ?keys);

        match resource {
            ConfigResource::Topic => match self.topic_metadata(&TopicId::Name(name.into())).await {
                Ok(topic_metadata) => {
                    let error_code = ErrorCode::None;

                    Ok(DescribeConfigsResult {
                        error_code: error_code.into(),
                        error_message: Some(error_code.to_string()),
                        resource_type: i8::from(resource),
                        resource_name: name.into(),
                        configs: topic_metadata.topic.configs.map(|configs| {
                            configs
                                .iter()
                                .map(|config| DescribeConfigsResourceResult {
                                    name: config.name.clone(),
                                    value: config.value.clone(),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(ConfigSource::DefaultConfig.into()),
                                    is_sensitive: false,
                                    synonyms: Some([].into()),
                                    config_type: Some(ConfigType::String.into()),
                                    documentation: Some("".into()),
                                })
                                .collect()
                        }),
                    })
                }
                Err(_) => todo!(),
            },

            _ => todo!(),
        }
    }

    async fn list_groups(&mut self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        debug!(?states_filter);

        let location = Path::from(format!("clusters/{}/groups/consumers/", self.cluster,));
        let list_result = self
            .object_store
            .list_with_delimiter(Some(&location))
            .await
            .inspect(|list_result| debug!(?list_result))
            .inspect_err(|error| error!(?error, cluster = self.cluster))?;

        let mut listed_groups = vec![];

        for prefix in list_result.common_prefixes {
            if let Some(group_id) = prefix.parts().last() {
                listed_groups.push(ListedGroup {
                    group_id: group_id.as_ref().into(),
                    protocol_type: "consumer".into(),
                    group_state: Some("Unknown".into()),
                    group_type: None,
                });
            }
        }

        Ok(listed_groups)
    }

    async fn delete_groups(
        &mut self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        debug!(?group_ids);

        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                let location = Path::from(format!(
                    "clusters/{}/groups/consumers/{}.json",
                    self.cluster, group_id,
                ));

                let had_group_state = self
                    .object_store
                    .delete(&location)
                    .await
                    .inspect(|outcome| debug!(group_id, ?outcome))
                    .inspect_err(|err| error!(group_id, ?err))
                    .is_ok();

                debug!(group_id, had_group_state);

                let prefix = Path::from(format!(
                    "clusters/{}/groups/consumers/{}",
                    self.cluster, group_id,
                ));

                let locations = self
                    .object_store
                    .list(Some(&prefix))
                    .map_ok(|m| m.location)
                    .boxed();

                let deleted_committed_offsets = self
                    .object_store
                    .delete_stream(locations)
                    .try_collect::<Vec<Path>>()
                    .await?;

                debug!(group_id, ?deleted_committed_offsets);

                results.push(DeletableGroupResult {
                    group_id: group_id.into(),
                    error_code: if had_group_state || !deleted_committed_offsets.is_empty() {
                        ErrorCode::None
                    } else {
                        ErrorCode::GroupIdNotFound
                    }
                    .into(),
                });
            }
        }

        Ok(results)
    }

    async fn describe_groups(
        &mut self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        debug!(?group_ids, include_authorized_operations);
        let mut results = vec![];
        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                let location = Path::from(format!(
                    "clusters/{}/groups/consumers/{}.json",
                    self.cluster, group_id,
                ));

                match self
                    .get::<GroupDetail>(&location)
                    .await
                    .inspect(|o| debug!(?o, group_id))
                    .inspect_err(|err| error!(?err, group_id))
                {
                    Ok((group_detail, _)) => {
                        results.push(NamedGroupDetail::found(group_id.into(), group_detail));
                    }

                    Err(Error::ObjectStore(object_store::Error::NotFound { .. })) => {
                        results.push(NamedGroupDetail::found(
                            group_id.into(),
                            GroupDetail::default(),
                        ));
                    }

                    Err(_) => {
                        results.push(NamedGroupDetail::error_code(
                            group_id.into(),
                            ErrorCode::UnknownServerError,
                        ));
                    }
                }
            }
        }

        Ok(results)
    }

    async fn update_group(
        &mut self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        debug!(?group_id, ?detail, ?version);

        let location = Path::from(format!(
            "clusters/{}/groups/consumers/{}.json",
            self.cluster, group_id,
        ));

        self.put(
            &location,
            detail,
            json_content_type(),
            version.map(Into::into),
        )
        .await
        .map(Into::into)
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
            ?producer_epoch,
        );

        #[derive(Clone, Debug)]
        enum InitProducer {
            Completed(ProducerIdResponse),
            NeedToRollback {
                producer_id: i64,
                producer_epoch: i16,
            },
        }

        if let Some(transaction_id) = transaction_id {
            match self
                .meta
                .with_mut(&self.object_store, |meta| {
                    debug!(?meta);
                    match (producer_id, producer_epoch) {
                        (Some(-1), Some(-1)) => {
                            match meta.transactions.entry(transaction_id.to_string()) {
                                Entry::Vacant(vacant) => {
                                    let id = meta
                                        .producers
                                        .last_key_value()
                                        .map_or(1.into(), |(k, _v)| k + 1);

                                    let mut pd = ProducerDetail::default();
                                    assert_eq!(None, pd.sequences.insert(0, BTreeMap::new()));
                                    assert_eq!(None, meta.producers.insert(id, pd));

                                    let mut epochs = BTreeMap::new();
                                    assert_eq!(
                                        None,
                                        epochs.insert(
                                            0,
                                            TxnDetail {
                                                transaction_timeout_ms,
                                                ..Default::default()
                                            },
                                        )
                                    );

                                    _ = vacant.insert(Txn {
                                        producer: id,
                                        epochs,
                                    });

                                    Ok(InitProducer::Completed(ProducerIdResponse {
                                        id,
                                        epoch: 0,
                                        error: ErrorCode::None,
                                    }))
                                }

                                Entry::Occupied(mut occupied) => {
                                    if let Some((current_epoch, txn_detail)) =
                                        occupied.get().epochs.last_key_value()
                                    {
                                        if txn_detail.state == Some(TxnState::Begin) {
                                            Ok(InitProducer::NeedToRollback {
                                                producer_id: occupied.get().producer,
                                                producer_epoch: *current_epoch,
                                            })
                                        } else {
                                            let id = occupied.get().producer;
                                            let epoch = current_epoch + 1;

                                            _ = meta.producers.entry(id).and_modify(|pd| {
                                                assert_eq!(
                                                    None,
                                                    pd.sequences.insert(epoch, BTreeMap::new())
                                                );
                                            });

                                            assert_eq!(
                                                None,
                                                occupied.get_mut().epochs.insert(
                                                    epoch,
                                                    TxnDetail {
                                                        transaction_timeout_ms,
                                                        ..Default::default()
                                                    }
                                                )
                                            );

                                            Ok(InitProducer::Completed(ProducerIdResponse {
                                                id,
                                                epoch,
                                                error: ErrorCode::None,
                                            }))
                                        }
                                    } else {
                                        todo!()
                                    }
                                }
                            }
                        }

                        (producer, epoch) => {
                            error!(?producer, ?epoch);
                            Ok(InitProducer::Completed(ProducerIdResponse {
                                id: -1,
                                epoch: -1,
                                error: ErrorCode::UnknownServerError,
                            }))
                        }
                    }
                })
                .await?
            {
                InitProducer::Completed(completed) => Ok(completed),
                InitProducer::NeedToRollback {
                    producer_id: rollback_producer_id,
                    producer_epoch: rollback_producer_epoch,
                } => {
                    let error_code = self
                        .txn_end(
                            transaction_id,
                            rollback_producer_id,
                            rollback_producer_epoch,
                            false,
                        )
                        .await?;

                    debug!(?rollback_producer_id, ?rollback_producer_epoch, ?error_code);

                    if error_code == ErrorCode::None {
                        return self
                            .init_producer(
                                Some(transaction_id),
                                transaction_timeout_ms,
                                producer_id,
                                producer_epoch,
                            )
                            .await;
                    } else {
                        Ok(ProducerIdResponse {
                            id: -1,
                            epoch: -1,
                            error: ErrorCode::UnknownServerError,
                        })
                    }
                }
            }
        } else {
            self.meta
                .with_mut(&self.object_store, |meta| {
                    debug!(?meta);
                    match (producer_id, producer_epoch) {
                        (Some(-1), Some(-1)) => {
                            let producer = meta
                                .producers
                                .last_key_value()
                                .map_or(1.into(), |(k, _v)| k + 1);

                            let epoch = 0;
                            let mut pd = ProducerDetail::default();
                            assert_eq!(None, pd.sequences.insert(epoch, BTreeMap::new()));
                            debug!(?producer, ?pd);
                            assert_eq!(None, meta.producers.insert(producer, pd));

                            Ok(ProducerIdResponse {
                                id: producer,
                                epoch,
                                ..Default::default()
                            })
                        }

                        (producer, epoch) => {
                            error!(?producer, ?epoch);
                            Ok(ProducerIdResponse {
                                id: -1,
                                epoch: -1,
                                error: ErrorCode::UnknownServerError,
                            })
                        }
                    }
                })
                .await
        }
    }

    async fn txn_add_offsets(
        &mut self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        debug!(transaction_id, producer_id, producer_epoch, group_id);

        Ok(ErrorCode::None)
    }

    async fn txn_add_partitions(
        &mut self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        debug!(?partitions);

        match partitions {
            TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id,
                producer_id,
                producer_epoch,
                ref topics,
            } => {
                self.meta
                    .with_mut(&self.object_store, |meta| {
                        let Some(transaction) = meta.transactions.get_mut(&transaction_id) else {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(AddPartitionsToTxnPartitionResult {
                                        partition_index: *partition_index,
                                        partition_error_code: ErrorCode::TransactionalIdNotFound
                                            .into(),
                                    });
                                }

                                results.push(AddPartitionsToTxnTopicResult {
                                    name: topic.name.clone(),
                                    results_by_partition: Some(results_by_partition),
                                })
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
                        };

                        if transaction.producer != producer_id {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(AddPartitionsToTxnPartitionResult {
                                        partition_index: *partition_index,
                                        partition_error_code: ErrorCode::UnknownProducerId.into(),
                                    });
                                }

                                results.push(AddPartitionsToTxnTopicResult {
                                    name: topic.name.clone(),
                                    results_by_partition: Some(results_by_partition),
                                })
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
                        }

                        let Some(mut current_epoch) = transaction.epochs.last_entry() else {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(AddPartitionsToTxnPartitionResult {
                                        partition_index: *partition_index,
                                        partition_error_code: ErrorCode::ProducerFenced.into(),
                                    });
                                }

                                results.push(AddPartitionsToTxnTopicResult {
                                    name: topic.name.clone(),
                                    results_by_partition: Some(results_by_partition),
                                })
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
                        };

                        if &producer_epoch != current_epoch.key() {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(AddPartitionsToTxnPartitionResult {
                                        partition_index: *partition_index,
                                        partition_error_code: ErrorCode::ProducerFenced.into(),
                                    });
                                }

                                results.push(AddPartitionsToTxnTopicResult {
                                    name: topic.name.clone(),
                                    results_by_partition: Some(results_by_partition),
                                })
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
                        }

                        let txn_detail = current_epoch.get_mut();

                        let mut results = vec![];

                        for topic in topics {
                            let mut results_by_partition = vec![];

                            for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                _ = txn_detail
                                    .produces
                                    .entry(topic.name.clone())
                                    .or_default()
                                    .entry(*partition_index)
                                    .or_default();

                                results_by_partition.push(AddPartitionsToTxnPartitionResult {
                                    partition_index: *partition_index,
                                    partition_error_code: i16::from(ErrorCode::None),
                                });
                            }

                            results.push(AddPartitionsToTxnTopicResult {
                                name: topic.name.clone(),
                                results_by_partition: Some(results_by_partition),
                            })
                        }

                        txn_detail.started_at = Some(SystemTime::now());
                        txn_detail.state = Some(TxnState::Begin);

                        Ok(TxnAddPartitionsResponse::VersionZeroToThree(results))
                    })
                    .await
            }

            TxnAddPartitionsRequest::VersionFourPlus { .. } => {
                todo!()
            }
        }
    }

    async fn txn_offset_commit(
        &mut self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        debug!(?offsets);
        self.meta
            .with_mut(&self.object_store, |meta| {
                let Some(transaction) = meta.transactions.get_mut(&offsets.transaction_id) else {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::TransactionalIdNotFound,
                    );
                };

                if transaction.producer != offsets.producer_id {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::UnknownProducerId,
                    );
                }

                let Some(mut current_epoch) = transaction.epochs.last_entry() else {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::ProducerFenced,
                    );
                };

                if &offsets.producer_epoch != current_epoch.key() {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::ProducerFenced,
                    );
                }

                let txn_detail = current_epoch.get_mut();

                let mut responses = vec![];

                for topic in &offsets.topics {
                    let mut partition_responses = vec![];

                    if let Some(partitions) = topic.partitions.as_deref() {
                        for partition in partitions {
                            _ = txn_detail
                                .offsets
                                .entry(offsets.group_id.clone())
                                .or_default()
                                .entry(topic.name.clone())
                                .or_default()
                                .insert(
                                    partition.partition_index,
                                    TxnCommitOffset {
                                        committed_offset: partition.committed_offset,
                                        leader_epoch: partition.committed_leader_epoch,
                                        metadata: partition.committed_metadata.clone(),
                                    },
                                );

                            partition_responses.push(TxnOffsetCommitResponsePartition {
                                partition_index: partition.partition_index,
                                error_code: ErrorCode::None.into(),
                            });
                        }
                    }

                    responses.push(TxnOffsetCommitResponseTopic {
                        name: topic.name.to_string(),
                        partitions: Some(partition_responses),
                    });
                }

                Ok(responses)
            })
            .await
    }

    async fn txn_end(
        &mut self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        debug!(transaction_id, producer_id, producer_epoch, committed);

        let produced = self
            .meta
            .with_mut(&self.object_store, |meta| {
                debug!(transactions = ?meta.transactions);

                let Some(transaction) = meta.transactions.get_mut(transaction_id) else {
                    return Err(Error::Api(ErrorCode::TransactionalIdNotFound));
                };

                if transaction.producer != producer_id {
                    return Err(Error::Api(ErrorCode::UnknownProducerId));
                }

                let Some(mut current_epoch) = transaction.epochs.last_entry() else {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                };

                if &producer_epoch != current_epoch.key() {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                }

                let txn_detail = current_epoch.get_mut();

                let mut produced = vec![];

                if txn_detail.state == Some(TxnState::Begin) {
                    assert_eq!(
                        Some(TxnState::Begin),
                        txn_detail.state.replace(if committed {
                            TxnState::PrepareCommit
                        } else {
                            TxnState::PrepareAbort
                        })
                    );

                    for (topic, partitions) in &txn_detail.produces {
                        for (partition, offset_range) in partitions {
                            debug!(?topic, partition, ?offset_range);

                            if offset_range.is_some() {
                                produced.push(Topition::new(topic.to_owned(), *partition));
                            }
                        }
                    }
                }

                Ok(produced)
            })
            .await
            .inspect(|produced| debug!(?produced))
            .inspect_err(|err| error!(?err))?;

        for topition in produced {
            debug!(?topition);

            let control_batch: Bytes = if committed {
                ControlBatch::default().commit().try_into()?
            } else {
                ControlBatch::default().abort().try_into()?
            };

            let end_transaction_marker: Bytes = EndTransactionMarker::default().try_into()?;

            let batch = inflated::Batch::builder()
                .record(
                    Record::builder()
                        .key(control_batch.into())
                        .value(end_transaction_marker.into()),
                )
                .attributes(
                    BatchAttribute::default()
                        .control(true)
                        .transaction(true)
                        .into(),
                )
                .producer_id(producer_id)
                .producer_epoch(producer_epoch)
                .base_sequence(-1)
                .build()
                .and_then(TryInto::try_into)
                .inspect(|deflated| debug!(?deflated))?;

            _ = self
                .produce(Some(transaction_id), &topition, batch)
                .await
                .inspect(|offset| {
                    debug!(
                        offset,
                        ?topition,
                        producer_id,
                        producer_epoch,
                        transaction_id,
                        committed,
                    )
                })
                .inspect_err(|err| {
                    error!(
                        ?err,
                        ?topition,
                        producer_id,
                        producer_epoch,
                        transaction_id,
                        committed,
                    )
                })?;
        }

        let offsets_to_commit = self
            .meta
            .with_mut(&self.object_store, |meta| {
                debug!(transactions = ?meta.transactions);

                let Some(transaction) = meta.transactions.get_mut(transaction_id) else {
                    return Err(Error::Api(ErrorCode::TransactionalIdNotFound));
                };

                if transaction.producer != producer_id {
                    return Err(Error::Api(ErrorCode::UnknownProducerId));
                }

                let Some(current_epoch) = transaction.epochs.last_entry() else {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                };

                if &producer_epoch != current_epoch.key() {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                }

                let mut overlaps =
                    meta.overlapping_transactions(transaction_id, producer_id, producer_epoch)?;
                debug!(?overlaps);

                let mut offsets_to_commit: BTreeMap<
                    Group,
                    BTreeMap<Topic, BTreeMap<Partition, TxnCommitOffset>>,
                > = BTreeMap::new();

                if overlaps.iter().all(|txn_id| txn_id.state.is_prepared()) {
                    let txn_ids = {
                        overlaps.push(TxnId {
                            transaction: transaction_id.into(),
                            producer_id,
                            producer_epoch,
                            state: if committed {
                                TxnState::PrepareCommit
                            } else {
                                TxnState::PrepareAbort
                            },
                        });

                        overlaps
                    };

                    for txn_id in txn_ids {
                        debug!(?txn_id);

                        if let Some(txn) = meta.transactions.get_mut(txn_id.transaction.as_str()) {
                            if let Some(txn_detail) = txn.epochs.get_mut(&txn_id.producer_epoch) {
                                debug!(?txn_detail);

                                match txn_detail.state {
                                    None | Some(TxnState::PrepareCommit) => {
                                        _ = txn_detail.state.replace(TxnState::Committed);
                                    }

                                    Some(TxnState::PrepareAbort) => {
                                        _ = txn_detail.state.replace(TxnState::Aborted);
                                    }

                                    otherwise => {
                                        warn!(
                                            transaction = txn_id.transaction,
                                            producer = txn_id.producer_id,
                                            epoch = txn_id.producer_epoch,
                                            ?otherwise,
                                        );

                                        continue;
                                    }
                                }

                                if txn_id.state == TxnState::PrepareCommit {
                                    for (group, topics) in txn_detail.offsets.iter() {
                                        for (topic, partitions) in topics.iter() {
                                            for (partition, committed_offset) in partitions {
                                                _ = offsets_to_commit
                                                    .entry(group.to_owned())
                                                    .or_default()
                                                    .entry(topic.to_owned())
                                                    .or_default()
                                                    .insert(
                                                        *partition,
                                                        committed_offset.to_owned(),
                                                    );
                                            }
                                        }
                                    }
                                }

                                txn_detail.produces.clear();
                                txn_detail.offsets.clear();
                                _ = txn_detail.started_at.take();
                            }
                        }
                    }
                }

                Ok(offsets_to_commit)
            })
            .await
            .inspect(|outcome| debug!(?outcome))
            .inspect_err(|err| error!(?err))?;

        debug!(?offsets_to_commit);

        for (group, topics) in offsets_to_commit.iter() {
            let mut offsets = vec![];

            for (topic, partitions) in topics.iter() {
                for (partition, txn_co) in partitions {
                    let tp = Topition::new(topic.to_owned(), *partition);
                    let ocr = OffsetCommitRequest {
                        offset: txn_co.committed_offset,
                        leader_epoch: txn_co.leader_epoch,
                        timestamp: None,
                        metadata: txn_co.metadata.clone(),
                    };

                    offsets.push((tp, ocr));
                }
            }

            _ = self.offset_commit(group, None, &offsets[..]).await?;
        }

        Ok(ErrorCode::None)
    }
}

#[derive(Debug, Clone)]
struct Metron<O> {
    request_duration: Histogram<u64>,
    request_error: Counter<u64>,

    cluster: String,
    object_store: O,
}

impl<O> Display for Metron<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metron").finish()
    }
}

impl<O> Metron<O>
where
    O: ObjectStore,
{
    fn new(object_store: O, cluster: &str) -> Self {
        let meter = global::meter_with_scope(
            InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
                .with_version(env!("CARGO_PKG_VERSION"))
                .with_schema_url(SCHEMA_URL)
                .build(),
        );

        Self {
            cluster: cluster.into(),
            request_duration: meter
                .u64_histogram("object_store_request_duration")
                .with_unit("ms")
                .with_description("The object store request latencies in milliseconds")
                .build(),
            request_error: meter
                .u64_counter("object_store_request_error")
                .with_description("The object store request errors")
                .build(),

            object_store,
        }
    }
}

#[async_trait]
impl<O> ObjectStore for Metron<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "put_opts"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        self.object_store
            .put_opts(location, payload, opts)
            .await
            .inspect(|_put_result| {
                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                let mut additional = vec![KeyValue::new("reason", format!("{err:?}"))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "put_multipart_opts"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        self.object_store
            .put_multipart_opts(location, opts)
            .await
            .inspect(|_put_result| {
                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                let mut additional = vec![KeyValue::new("reason", format!("{err:?}"))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "get_opts"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        self.object_store
            .get_opts(location, options)
            .await
            .inspect(|_get_result| {
                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                let mut additional = vec![KeyValue::new("reason", format!("{err:?}"))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    async fn delete(&self, location: &Path) -> Result<(), object_store::Error> {
        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "delete"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        self.object_store
            .delete(location)
            .await
            .inspect(|_get_result| {
                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                let mut additional = vec![KeyValue::new("reason", format!("{err:?}"))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'_, Result<ObjectMeta, object_store::Error>> {
        self.object_store.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "list_with_delimiter"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        self.object_store
            .list_with_delimiter(prefix)
            .await
            .inspect(|_get_result| {
                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                let mut additional = vec![KeyValue::new("reason", format!("{err:?}"))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<(), object_store::Error> {
        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "copy"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        self.object_store
            .copy(from, to)
            .await
            .inspect(|_get_result| {
                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                let mut additional = vec![KeyValue::new("reason", format!("{err:?}"))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<(), object_store::Error> {
        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "copy_if_not_exists"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        self.object_store
            .copy_if_not_exists(from, to)
            .await
            .inspect(|_get_result| {
                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                let mut additional = vec![KeyValue::new("reason", format!("{err:?}"))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_key_b_tree_map() -> Result<()> {
        let mut b = BTreeMap::new();
        assert_eq!(None, b.insert(65, "a"));

        let encoded = serde_json::to_vec(&b)?;

        let decoded = serde_json::from_slice::<BTreeMap<i32, &str>>(&encoded[..])?;
        assert_eq!(Some(&"a"), decoded.get(&65));

        Ok(())
    }
}
