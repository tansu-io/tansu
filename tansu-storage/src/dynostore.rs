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
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    fmt::Debug,
    io::{BufReader, Cursor},
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::TryStreamExt, StreamExt};
use object_store::{
    path::Path, Attribute, AttributeValue, Attributes, DynObjectStore, GetOptions, ObjectStore,
    PutMode, PutOptions, PutPayload, PutResult, TagSet, UpdateVersion,
};
use rand::{prelude::*, thread_rng};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tansu_kafka_sans_io::{
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
    create_topics_request::CreatableTopic,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::{deflated, inflated, Record},
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
    BatchAttribute, ConfigResource, ConfigSource, ConfigType, ControlBatch, Decoder, Encoder,
    EndTransactionMarker, ErrorCode, IsolationLevel,
};
use tracing::{debug, error};
use uuid::Uuid;

use crate::{
    BrokerRegistationRequest, Error, GroupDetail, ListOffsetRequest, ListOffsetResponse,
    MetadataResponse, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result, Storage,
    TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest,
    TxnState, UpdateError, Version, NULL_TOPIC_ID,
};

const APPLICATION_JSON: &str = "application/json";

#[derive(Clone, Debug)]
pub struct DynoStore {
    cluster: String,
    node: i32,
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

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct ProducerDetail {
    sequences: BTreeMap<ProducerEpoch, BTreeMap<String, BTreeMap<i32, Sequence>>>,
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
    produces: BTreeMap<String, BTreeMap<i32, Option<TxnProduceOffset>>>,
    offsets: BTreeMap<String, BTreeMap<i32, TxnCommitOffset>>,
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

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
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
            watermarks: BTreeMap::new(),
            meta: ConditionData::<Meta>::new(cluster),
            object_store: Arc::new(object_store),
        }
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

        let encoded = get_result.bytes().await?;
        let r = BufReader::new(&encoded[..]);

        let payload = serde_json::from_reader(r)?;

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
        P: Serialize + DeserializeOwned + Debug,
    {
        debug!(?location, ?attributes, ?update_version, ?value);

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
            .inspect_err(|error| debug!(?location, ?error))
        {
            Ok(put_result) => Ok(put_result),

            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => {
                let (current, version) = self
                    .get(location)
                    .await
                    .inspect_err(|error| error!(?location, ?error))?;

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
        broker_registration: BrokerRegistationRequest,
    ) -> Result<()> {
        debug!(?broker_registration);

        let payload = serde_json::to_vec(&broker_registration)
            .map(Bytes::from)
            .map(PutPayload::from)?;

        let location = Path::from(format!(
            "clusters/{}/brokers/{}.json",
            self.cluster, self.node
        ));

        let options = PutOptions {
            mode: PutMode::Overwrite,
            tags: TagSet::default(),
            attributes: json_content_type(),
        };

        let put_result = self
            .object_store
            .put_opts(&location, payload, options)
            .await?;

        debug!(?location, ?put_result);

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
                        .inspect(|put_result| debug!(?location, ?put_result))
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
        let location = Path::from(format!("clusters/{}/brokers/", self.cluster));
        debug!(?location);

        let mut brokers = vec![];

        let mut list_stream = self.object_store.list(Some(&location));

        while let Some(meta) = list_stream
            .next()
            .await
            .inspect(|meta| debug!(?meta))
            .transpose()?
        {
            let Ok(get_result) = self
                .object_store
                .get(&meta.location)
                .await
                .inspect_err(|error| error!(?error, ?location))
            else {
                continue;
            };

            let Ok(encoded) = get_result
                .bytes()
                .await
                .inspect_err(|error| error!(?error, ?location))
            else {
                continue;
            };

            let Ok(broker_registration) =
                serde_json::from_slice::<BrokerRegistationRequest>(&encoded[..])
                    .inspect_err(|error| error!(?error, ?location))
            else {
                continue;
            };

            let Some(listener) = broker_registration
                .listeners
                .iter()
                .find(|listener| listener.name.as_str() == "broker")
            else {
                continue;
            };

            brokers.push(DescribeClusterBroker {
                broker_id: broker_registration.broker_id,
                host: listener.host.clone(),
                port: listener.port as i32,
                rack: broker_registration.rack,
            });
        }

        Ok(brokers)
    }

    async fn produce(
        &mut self,
        transaction_id: Option<&str>,
        topition: &Topition,
        mut deflated: deflated::Batch,
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
                .await?;

            if transaction_id.is_none() {
                deflated.producer_id = -1;
                deflated.producer_epoch = -1;
            }
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
                let offset = watermark.high.map_or(0, |high| high + 1);
                watermark.high = watermark.high.map_or(Some(0), |high| {
                    Some(high + deflated.last_offset_delta as i64 + 1i64)
                });
                Ok(offset)
            })
            .await?;

        let attributes = BatchAttribute::try_from(deflated.attributes)?;

        if let Some(transaction_id) = transaction_id {
            if attributes.transaction {
                self.meta
                    .with_mut(&self.object_store, |meta| {
                        if let Some(transaction) = meta.transactions.get_mut(transaction_id) {
                            if let Some(txn_detail) =
                                transaction.epochs.get_mut(&deflated.producer_epoch)
                            {
                                let offset_end = deflated.last_offset_delta as i64 + 1i64;

                                _ = txn_detail
                                    .produces
                                    .entry(topition.topic.clone())
                                    .or_default()
                                    .entry(topition.partition)
                                    .and_modify(|entry| {
                                        entry
                                            .get_or_insert(TxnProduceOffset {
                                                offset_start: offset,
                                                offset_end,
                                            })
                                            .offset_end = offset_end;
                                    })
                                    .or_insert(Some(TxnProduceOffset {
                                        offset_start: offset,
                                        offset_end,
                                    }));
                            }
                        }

                        Ok(())
                    })
                    .await?;
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
            .inspect_err(|error| error!(?error))?;

        Ok(offset)
    }

    async fn fetch(
        &mut self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        debug!(?topition, ?offset, ?min_bytes, ?max_bytes, ?isolation);

        let location = Path::from(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/records/",
            self.cluster, topition.topic, topition.partition
        ));

        let mut offsets = BTreeSet::new();

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

            _ = offsets.insert(offset);
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
                .inspect_err(|error| error!(?error, ?location))
                .map_err(|_| Error::Api(ErrorCode::UnknownServerError))
                .and_then(|encoded| self.decode(encoded))?;
            batch.base_offset = offset;
            batches.push(batch);
        }

        Ok(batches)
    }

    async fn offset_stage(&mut self, topition: &Topition) -> Result<OffsetStage> {
        debug!(?topition);

        self.watermarks
            .entry(topition.to_owned())
            .or_insert(ConditionData::<Watermark>::new(
                self.cluster.as_str(),
                topition,
            ))
            .with(&self.object_store, |watermark| {
                Ok(OffsetStage {
                    last_stable: -1,
                    high_watermark: watermark.high.unwrap_or(0),
                    log_start: watermark.low.unwrap_or(0),
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
                .map_or(ErrorCode::UnknownServerError, |_| ErrorCode::None);

            responses.push((topition.to_owned(), error_code));
        }

        Ok(responses)
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

        let location = Path::from(format!("clusters/{}/brokers/", self.cluster));

        let mut brokers = vec![];

        let mut list_stream = self.object_store.list(Some(&location));

        while let Some(meta) = list_stream
            .next()
            .await
            .inspect(|meta| debug!(?meta))
            .transpose()?
        {
            let Ok(payload) = self
                .object_store
                .get(&meta.location)
                .await
                .inspect_err(|error| error!(?error, ?location))
            else {
                continue;
            };

            let Ok(encoded) = payload
                .bytes()
                .await
                .inspect_err(|error| error!(?error, ?location))
            else {
                continue;
            };

            let Ok(broker_registration) =
                serde_json::from_slice::<BrokerRegistationRequest>(&encoded[..])
                    .inspect_err(|error| error!(?error, ?location))
            else {
                continue;
            };

            let Some(listener) = broker_registration
                .listeners
                .iter()
                .find(|listener| listener.name.as_str() == "broker")
            else {
                continue;
            };

            brokers.push(MetadataResponseBroker {
                node_id: broker_registration.broker_id,
                host: listener.host.clone(),
                port: listener.port as i32,
                rack: broker_registration.rack,
            });
        }

        let responses = match topics {
            Some(topics) if !topics.is_empty() => {
                let mut responses = vec![];

                for topic in topics {
                    let response = match self
                        .topic_metadata(topic)
                        .await
                        .inspect_err(|error| error!(?error, ?location))
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
                                    TopicId::Id(_) => Some("".into()),
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
                debug!(?location);

                let mut responses = vec![];

                let Ok(list_result) = self
                    .object_store
                    .list_with_delimiter(Some(&location))
                    .await
                    .inspect_err(|error| error!(?error, ?location))
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
                        .inspect_err(|error| error!(?error, ?location))
                    else {
                        continue;
                    };

                    let Ok(encoded) = payload
                        .bytes()
                        .await
                        .inspect_err(|error| error!(?error, ?location))
                    else {
                        continue;
                    };

                    let Ok(topic_metadata) = serde_json::from_slice::<TopicMetadata>(&encoded[..])
                        .inspect_err(|error| error!(?error, ?location))
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
                Ok(topic_metadata) => Ok(DescribeConfigsResult {
                    error_code: ErrorCode::None.into(),
                    error_message: Some("None".into()),
                    resource_type: i8::from(resource),
                    resource_name: name.into(),
                    configs: topic_metadata.topic.configs.map(|configs| {
                        configs
                            .iter()
                            .map(|config| DescribeConfigsResourceResult {
                                name: config.name.clone(),
                                value: config.value.clone(),
                                read_only: false,
                                is_default: Some(false),
                                config_source: Some(ConfigSource::DefaultConfig.into()),
                                is_sensitive: false,
                                synonyms: Some([].into()),
                                config_type: Some(ConfigType::String.into()),
                                documentation: Some("".into()),
                            })
                            .collect()
                    }),
                }),
                Err(_) => todo!(),
            },

            _ => todo!(),
        }
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
                            if offset_range.is_some() {
                                produced.push(Topition::new(topic.to_owned(), *partition));
                            }
                        }
                    }
                }

                Ok(produced)
            })
            .await?;

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

            _ = self.produce(Some(transaction_id), &topition, batch).await?;
        }

        Ok(ErrorCode::None)
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
