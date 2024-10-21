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

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::io::BufReader;
use std::sync::Arc;
use std::time::SystemTime;
use std::{collections::BTreeMap, fmt::Debug, io::Cursor, str::FromStr, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::TryStreamExt;
use futures::StreamExt;
use object_store::{
    path::Path, Attribute, AttributeValue, Attributes, ObjectStore, PutMode, PutOptions,
    PutPayload, TagSet, UpdateVersion,
};
use object_store::{DynObjectStore, GetOptions, PutResult};
use rand::{prelude::*, thread_rng};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tansu_kafka_sans_io::describe_configs_response::DescribeConfigsResourceResult;
use tansu_kafka_sans_io::{
    create_topics_request::CreatableTopic,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::{deflated, inflated},
    ConfigResource, Encoder, ErrorCode,
};
use tansu_kafka_sans_io::{ConfigSource, ConfigType, Decoder};
use tracing::{debug, error};
use uuid::Uuid;

use crate::{
    BrokerRegistationRequest, Error, GroupDetail, ListOffsetRequest, ListOffsetResponse,
    MetadataResponse, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result, Storage,
    TopicId, Topition, UpdateError, Version, NULL_TOPIC_ID,
};

const APPLICATION_JSON: &str = "application/json";

#[derive(Clone, Debug)]
pub struct DynoStore {
    cluster: String,
    node: i32,
    watermarks: BTreeMap<Topition, ConditionData<Watermark>>,
    producers: ConditionData<BTreeMap<i64, Producer>>,

    object_store: Arc<DynObjectStore>,
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
        debug!(?self, ?object_store);

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

        debug!(?self, ?object_store);

        Ok(())
    }

    async fn with_mut<E, F>(&mut self, object_store: &impl ObjectStore, f: F) -> Result<E>
    where
        E: Debug,
        F: Fn(&mut D) -> Result<E>,
    {
        debug!(?self, ?object_store);

        loop {
            let outcome = f(&mut self.data)?;
            debug!(?self, ?outcome);

            let payload = serde_json::to_vec(&self.data)
                .map(Bytes::from)
                .map(PutPayload::from)?;

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

                    return Ok(outcome);
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
        debug!(?self, ?object_store);

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
    low: i64,
    high: i64,
    stable: i64,
    producers: BTreeMap<i64, WatermarkSequence>,
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

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct Producer {
    epoch: i16,
    updated: SystemTime,
}

impl Default for Producer {
    fn default() -> Self {
        Self {
            epoch: 0,
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
            producers: ConditionData {
                path: Path::from(format!("clusters/{}/producers.json", cluster)),
                version: None,
                attributes: Attributes::new(),
                tags: TagSet::default(),
                data: BTreeMap::new(),
            },
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

    async fn produce(&mut self, topition: &Topition, deflated: deflated::Batch) -> Result<i64> {
        debug!(?topition, ?deflated);

        if deflated.producer_id > 0 {
            self.producers
                .with(&self.object_store, |producers| {
                    debug!(?producers, ?deflated.producer_id);
                    producers.get(&deflated.producer_id).map_or(
                        Err(Error::Api(ErrorCode::UnknownProducerId)),
                        |producer| {
                            if producer.epoch == deflated.producer_epoch {
                                Ok(())
                            } else {
                                Err(Error::Api(ErrorCode::ProducerFenced))
                            }
                        },
                    )
                })
                .await?
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

                if deflated.producer_id > 0 {
                    if let Some(ws) = watermark.producers.get_mut(&deflated.producer_id) {
                        debug!(?ws);

                        match ws.epoch.cmp(&deflated.producer_epoch) {
                            Ordering::Equal => match ws.sequence.cmp(&deflated.base_sequence) {
                                Ordering::Equal => {
                                    debug!(?ws, ?deflated.base_sequence);

                                    ws.sequence += deflated.last_offset_delta + 1;

                                    let offset = watermark.high;
                                    watermark.high += deflated.last_offset_delta as i64 + 1i64;
                                    Ok(offset)
                                }

                                Ordering::Greater => {
                                    debug!(?ws, ?deflated.base_sequence);
                                    Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
                                }

                                Ordering::Less => {
                                    debug!(?ws, ?deflated.base_sequence);
                                    Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
                                }
                            },

                            Ordering::Greater => Err(Error::Api(ErrorCode::ProducerFenced)),

                            Ordering::Less => Err(Error::Api(ErrorCode::InvalidProducerEpoch)),
                        }
                    } else {
                        let offset = watermark.high;
                        watermark.high += deflated.last_offset_delta as i64 + 1i64;
                        _ = watermark.producers.insert(
                            deflated.producer_id,
                            WatermarkSequence {
                                epoch: deflated.producer_epoch,
                                sequence: deflated.last_offset_delta + 1,
                                ..Default::default()
                            },
                        );
                        Ok(offset)
                    }
                } else {
                    let offset = watermark.high;
                    watermark.high += deflated.last_offset_delta as i64 + 1i64;
                    Ok(offset)
                }
            })
            .await?;

        let location = Path::from(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/records/{:0>20}.batch",
            self.cluster, topition.topic, topition.partition, offset,
        ));

        let payload = self.encode(deflated)?;
        let attributes = Attributes::new();

        let options = PutOptions {
            mode: PutMode::Create,
            tags: TagSet::default(),
            attributes,
        };

        _ = self
            .object_store
            .put_opts(&location, payload, options)
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
    ) -> Result<deflated::Batch> {
        debug!(?topition, ?offset, ?min_bytes, ?max_bytes);

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

        let greater_or_equal = offsets.split_off(&offset);

        let Some(offset) = greater_or_equal.first() else {
            return inflated::Batch::builder()
                .build()
                .and_then(TryInto::try_into)
                .map_err(Into::into);
        };

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

        get_result
            .bytes()
            .await
            .inspect_err(|error| error!(?error, ?location))
            .map_err(|_| Error::Api(ErrorCode::UnknownServerError))
            .and_then(|encoded| self.decode(encoded))
            .map(|mut deflated| {
                deflated.base_offset = *offset;
                deflated
            })
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
                    last_stable: watermark.stable,
                    high_watermark: watermark.high,
                    log_start: watermark.low,
                })
            })
            .await
    }

    async fn list_offsets(
        &mut self,
        offsets: &[(Topition, ListOffsetRequest)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        debug!(?offsets);

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
                                    offset: Some(watermark.low),
                                })
                            })
                            .await?
                    }
                    ListOffsetRequest::Latest => {
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
                                    offset: Some(watermark.high),
                                })
                            })
                            .await?
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

        if let Some(_transaction_id) = transaction_id {
            self.producers
                .with_mut(&self.object_store, |producers| {
                    debug!(?producers);
                    match (producer_id, producer_epoch) {
                        (Some(-1), Some(-1)) => {
                            let id = producers.last_key_value().map_or(1, |(k, _)| k + 1);
                            _ = producers.insert(id, Producer::default());

                            Ok(ProducerIdResponse {
                                id,
                                epoch: 0,
                                ..Default::default()
                            })
                        }

                        (Some(producer_id), Some(producer_epoch))
                            if producer_id > 0 && producer_epoch >= 0 =>
                        {
                            match producers.get(&producer_id) {
                                Some(Producer { epoch, .. }) if producer_epoch == *epoch => {
                                    if let Some(epoch) = epoch.checked_add(1) {
                                        _ = producers.insert(
                                            producer_id,
                                            Producer {
                                                epoch,
                                                ..Default::default()
                                            },
                                        );

                                        Ok(ProducerIdResponse {
                                            id: producer_id,
                                            epoch,
                                            ..Default::default()
                                        })
                                    } else {
                                        let id =
                                            producers.last_key_value().map_or(1, |(k, _)| k + 1);
                                        _ = producers.insert(id, Producer::default());

                                        Ok(ProducerIdResponse {
                                            id,
                                            epoch: 0,
                                            ..Default::default()
                                        })
                                    }
                                }

                                Some(_) | None => Ok(ProducerIdResponse {
                                    id: -1,
                                    epoch: -1,
                                    error: ErrorCode::InvalidProducerEpoch,
                                }),
                            }
                        }

                        (None, None) => todo!(),
                        (None, Some(_)) => todo!(),
                        (Some(_), None) => todo!(),
                        (_, _) => todo!(),
                    }
                })
                .await
        } else {
            self.producers
                .with_mut(&self.object_store, |producers| {
                    debug!(?producers);
                    match (producer_id, producer_epoch) {
                        (Some(-1), Some(-1)) => {
                            let id = producers.last_key_value().map_or(1, |(k, _)| k + 1);
                            _ = producers.insert(id, Producer::default());

                            Ok(ProducerIdResponse {
                                id,
                                epoch: 0,
                                ..Default::default()
                            })
                        }

                        (None, None) => todo!(),
                        (None, Some(_)) => todo!(),
                        (Some(_), None) => todo!(),
                        (_, _) => todo!(),
                    }
                })
                .await
        }
    }
}
