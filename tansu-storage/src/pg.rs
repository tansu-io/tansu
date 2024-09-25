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
    collections::BTreeMap,
    marker::PhantomData,
    str::FromStr,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use deadpool_postgres::{Manager, ManagerConfig, Object, Pool, RecyclingMethod};
use rand::{prelude::*, thread_rng};
use tansu_kafka_sans_io::{
    create_topics_request::CreatableTopic,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
    describe_cluster_response::DescribeClusterBroker,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::{deflated, inflated, Header, Record},
    to_system_time, to_timestamp, ErrorCode, ScramMechanism,
};
use tokio_postgres::{error::SqlState, Config, NoTls};
use tracing::{debug, error};
use uuid::Uuid;

use crate::{
    BrokerRegistationRequest, Error, ListOffsetRequest, ListOffsetResponse, MetadataResponse,
    OffsetCommitRequest, OffsetStage, Result, ScramCredential, Storage, TopicId, Topition,
};

const NULL_TOPIC_ID: [u8; 16] = [0; 16];

#[derive(Clone, Debug)]
pub struct Postgres {
    cluster: String,
    node: i32,
    pool: Pool,
}

#[derive(Clone, Default, Debug)]
pub struct Builder<C, N, P> {
    cluster: C,
    node: N,
    pool: P,
}

impl<C, N, P> Builder<C, N, P> {
    pub fn cluster(self, cluster: impl Into<String>) -> Builder<String, N, P> {
        Builder {
            cluster: cluster.into(),
            node: self.node,
            pool: self.pool,
        }
    }
}

impl<C, N, P> Builder<C, N, P> {
    pub fn node(self, node: i32) -> Builder<C, i32, P> {
        Builder {
            cluster: self.cluster,
            node,
            pool: self.pool,
        }
    }
}

impl Builder<String, i32, Pool> {
    pub fn build(self) -> Postgres {
        Postgres {
            cluster: self.cluster,
            node: self.node,
            pool: self.pool,
        }
    }
}

impl<C, N> FromStr for Builder<C, N, Pool>
where
    C: Default,
    N: Default,
{
    type Err = Error;

    fn from_str(config: &str) -> Result<Self, Self::Err> {
        let pg_config = Config::from_str(config)?;

        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };

        let mgr = Manager::from_config(pg_config, NoTls, mgr_config);

        Pool::builder(mgr)
            .max_size(16)
            .build()
            .map(|pool| Self {
                pool,
                node: N::default(),
                cluster: C::default(),
            })
            .map_err(Into::into)
    }
}

impl Postgres {
    pub fn builder(
        connection: &str,
    ) -> Result<Builder<PhantomData<String>, PhantomData<i32>, Pool>> {
        debug!(connection);
        Builder::from_str(connection)
    }

    async fn connection(&self) -> Result<Object> {
        self.pool.get().await.map_err(Into::into)
    }
}

#[async_trait]
impl Storage for Postgres {
    async fn register_broker(&self, broker_registration: BrokerRegistationRequest) -> Result<()> {
        debug!(?broker_registration);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let prepared = tx
            .prepare(concat!(
                "insert into cluster",
                " (name) values ($1)",
                " on conflict (name)",
                " do update set",
                " last_updated = excluded.last_updated",
                " returning id"
            ))
            .await?;

        debug!(?prepared);

        let row = tx
            .query_one(&prepared, &[&broker_registration.cluster_id])
            .await?;

        let cluster_id: i32 = row.get(0);
        debug!(?cluster_id);

        let prepared = tx
            .prepare(concat!(
                "insert into broker",
                " (cluster, node, rack, incarnation)",
                " values ($1, $2, $3, gen_random_uuid())",
                " on conflict (cluster, node)",
                " do update set",
                " incarnation = excluded.incarnation",
                ", last_updated = excluded.last_updated",
                " returning id"
            ))
            .await?;
        debug!(?prepared);

        let row = tx
            .query_one(
                &prepared,
                &[
                    &cluster_id,
                    &broker_registration.broker_id,
                    &broker_registration.rack,
                ],
            )
            .await?;

        let broker_id: i32 = row.get(0);
        debug!(?broker_id);

        let prepared = tx
            .prepare(concat!("delete from listener where broker=$1",))
            .await?;
        debug!(?prepared);

        let rows = tx.execute(&prepared, &[&broker_id]).await?;
        debug!(?rows);

        let prepared = tx
            .prepare(concat!(
                "insert into listener",
                " (broker, name, host, port)",
                " values ($1, $2, $3, $4)",
                " returning id"
            ))
            .await?;

        debug!(?prepared);

        for listener in broker_registration.listeners {
            debug!(?listener);

            let rows = tx
                .execute(
                    &prepared,
                    &[
                        &broker_id,
                        &listener.name,
                        &listener.host.as_str(),
                        &(listener.port as i32),
                    ],
                )
                .await?;

            debug!(?rows);
        }

        tx.commit().await?;

        Ok(())
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let c = self.connection().await?;

        let prepared = c
            .prepare(concat!(
                "select",
                " broker.id, host, port, rack",
                " from broker, cluster, listener",
                " where",
                " cluster.name = $1",
                " and listener.name = $2",
                " and broker.cluster = cluster.id",
                " and listener.broker = broker.id"
            ))
            .await?;

        let mut brokers = vec![];

        let rows = c
            .query(&prepared, &[&self.cluster.as_str(), &"broker"])
            .await?;

        for row in rows {
            let broker_id = row.try_get::<_, i32>(0)?;
            let host = row.try_get::<_, String>(1)?;
            let port = row.try_get::<_, i32>(2)?;
            let rack = row.try_get::<_, Option<String>>(3)?;

            brokers.push(DescribeClusterBroker {
                broker_id,
                host,
                port,
                rack,
            });
        }

        Ok(brokers)
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        debug!(?topic, ?validate_only);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let prepared = tx
            .prepare(concat!(
                "insert into topic",
                " (cluster, name, partitions, replication_factor, is_internal)",
                " select cluster.id, $2, $3, $4, false",
                " from cluster",
                " where cluster.name = $1",
                " returning topic.id",
            ))
            .await?;

        let topic_id = tx
            .query_one(
                &prepared,
                &[
                    &self.cluster.as_str(),
                    &topic.name.as_str(),
                    &topic.num_partitions,
                    &(topic.replication_factor as i32),
                ],
            )
            .await
            .map(|row| row.get(0))
            .map_err(|error| {
                if error
                    .code()
                    .is_some_and(|code| *code == SqlState::UNIQUE_VIOLATION)
                {
                    Error::Api(ErrorCode::TopicAlreadyExists)
                } else {
                    error.into()
                }
            })?;

        debug!(?topic_id);

        if let Some(configs) = topic.configs {
            let prepared = tx
                .prepare(concat!(
                    "insert into topic_configuration",
                    " (topic, name, value)",
                    " values ($1, $2, $3)"
                ))
                .await?;

            for config in configs {
                debug!(?config);

                _ = tx
                    .execute(
                        &prepared,
                        &[&topic_id, &config.name.as_str(), &config.value.as_deref()],
                    )
                    .await;
            }
        }

        tx.commit().await?;

        Ok(topic_id)
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        let c = self.connection().await?;

        let delete_records = c
            .prepare(concat!(
                "delete from record",
                " using  topic, cluster",
                " where",
                " cluster.name=$1",
                " and topic.name = $2",
                " and record.partition = $3",
                " and record.id >= $4",
                " and topic.cluster = cluster.id",
                " and record.topic = topic.id",
            ))
            .await
            .inspect_err(|err| error!(?err, ?topics))?;

        let mut responses = vec![];

        for topic in topics {
            let mut partition_responses = vec![];

            if let Some(ref partitions) = topic.partitions {
                for partition in partitions {
                    _ = c
                        .execute(
                            &delete_records,
                            &[
                                &self.cluster,
                                &topic.name,
                                &partition.partition_index,
                                &partition.offset,
                            ],
                        )
                        .await
                        .inspect_err(|err| {
                            let cluster = self.cluster.as_str();
                            let topic = topic.name.as_str();
                            let partition_index = partition.partition_index;
                            let offset = partition.offset;

                            error!(?err, ?cluster, ?topic, ?partition_index, ?offset)
                        })?;

                    let prepared = c
                        .prepare(concat!(
                            "select",
                            " id as offset",
                            " from",
                            " record",
                            " join (",
                            " select",
                            " coalesce(min(record.id), (select last_value from record_id_seq)) as offset",
                            " from record, topic, cluster",
                            " where",
                            " topic.cluster = cluster.id",
                            " and cluster.name = $1",
                            " and topic.name = $2",
                            " and record.partition = $3",
                            " and record.topic = topic.id) as minimum",
                            " on record.id = minimum.offset",
                        ))
                        .await
                        .inspect_err(|err| {
                            let cluster = self.cluster.as_str();
                            let topic = topic.name.as_str();
                            let partition_index = partition.partition_index;
                            let offset = partition.offset;

                            error!(?err, ?cluster, ?topic, ?partition_index, ?offset)
                        })?;

                    let partition_result = c
                        .query_opt(
                            &prepared,
                            &[&self.cluster, &topic.name, &partition.partition_index],
                        )
                        .await
                        .inspect_err(|err| {
                            let cluster = self.cluster.as_str();
                            let topic = topic.name.as_str();
                            let partition_index = partition.partition_index;
                            let offset = partition.offset;

                            error!(?err, ?cluster, ?topic, ?partition_index, ?offset)
                        })
                        .map_or(
                            Ok(DeleteRecordsPartitionResult {
                                partition_index: partition.partition_index,
                                low_watermark: 0,
                                error_code: ErrorCode::UnknownServerError.into(),
                            }),
                            |row| {
                                row.map_or(
                                    Ok(DeleteRecordsPartitionResult {
                                        partition_index: partition.partition_index,
                                        low_watermark: 0,
                                        error_code: ErrorCode::UnknownServerError.into(),
                                    }),
                                    |row| {
                                        row.try_get::<_, i64>(0).map(|low_watermark| {
                                            DeleteRecordsPartitionResult {
                                                partition_index: partition.partition_index,
                                                low_watermark,
                                                error_code: ErrorCode::None.into(),
                                            }
                                        })
                                    },
                                )
                            },
                        )?;

                    partition_responses.push(partition_result);
                }
            }

            responses.push(DeleteRecordsTopicResult {
                name: topic.name.clone(),
                partitions: Some(partition_responses),
            });
        }
        Ok(responses)
    }

    async fn delete_topic(&self, name: &str) -> Result<u64> {
        let c = self.connection().await?;

        let delete_topic = c.prepare("delete from topic where name=$1 cascade").await?;

        c.execute(&delete_topic, &[&name]).await.map_err(Into::into)
    }

    async fn produce(&self, topition: &'_ Topition, deflated: deflated::Batch) -> Result<i64> {
        debug!(?topition, ?deflated);
        let mut c = self.connection().await?;

        let tx = c.transaction().await?;

        let insert_record = tx
            .prepare(concat!(
                "insert into record",
                " (topic, partition, producer_id, sequence, timestamp, k, v)",
                " select",
                " topic.id, $2, $3, $4, $5, $6, $7",
                " from topic",
                " where topic.name = $1",
                " returning id"
            ))
            .await
            .inspect_err(|err| error!(?err))?;

        let insert_header = tx
            .prepare(concat!(
                "insert into header",
                " (record, k, v)",
                " values",
                " ($1, $2, $3)",
            ))
            .await
            .inspect_err(|err| error!(?err))?;

        let inflated = inflated::Batch::try_from(deflated)?;
        let mut offsets = vec![];

        for record in inflated.records {
            debug!(?record);

            let topic = topition.topic();
            let partition = topition.partition();
            let key = record.key.as_deref();
            let value = record.value.as_deref();

            let row = tx
                .query_one(
                    &insert_record,
                    &[
                        &topic,
                        &partition,
                        &inflated.producer_id,
                        &inflated.base_sequence,
                        &(to_system_time(inflated.base_timestamp + record.timestamp_delta)?),
                        &key,
                        &value,
                    ],
                )
                .await
                .inspect_err(|err| error!(?err, ?topic, ?partition, ?key, ?value))?;

            let offset: i64 = row.get(0);
            debug!(?offset);

            for header in record.headers {
                let key = header.key.as_deref();
                let value = header.value.as_deref();

                _ = tx
                    .execute(&insert_header, &[&offset, &key, &value])
                    .await
                    .inspect_err(|err| {
                        error!(?err, ?topic, ?partition, ?offset, ?key, ?value);
                    });
            }

            offsets.push(offset);
        }

        tx.commit().await?;

        Ok(offsets.first().copied().unwrap_or(-1))
    }

    async fn fetch(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
    ) -> Result<deflated::Batch> {
        debug!(?topition, ?offset);
        let c = self.connection().await?;

        let select_batch = c
            .prepare(concat!(
                "with sized as (",
                " select",
                " record.id",
                ", timestamp",
                ", k",
                ", v",
                ", sum(coalesce(length(k), 0) + coalesce(length(v), 0))",
                " over (order by record.id) as bytes",
                " from cluster, record, topic",
                " where",
                " cluster.name = $1",
                " and topic.name = $2",
                " and record.partition = $3",
                " and record.id >= $4",
                " and topic.cluster = cluster.id",
                " and record.topic = topic.id",
                ") select * from sized",
                " where bytes < $5",
            ))
            .await
            .inspect_err(|err| error!(?err))?;

        let select_headers = c
            .prepare(concat!("select k, v from header where record = $1"))
            .await
            .inspect_err(|err| error!(?err))?;

        let records = c
            .query(
                &select_batch,
                &[
                    &self.cluster,
                    &topition.topic(),
                    &topition.partition(),
                    &offset,
                    &(max_bytes as i64),
                ],
            )
            .await?;

        let mut bytes = 0;

        let batch = if let Some(first) = records.first() {
            let base_offset: i64 = first.try_get(0)?;
            debug!(?base_offset);

            let base_timestamp = first
                .try_get::<_, SystemTime>(1)
                .map_err(Error::from)
                .and_then(|system_time| to_timestamp(system_time).map_err(Into::into))?;

            let mut batch_builder = inflated::Batch::builder()
                .base_offset(base_offset)
                .base_timestamp(base_timestamp);

            for record in records.iter() {
                let offset = record.try_get::<_, i64>(0)?;
                let offset_delta = i32::try_from(offset - base_offset)?;

                let timestamp_delta = first
                    .try_get::<_, SystemTime>(1)
                    .map_err(Error::from)
                    .and_then(|system_time| {
                        to_timestamp(system_time)
                            .map(|timestamp| timestamp - base_timestamp)
                            .map_err(Into::into)
                    })?;

                let k = record
                    .try_get::<_, Option<&[u8]>>(2)
                    .map(|o| o.map(Bytes::copy_from_slice))?;

                let v = record
                    .try_get::<_, Option<&[u8]>>(3)
                    .map(|o| o.map(Bytes::copy_from_slice))?;

                bytes += record.try_get::<_, i64>(4)?;

                let mut record_builder = Record::builder()
                    .offset_delta(offset_delta)
                    .timestamp_delta(timestamp_delta)
                    .key(k.into())
                    .value(v.into());

                for header in c.query(&select_headers, &[&offset]).await? {
                    let mut header_builder = Header::builder();

                    if let Some(k) = header.try_get::<_, Option<&[u8]>>(0)? {
                        bytes += i64::try_from(k.len())?;

                        header_builder = header_builder.key(k.to_vec());
                    }

                    if let Some(v) = header.try_get::<_, Option<&[u8]>>(1)? {
                        bytes += i64::try_from(v.len())?;

                        header_builder = header_builder.value(v.to_vec());
                    }

                    record_builder = record_builder.header(header_builder);
                }

                batch_builder = batch_builder.record(record_builder);

                if bytes > (min_bytes as i64) {
                    break;
                }
            }

            batch_builder
        } else {
            inflated::Batch::builder()
        };

        batch
            .build()
            .and_then(TryInto::try_into)
            .map_err(Into::into)
    }

    async fn offset_stage(&self, topition: &'_ Topition) -> Result<OffsetStage> {
        let c = self.connection().await?;

        let prepared = c
            .prepare(concat!(
                "select",
                " coalesce(min(record.id), (select last_value from record_id_seq)) as log_start",
                ", coalesce(max(record.id), (select last_value from record_id_seq)) as high_watermark",
                " from cluster, record, topic",
                " where",
                " cluster.name = $1",
                " and topic.name = $2",
                " and record.partition = $3",
                " and topic.cluster = cluster.id",
                " and record.topic = topic.id",
            ))
            .await?;

        let row = c
            .query_one(
                &prepared,
                &[&self.cluster, &topition.topic(), &topition.partition()],
            )
            .await
            .inspect_err(|err| error!(?topition, ?prepared, ?err))?;

        let log_start = row
            .try_get::<_, i64>(0)
            .inspect_err(|err| error!(?topition, ?prepared, ?err))?;

        let high_watermark = row
            .try_get::<_, i64>(0)
            .inspect_err(|err| error!(?topition, ?prepared, ?err))?;

        let last_stable = high_watermark;

        Ok(OffsetStage {
            last_stable,
            high_watermark,
            log_start,
        })
    }

    async fn offset_commit(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let _ = group;
        let _ = retention;

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let prepared = tx
            .prepare(concat!(
                "insert into consumer_offset ",
                " (grp, topic, partition, committed_offset, leader_epoch, timestamp, metadata) ",
                " select",
                " $1, topic.id, $3, $4, $5, $6, $7 ",
                " from topic",
                " where topic.name = $2",
                " on conflict (grp, topic, partition)",
                " do update set",
                " committed_offset = excluded.committed_offset,",
                " leader_epoch = excluded.leader_epoch,",
                " timestamp = excluded.timestamp,",
                " metadata = excluded.metadata",
            ))
            .await?;

        let mut responses = vec![];

        for (topition, offset) in offsets {
            _ = tx
                .execute(
                    &prepared,
                    &[
                        &group,
                        &topition.topic(),
                        &topition.partition(),
                        &offset.offset,
                        &offset.leader_epoch,
                        &offset.timestamp,
                        &offset.metadata,
                    ],
                )
                .await?;

            responses.push((topition.to_owned(), ErrorCode::None));
        }

        tx.commit().await?;

        Ok(responses)
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        let _ = group_id;
        let _ = topics;
        let _ = require_stable;

        let c = self.connection().await?;

        let prepared = c
            .prepare(concat!(
                "select",
                " committed_offset",
                " from consumer_offset, topic",
                " where grp=$1",
                " and topic.name=$2",
                " and consumer_offset.topic = topic.id",
                " and partition=$3",
            ))
            .await?;

        let mut offsets = BTreeMap::new();

        for topic in topics {
            let offset = c
                .query_opt(&prepared, &[&group_id, &topic.topic(), &topic.partition()])
                .await?
                .map_or(Ok(-1), |row| row.try_get::<_, i64>(0))?;

            _ = offsets.insert(topic.to_owned(), offset);
        }

        Ok(offsets)
    }

    async fn list_offsets(
        &self,
        offsets: &[(Topition, ListOffsetRequest)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        debug!(?offsets);

        let c = self.connection().await?;

        let mut responses = vec![];

        for (topition, offset_type) in offsets {
            let query = match offset_type {
                ListOffsetRequest::Earliest => concat!(
                    "select",
                    " id as offset, timestamp",
                    " from",
                    " record",
                    " join (",
                    " select",
                    " coalesce(min(record.id), (select last_value from record_id_seq)) as offset",
                    " from record, topic, cluster",
                    " where",
                    " topic.cluster = cluster.id",
                    " and cluster.name = $1",
                    " and topic.name = $2",
                    " and record.partition = $3",
                    " and record.topic = topic.id) as minimum",
                    " on record.id = minimum.offset",
                ),
                ListOffsetRequest::Latest => concat!(
                    "select",
                    " id as offset, timestamp",
                    " from",
                    " record",
                    " join (",
                    " select",
                    " coalesce(max(record.id), (select last_value from record_id_seq)) as offset",
                    " from record, topic, cluster",
                    " where",
                    " topic.cluster = cluster.id",
                    " and cluster.name = $1",
                    " and topic.name = $2",
                    " and record.partition = $3",
                    " and record.topic = topic.id) as maximum",
                    " on record.id = maximum.offset",
                ),
                ListOffsetRequest::Timestamp(_) => concat!(
                    "select",
                    " id as offset, timestamp",
                    " from",
                    " record",
                    " join (",
                    " select",
                    " coalesce(min(record.id), (select last_value from record_id_seq)) as offset",
                    " from record, topic, cluster",
                    " where",
                    " topic.cluster = cluster.id",
                    " and cluster.name = $1",
                    " and topic.name = $2",
                    " and record.partition = $3",
                    " and record.timestamp >= $4",
                    " and record.topic = topic.id) as minimum",
                    " on record.id = minimum.offset",
                ),
            };

            let prepared = c.prepare(query).await.inspect_err(|err| error!(?err))?;

            let list_offset = match offset_type {
                ListOffsetRequest::Earliest | ListOffsetRequest::Latest => {
                    c.query_opt(
                        &prepared,
                        &[&self.cluster, &topition.topic(), &topition.partition()],
                    )
                    .await
                }
                ListOffsetRequest::Timestamp(timestamp) => {
                    c.query_opt(
                        &prepared,
                        &[
                            &self.cluster.as_str(),
                            &topition.topic(),
                            &topition.partition(),
                            timestamp,
                        ],
                    )
                    .await
                }
            }
            .inspect_err(|err| {
                let cluster = self.cluster.as_str();
                error!(?err, ?cluster, ?topition);
            })?
            .map_or_else(
                || {
                    let timestamp = Some(SystemTime::now());
                    let offset = Some(0);

                    Ok(ListOffsetResponse {
                        timestamp,
                        offset,
                        ..Default::default()
                    })
                },
                |row| {
                    row.try_get::<_, i64>(0).map(Some).and_then(|offset| {
                        row.try_get::<_, SystemTime>(1).map(Some).map(|timestamp| {
                            ListOffsetResponse {
                                timestamp,
                                offset,
                                ..Default::default()
                            }
                        })
                    })
                },
            )?;

            responses.push((topition.clone(), list_offset));
        }

        Ok(responses).inspect(|r| debug!(?r))
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        debug!(?topics);

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let prepared = c
            .prepare(concat!(
                "select node, host, port, rack",
                " from broker, cluster, listener",
                " where cluster.name = $1",
                " and broker.cluster = cluster.id",
                " and listener.broker = broker.id",
                " and listener.name = 'broker'"
            ))
            .await
            .inspect_err(|err| error!(?err))?;

        let rows = c
            .query(&prepared, &[&self.cluster])
            .await
            .inspect_err(|err| error!(?err))?;

        let mut brokers = vec![];

        for row in rows {
            let node_id = row.try_get::<_, i32>(0)?;
            let host = row.try_get::<_, String>(1)?;
            let port = row.try_get::<_, i32>(2)?;
            let rack = row.try_get::<_, Option<String>>(3)?;

            brokers.push(MetadataResponseBroker {
                node_id,
                host,
                port,
                rack,
            });
        }

        debug!(?brokers);

        let responses = match topics {
            Some(topics) if !topics.is_empty() => {
                let mut responses = vec![];

                for topic in topics {
                    responses.push(match topic {
                        TopicId::Name(name) => {
                            let prepared = c
                                .prepare(concat!(
                                    "select",
                                    " topic.id, topic.name, is_internal, partitions, replication_factor",
                                    " from topic, cluster",
                                    " where cluster.name = $1",
                                    " and topic.name = $2",
                                    " and topic.cluster = cluster.id",
                                ))
                                .await
                                .inspect_err(|err|{let cluster = self.cluster.as_str();error!(?err, ?cluster, ?name);})?;

                            match c
                                .query_opt(&prepared, &[&self.cluster, &name.as_str()])
                                .await
                                .inspect_err(|err|error!(?err))
                            {
                                Ok(Some(row)) => {
                                    let error_code = ErrorCode::None.into();
                                    let topic_id = row
                                        .try_get::<_, Uuid>(0)
                                        .map(|uuid| uuid.into_bytes())
                                        .map(Some)?;
                                    let name = row.try_get::<_, String>(1).map(Some)?;
                                    let is_internal = row.try_get::<_, bool>(2).map(Some)?;
                                    let partitions = row.try_get::<_, i32>(3)?;
                                    let replication_factor = row.try_get::<_, i32>(4)?;

                                    debug!(?error_code, ?topic_id, ?name, ?is_internal, ?partitions, ?replication_factor);

                                    let mut rng = thread_rng();
                                    let mut broker_ids:Vec<_> = brokers.iter().map(|broker|broker.node_id).collect();
                                    broker_ids.shuffle(&mut rng);

                                    let mut brokers = broker_ids.into_iter().cycle();

                                    let partitions = Some((0..partitions).map(|partition_index| {
                                        let leader_id = brokers.next().expect("cycling");

                                        let replica_nodes = Some((0..replication_factor).map(|_replica|brokers.next().expect("cycling")).collect());
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
                                    }).collect());


                                    MetadataResponseTopic {
                                        error_code,
                                        name,
                                        topic_id,
                                        is_internal,
                                        partitions,
                                        topic_authorized_operations: Some(-2147483648),
                                    }
                                }

                                Ok(None) => {
                                    MetadataResponseTopic {
                                        error_code: ErrorCode::UnknownTopicOrPartition.into(),
                                        name: Some(name.into()),
                                        topic_id: Some(NULL_TOPIC_ID),
                                        is_internal: Some(false),
                                        partitions: Some([].into()),
                                        topic_authorized_operations: Some(-2147483648),
                                    }
                                }

                                Err(reason) => {
                                    debug!(?reason);
                                    MetadataResponseTopic {
                                        error_code: ErrorCode::UnknownTopicOrPartition.into(),
                                        name: Some(name.into()),
                                        topic_id: Some(NULL_TOPIC_ID),
                                        is_internal: Some(false),
                                        partitions: Some([].into()),
                                        topic_authorized_operations: Some(-2147483648),
                                    }
                                }
                            }
                        }
                        TopicId::Id(id) => {
                            debug!(?id);
                            let prepared = c
                                .prepare(concat!(
                                    "select",
                                    " topic.id, topic.name, is_internal, partitions, replication_factor",
                                    " from topic, cluster",
                                    " where cluster.name = $1",
                                    " and topic.id = $2",
                                    " and topic.cluster = cluster.id",
                                ))
                                .await
                                .inspect_err(|error|error!(?error))?;

                            match c
                                .query_one(&prepared, &[&self.cluster, &id])
                                .await
                            {
                                Ok(row) => {
                                    let error_code = ErrorCode::None.into();
                                    let topic_id = row
                                        .try_get::<_, Uuid>(0)
                                        .map(|uuid| uuid.into_bytes())
                                        .map(Some)?;
                                    let name = row.try_get::<_, String>(1).map(Some)?;
                                    let is_internal = row.try_get::<_, bool>(2).map(Some)?;
                                    let partitions = row.try_get::<_, i32>(3)?;
                                    let replication_factor = row.try_get::<_, i32>(4)?;

                                    debug!(?error_code, ?topic_id, ?name, ?is_internal, ?partitions, ?replication_factor);

                                    let mut rng = thread_rng();
                                    let mut broker_ids:Vec<_> = brokers.iter().map(|broker|broker.node_id).collect();
                                    broker_ids.shuffle(&mut rng);

                                    let mut brokers = broker_ids.into_iter().cycle();

                                    let partitions = Some((0..partitions).map(|partition_index| {
                                        let leader_id = brokers.next().expect("cycling");

                                        let replica_nodes = Some((0..replication_factor).map(|_replica|brokers.next().expect("cycling")).collect());
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
                                    }).collect());


                                    MetadataResponseTopic {
                                        error_code,
                                        name,
                                        topic_id,
                                        is_internal,
                                        partitions,
                                        topic_authorized_operations: Some(-2147483648),
                                    }
                                }
                                Err(reason) => {
                                    debug!(?reason);
                                    MetadataResponseTopic {
                                        error_code: ErrorCode::UnknownTopicOrPartition.into(),
                                        name: None,
                                        topic_id: Some(id.into_bytes()),
                                        is_internal: Some(false),
                                        partitions: Some([].into()),
                                        topic_authorized_operations: Some(-2147483648),
                                    }
                                }
                            }
                        }
                    });
                }

                responses
            }

            _ => {
                let mut responses = vec![];

                let prepared = c
                    .prepare(concat!(
                        "select",
                        " topic.id, topic.name, is_internal, partitions, replication_factor",
                        " from topic, cluster",
                        " where cluster.name = $1",
                        " and topic.cluster = cluster.id",
                    ))
                    .await
                    .inspect_err(|err| error!(?err))?;

                match c
                    .query(&prepared, &[&self.cluster])
                    .await
                    .inspect_err(|err| error!(?err))
                {
                    Ok(rows) => {
                        for row in rows {
                            let error_code = ErrorCode::None.into();
                            let topic_id = row
                                .try_get::<_, Uuid>(0)
                                .map(|uuid| uuid.into_bytes())
                                .map(Some)?;
                            let name = row.try_get::<_, String>(1).map(Some)?;
                            let is_internal = row.try_get::<_, bool>(2).map(Some)?;
                            let partitions = row.try_get::<_, i32>(3)?;
                            let replication_factor = row.try_get::<_, i32>(4)?;

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
                    }
                    Err(reason) => {
                        debug!(?reason);
                        responses.push(MetadataResponseTopic {
                            error_code: ErrorCode::UnknownTopicOrPartition.into(),
                            name: None,
                            topic_id: Some(NULL_TOPIC_ID),
                            is_internal: Some(false),
                            partitions: Some([].into()),
                            topic_authorized_operations: Some(-2147483648),
                        });
                    }
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

    async fn upsert_user_scram_credential(
        &self,
        username: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let c = self.connection().await?;

        let prepared = c
            .prepare(concat!(
                "insert into scram_credential ",
                " (username, mechanism, salt, iterations, stored_key, server_key) ",
                " values",
                " ($1, $2, $3, $4, $5, $6)",
                " on conflict (username, mechanism)",
                " do update set",
                " salt = excluded.salt",
                ", iterations = excluded.iterations",
                ", stored_key = excluded.stored_key",
                ", server_key = excluded.server_key",
                ", last_updated = excluded.last_updated",
            ))
            .await
            .inspect_err(|err| error!(?err, ?username, ?mechanism,))?;

        _ = c
            .execute(
                &prepared,
                &[
                    &username,
                    &i32::from(mechanism),
                    &&credential.salt()[..],
                    &credential.iterations,
                    &&credential.stored_key()[..],
                    &&credential.server_key()[..],
                ],
            )
            .await
            .inspect_err(|err| error!(?err, ?username, ?mechanism,))?;

        Ok(())
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        let c = self.connection().await?;

        let prepared = c
            .prepare(concat!(
                "select from scram_credential",
                " salt, iterations, stored_key, server_key",
                " where",
                " username = $1",
                ", and mechanism = $2",
            ))
            .await
            .inspect_err(|err| error!(?err, ?user, ?mechanism,))?;

        c.query_opt(&prepared, &[&user, &i32::from(mechanism)])
            .await
            .map_err(Into::into)
            .and_then(|maybe| {
                if let Some(row) = maybe {
                    let salt = row.try_get::<_, &[u8]>(0).map(Bytes::copy_from_slice)?;
                    let iterations = row.try_get::<_, i32>(1)?;
                    let stored_key = row.try_get::<_, &[u8]>(2).map(Bytes::copy_from_slice)?;
                    let server_key = row.try_get::<_, &[u8]>(3).map(Bytes::copy_from_slice)?;

                    Ok(Some(ScramCredential::new(
                        salt, iterations, stored_key, server_key,
                    )))
                } else {
                    Ok(None)
                }
            })
            .inspect_err(|err| error!(?err, ?user, ?mechanism,))
    }
}

#[cfg(test)]
mod tests {

    // use deadpool_postgres::{ManagerConfig, Pool};
    // use tansu_kafka_sans_io::record::Record;
    // use tokio_postgres::NoTls;
    // use url::Url;

    // use super::*;

    // #[test]
    // fn earl() -> Result<()> {
    //     let q = Url::parse("postgresql://")?;
    //     assert_eq!("localhost", q.host_str().unwrap());

    //     Ok(())
    // }

    // #[tokio::test]
    // async fn topic_id() -> Result<()> {
    //     let pg_config =
    //         tokio_postgres::Config::from_str("host=localhost user=postgres password=postgres")?;

    //     let mgr_config = ManagerConfig {
    //         recycling_method: deadpool_postgres::RecyclingMethod::Fast,
    //     };

    //     let mgr = deadpool_postgres::Manager::from_config(pg_config, NoTls, mgr_config);
    //     let pool = Pool::builder(mgr).max_size(16).build().unwrap();

    //     let mut storage = Postgres { pool };

    //     let topic = "abc";
    //     let partition = 3;

    //     _ = storage.create_topic(topic, partition, &[]).await?;

    //     let def = &b"def"[..];

    //     let deflated: deflated::Batch = inflated::Batch::builder()
    //         .record(Record::builder().value(def.into()))
    //         .build()
    //         .and_then(TryInto::try_into)?;

    //     let topition = Topition::new(topic, partition);

    //     _ = storage.produce(&topition, deflated).await?;

    //     let offsets = [(
    //         Topition::new(topic, 1),
    //         OffsetCommitRequest {
    //             offset: 12321,
    //             leader_epoch: None,
    //             timestamp: None,
    //             metadata: None,
    //         },
    //     )];

    //     let group: &str = "some_group";
    //     let retention = None;

    //     storage
    //         .offset_commit(group, retention, &offsets[..])
    //         .await?;

    //     let offsets = [(
    //         Topition::new(topic, 1),
    //         OffsetCommitRequest {
    //             offset: 32123,
    //             leader_epoch: None,
    //             timestamp: None,
    //             metadata: None,
    //         },
    //     )];

    //     storage
    //         .offset_commit(group, retention, &offsets[..])
    //         .await?;

    //     Ok(())
    // }
}
