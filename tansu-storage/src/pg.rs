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
use serde_json::Value;
use tansu_kafka_sans_io::{
    create_topics_request::CreatableTopic,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::{deflated, inflated, Header, Record},
    to_system_time, to_timestamp, ConfigResource, ConfigSource, ConfigType, ErrorCode,
};
use tokio_postgres::{error::SqlState, Config, NoTls, Transaction};
use tracing::{debug, error};
use uuid::Uuid;

use crate::{
    BrokerRegistationRequest, Error, GroupDetail, ListOffsetRequest, ListOffsetResponse,
    MetadataResponse, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result, Storage,
    TopicId, Topition, UpdateError, Version, NULL_TOPIC_ID,
};

const DELETE_CONSUMER_OFFSETS_FOR_TOPIC: &str = concat!(
    "delete from consumer_offset",
    " using",
    " cluster",
    ", topic",
    " where",
    " consumer_offset.topic = topic.id",
    " and",
    " topic.cluster = cluster.id",
    " and",
    " cluster.name = $1",
    " and",
    " topic.<COLUMN> = $2"
);

const DELETE_HEADERS_FOR_TOPIC: &str = concat!(
    "delete from header",
    " using",
    " cluster",
    ", record",
    ", topic",
    " where",
    " header.record = record.id",
    " and",
    " record.topic = topic.id",
    " and",
    " topic.cluster = cluster.id",
    " and",
    " cluster.name = $1",
    " and",
    " topic.<COLUMN> = $2"
);

const DELETE_RECORDS_FOR_TOPIC: &str = concat!(
    "delete from record",
    " using",
    " cluster",
    ", topic",
    " where",
    " record.topic = topic.id",
    " and",
    " topic.cluster = cluster.id",
    " and",
    " cluster.name = $1",
    " and",
    " topic.<COLUMN> = $2"
);

const DELETE_TOPIC: &str = concat!(
    "delete from topic",
    " using",
    " cluster",
    " where",
    " topic.cluster = cluster.id",
    " and",
    " cluster.name = $1",
    " and",
    " topic.<COLUMN> = $2"
);

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

    async fn delete_for_topic(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        topic: &TopicId,
    ) -> Result<u64> {
        match topic {
            TopicId::Id(id) => {
                let sql = sql.replace("<COLUMN>", "id");

                let prepared = tx.prepare(&sql).await.inspect_err(|err| error!(?err))?;

                tx.execute(&prepared, &[&self.cluster, &id])
                    .await
                    .inspect_err(|err| error!(?err))
                    .map_err(Into::into)
            }
            TopicId::Name(name) => {
                let sql = sql.replace("<COLUMN>", "name");

                let prepared = tx.prepare(&sql).await.inspect_err(|err| error!(?err))?;

                tx.execute(&prepared, &[&self.cluster, &name])
                    .await
                    .inspect_err(|err| error!(?err))
                    .map_err(Into::into)
            }
        }
    }
}

#[async_trait]
impl Storage for Postgres {
    async fn register_broker(
        &mut self,
        broker_registration: BrokerRegistationRequest,
    ) -> Result<()> {
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

    async fn brokers(&mut self) -> Result<Vec<DescribeClusterBroker>> {
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

    async fn create_topic(&mut self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
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
            .await
            .inspect_err(|err| error!(?err, ?topic, ?validate_only))?;

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
            .inspect_err(|err| error!(?err, ?topic, ?validate_only))
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
                    .await
                    .inspect_err(|err| error!(?err, ?config));
            }
        }

        tx.commit().await.inspect_err(|err| error!(?err))?;

        Ok(topic_id)
    }

    async fn delete_records(
        &mut self,
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

    async fn delete_topic(&mut self, topic: &TopicId) -> Result<ErrorCode> {
        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        for (description, sql) in [
            ("consumer offsets", DELETE_CONSUMER_OFFSETS_FOR_TOPIC),
            ("headers", DELETE_HEADERS_FOR_TOPIC),
            ("records", DELETE_RECORDS_FOR_TOPIC),
        ] {
            let rows = self.delete_for_topic(&tx, sql, topic).await?;
            debug!(?topic, ?rows, ?description);
        }

        let topic_deletion_result =
            self.delete_for_topic(&tx, DELETE_TOPIC, topic)
                .await
                .map(|rows| match rows {
                    0 => ErrorCode::UnknownTopicOrPartition,
                    1 => ErrorCode::None,
                    otherwise => {
                        error!(?otherwise, ?DELETE_TOPIC, ?topic, ?self.cluster);
                        ErrorCode::None
                    }
                });

        tx.commit()
            .await
            .inspect_err(|err| error!(?err))
            .map_err(Into::into)
            .and(topic_deletion_result)
    }

    async fn produce(&mut self, topition: &'_ Topition, deflated: deflated::Batch) -> Result<i64> {
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

        let inflated = inflated::Batch::try_from(deflated).inspect_err(|err| error!(?err))?;
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
        &mut self,
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

    async fn offset_stage(&mut self, topition: &'_ Topition) -> Result<OffsetStage> {
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
        &mut self,
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
        &mut self,
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
        &mut self,
        offsets: &[(Topition, ListOffsetRequest)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        debug!(?offsets);

        let c = self.connection().await?;

        let mut responses = vec![];

        for (topition, offset_type) in offsets {
            let query = match offset_type {
                ListOffsetRequest::Earliest => include_str!("pg/list_earliest_offset.sql"),
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

            debug!(?query);

            let prepared = c
                .prepare(query)
                .await
                .inspect_err(|err| error!(?err))
                .inspect(|prepared| debug!(?prepared))?;

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
            })
            .inspect(|result| debug!(?result))?
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
                    debug!(?row);

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

    async fn metadata(&mut self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
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

    async fn describe_config(
        &mut self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        debug!(?name, ?resource, ?keys);

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let prepared = c
            .prepare(concat!(
                "select topic.id",
                " from cluster, topic",
                " where cluster.name = $1",
                " and topic.name = $2",
                " and topic.cluster = cluster.id",
            ))
            .await
            .inspect_err(|err| error!(?err))?;

        if let Some(row) = c
            .query_opt(&prepared, &[&self.cluster.as_str(), &name])
            .await
            .inspect_err(|err| error!(?err))?
        {
            let id = row.try_get::<_, Uuid>(0)?;

            let prepared = c
                .prepare(concat!(
                    "select name, value",
                    " from topic_configuration",
                    " where topic_configuration.topic = $1",
                ))
                .await
                .inspect_err(|err| error!(?err))?;

            let rows = c
                .query(&prepared, &[&id])
                .await
                .inspect_err(|err| error!(?err))?;

            let mut configs = vec![];

            for row in rows {
                let name = row.try_get::<_, String>(0)?;
                let value = row
                    .try_get::<_, Option<String>>(1)
                    .map(|value| value.unwrap_or_default())
                    .map(Some)?;

                configs.push(DescribeConfigsResourceResult {
                    name,
                    value,
                    read_only: false,
                    is_default: Some(false),
                    config_source: Some(ConfigSource::DefaultConfig.into()),
                    is_sensitive: false,
                    synonyms: Some([].into()),
                    config_type: Some(ConfigType::String.into()),
                    documentation: Some("".into()),
                });
            }

            let error_code = ErrorCode::None;

            Ok(DescribeConfigsResult {
                error_code: error_code.into(),
                error_message: Some(error_code.to_string()),
                resource_type: i8::from(resource),
                resource_name: name.into(),
                configs: Some(configs),
            })
        } else {
            let error_code = ErrorCode::UnknownTopicOrPartition;

            Ok(DescribeConfigsResult {
                error_code: error_code.into(),
                error_message: Some(error_code.to_string()),
                resource_type: i8::from(resource),
                resource_name: name.into(),
                configs: Some([].into()),
            })
        }
    }

    async fn update_group(
        &mut self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let prepared = tx
            .prepare(concat!(
                "insert into consumer_group",
                " (grp, cluster, e_tag, detail)",
                " select $1, cluster.id, $3, $4",
                " from cluster",
                " where cluster.name = $2",
                " on conflict (grp, cluster)",
                " do update set",
                " detail = excluded.detail, e_tag = $5",
                " where consumer_group.e_tag = $3",
                " returning grp, cluster, e_tag, detail",
            ))
            .await
            .inspect_err(|err| error!(?err))?;

        let existing_e_tag = version
            .as_ref()
            .map_or(Ok(Uuid::new_v4()), |version| {
                version
                    .e_tag
                    .as_ref()
                    .map_or(Err(UpdateError::MissingEtag::<GroupDetail>), |e_tag| {
                        Uuid::from_str(e_tag.as_str()).map_err(Into::into)
                    })
            })
            .inspect_err(|err| error!(?err))?;

        let new_e_tag = Uuid::new_v4();

        let value = serde_json::to_value(detail)?;

        let outcome = if let Some(row) = tx
            .query_opt(
                &prepared,
                &[
                    &group_id,
                    &self.cluster.as_str(),
                    &existing_e_tag,
                    &value,
                    &new_e_tag,
                ],
            )
            .await
            .inspect(|row| debug!(?row))
            .inspect_err(|err| error!(?err))?
        {
            row.try_get::<_, Uuid>(2)
                .inspect_err(|err| error!(?err))
                .map_err(Into::into)
                .map(|uuid| uuid.to_string())
                .map(Some)
                .map(|e_tag| Version {
                    e_tag,
                    version: None,
                })
        } else {
            let prepared = tx
                .prepare(concat!(
                    "select",
                    " cg.e_tag, cg.detail",
                    " from cluster c, consumer_group cg",
                    " where",
                    " cg.grp = $1",
                    " and c.name = $2",
                    " and c.id = cg.cluster"
                ))
                .await
                .inspect_err(|err| error!(?err))?;

            let row = tx
                .query_one(&prepared, &[&group_id, &self.cluster.as_str()])
                .await
                .inspect(|row| debug!(?row))
                .inspect_err(|err| error!(?err))?;

            let version = row
                .try_get::<_, Uuid>(0)
                .inspect_err(|err| error!(?err))
                .map(|uuid| uuid.to_string())
                .map(Some)
                .map(|e_tag| Version {
                    e_tag,
                    version: None,
                })?;

            let value = row.try_get::<_, Value>(1)?;
            let current = serde_json::from_value::<GroupDetail>(value)?;

            Err(UpdateError::Outdated { current, version })
        };

        tx.commit().await.inspect_err(|err| error!(?err))?;

        outcome
    }

    async fn init_producer(
        &mut self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        if let Some(_transaction_id) = transaction_id {
            Ok(ProducerIdResponse::default())
        } else if producer_id.is_some_and(|producer_id| producer_id == -1)
            && producer_epoch.is_some_and(|producer_epoch| producer_epoch == -1)
        {
            let c = self.connection().await.inspect_err(|err| error!(?err))?;

            let prepared = c
                .prepare(concat!(
                    "insert into producer",
                    " (transaction_id",
                    ", transaction_timeout_ms)",
                    " values ($1, $2)",
                    " returning id, epoch"
                ))
                .await
                .inspect_err(|err| error!(?err))?;

            debug!(?prepared);

            let row = c
                .query_one(&prepared, &[&transaction_id, &transaction_timeout_ms])
                .await
                .inspect_err(|err| error!(?err))?;

            let id = row.get(0);
            let epoch: i32 = row.get(1);

            i16::try_from(epoch)
                .map(|epoch| ProducerIdResponse {
                    error: ErrorCode::None,
                    id,
                    epoch,
                })
                .map_err(Into::into)
        } else {
            Ok(ProducerIdResponse::default())
        }
    }
}
