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
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::{deflated, inflated, Record},
    to_system_time, to_timestamp, ErrorCode,
};
use tokio_postgres::{error::SqlState, Config, NoTls};
use tracing::debug;
use uuid::Uuid;

use crate::{
    BrokerRegistationRequest, Error, ListOffsetRequest, ListOffsetResponse, MetadataResponse,
    OffsetCommitRequest, Result, Storage, TopicId, Topition,
};

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

    async fn delete_topic(&self, name: &str) -> Result<u64> {
        let c = self.connection().await?;

        let delete_topic = c.prepare("delete from topic where name=$1 cascade").await?;

        c.execute(&delete_topic, &[&name]).await.map_err(Into::into)
    }

    async fn produce(&self, topition: &'_ Topition, deflated: deflated::Batch) -> Result<i64> {
        debug!(?topition, ?deflated);
        let mut c = self.connection().await?;

        let tx = c.transaction().await?;

        let prepared = tx
            .prepare(concat!(
                "insert into record",
                " (topic, partition, producer_id, sequence, timestamp, k, v)",
                " select",
                " topic.id, $2, $3, $4, $5, $6, $7",
                " from topic",
                " where topic.name = $1",
                " returning id"
            ))
            .await?;

        let inflated = inflated::Batch::try_from(deflated)?;
        let mut offsets = vec![];

        for record in inflated.records {
            debug!(?record);

            let row = tx
                .query_one(
                    &prepared,
                    &[
                        &topition.topic(),
                        &topition.partition(),
                        &inflated.producer_id,
                        &inflated.base_sequence,
                        &(to_system_time(inflated.base_timestamp + record.timestamp_delta)?),
                        &record.key.as_deref(),
                        &record.value.as_deref(),
                    ],
                )
                .await?;

            let offset: i64 = row.get(0);
            debug!(?offset);
            offsets.push(offset);
        }

        tx.commit().await?;

        Ok(offsets.first().copied().unwrap_or(-1))
    }

    async fn fetch(&self, topition: &'_ Topition, offset: i64) -> Result<deflated::Batch> {
        let c = self.connection().await?;

        let prepared = c
            .prepare(concat!(
                "select record.id, timestamp, k, v from record, topic",
                " where",
                " record.topic = topic.id",
                " and",
                " topic.name = $1",
                " and",
                " record.partition = $2",
                " and",
                " record.offset >= $3"
            ))
            .await?;

        let rows = c
            .query(
                &prepared,
                &[&topition.topic(), &topition.partition(), &offset],
            )
            .await?;

        let builder = if let Some(first) = rows.first() {
            let base_offset: i64 = first.try_get(0)?;

            let base_timestamp = first
                .try_get::<_, SystemTime>(1)
                .map_err(Error::from)
                .and_then(|system_time| to_timestamp(system_time).map_err(Into::into))?;

            let mut builder = inflated::Batch::builder()
                .base_offset(base_offset)
                .base_timestamp(base_timestamp);

            for record in rows.iter() {
                let offset_delta = record
                    .try_get::<_, i64>(0)
                    .map_err(Error::from)
                    .map(|offset| offset - base_offset)
                    .and_then(|delta| i32::try_from(delta).map_err(Into::into))?;

                let timestamp_delta = first
                    .try_get::<_, SystemTime>(1)
                    .map_err(Error::from)
                    .and_then(|system_time| {
                        to_timestamp(system_time)
                            .map(|timestamp| timestamp - base_timestamp)
                            .map_err(Into::into)
                    })?;

                let k = record
                    .get::<_, Option<&[u8]>>(2)
                    .map(Bytes::copy_from_slice);

                let v = record
                    .get::<_, Option<&[u8]>>(3)
                    .map(Bytes::copy_from_slice);

                builder = builder.record(
                    Record::builder()
                        .offset_delta(offset_delta)
                        .timestamp_delta(timestamp_delta)
                        .key(k.into())
                        .value(v.into()),
                )
            }

            builder
        } else {
            inflated::Batch::builder()
        };

        builder
            .build()
            .and_then(TryInto::try_into)
            .map_err(Into::into)
    }

    async fn last_stable_offset(&self, topition: &'_ Topition) -> Result<i64> {
        self.high_watermark(topition).await
    }

    async fn high_watermark(&self, topition: &'_ Topition) -> Result<i64> {
        let _ = topition;
        let c = self.connection().await?;

        let prepared = c
            .prepare(concat!(
                "select max(record.id) from record, topic",
                " where topic.name = $1",
                " and topic.id = record.topic",
                " and record.partition = $2"
            ))
            .await?;

        let rows = c
            .query(&prepared, &[&topition.topic(), &topition.partition()])
            .await?;

        if let Some(row) = rows.first() {
            row.try_get::<_, i64>(0).map_err(Into::into)
        } else {
            Ok(-1)
        }
    }

    async fn offset_commit(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<()> {
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
                .await?
        }

        tx.commit().await?;

        Ok(())
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
        todo!()
    }

    async fn list_offsets(
        &self,
        offsets: &[(Topition, ListOffsetRequest)],
    ) -> Result<&[(Topition, ListOffsetResponse)]> {
        let _ = offsets;

        todo!()
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        debug!(?topics);

        let c = self.connection().await?;

        let prepared = c
            .prepare(concat!(
                "select node, host, port, rack",
                " from broker, cluster, listener",
                " where cluster.name = $1",
                " and broker.cluster = cluster.id",
                " and listener.broker = broker.id",
                " and listener.name = 'broker'"
            ))
            .await?;

        let rows = c.query(&prepared, &[&self.cluster]).await?;

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

        let responses = if let Some(topics) = topics {
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
                            .await?;

                        match c
                            .query_one(&prepared, &[&self.cluster, &name.as_str()])
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
                                                offline_replicas: Some([].into()) }



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
                                    name: Some(name.into()),
                                    topic_id: None,
                                    is_internal: None,
                                    partitions: None,
                                    topic_authorized_operations: None,
                                }
                            }
                        }
                    }
                    TopicId::Id(id) => {
                        debug!(?id);
                        MetadataResponseTopic {
                            error_code: ErrorCode::UnknownTopicOrPartition.into(),
                            name: None,
                            topic_id: None,
                            is_internal: None,
                            partitions: None,
                            topic_authorized_operations: None,
                        }
                    }
                });
            }

            responses
        } else {
            let mut responses = vec![];

            let prepared = c
                .prepare(concat!(
                    "select",
                    " topic.id, topic.name, is_internal, partitions, replication_factor",
                    " from topic, cluster",
                    " where cluster.name = $1",
                    " and topic.cluster = cluster.id",
                ))
                .await?;

            match c.query(&prepared, &[&self.cluster]).await {
                Ok(rows) => {
                    for row in rows {
                        let error_code = ErrorCode::None.into();
                        let topic_id = row
                            .try_get::<_, Uuid>(0)
                            .map(|uuid| uuid.into_bytes())
                            .map(Some)?;
                        let name = row.try_get::<_, String>(1).map(Some)?;
                        let is_internal = row.try_get::<_, bool>(2).map(Some)?;
                        let _partitions = row.try_get::<_, i32>(3)?;

                        responses.push(MetadataResponseTopic {
                            error_code,
                            name,
                            topic_id,
                            is_internal,
                            partitions: Some([].into()),
                            topic_authorized_operations: Some(-2147483648),
                        });
                    }
                }
                Err(reason) => {
                    debug!(?reason);
                    responses.push(MetadataResponseTopic {
                        error_code: ErrorCode::UnknownTopicOrPartition.into(),
                        name: None,
                        topic_id: None,
                        is_internal: None,
                        partitions: None,
                        topic_authorized_operations: None,
                    });
                }
            }

            responses
        };

        Ok(MetadataResponse {
            cluster: Some(self.cluster.clone()),
            controller: Some(self.node),
            brokers,
            topics: responses,
        })
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
