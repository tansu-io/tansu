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
    str::FromStr,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use deadpool_postgres::{Manager, ManagerConfig, Object, Pool, RecyclingMethod};
use tansu_kafka_sans_io::{
    record::{deflated, inflated, Record},
    to_system_time, to_timestamp,
};
use tokio_postgres::{Config, NoTls};
use tracing::debug;
use uuid::Uuid;

use crate::{
    Error, ListOffsetRequest, ListOffsetResponse, OffsetCommitRequest, Result, Storage, Topition,
};

#[derive(Clone, Debug)]
pub struct Postgres {
    pub pool: Pool,
}

impl FromStr for Postgres {
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
            .map(|pool| Self { pool })
            .map_err(Into::into)
    }
}

impl Postgres {
    async fn connection(&self) -> Result<Object> {
        self.pool.get().await.map_err(Into::into)
    }
}

#[async_trait]
impl Storage for Postgres {
    async fn create_topic(
        &self,
        name: &str,
        partitions: i32,
        config: &[(&str, Option<&str>)],
    ) -> Result<Uuid> {
        debug!(?name, ?partitions, ?config);

        let _ = config;

        let c = self.connection().await?;

        let insert_topic = c
            .prepare("insert into topic (name, partitions) values ($1, $2) returning id")
            .await?;

        c.query_one(&insert_topic, &[&name, &partitions])
            .await
            .map(|row| row.get(0))
            .map_err(Into::into)
    }

    async fn delete_topic(&self, name: &str) -> Result<u64> {
        let c = self.connection().await?;

        let delete_topic = c.prepare("delete from topic where name=$1 cascade").await?;

        c.execute(&delete_topic, &[&name]).await.map_err(Into::into)
    }

    async fn produce(&self, topition: &'_ Topition, deflated: deflated::Batch) -> Result<i64> {
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
