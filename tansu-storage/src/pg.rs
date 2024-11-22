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
    cmp::Ordering,
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
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
    create_topics_request::CreatableTopic,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::{deflated, inflated, Header, Record},
    to_system_time, to_timestamp,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
    BatchAttribute, ConfigResource, ConfigSource, ConfigType, ControlBatch, EndTransactionMarker,
    ErrorCode, IsolationLevel,
};
use tokio_postgres::{error::SqlState, Config, NoTls, Row, Transaction};
use tracing::{debug, error};
use uuid::Uuid;

use crate::{
    BrokerRegistationRequest, Error, GroupDetail, ListOffsetRequest, ListOffsetResponse,
    MetadataResponse, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result, Storage,
    TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest,
    TxnState, UpdateError, Version, NULL_TOPIC_ID,
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

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Txn {
    name: String,
    producer_id: i64,
    producer_epoch: i16,
    status: TxnState,
}

impl TryFrom<Row> for Txn {
    type Error = Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let name = row
            .try_get::<_, String>(0)
            .inspect_err(|err| error!(?err))?;
        let producer_id = row.try_get::<_, i64>(1).inspect_err(|err| error!(?err))?;
        let producer_epoch = row.try_get::<_, i16>(2).inspect_err(|err| error!(?err))?;
        let status = row
            .try_get::<_, Option<String>>(3)
            .map_err(Into::into)
            .and_then(|status| status.map_or(Ok(TxnState::Begin), TxnState::try_from))
            .inspect_err(|err| error!(?err))?;

        Ok(Self {
            name,
            producer_id,
            producer_epoch,
            status,
        })
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

    fn idempotent_sequence_check(
        producer_epoch: &i16,
        sequence: &i32,
        deflated: &deflated::Batch,
    ) -> Result<i32> {
        debug!(?producer_epoch, ?sequence, ?deflated);

        match producer_epoch.cmp(&deflated.producer_epoch) {
            Ordering::Equal => match sequence.cmp(&deflated.base_sequence) {
                Ordering::Equal => Ok(deflated.last_offset_delta + 1),

                Ordering::Greater => {
                    debug!(?sequence, ?deflated.base_sequence);
                    Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
                }

                Ordering::Less => {
                    debug!(?sequence, ?deflated.base_sequence);
                    Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
                }
            },

            Ordering::Greater => Err(Error::Api(ErrorCode::ProducerFenced)),

            Ordering::Less => Err(Error::Api(ErrorCode::InvalidProducerEpoch)),
        }
    }

    async fn idempotent_message_check(
        &mut self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: &deflated::Batch,
        tx: &Transaction<'_>,
    ) -> Result<()> {
        debug!(transaction_id, ?deflated);
        let prepared = tx
            .prepare(include_str!("pg/producer_epoch_current_for_producer.sql"))
            .await
            .inspect_err(|err| error!(?err))?;

        if let Some(row) = tx
            .query_opt(&prepared, &[&self.cluster, &deflated.producer_id])
            .await
            .inspect_err(|err| error!(?err))?
        {
            let current_epoch = row
                .try_get::<_, i16>(0)
                .inspect_err(|err| error!(self.cluster, deflated.producer_id, ?err))?;

            let prepared = tx
                .prepare(include_str!("pg/producer_select_for_update.sql"))
                .await
                .inspect_err(|err| error!(?err))?;

            let row = tx
                .query_one(
                    &prepared,
                    &[
                        &self.cluster,
                        &topition.topic(),
                        &topition.partition(),
                        &deflated.producer_id,
                        &deflated.producer_epoch,
                    ],
                )
                .await
                .inspect_err(|err| {
                    error!(
                        self.cluster,
                        ?topition,
                        deflated.producer_id,
                        deflated.producer_epoch,
                        ?err
                    )
                })?;

            let sequence = row.try_get::<_, i32>(0).inspect_err(|err| error!(?err))?;

            debug!(
                self.cluster,
                ?topition,
                deflated.producer_id,
                deflated.producer_epoch,
                current_epoch,
                sequence,
            );

            let increment = Self::idempotent_sequence_check(&current_epoch, &sequence, deflated)?;

            debug!(increment);

            let prepared = tx
                .prepare(include_str!("pg/producer_detail_insert.sql"))
                .await
                .inspect_err(|err| error!(?err))?;

            assert_eq!(
                1,
                tx.execute(
                    &prepared,
                    &[
                        &self.cluster,
                        &topition.topic(),
                        &topition.partition(),
                        &deflated.producer_id,
                        &deflated.producer_epoch,
                        &increment,
                    ],
                )
                .await
                .inspect_err(|err| error!(?err))?
            );

            Ok(())
        } else {
            Err(Error::Api(ErrorCode::UnknownProducerId))
        }
    }

    async fn watermark_select_for_update(
        &mut self,
        topition: &Topition,
        tx: &Transaction<'_>,
    ) -> Result<(Option<i64>, Option<i64>, Option<i64>)> {
        let prepared = tx
            .prepare(include_str!("pg/watermark_select_for_update.sql"))
            .await
            .inspect_err(|err| error!(?err))?;

        if let Some(row) = tx
            .query_opt(
                &prepared,
                &[&self.cluster, &topition.topic(), &topition.partition()],
            )
            .await
            .inspect_err(|err| error!(?err, cluster = ?self.cluster, ?topition))?
        {
            Ok((
                row.try_get::<_, Option<i64>>(0)
                    .inspect_err(|err| error!(?err))?,
                row.try_get::<_, Option<i64>>(1)
                    .inspect_err(|err| error!(?err))?,
                row.try_get::<_, Option<i64>>(2)
                    .inspect_err(|err| error!(?err))?,
            ))
        } else {
            Err(Error::Api(ErrorCode::UnknownTopicOrPartition))
        }
    }

    async fn produce_in_tx(
        &mut self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        tx: &Transaction<'_>,
    ) -> Result<i64> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?topition, ?deflated);

        let topic = topition.topic();
        let partition = topition.partition();

        if deflated.is_idempotent() {
            self.idempotent_message_check(transaction_id, topition, &deflated, tx)
                .await
                .inspect_err(|err| error!(?err))?;
        }

        let prepared = tx
            .prepare(include_str!("pg/txn_topition_select_txns.sql"))
            .await
            .inspect_err(|err| error!(?err))?;

        let topition_is_in_a_txn = tx
            .query_one(&prepared, &[&self.cluster, &topic, &partition])
            .await
            .and_then(|row| row.try_get::<_, i64>(0))
            .inspect_err(|err| error!(?err))
            .inspect(|in_progress_transactions| debug!(?in_progress_transactions))
            .is_ok_and(|in_progress_transactions| in_progress_transactions > 0);
        debug!(?topition_is_in_a_txn);

        let (low, high, stable) = self.watermark_select_for_update(topition, tx).await?;

        debug!(?low, ?high, ?stable);

        let insert_record = tx
            .prepare(include_str!("pg/record_insert.sql"))
            .await
            .inspect_err(|err| error!(?err))?;

        let insert_header = tx
            .prepare(include_str!("pg/header_insert.sql"))
            .await
            .inspect_err(|err| error!(?err))?;

        let inflated = inflated::Batch::try_from(deflated).inspect_err(|err| error!(?err))?;

        let attributes = BatchAttribute::try_from(inflated.attributes)?;

        let records_len = i64::try_from(inflated.records.len())?;

        for (delta, record) in inflated.records.iter().enumerate() {
            let delta = i64::try_from(delta)?;
            let offset = high.map_or(delta, |high| high + delta + 1);
            let key = record.key.as_deref();
            let value = record.value.as_deref();

            debug!(?delta, ?record, ?offset);

            _ = tx
                .execute(
                    &insert_record,
                    &[
                        &self.cluster,
                        &topic,
                        &partition,
                        &offset,
                        &if transaction_id.is_none() {
                            None
                        } else {
                            Some(inflated.producer_id)
                        },
                        &if transaction_id.is_none() {
                            None
                        } else {
                            Some(inflated.producer_epoch)
                        },
                        &(to_system_time(inflated.base_timestamp + record.timestamp_delta)?),
                        &key,
                        &value,
                    ],
                )
                .await
                .inspect(|n| debug!(?n))
                .inspect_err(|err| error!(?err, ?topic, ?partition, ?offset, ?key, ?value))?;

            for header in record.headers.iter().as_ref() {
                let key = header.key.as_deref();
                let value = header.value.as_deref();

                _ = tx
                    .execute(
                        &insert_header,
                        &[&self.cluster, &topic, &partition, &offset, &key, &value],
                    )
                    .await
                    .inspect_err(|err| {
                        error!(?err, ?topic, ?partition, ?offset, ?key, ?value);
                    });
            }
        }

        if let Some(transaction_id) = transaction_id {
            if attributes.transaction {
                let txn_produce_offset_insert = tx
                    .prepare(include_str!("pg/txn_produce_offset_insert.sql"))
                    .await
                    .inspect_err(|err| error!(?err))?;

                let offset_start = high.unwrap_or_default();
                let offset_end = high.map_or(records_len - 1, |high| high + records_len);

                _ = tx
                    .execute(
                        &txn_produce_offset_insert,
                        &[
                            &self.cluster,
                            &transaction_id,
                            &inflated.producer_id,
                            &inflated.producer_epoch,
                            &topic,
                            &partition,
                            &offset_start,
                            &offset_end,
                        ],
                    )
                    .await
                    .inspect(|n| debug!(cluster = ?self.cluster, ?transaction_id, ?inflated.producer_id, ?inflated.producer_epoch, ?topic, ?partition, ?offset_start, ?offset_end, ?n))
                    .inspect_err(|err| error!(?err))?;
            }
        }

        let prepared = tx
            .prepare(include_str!("pg/watermark_update.sql"))
            .await
            .inspect_err(|err| error!(?err))?;

        let stable = if topition_is_in_a_txn {
            stable.unwrap_or_default()
        } else {
            stable.map_or(records_len - 1, |high| high + records_len)
        };

        _ = tx
            .execute(
                &prepared,
                &[
                    &self.cluster,
                    &topic,
                    &partition,
                    &low.unwrap_or_default(),
                    &high.map_or(records_len - 1, |high| high + records_len),
                    &stable,
                ],
            )
            .await
            .inspect(|n| debug!(?n))
            .inspect_err(|err| error!(?err))?;

        Ok(high.map_or(0, |high| high + 1))
    }

    async fn end_in_tx(
        &mut self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
        tx: &Transaction<'_>,
    ) -> Result<ErrorCode> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?producer_id, ?producer_epoch, ?committed);

        let prepared = tx
            .prepare(include_str!("pg/txn_select_produced_topitions.sql"))
            .await
            .inspect(|prepared| debug!(?prepared))
            .inspect_err(|err| error!(?err))?;

        let mut overlaps = vec![];

        for row in tx
            .query(
                &prepared,
                &[
                    &self.cluster,
                    &transaction_id,
                    &producer_id,
                    &producer_epoch,
                ],
            )
            .await
            .inspect(|n| debug!(?self.cluster, ?transaction_id, ?producer_id, ?producer_epoch, ?n))
            .inspect_err(|err| error!(?err))?
        {
            let topic = row.try_get::<_, String>(0)?;
            let partition = row.try_get::<_, i32>(1)?;

            let topition = Topition::new(topic.clone(), partition);

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

            let offset = self
                .produce_in_tx(Some(transaction_id), &topition, batch, tx)
                .await?;

            debug!(offset, ?topition);

            let prepared = tx
                .prepare(include_str!(
                    "pg/txn_produce_offset_select_offset_range.sql"
                ))
                .await
                .inspect(|prepared| debug!(?prepared))
                .inspect_err(|err| error!(?err))?;

            let row = tx
                .query_one(
                    &prepared,
                    &[
                        &self.cluster,
                        &transaction_id,
                        &producer_id,
                        &producer_epoch,
                        &topic,
                        &partition,
                    ],
                )
                .await
                .inspect_err(|err| error!(?err))?;

            let offset_start = row.try_get::<_, i64>(0)?;
            let offset_end = row.try_get::<_, i64>(1)?;
            debug!(offset_start, offset_end);

            let prepared = tx
                .prepare(include_str!(
                    "pg/txn_produce_offset_select_overlapping_txn.sql"
                ))
                .await
                .inspect(|prepared| debug!(?prepared))
                .inspect_err(|err| error!(?err))?;

            let rows = tx
                .query(
                    &prepared,
                    &[
                        &self.cluster,
                        &transaction_id,
                        &producer_id,
                        &producer_epoch,
                        &topic,
                        &partition,
                        &offset_end,
                    ],
                )
                .await
                .inspect_err(|err| error!(?err))?;

            for row in rows {
                overlaps.push(Txn::try_from(row)?);
            }
        }

        if overlaps.iter().all(|txn| txn.status.is_prepared()) {
            let txns = {
                let mut txns = Vec::with_capacity(overlaps.len() + 1);

                txns.append(&mut overlaps);

                txns.push(Txn {
                    name: transaction_id.into(),
                    producer_id,
                    producer_epoch,
                    status: if committed {
                        TxnState::PrepareCommit
                    } else {
                        TxnState::PrepareAbort
                    },
                });

                txns
            };

            debug!(?txns);

            for txn in txns {
                debug!(?txn);

                if txn.status == TxnState::PrepareCommit {
                    let prepared = tx
                        .prepare(include_str!("pg/watermark_insert_from_txn.sql"))
                        .await
                        .inspect(|prepared| debug!(?prepared))
                        .inspect_err(|err| error!(?err))?;

                    _ = tx
                        .execute(
                            &prepared,
                            &[
                                &self.cluster,
                                &txn.name,
                                &txn.producer_id,
                                &txn.producer_epoch,
                            ],
                        )
                        .await
                        .inspect(|n| debug!(cluster = ?self.cluster, ?txn, ?n))
                        .inspect_err(|err| error!(?err))?;
                }

                let prepared = tx
                    .prepare(include_str!("pg/txn_produce_offset_delete_by_txn.sql"))
                    .await
                    .inspect(|prepared| debug!(?prepared))
                    .inspect_err(|err| error!(?err))?;

                _ = tx
                    .execute(
                        &prepared,
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                        ],
                    )
                    .await
                    .inspect(|n| debug!(?self.cluster, ?txn, ?n))
                    .inspect_err(|err| error!(?err))?;

                let prepared = tx
                    .prepare(include_str!("pg/txn_topition_delete_by_txn.sql"))
                    .await
                    .inspect(|prepared| debug!(?prepared))
                    .inspect_err(|err| error!(?err))?;

                _ = tx
                    .execute(
                        &prepared,
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                        ],
                    )
                    .await
                    .inspect(|n| debug!(cluster = ?self.cluster, ?txn, ?n))
                    .inspect_err(|err| error!(?err))?;

                if txn.status == TxnState::PrepareCommit {
                    let prepared = tx
                        .prepare(include_str!("pg/consumer_offset_insert_from_txn.sql"))
                        .await
                        .inspect(|prepared| debug!(?prepared))
                        .inspect_err(|err| error!(?err))?;

                    _ = tx
                        .execute(
                            &prepared,
                            &[
                                &self.cluster,
                                &txn.name,
                                &txn.producer_id,
                                &txn.producer_epoch,
                            ],
                        )
                        .await
                        .inspect(|n| debug!(cluster = ?self.cluster, ?txn, ?n))
                        .inspect_err(|err| error!(?err))?;
                }

                let prepared = tx
                    .prepare(include_str!("pg/txn_offset_commit_tp_delete_by_txn.sql"))
                    .await
                    .inspect(|prepared| debug!(?prepared))
                    .inspect_err(|err| error!(?err))?;

                _ = tx
                    .execute(
                        &prepared,
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                        ],
                    )
                    .await
                    .inspect(|n| debug!(cluster = ?self.cluster, ?txn, ?n))
                    .inspect_err(|err| error!(?err))?;

                let prepared = tx
                    .prepare(include_str!("pg/txn_offset_commit_delete_by_txn.sql"))
                    .await
                    .inspect(|prepared| debug!(?prepared))
                    .inspect_err(|err| error!(?err))?;

                _ = tx
                    .execute(
                        &prepared,
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                        ],
                    )
                    .await
                    .inspect(|n| debug!(cluster = ?self.cluster, ?txn, ?n))
                    .inspect_err(|err| error!(?err))?;

                let prepared = tx
                    .prepare(include_str!("pg/txn_status_update.sql"))
                    .await
                    .inspect(|prepared| debug!(?prepared))
                    .inspect_err(|err| error!(?err))?;

                let outcome = if txn.status == TxnState::PrepareCommit {
                    String::from(TxnState::Committed)
                } else if txn.status == TxnState::PrepareAbort {
                    String::from(TxnState::Aborted)
                } else {
                    String::from(txn.status)
                };

                _ = tx
                    .execute(
                        &prepared,
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                            &outcome,
                        ],
                    )
                    .await
                    .inspect(|n| debug!(cluster = self.cluster, ?txn, n))
                    .inspect_err(|err| error!(?err))?;
            }
        } else {
            debug!(?overlaps);

            let prepared = tx
                .prepare(include_str!("pg/txn_status_update.sql"))
                .await
                .inspect(|prepared| debug!(?prepared))
                .inspect_err(|err| error!(?err))?;

            let outcome = if committed {
                String::from(TxnState::PrepareCommit)
            } else {
                String::from(TxnState::PrepareAbort)
            };

            _ = tx
                .execute(
                    &prepared,
                    &[
                        &self.cluster,
                        &transaction_id,
                        &producer_id,
                        &producer_epoch,
                        &outcome,
                    ],
                )
                .await
                .inspect(|n| {
                    debug!(
                        cluster = self.cluster,
                        transaction_id, producer_id, producer_epoch, n
                    )
                })
                .inspect_err(|err| error!(?err))?;
        }

        Ok(ErrorCode::None)
    }
}

#[async_trait]
impl Storage for Postgres {
    async fn register_broker(
        &mut self,
        broker_registration: BrokerRegistationRequest,
    ) -> Result<()> {
        debug!(cluster = self.cluster, ?broker_registration);

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
        debug!(cluster = self.cluster);

        let c = self.connection().await?;

        let prepared = c
            .prepare(concat!(
                "select",
                " node, host, port, rack",
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
        debug!(cluster = self.cluster, ?topic, validate_only);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let prepared = tx
            .prepare(include_str!("pg/topic_insert.sql"))
            .await
            .inspect_err(|err| error!(?err, ?topic, ?validate_only))?;

        let topic_uuid = tx
            .query_one(
                &prepared,
                &[
                    &self.cluster,
                    &topic.name,
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

        debug!(?topic_uuid, cluster = self.cluster, ?topic);

        for partition in 0..topic.num_partitions {
            let prepared = tx
                .prepare(include_str!("pg/topition_insert.sql"))
                .await
                .inspect_err(|err| error!(?err))?;

            _ = tx
                .query_one(&prepared, &[&self.cluster, &topic.name, &partition])
                .await
                .inspect_err(|err| error!(?err))?;
        }

        for partition in 0..topic.num_partitions {
            let prepared = tx
                .prepare(include_str!("pg/watermark_insert.sql"))
                .await
                .inspect_err(|err| error!(?err))?;

            _ = tx
                .query_one(&prepared, &[&self.cluster, &topic.name, &partition])
                .await
                .inspect_err(|err| error!(?err))?;
        }

        if let Some(configs) = topic.configs {
            let prepared = tx
                .prepare(include_str!("pg/topic_configuration_insert.sql"))
                .await
                .inspect_err(|err| error!(?err))?;

            for config in configs {
                debug!(?config);

                _ = tx
                    .execute(
                        &prepared,
                        &[
                            &self.cluster,
                            &topic.name,
                            &config.name,
                            &config.value.as_deref(),
                        ],
                    )
                    .await
                    .inspect_err(|err| error!(?err, ?config));
            }
        }

        tx.commit().await.inspect_err(|err| error!(?err))?;

        Ok(topic_uuid)
    }

    async fn delete_records(
        &mut self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        debug!(cluster = self.cluster, ?topics);

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
        debug!(cluster = self.cluster, ?topic);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let row = match topic {
            TopicId::Id(id) => {
                let prepared = tx
                    .prepare(include_str!("pg/topic_select_uuid.sql"))
                    .await
                    .inspect_err(|err| error!(?err))?;

                tx.query_opt(&prepared, &[&self.cluster, &id])
                    .await
                    .inspect_err(|err| error!(?err))?
            }

            TopicId::Name(name) => {
                let prepared = tx
                    .prepare(include_str!("pg/topic_select_name.sql"))
                    .await
                    .inspect_err(|err| error!(?err))?;

                tx.query_opt(&prepared, &[&self.cluster, &name])
                    .await
                    .inspect_err(|err| error!(?err))?
            }
        };

        let Some(row) = row else {
            return Ok(ErrorCode::UnknownTopicOrPartition);
        };

        let topic_name = row.try_get::<_, String>(1)?;

        for (description, sql) in [
            (
                "consumer offsets",
                include_str!("pg/consumer_offset_delete_by_topic.sql"),
            ),
            (
                "watermarks",
                include_str!("pg/watermark_delete_by_topic.sql"),
            ),
            ("headers", include_str!("pg/header_delete_by_topic.sql")),
            ("records", include_str!("pg/record_delete_by_topic.sql")),
            (
                "txn_offset_commit_tp",
                include_str!("pg/txn_offset_commit_tp_delete_by_topic.sql"),
            ),
            (
                "txn_produce_offset_delete",
                include_str!("pg/txn_produce_offset_delete_by_topic.sql"),
            ),
            (
                "txn_topition",
                include_str!("pg/txn_topition_delete_by_topic.sql"),
            ),
            (
                "producer_detail",
                include_str!("pg/producer_detail_delete_by_topic.sql"),
            ),
            ("topitions", include_str!("pg/topition_delete_by_topic.sql")),
        ] {
            let prepared = tx.prepare(sql).await.inspect_err(|err| error!(?err))?;

            let rows = tx
                .execute(&prepared, &[&self.cluster, &topic_name])
                .await
                .inspect_err(|err| error!(?err))?;

            debug!(?topic, ?rows, ?description);
        }

        let prepared = tx
            .prepare(include_str!("pg/topic_delete_by.sql"))
            .await
            .inspect_err(|err| error!(?err))?;

        _ = tx
            .execute(&prepared, &[&self.cluster, &topic_name])
            .await
            .inspect_err(|err| error!(?err))?;

        tx.commit().await.inspect_err(|err| error!(?err))?;

        Ok(ErrorCode::None)
    }

    async fn produce(
        &mut self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        debug!(cluster = self.cluster, transaction_id, ?topition, ?deflated);

        let mut c = self.connection().await?;

        let tx = c.transaction().await?;

        let high = self
            .produce_in_tx(transaction_id, topition, deflated, &tx)
            .await?;

        tx.commit().await?;

        Ok(high)
    }

    async fn fetch(
        &mut self,
        topition: &Topition,
        offset: i64,
        _min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        debug!(cluster = self.cluster, ?topition, offset, ?isolation);

        let high_watermark = self.offset_stage(topition).await.map(|offset_stage| {
            if isolation == IsolationLevel::ReadCommitted {
                offset_stage.last_stable
            } else {
                offset_stage.high_watermark
            }
        })?;

        let c = self.connection().await?;

        let select_batch = c
            .prepare(include_str!("pg/record_fetch.sql"))
            .await
            .inspect_err(|err| {
                error!(
                    ?err,
                    cluster = self.cluster,
                    ?topition,
                    offset,
                    max_bytes,
                    high_watermark
                )
            })?;

        let select_headers = c
            .prepare(include_str!("pg/header_fetch.sql"))
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
                    &high_watermark,
                ],
            )
            .await
            .inspect_err(|err| error!(?err))?;

        let mut batches = vec![];

        if let Some(first) = records.first() {
            let mut batch_builder = inflated::Batch::builder()
                .base_offset(first.try_get::<_, i64>(0).inspect_err(|err| error!(?err))?)
                .base_timestamp(
                    first
                        .try_get::<_, SystemTime>(1)
                        .map_err(Error::from)
                        .and_then(|system_time| to_timestamp(system_time).map_err(Into::into))
                        .inspect_err(|err| error!(?err))?,
                )
                .producer_id(
                    first
                        .try_get::<_, Option<i64>>(5)
                        .map(|producer_id| producer_id.unwrap_or(-1))
                        .inspect_err(|err| error!(?err))?,
                )
                .producer_epoch(
                    first
                        .try_get::<_, Option<i16>>(6)
                        .map(|producer_epoch| producer_epoch.unwrap_or(-1))
                        .inspect_err(|err| error!(?err))?,
                );

            for record in records.iter() {
                debug!(?record);

                let producer_id = record
                    .try_get::<_, Option<i64>>(5)
                    .map(|producer_id| producer_id.unwrap_or(-1))
                    .inspect_err(|err| error!(?err))?;
                let producer_epoch = record
                    .try_get::<_, Option<i16>>(6)
                    .map(|producer_epoch| producer_epoch.unwrap_or(-1))
                    .inspect_err(|err| error!(?err))?;

                if batch_builder.producer_id != producer_id
                    || batch_builder.producer_epoch != producer_epoch
                {
                    batches.push(batch_builder.build().and_then(TryInto::try_into)?);

                    batch_builder = inflated::Batch::builder()
                        .base_offset(first.try_get::<_, i64>(0).inspect_err(|err| error!(?err))?)
                        .base_timestamp(
                            first
                                .try_get::<_, SystemTime>(1)
                                .map_err(Error::from)
                                .and_then(|system_time| {
                                    to_timestamp(system_time).map_err(Into::into)
                                })
                                .inspect_err(|err| error!(?err))?,
                        )
                        .producer_id(producer_id)
                        .producer_epoch(producer_epoch);
                }

                let offset = record
                    .try_get::<_, i64>(0)
                    .inspect_err(|err| error!(?err))?;
                let offset_delta = i32::try_from(offset - batch_builder.base_offset)?;

                let timestamp_delta = first
                    .try_get::<_, SystemTime>(1)
                    .map_err(Error::from)
                    .and_then(|system_time| {
                        to_timestamp(system_time)
                            .map(|timestamp| timestamp - batch_builder.base_timestamp)
                            .map_err(Into::into)
                    })
                    .inspect(|timestamp| debug!(?timestamp))
                    .inspect_err(|err| error!(?err))?;

                let k = record
                    .try_get::<_, Option<&[u8]>>(2)
                    .map(|o| o.map(Bytes::copy_from_slice))
                    .inspect(|k| debug!(?k))
                    .inspect_err(|err| error!(?err))?;

                let v = record
                    .try_get::<_, Option<&[u8]>>(3)
                    .map(|o| o.map(Bytes::copy_from_slice))
                    .inspect(|v| debug!(?v))
                    .inspect_err(|err| error!(?err))?;

                let mut record_builder = Record::builder()
                    .offset_delta(offset_delta)
                    .timestamp_delta(timestamp_delta)
                    .key(k.into())
                    .value(v.into());

                for header in c
                    .query(
                        &select_headers,
                        &[
                            &self.cluster,
                            &topition.topic(),
                            &topition.partition(),
                            &offset,
                        ],
                    )
                    .await
                    .inspect(|row| debug!(?row))
                    .inspect_err(|err| error!(?err))?
                {
                    let mut header_builder = Header::builder();

                    if let Some(k) = header
                        .try_get::<_, Option<&[u8]>>(0)
                        .inspect_err(|err| error!(?err))?
                    {
                        header_builder = header_builder.key(k.to_vec());
                    }

                    if let Some(v) = header
                        .try_get::<_, Option<&[u8]>>(1)
                        .inspect_err(|err| error!(?err))?
                    {
                        header_builder = header_builder.value(v.to_vec());
                    }

                    record_builder = record_builder.header(header_builder);
                }

                batch_builder = batch_builder.record(record_builder);
            }

            batches.push(batch_builder.build().and_then(TryInto::try_into)?);
        } else {
            batches.push(
                inflated::Batch::builder()
                    .build()
                    .and_then(TryInto::try_into)?,
            );
        }

        Ok(batches)
    }

    async fn offset_stage(&mut self, topition: &Topition) -> Result<OffsetStage> {
        debug!(cluster = self.cluster, ?topition);
        let c = self.connection().await?;

        let prepared = c
            .prepare(include_str!("pg/watermark_select.sql"))
            .await
            .inspect_err(|err| error!(?topition, ?err))?;

        let row = c
            .query_one(
                &prepared,
                &[&self.cluster, &topition.topic(), &topition.partition()],
            )
            .await
            .inspect_err(|err| error!(?topition, ?prepared, ?err))?;

        let log_start = row
            .try_get::<_, Option<i64>>(0)
            .inspect_err(|err| error!(?topition, ?prepared, ?err))?
            .unwrap_or_default();

        let high_watermark = row
            .try_get::<_, Option<i64>>(1)
            .inspect_err(|err| error!(?topition, ?prepared, ?err))?
            .unwrap_or_default();

        let last_stable = row
            .try_get::<_, Option<i64>>(2)
            .inspect_err(|err| error!(?topition, ?prepared, ?err))?
            .unwrap_or_default();

        debug!(
            cluster = self.cluster,
            ?topition,
            log_start,
            high_watermark,
            last_stable
        );

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
        debug!(cluster = self.cluster, ?group, ?retention);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let prepared = tx
            .prepare(include_str!("pg/consumer_offset_insert.sql"))
            .await
            .inspect_err(|err| error!(?err))?;

        let mut responses = vec![];

        for (topition, offset) in offsets {
            _ = tx
                .execute(
                    &prepared,
                    &[
                        &self.cluster,
                        &topition.topic(),
                        &topition.partition(),
                        &group,
                        &offset.offset,
                        &offset.leader_epoch,
                        &offset.timestamp,
                        &offset.metadata,
                    ],
                )
                .await
                .inspect_err(|err| error!(?err))?;

            responses.push((topition.to_owned(), ErrorCode::None));
        }

        tx.commit().await.inspect_err(|err| error!(?err))?;

        Ok(responses)
    }

    async fn offset_fetch(
        &mut self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        debug!(cluster = self.cluster, ?group_id, ?topics, ?require_stable);

        let c = self.connection().await?;

        let prepared = c
            .prepare(include_str!("pg/consumer_offset_select.sql"))
            .await
            .inspect_err(|err| error!(?err))?;

        let mut offsets = BTreeMap::new();

        for topic in topics {
            let offset = c
                .query_opt(
                    &prepared,
                    &[&self.cluster, &group_id, &topic.topic(), &topic.partition()],
                )
                .await
                .inspect_err(|err| error!(?err))?
                .map_or(Ok(-1), |row| row.try_get::<_, i64>(0))?;

            _ = offsets.insert(topic.to_owned(), offset);
        }

        Ok(offsets)
    }

    async fn list_offsets(
        &mut self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffsetRequest)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        debug!(cluster = self.cluster, ?offsets);

        let c = self.connection().await?;

        let mut responses = vec![];

        for (topition, offset_type) in offsets {
            let query = match (offset_type, isolation_level) {
                (ListOffsetRequest::Earliest, _) => include_str!("pg/list_earliest_offset.sql"),
                (ListOffsetRequest::Latest, IsolationLevel::ReadCommitted) => {
                    include_str!("pg/list_latest_offset_committed.sql")
                }
                (ListOffsetRequest::Latest, IsolationLevel::ReadUncommitted) => {
                    include_str!("pg/list_latest_offset_uncommitted.sql")
                }
                (ListOffsetRequest::Timestamp(_), _) => {
                    include_str!("pg/list_latest_offset_timestamp.sql")
                }
            };

            debug!(?query);

            let prepared = c
                .prepare(query)
                .await
                .inspect_err(|err| error!(?err))
                .inspect(|prepared| debug!(?prepared))?;

            let list_offset = match offset_type {
                ListOffsetRequest::Earliest | ListOffsetRequest::Latest => c
                    .query_opt(
                        &prepared,
                        &[&self.cluster, &topition.topic(), &topition.partition()],
                    )
                    .await
                    .inspect_err(|err| error!(?err, cluster = self.cluster, ?topition)),

                ListOffsetRequest::Timestamp(timestamp) => c
                    .query_opt(
                        &prepared,
                        &[
                            &self.cluster.as_str(),
                            &topition.topic(),
                            &topition.partition(),
                            timestamp,
                        ],
                    )
                    .await
                    .inspect_err(|err| error!(?err)),
            }
            .inspect_err(|err| {
                error!(?err, cluster = self.cluster, ?topition);
            })
            .inspect(|result| debug!(?result))?
            .map_or_else(
                || {
                    // this might need to be -1..
                    let timestamp = Some(SystemTime::now());
                    let offset = Some(0);
                    debug!(
                        cluster = self.cluster,
                        ?topition,
                        ?offset_type,
                        offset,
                        ?timestamp
                    );

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
                            debug!(
                                cluster = self.cluster,
                                ?topition,
                                ?offset_type,
                                offset,
                                ?timestamp
                            );

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
        debug!(cluster = self.cluster, ?topics);

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let prepared = c
            .prepare(include_str!("pg/broker_metadata_select.sql"))
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
                                .prepare(include_str!("pg/topic_select_name.sql"))
                                .await
                                .inspect_err(|err| {
                                    error!(?err, cluster = self.cluster, ?name);
                                })?;

                            match c
                                .query_opt(&prepared, &[&self.cluster, &name.as_str()])
                                .await
                                .inspect_err(|err| error!(?err))
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
                                                        .map(|_replica| {
                                                            brokers.next().expect("cycling")
                                                        })
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

                                Ok(None) => MetadataResponseTopic {
                                    error_code: ErrorCode::UnknownTopicOrPartition.into(),
                                    name: Some(name.into()),
                                    topic_id: Some(NULL_TOPIC_ID),
                                    is_internal: Some(false),
                                    partitions: Some([].into()),
                                    topic_authorized_operations: Some(-2147483648),
                                },

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
                                .prepare(include_str!("pg/topic_select_uuid.sql"))
                                .await
                                .inspect_err(|error| error!(?error))?;

                            match c.query_one(&prepared, &[&self.cluster, &id]).await {
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
                                                        .map(|_replica| {
                                                            brokers.next().expect("cycling")
                                                        })
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
                    .prepare(include_str!("pg/topic_by_cluster.sql"))
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
        debug!(cluster = self.cluster, name, ?resource, ?keys);

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
        debug!(cluster = self.cluster, group_id, ?detail, ?version);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let prepared = tx
            .prepare(include_str!("pg/consumer_group_insert.sql"))
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
            .inspect_err(|err| error!(?err))
            .inspect(|existing_e_tag| debug!(?existing_e_tag))?;

        let new_e_tag = Uuid::new_v4();
        debug!(?new_e_tag);

        let detail = serde_json::to_value(detail).inspect(|detail| debug!(?detail))?;

        let outcome = if let Some(row) = tx
            .query_opt(
                &prepared,
                &[
                    &self.cluster,
                    &group_id,
                    &existing_e_tag,
                    &new_e_tag,
                    &detail,
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
                .inspect(|version| debug!(?version))
        } else {
            let prepared = tx
                .prepare(include_str!("pg/consumer_group_detail.sql"))
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
                })
                .inspect(|version| debug!(?version))?;

            let value = row.try_get::<_, Value>(1)?;
            let current =
                serde_json::from_value::<GroupDetail>(value).inspect(|current| debug!(?current))?;

            Err(UpdateError::Outdated { current, version })
        };

        tx.commit().await.inspect_err(|err| error!(?err))?;

        debug!(?outcome);

        outcome
    }

    async fn init_producer(
        &mut self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        debug!(
            cluster = self.cluster,
            transaction_id, producer_id, producer_epoch
        );

        match (producer_id, producer_epoch, transaction_id) {
            (Some(-1), Some(-1), Some(transaction_id)) => {
                let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
                let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

                let prepared = tx
                    .prepare(include_str!("pg/producer_epoch_for_current_txn.sql"))
                    .await
                    .inspect_err(|err| error!(?err))?;

                if let Some(row) = tx
                    .query_opt(&prepared, &[&self.cluster, &transaction_id])
                    .await
                    .inspect_err(|err| error!(?err))?
                {
                    let id: i64 = row.try_get(0).inspect_err(|err| error!(?err))?;
                    let epoch: i16 = row.try_get(1).inspect_err(|err| error!(?err))?;
                    let status = row
                        .try_get::<_, Option<String>>(2)
                        .inspect_err(|err| error!(?err))?
                        .map_or(Ok(None), |status| {
                            TxnState::from_str(status.as_str()).map(Some)
                        })?;

                    debug!(transaction_id, id, epoch, ?status);

                    if let Some(TxnState::Begin) = status {
                        let error = self
                            .end_in_tx(transaction_id, id, epoch, false, &tx)
                            .await?;

                        if error != ErrorCode::None {
                            _ = tx
                                .rollback()
                                .await
                                .inspect_err(|err| error!(?err, ?transaction_id, id, epoch));

                            return Ok(ProducerIdResponse { error, id, epoch });
                        }
                    }
                }

                let prepared = tx
                    .prepare(include_str!("pg/txn_select_name.sql"))
                    .await
                    .inspect_err(|err| error!(?err))?;

                let (producer, epoch) = if let Some(row) = tx
                    .query_opt(&prepared, &[&self.cluster, &transaction_id])
                    .await
                    .inspect_err(|err| error!(?err))?
                {
                    let producer: i64 = row.try_get(0).inspect_err(|err| error!(?err))?;

                    let prepared = tx
                        .prepare(include_str!("pg/producer_epoch_insert.sql"))
                        .await
                        .inspect_err(|err| error!(?err))?;

                    let row = tx
                        .query_one(&prepared, &[&self.cluster, &producer])
                        .await
                        .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                    let epoch: i16 = row.try_get(0)?;

                    (producer, epoch)
                } else {
                    let prepared = tx
                        .prepare(include_str!("pg/producer_insert.sql"))
                        .await
                        .inspect_err(|err| error!(?err))?;

                    let row = tx
                        .query_one(&prepared, &[&self.cluster])
                        .await
                        .inspect_err(|err| error!(?err))?;

                    let producer: i64 = row.try_get(0).inspect_err(|err| error!(?err))?;

                    let prepared = tx
                        .prepare(include_str!("pg/producer_epoch_insert.sql"))
                        .await
                        .inspect_err(|err| error!(?err))?;

                    let row = tx
                        .query_one(&prepared, &[&self.cluster, &producer])
                        .await
                        .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                    let epoch: i16 = row.try_get(0)?;

                    let prepared = tx
                        .prepare(include_str!("pg/txn_insert.sql"))
                        .await
                        .inspect_err(|err| error!(?err))?;

                    assert_eq!(
                        1,
                        tx.execute(&prepared, &[&self.cluster, &transaction_id, &producer])
                            .await
                            .inspect_err(|err| error!(
                                self.cluster,
                                transaction_id,
                                producer,
                                ?err
                            ))?
                    );

                    (producer, epoch)
                };

                debug!(transaction_id, producer, epoch);

                let prepared = tx
                    .prepare(include_str!("pg/txn_detail_insert.sql"))
                    .await
                    .inspect_err(|err| error!(?err))?;

                assert_eq!(
                    1,
                    tx.execute(
                        &prepared,
                        &[
                            &self.cluster,
                            &transaction_id,
                            &producer,
                            &epoch,
                            &transaction_timeout_ms
                        ]
                    )
                    .await
                    .inspect_err(|err| error!(
                        self.cluster,
                        transaction_id,
                        producer,
                        epoch,
                        transaction_timeout_ms,
                        ?err
                    ))?
                );

                let error = match tx.commit().await.inspect_err(|err| {
                    error!(
                        ?err,
                        cluster = self.cluster,
                        transaction_id,
                        producer,
                        epoch
                    )
                }) {
                    Ok(()) => ErrorCode::None,
                    Err(_) => ErrorCode::UnknownServerError,
                };

                Ok(ProducerIdResponse {
                    error,
                    id: producer,
                    epoch,
                })
            }

            (Some(-1), Some(-1), None) => {
                let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
                let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

                let prepared = tx
                    .prepare(include_str!("pg/producer_insert.sql"))
                    .await
                    .inspect_err(|err| error!(?err))?;

                let row = tx
                    .query_one(&prepared, &[&self.cluster])
                    .await
                    .inspect_err(|err| error!(self.cluster, ?err))?;

                let producer = row.try_get(0)?;

                let prepared = tx
                    .prepare(include_str!("pg/producer_epoch_insert.sql"))
                    .await
                    .inspect_err(|err| error!(?err))?;

                let row = tx
                    .query_one(&prepared, &[&self.cluster, &producer])
                    .await
                    .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                let epoch: i16 = row.try_get(0)?;

                let error = match tx
                    .commit()
                    .await
                    .inspect_err(|err| error!(?err, ?transaction_id, producer, epoch))
                {
                    Ok(()) => ErrorCode::None,
                    Err(_) => ErrorCode::UnknownServerError,
                };

                Ok(ProducerIdResponse {
                    error,
                    id: producer,
                    epoch,
                })
            }

            (_, _, _) => todo!(),
        }
    }

    async fn txn_add_offsets(
        &mut self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        debug!(
            cluster = self.cluster,
            transaction_id, producer_id, producer_epoch, group_id
        );

        Ok(ErrorCode::None)
    }

    async fn txn_add_partitions(
        &mut self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        debug!(cluster = self.cluster, ?partitions);

        match partitions {
            TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id,
                producer_id,
                producer_epoch,
                topics,
            } => {
                debug!(?transaction_id, ?producer_id, ?producer_epoch, ?topics);

                let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
                let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

                let prepared = tx
                    .prepare(include_str!("pg/txn_topition_insert.sql"))
                    .await
                    .inspect_err(|err| error!(?err))?;

                let mut results = vec![];

                for topic in topics {
                    let mut results_by_partition = vec![];

                    for partition_index in topic.partitions.unwrap_or(vec![]) {
                        _ = tx
                            .execute(
                                &prepared,
                                &[
                                    &self.cluster,
                                    &topic.name,
                                    &partition_index,
                                    &transaction_id,
                                    &producer_id,
                                    &producer_epoch,
                                ],
                            )
                            .await
                            .inspect_err(|err| {
                                error!(
                                    ?err,
                                    cluster = self.cluster,
                                    topic = topic.name,
                                    partition_index,
                                    transaction_id
                                )
                            })?;

                        results_by_partition.push(AddPartitionsToTxnPartitionResult {
                            partition_index,
                            partition_error_code: i16::from(ErrorCode::None),
                        });
                    }

                    results.push(AddPartitionsToTxnTopicResult {
                        name: topic.name,
                        results_by_partition: Some(results_by_partition),
                    })
                }

                let prepared = tx
                    .prepare(include_str!("pg/txn_detail_update_started_at.sql"))
                    .await
                    .inspect_err(|err| error!(?err))?;

                _ = tx
                    .execute(
                        &prepared,
                        &[
                            &self.cluster,
                            &transaction_id,
                            &producer_id,
                            &producer_epoch,
                        ],
                    )
                    .await
                    .inspect_err(|err| {
                        error!(
                            ?err,
                            cluster = self.cluster,
                            transaction_id,
                            producer_id,
                            producer_epoch,
                        )
                    })?;

                tx.commit().await?;

                Ok(TxnAddPartitionsResponse::VersionZeroToThree(results))
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
        debug!(cluster = self.cluster, ?offsets);

        let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
        let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

        let prepared = tx
            .prepare(include_str!("pg/producer_epoch_for_current_txn.sql"))
            .await
            .inspect_err(|err| error!(?err))?;

        let (producer_id, producer_epoch) = if let Some(row) = tx
            .query_opt(&prepared, &[&self.cluster, &offsets.transaction_id])
            .await
            .inspect_err(|err| error!(?err))?
        {
            let producer_id = row
                .try_get::<_, i64>(0)
                .map(Some)
                .inspect_err(|err| error!(?err))?;

            let epoch = row
                .try_get::<_, i16>(1)
                .map(Some)
                .inspect_err(|err| error!(?err))?;

            (producer_id, epoch)
        } else {
            (None, None)
        };

        debug!(?producer_id, ?producer_epoch);

        let prepared = tx
            .prepare(include_str!("pg/txn_offset_commit_insert.sql"))
            .await
            .inspect_err(|err| error!(?err))?;

        _ = tx
            .execute(
                &prepared,
                &[
                    &self.cluster,
                    &offsets.transaction_id,
                    &offsets.group_id,
                    &offsets.producer_id,
                    &offsets.producer_epoch,
                    &offsets.generation_id,
                    &offsets.member_id,
                ],
            )
            .await
            .inspect_err(|err| error!(?err))?;

        let prepared = tx
            .prepare(include_str!("pg/txn_offset_commit_tp_insert.sql"))
            .await
            .inspect_err(|err| error!(?err))?;

        let mut topics = vec![];

        for topic in offsets.topics {
            let mut partitions = vec![];

            for partition in topic.partitions.unwrap_or(vec![]) {
                if producer_id.is_some_and(|producer_id| producer_id == offsets.producer_id) {
                    if producer_epoch
                        .is_some_and(|producer_epoch| producer_epoch == offsets.producer_epoch)
                    {
                        _ = tx
                            .execute(
                                &prepared,
                                &[
                                    &self.cluster,
                                    &offsets.transaction_id,
                                    &offsets.group_id,
                                    &offsets.producer_id,
                                    &offsets.producer_epoch,
                                    &topic.name,
                                    &partition.partition_index,
                                    &partition.committed_offset,
                                    &partition.committed_leader_epoch,
                                    &partition.committed_metadata,
                                ],
                            )
                            .await
                            .inspect_err(|err| error!(?err))?;

                        partitions.push(TxnOffsetCommitResponsePartition {
                            partition_index: partition.partition_index,
                            error_code: i16::from(ErrorCode::None),
                        });
                    } else {
                        partitions.push(TxnOffsetCommitResponsePartition {
                            partition_index: partition.partition_index,
                            error_code: i16::from(ErrorCode::InvalidProducerEpoch),
                        });
                    }
                } else {
                    partitions.push(TxnOffsetCommitResponsePartition {
                        partition_index: partition.partition_index,
                        error_code: i16::from(ErrorCode::UnknownProducerId),
                    });
                }
            }

            topics.push(TxnOffsetCommitResponseTopic {
                name: topic.name,
                partitions: Some(partitions),
            });
        }

        tx.commit().await?;

        Ok(topics)
    }

    async fn txn_end(
        &mut self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        debug!(cluster = ?self.cluster, transaction_id, producer_id, producer_epoch, committed);

        let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
        let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

        let error_code = self
            .end_in_tx(transaction_id, producer_id, producer_epoch, committed, &tx)
            .await?;

        tx.commit().await?;

        Ok(error_code)
    }
}
