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

use std::collections::BTreeMap;

use bytes::Bytes;
use common::{StorageType, alphanumeric_string, init_tracing, register_broker};
use rand::{prelude::*, rng};
use tansu_broker::Result;
use tansu_sans_io::{
    BatchAttribute, ControlBatch, EndTransactionMarker, ErrorCode, IsolationLevel,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
    create_topics_request::CreatableTopic,
    record::{Record, inflated},
};
use tansu_storage::{Storage, StorageContainer, Topition, TxnAddPartitionsRequest};
use tracing::{debug, error};
use url::Url;
use uuid::Uuid;

pub mod common;

pub async fn simple_non_txn(
    cluster_id: impl Into<String>,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &mut sc).await?;

    let input_topic_name: String = alphanumeric_string(15);
    debug!(?input_topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    // create input topic
    //
    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(input_topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await?;
    debug!(?topic_id);

    let transaction_timeout_ms = 10_000;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(input_topic_name.clone(), partition_index);
    let records = 6;

    let mut offset_producer = BTreeMap::new();
    let mut bytes = 0;

    for n in 0..records {
        let producer = sc
            .init_producer(None, transaction_timeout_ms, Some(-1), Some(-1))
            .await?;

        let value = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());
        bytes += value.len();

        let batch = inflated::Batch::builder()
            .record(Record::builder().value(value.clone().into()))
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))?;

        debug!(n, bytes, ?batch);

        let offset = sc
            .produce(None, &topition, batch)
            .await
            .inspect(|offset| debug!(?offset))?;

        assert_eq!(None, offset_producer.insert(offset, (producer, value)));
    }

    let offset = offset_producer
        .first_key_value()
        .map(|tuple| tuple.0)
        .copied()
        .unwrap();

    let min_bytes = 1;
    let max_bytes = 50 * 1024;
    let isolation = IsolationLevel::ReadUncommitted;

    let batches = sc
        .fetch(&topition, offset, min_bytes, max_bytes as u32, isolation)
        .await
        .and_then(|batches| {
            batches.into_iter().try_fold(Vec::new(), |mut acc, batch| {
                inflated::Batch::try_from(batch)
                    .map(|inflated| {
                        acc.push(inflated);
                        acc
                    })
                    .map_err(Into::into)
            })
        })?;

    debug!(min_bytes, max_bytes, ?isolation, ?batches);

    assert!(!batches.is_empty());
    assert_eq!(
        offset_producer.first_key_value().unwrap().0,
        &batches[0].base_offset
    );
    assert_eq!(
        records,
        batches
            .iter()
            .map(|batch| batch.records.len())
            .sum::<usize>()
    );

    for record in &batches[0].records {
        let offset = batches[0].base_offset + (record.offset_delta as i64);

        assert_eq!(None, record.key);
        assert_eq!(
            offset_producer
                .get(&offset)
                .map(|(_producer, value)| value)
                .cloned(),
            record.value()
        )
    }

    Ok(())
}

pub async fn with_txn(
    cluster_id: impl Into<String>,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await
        .inspect_err(|err| error!(?err, topic_name, num_partitions, replication_factor))?;
    debug!(?topic_id);

    let transaction_timeout_ms = 10_000;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);
    let records = 6;

    let mut offset_producer = BTreeMap::new();
    let mut bytes = 0;

    let transaction_id: String = alphanumeric_string(10);
    let transactions = 1;

    for transaction in 0..transactions {
        let txn_producer = sc
            .init_producer(
                Some(transaction_id.as_str()),
                transaction_timeout_ms,
                Some(-1),
                Some(-1),
            )
            .await
            .inspect_err(|err| error!(?err, transaction_id, transaction_timeout_ms))?;
        debug!(transaction, ?txn_producer);

        let add_partitions = sc
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: txn_producer.id,
                producer_epoch: txn_producer.epoch,
                topics: [AddPartitionsToTxnTopic::default()
                    .name(topic_name.clone())
                    .partitions(Some([partition_index].into()))]
                .into(),
            })
            .await
            .inspect_err(|err| {
                error!(
                    ?err,
                    transaction_id,
                    producer_id = txn_producer.id,
                    producer_epoch = txn_producer.epoch,
                    topic_name,
                    partition_index
                )
            })?;

        assert_eq!(
            [AddPartitionsToTxnTopicResult::default()
                .name(topic_name.clone())
                .results_by_partition(Some(
                    [AddPartitionsToTxnPartitionResult::default()
                        .partition_index(partition_index)
                        .partition_error_code(ErrorCode::None.into())]
                    .into()
                ))],
            add_partitions.zero_to_three()
        );

        for base_sequence in 0..records {
            let key = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());
            bytes += key.len();

            let value = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());
            bytes += value.len();

            let batch = inflated::Batch::builder()
                .record(
                    Record::builder()
                        .key(key.clone().into())
                        .value(value.clone().into()),
                )
                .attributes(BatchAttribute::default().transaction(true).into())
                .producer_id(txn_producer.id)
                .producer_epoch(txn_producer.epoch)
                .base_sequence(base_sequence)
                .build()
                .and_then(TryInto::try_into)
                .inspect(|deflated| debug!(?deflated))?;

            debug!(base_sequence, bytes, ?batch);

            let offset = sc
                .produce(Some(transaction_id.as_str()), &topition, batch)
                .await
                .inspect(|offset| debug!(?offset))
                .inspect_err(|err| {
                    error!(
                        ?err,
                        transaction_id,
                        producer_id = txn_producer.id,
                        producer_epoch = txn_producer.epoch,
                        base_sequence,
                        ?topition,
                    )
                })?;

            debug!(offset);

            assert_eq!(
                None,
                offset_producer.insert(offset, (txn_producer, key, value))
            );
        }

        // commit the transaction
        //
        assert_eq!(
            ErrorCode::None,
            sc.txn_end(
                transaction_id.as_str(),
                txn_producer.id,
                txn_producer.epoch,
                true
            )
            .await?
        );

        let last_offset_in_tx = offset_producer
            .last_key_value()
            .map(|(offset, _)| *offset)
            .unwrap();

        // the end transaction marker that we expect to have been written
        //
        assert_eq!(
            None,
            offset_producer.insert(
                last_offset_in_tx + 1,
                (
                    txn_producer,
                    ControlBatch::default().commit().try_into()?,
                    EndTransactionMarker::default().try_into()?
                )
            )
        );
    }

    let offset = offset_producer
        .first_key_value()
        .map(|tuple| tuple.0)
        .copied()
        .unwrap();

    let min_bytes = 1;
    let max_bytes = 50 * 1024;
    let isolation = IsolationLevel::ReadUncommitted;

    let batches = sc
        .fetch(&topition, offset, min_bytes, max_bytes as u32, isolation)
        .await
        .and_then(|batches| {
            batches.into_iter().try_fold(Vec::new(), |mut acc, batch| {
                inflated::Batch::try_from(batch)
                    .map(|inflated| {
                        acc.push(inflated);
                        acc
                    })
                    .map_err(Into::into)
            })
        })?;

    debug!(min_bytes, max_bytes, ?isolation, ?batches);

    assert_eq!(
        records * transactions + transactions,
        batches
            .iter()
            .map(|batch| batch.records.len() as i32)
            .sum::<i32>()
    );

    for batch in batches {
        for record in batch.records {
            let offset = batch.base_offset + (record.offset_delta as i64);

            let producer = offset_producer.get(&offset).map(|v| v.0).unwrap();

            assert_eq!(producer.id, batch.producer_id);
            assert_eq!(producer.epoch, batch.producer_epoch);

            assert_eq!(
                offset_producer
                    .get(&offset)
                    .map(|(_producer, key, _value)| key)
                    .cloned(),
                record.key()
            );

            assert_eq!(
                offset_producer
                    .get(&offset)
                    .map(|(_producer, _key, value)| value)
                    .cloned(),
                record.value()
            );
        }
    }

    Ok(())
}

pub async fn with_multiple_txn(
    cluster_id: impl Into<String>,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await
        .inspect_err(|err| error!(?err, topic_name, num_partitions, replication_factor))?;
    debug!(?topic_id);

    let transaction_timeout_ms = 10_000;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);
    let records = 6;

    let mut offset_producer = BTreeMap::new();
    let mut bytes = 0;

    let transaction_id: String = alphanumeric_string(10);
    let transactions: i32 = 3;

    for transaction in 0..transactions {
        let txn_producer = sc
            .init_producer(
                Some(transaction_id.as_str()),
                transaction_timeout_ms,
                Some(-1),
                Some(-1),
            )
            .await
            .inspect_err(|err| error!(?err, transaction_id, transaction_timeout_ms))?;
        debug!(transaction, ?txn_producer);

        let add_partitions = sc
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: txn_producer.id,
                producer_epoch: txn_producer.epoch,
                topics: [AddPartitionsToTxnTopic::default()
                    .name(topic_name.clone())
                    .partitions(Some([partition_index].into()))]
                .into(),
            })
            .await
            .inspect_err(|err| {
                error!(
                    ?err,
                    transaction_id,
                    ?txn_producer,
                    topic_name,
                    partition_index
                )
            })?;

        assert_eq!(
            [AddPartitionsToTxnTopicResult::default()
                .name(topic_name.clone())
                .results_by_partition(Some(
                    [AddPartitionsToTxnPartitionResult::default()
                        .partition_index(partition_index)
                        .partition_error_code(ErrorCode::None.into())]
                    .into()
                ))],
            add_partitions.zero_to_three()
        );

        for base_sequence in 0..records {
            let key = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());
            bytes += key.len();

            let value = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());
            bytes += value.len();

            let batch = inflated::Batch::builder()
                .record(
                    Record::builder()
                        .key(key.clone().into())
                        .value(value.clone().into()),
                )
                .attributes(BatchAttribute::default().transaction(true).into())
                .producer_id(txn_producer.id)
                .producer_epoch(txn_producer.epoch)
                .base_sequence(base_sequence)
                .build()
                .and_then(TryInto::try_into)
                .inspect(|deflated| debug!(?deflated))?;

            debug!(base_sequence, bytes, ?batch);

            let offset = sc
                .produce(Some(transaction_id.as_str()), &topition, batch)
                .await
                .inspect(|offset| debug!(offset, ?txn_producer, base_sequence, ?key, ?value))
                .inspect_err(|err| error!(?err, ?txn_producer, base_sequence, ?key, ?value))?;

            assert_eq!(
                None,
                offset_producer.insert(offset, (txn_producer, key, value))
            );
        }

        // commit the transaction
        //
        assert_eq!(
            ErrorCode::None,
            sc.txn_end(
                transaction_id.as_str(),
                txn_producer.id,
                txn_producer.epoch,
                true
            )
            .await
            .inspect_err(|err| error!(?err, transaction_id, ?txn_producer))?
        );

        let last_offset_in_tx = offset_producer
            .last_key_value()
            .map(|(offset, _)| *offset)
            .unwrap();

        // the end transaction marker that we expect to have been written
        //
        assert_eq!(
            None,
            offset_producer.insert(
                last_offset_in_tx + 1,
                (
                    txn_producer,
                    ControlBatch::default().commit().try_into()?,
                    EndTransactionMarker::default().try_into()?
                )
            )
        );
    }

    let offset = offset_producer
        .first_key_value()
        .map(|tuple| tuple.0)
        .copied()
        .unwrap();

    let min_bytes = 1;
    let max_bytes = 50 * 1024;
    let isolation = IsolationLevel::ReadUncommitted;

    let batches = sc
        .fetch(&topition, offset, min_bytes, max_bytes as u32, isolation)
        .await
        .and_then(|batches| {
            batches.into_iter().try_fold(Vec::new(), |mut acc, batch| {
                inflated::Batch::try_from(batch)
                    .map(|inflated| {
                        acc.push(inflated);
                        acc
                    })
                    .map_err(Into::into)
            })
        })
        .inspect_err(|err| error!(?err, ?topition, offset, min_bytes, max_bytes, ?isolation))?;

    debug!(min_bytes, max_bytes, ?isolation, ?batches);

    assert_eq!(
        offset_producer.first_key_value().unwrap().0,
        &batches[0].base_offset
    );

    assert_eq!(
        records * transactions + transactions,
        batches
            .iter()
            .map(|batch| batch.records.len() as i32)
            .sum::<i32>()
    );

    for batch in batches {
        for record in batch.records {
            let offset = batch.base_offset + (record.offset_delta as i64);

            assert_eq!(
                offset_producer
                    .get(&offset)
                    .map(|(_producer, key, _value)| key)
                    .cloned(),
                record.key()
            );

            assert_eq!(
                offset_producer
                    .get(&offset)
                    .map(|(_producer, _key, value)| value)
                    .cloned(),
                record.value()
            );
        }
    }

    Ok(())
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    async fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        common::storage_container(
            StorageType::Postgres,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn simple_non_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn with_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::with_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn with_multiple_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::with_multiple_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

mod in_memory {
    use super::*;

    async fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        common::storage_container(
            StorageType::InMemory,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn simple_non_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn with_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::with_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn with_multiple_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::with_multiple_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;

    async fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        common::storage_container(
            StorageType::Lite,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn simple_non_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn with_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::with_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn with_multiple_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::with_multiple_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "turso")]
mod turso {
    use super::*;

    async fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        common::storage_container(
            StorageType::Turso,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[ignore]
    #[tokio::test]
    async fn simple_non_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[ignore]
    #[tokio::test]
    async fn with_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::with_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[ignore]
    #[tokio::test]
    async fn with_multiple_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::with_multiple_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}
