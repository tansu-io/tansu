// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::{collections::BTreeMap, time::Duration};

use bytes::Bytes;
use common::{StorageType, alphanumeric_string, init_tracing, register_broker};
use rama::{Context, Service};
use rand::{prelude::*, rng};
use tansu_broker::Result;
use tansu_sans_io::{
    ErrorCode, FetchRequest, IsolationLevel, ListOffset, NULL_TOPIC_ID,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
    record::{Record, inflated},
};
use tansu_storage::{FetchService, ListOffsetResponse, Storage, Topition};
use tracing::{debug, error};
use url::Url;
use uuid::Uuid;

pub mod common;

pub async fn empty_topic<G>(cluster_id: Uuid, broker_id: i32, sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, sc.clone()).await?;

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
        .await?;
    debug!(?topic_id);

    let record_count = 0;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let max_wait_ms = 500;
    let min_bytes = 1;
    let max_bytes = Some(50 * 1024);
    let isolation_level = &IsolationLevel::ReadUncommitted;
    let topics = [FetchTopic::default()
        .topic(Some(topition.topic().to_string()))
        .topic_id(Some(NULL_TOPIC_ID))
        .partitions(Some(vec![
            FetchPartition::default()
                .partition(topition.partition())
                .current_leader_epoch(Some(-1))
                .fetch_offset(0)
                .last_fetched_epoch(Some(-1))
                .log_start_offset(Some(-1))
                .partition_max_bytes(50 * 1024)
                .replica_directory_id(None),
        ]))];

    let ctx = Context::with_state(sc);

    let fetch = FetchService
        .serve(
            ctx,
            FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(min_bytes)
                .max_bytes(max_bytes)
                .isolation_level(Some(isolation_level.into()))
                .topics(Some(topics.into())),
        )
        .await?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(fetch.error_code.unwrap())?
    );

    for response in fetch.responses.as_ref().unwrap_or(&vec![]) {
        for partition in response.partitions.as_ref().unwrap_or(&vec![]) {
            for batch in &partition.records.as_ref().unwrap().batches {
                assert_eq!(-1, batch.producer_id);
                assert_eq!(-1, batch.producer_epoch);
            }
        }
    }

    assert_eq!(
        record_count,
        fetch
            .responses
            .unwrap_or_default()
            .iter()
            .map(|response| {
                response
                    .partitions
                    .as_deref()
                    .unwrap_or(&[])
                    .iter()
                    .map(|partition| {
                        partition
                            .records
                            .as_ref()
                            .unwrap()
                            .batches
                            .iter()
                            .map(|batch| batch.record_count as i64)
                            .sum::<i64>()
                    })
                    .sum::<i64>()
            })
            .sum::<i64>()
    );

    Ok(())
}

pub async fn simple_non_txn<C, G>(cluster_id: C, broker_id: i32, sc: G) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, sc.clone()).await?;

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
        .await?;
    debug!(?topic_id);

    let transaction_timeout_ms = 10_000;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let list_offsets = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Latest)],
        )
        .await
        .inspect_err(|err| error!(?err))
        .inspect(|list_offsets| debug!(?list_offsets))?;

    assert_eq!(1, list_offsets.len());

    assert!(matches!(
        list_offsets[0].1,
        ListOffsetResponse {
            offset: Some(0),
            ..
        }
    ));

    let record_count = 6;

    let mut offset_producer = BTreeMap::new();
    let mut bytes = 0;

    for n in 0..record_count {
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

        debug!(offset);

        assert_eq!(None, offset_producer.insert(offset, (producer, value)));
    }

    let list_offsets = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Latest)],
        )
        .await
        .inspect_err(|err| error!(?err))
        .inspect(|list_offsets| debug!(?list_offsets))?;

    assert_eq!(1, list_offsets.len());

    assert!(matches!(
        list_offsets[0].1,
        ListOffsetResponse {
            offset: Some(records),
            ..
            } if records == record_count
    ));

    let max_wait_ms = 500;
    let min_bytes = 1;
    let max_bytes = Some(50 * 1024);
    let isolation_level = &IsolationLevel::ReadUncommitted;
    let topics = [FetchTopic::default()
        .topic(Some(topition.topic().to_string()))
        .topic_id(Some(NULL_TOPIC_ID))
        .partitions(Some(vec![
            FetchPartition::default()
                .partition(topition.partition())
                .current_leader_epoch(Some(-1))
                .fetch_offset(0)
                .last_fetched_epoch(Some(-1))
                .log_start_offset(Some(-1))
                .partition_max_bytes(50 * 1024)
                .replica_directory_id(None),
        ]))];

    let ctx = Context::with_state(sc);

    let fetch = FetchService
        .serve(
            ctx,
            FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(min_bytes)
                .max_bytes(max_bytes)
                .isolation_level(Some(isolation_level.into()))
                .topics(Some(topics.into())),
        )
        .await?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(fetch.error_code.unwrap())?
    );

    assert_eq!(
        record_count,
        fetch
            .responses
            .unwrap_or_default()
            .iter()
            .map(|response| {
                response
                    .partitions
                    .as_deref()
                    .unwrap_or(&[])
                    .iter()
                    .map(|partition| {
                        partition
                            .records
                            .as_ref()
                            .unwrap()
                            .batches
                            .iter()
                            .map(|batch| batch.record_count as i64)
                            .sum::<i64>()
                    })
                    .sum::<i64>()
            })
            .sum::<i64>()
    );

    Ok(())
}

/// A fetch offset falling inside a batch returns that batch whole, as
/// Kafka does: the client skips the records below the fetch offset. Only
/// batches ending before the fetch offset may be omitted.
pub async fn mid_batch<C, G>(cluster_id: C, broker_id: i32, sc: G) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, sc.clone()).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;
    debug!(?topic_id);

    let topition = Topition::new(topic_name.clone(), 0);

    let values: Vec<Bytes> = (0..6)
        .map(|_| Bytes::copy_from_slice(alphanumeric_string(15).as_bytes()))
        .collect();

    // two batches of three records: offsets 0..=2 and 3..=5
    for batch_values in values.chunks(3) {
        let producer = sc.init_producer(None, 10_000, Some(-1), Some(-1)).await?;

        let mut builder = inflated::Batch::builder()
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .last_offset_delta(batch_values.len() as i32 - 1);

        for (delta, value) in batch_values.iter().enumerate() {
            builder = builder.record(
                Record::builder()
                    .offset_delta(delta as i32)
                    .value(Some(value.clone())),
            );
        }

        let batch = builder.build().and_then(TryInto::try_into)?;

        _ = sc
            .produce(None, &topition, batch)
            .await
            .inspect(|offset| debug!(offset))?;
    }

    let min_bytes = 1;
    let max_bytes = 50 * 1024;
    let isolation = IsolationLevel::ReadUncommitted;
    let max_wait = Duration::from_millis(500);

    let high_watermark = values.len() as i64;

    for fetch_offset in 0..=high_watermark {
        let batches = sc
            .fetch(
                &topition,
                fetch_offset,
                min_bytes,
                max_bytes,
                isolation,
                max_wait,
            )
            .await
            .inspect_err(|err| error!(?err, fetch_offset))?
            .into_iter()
            .try_fold(Vec::new(), |mut acc, batch| {
                inflated::Batch::try_from(batch)
                    .map(|inflated| {
                        acc.push(inflated);
                        acc
                    })
                    .map_err(tansu_broker::Error::from)
            })?;

        // every record from the fetch offset to the high watermark is
        // returned, in order, and no returned batch ends before the
        // fetch offset
        let mut expected = fetch_offset;

        for batch in &batches {
            // the pg and lite engines return an empty placeholder batch
            // when there are no records to fetch
            if batch.records.is_empty() {
                continue;
            }

            assert!(
                batch.base_offset + i64::from(batch.last_offset_delta) >= fetch_offset,
                "fetch at {fetch_offset} returned a batch ending before it"
            );

            for record in &batch.records {
                let offset = batch.base_offset + i64::from(record.offset_delta);

                // a whole batch can begin before the fetch offset: the
                // client skips the records below it
                if offset < fetch_offset {
                    continue;
                }

                assert_eq!(expected, offset, "fetch at {fetch_offset}");
                assert_eq!(
                    values.get(offset as usize).cloned(),
                    record.value(),
                    "fetch at {fetch_offset}, record at {offset}"
                );

                expected += 1;
            }
        }

        assert_eq!(
            high_watermark, expected,
            "fetch at {fetch_offset} is missing records"
        );
    }

    Ok(())
}

#[cfg(feature = "postgres")]
mod pg {
    use std::sync::Arc;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String>,
        node: i32,
    ) -> Result<Arc<Box<dyn Storage>>> {
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
    async fn empty_topic() -> Result<()> {
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
    async fn mid_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::mid_batch(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use std::sync::Arc;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String>,
        node: i32,
    ) -> Result<Arc<Box<dyn Storage>>> {
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
    async fn empty_topic() -> Result<()> {
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
    async fn mid_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::mid_batch(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use std::sync::Arc;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String>,
        node: i32,
    ) -> Result<Arc<Box<dyn Storage>>> {
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
    async fn empty_topic() -> Result<()> {
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
    async fn mid_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::mid_batch(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use std::sync::Arc;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String>,
        node: i32,
    ) -> Result<Arc<Box<dyn Storage>>> {
        common::storage_container(
            StorageType::SlateDb,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn empty_topic() -> Result<()> {
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
    async fn mid_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::mid_batch(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}
