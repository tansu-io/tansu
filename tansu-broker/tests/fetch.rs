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

use std::collections::BTreeMap;

use bytes::Bytes;
use common::{FetchResponse, StorageType, alphanumeric_string, init_tracing, register_broker};
use rand::{prelude::*, rng};
use tansu_broker::{Result, broker::fetch::FetchRequest};
use tansu_sans_io::{
    ErrorCode, IsolationLevel, NULL_TOPIC_ID,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
    record::{Record, inflated},
};
use tansu_storage::{ListOffsetRequest, ListOffsetResponse, Storage, StorageContainer, Topition};
use tracing::{debug, error};
use url::Url;
use uuid::Uuid;

pub mod common;

pub async fn empty_topic(cluster_id: Uuid, broker_id: i32, mut sc: StorageContainer) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic {
                name: topic_name.clone(),
                num_partitions,
                replication_factor,
                assignments: assignments.clone(),
                configs: configs.clone(),
            },
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
    let topics = [FetchTopic {
        topic: Some(topition.topic().to_string()),
        topic_id: Some(NULL_TOPIC_ID),
        partitions: Some(vec![FetchPartition {
            partition: topition.partition(),
            current_leader_epoch: Some(-1),
            fetch_offset: 0,
            last_fetched_epoch: Some(-1),
            log_start_offset: Some(-1),
            partition_max_bytes: 50 * 1024,
            replica_directory_id: None,
        }]),
    }];

    let fetch: FetchResponse = FetchRequest::with_storage(sc.clone())
        .response(
            max_wait_ms,
            min_bytes,
            max_bytes,
            Some(isolation_level.into()),
            Some(&topics[..]),
        )
        .await
        .and_then(TryInto::try_into)?;

    assert_eq!(ErrorCode::None, fetch.error_code());

    for response in fetch.responses() {
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
            .responses()
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

pub async fn simple_non_txn(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic {
                name: topic_name.clone(),
                num_partitions,
                replication_factor,
                assignments: assignments.clone(),
                configs: configs.clone(),
            },
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
            &[(topition.clone(), ListOffsetRequest::Latest)],
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
            &[(topition.clone(), ListOffsetRequest::Latest)],
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
    let topics = [FetchTopic {
        topic: Some(topition.topic().to_string()),
        topic_id: Some(NULL_TOPIC_ID),
        partitions: Some(vec![FetchPartition {
            partition: topition.partition(),
            current_leader_epoch: Some(-1),
            fetch_offset: 0,
            last_fetched_epoch: Some(-1),
            log_start_offset: Some(-1),
            partition_max_bytes: 50 * 1024,
            replica_directory_id: None,
        }]),
    }];

    let fetch: FetchResponse = FetchRequest::with_storage(sc.clone())
        .response(
            max_wait_ms,
            min_bytes,
            max_bytes,
            Some(isolation_level.into()),
            Some(&topics[..]),
        )
        .await
        .and_then(TryInto::try_into)?;

    assert_eq!(ErrorCode::None, fetch.error_code());

    assert_eq!(
        record_count,
        fetch
            .responses()
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

mod pg {
    use super::*;

    fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        Url::parse("tcp://127.0.0.1/")
            .map_err(Into::into)
            .and_then(|advertised_listener| {
                common::storage_container(
                    StorageType::Postgres,
                    cluster,
                    node,
                    advertised_listener,
                    None,
                )
            })
    }

    #[tokio::test]
    async fn empty_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
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
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }
}

mod in_memory {
    use super::*;

    fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        Url::parse("tcp://127.0.0.1/")
            .map_err(Into::into)
            .and_then(|advertised_listener| {
                common::storage_container(
                    StorageType::InMemory,
                    cluster,
                    node,
                    advertised_listener,
                    None,
                )
            })
    }

    #[tokio::test]
    async fn empty_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
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
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }
}
