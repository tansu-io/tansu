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

use bytes::Bytes;
use common::{StorageType, alphanumeric_string, init_tracing, register_broker};
use rand::{prelude::*, rng};
use tansu_kafka_sans_io::{
    IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, inflated},
};
use tansu_server::Result;
use tansu_storage::{ListOffsetRequest, Storage, StorageContainer, Topition};
use tracing::debug;
use url::Url;
use uuid::Uuid;

pub mod common;

pub async fn new_topic(cluster_id: Uuid, broker_id: i32, mut sc: StorageContainer) -> Result<()> {
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

    let offsets = (0..num_partitions)
        .map(|partition| {
            (
                Topition::new(topic_name.clone(), partition),
                ListOffsetRequest::Latest,
            )
        })
        .collect::<Vec<_>>();

    for (_toptition, response) in sc
        .list_offsets(IsolationLevel::ReadUncommitted, &offsets[..])
        .await?
    {
        assert_eq!(Some(0), response.offset);
        assert_eq!(None, response.timestamp);
    }

    Ok(())
}

pub async fn single_record(
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

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let value = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());

    let batch = inflated::Batch::builder()
        .record(Record::builder().value(value.clone().into()))
        .build()
        .and_then(TryInto::try_into)
        .inspect(|deflated| debug!(?deflated))?;

    assert_eq!(
        0,
        sc.produce(None, &topition, batch)
            .await
            .inspect(|offset| debug!(?offset))?
    );

    let offsets = [(topition.clone(), ListOffsetRequest::Latest)];

    let responses = sc
        .list_offsets(IsolationLevel::ReadUncommitted, &offsets[..])
        .await?;

    assert_eq!(1, responses.len());
    assert_eq!(Some(1), responses[0].1.offset);

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
    async fn new_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::new_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn single_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_record(
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
    async fn new_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::new_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn single_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_record(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }
}
