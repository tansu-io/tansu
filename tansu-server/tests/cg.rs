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

use common::{alphanumeric_string, init_tracing, register_broker, StorageType};
use rand::{prelude::*, thread_rng};
use tansu_kafka_sans_io::{create_topics_request::CreatableTopic, ErrorCode};
use tansu_server::Result;
use tansu_storage::{OffsetCommitRequest, Storage, StorageContainer, Topition};
use tracing::debug;
use url::Url;
use uuid::Uuid;

pub mod common;

pub async fn offset_commit(
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

    let partition_index = thread_rng().gen_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let group_id: String = alphanumeric_string(15);

    let offset = thread_rng().gen_range(0..i64::MAX);

    let commit = sc
        .offset_commit(
            &group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;

    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), &[topition.clone()], None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(Some(&offset), offset_fetch.get(&topition));

    let co_tps = sc.committed_offset_topitions(&group_id).await?;
    assert!(co_tps.contains_key(&topition));
    assert_eq!(Some(&offset), co_tps.get(&topition));

    let groups = sc.list_groups(None).await?;
    assert_eq!(1, groups.len());
    assert_eq!(group_id, groups[0].group_id);

    Ok(())
}

pub async fn topic_delete_cascade_to_offset_commit(
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

    let partition_index = thread_rng().gen_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let group_id: String = alphanumeric_string(15);

    let offset = thread_rng().gen_range(0..i64::MAX);

    let commit = sc
        .offset_commit(
            &group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;

    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), &[topition.clone()], None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(Some(&offset), offset_fetch.get(&topition));

    assert_eq!(ErrorCode::None, sc.delete_topic(&topic_name.into()).await?);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), &[topition.clone()], None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(Some(&-1), offset_fetch.get(&topition));

    Ok(())
}

pub async fn consumer_group_delete_cascade_to_offset_commit(
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

    let partition_index = thread_rng().gen_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let group_id: String = alphanumeric_string(15);

    let offset = thread_rng().gen_range(0..i64::MAX);

    let commit = sc
        .offset_commit(
            &group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;

    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), &[topition.clone()], None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(Some(&offset), offset_fetch.get(&topition));

    let deleted = sc.delete_groups(Some(&[group_id.clone()])).await?;
    assert_eq!(1, deleted.len());
    assert_eq!(group_id, deleted[0].group_id);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(deleted[0].error_code)?);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), &[topition.clone()], None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(Some(&-1), offset_fetch.get(&topition));

    Ok(())
}

pub async fn delete_unknown_consumer_group(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let group_id: String = alphanumeric_string(15);

    let deleted = sc.delete_groups(Some(&[group_id.clone()])).await?;
    assert_eq!(1, deleted.len());
    assert_eq!(group_id, deleted[0].group_id);
    assert_eq!(
        ErrorCode::GroupIdNotFound,
        ErrorCode::try_from(deleted[0].error_code)?
    );

    Ok(())
}

pub async fn offset_commit_unknown_topition(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;

    let partition_index = thread_rng().gen_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let group_id: String = alphanumeric_string(15);

    let offset = thread_rng().gen_range(0..i64::MAX);

    let commit = sc
        .offset_commit(
            &group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;

    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::UnknownTopicOrPartition, commit[0].1);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), &[topition.clone()], None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(Some(&-1), offset_fetch.get(&topition));

    let groups = sc.list_groups(None).await?;
    assert_eq!(0, groups.len());

    Ok(())
}

pub async fn offset_fetch_unknown_topition(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;

    let partition_index = thread_rng().gen_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let group_id: String = alphanumeric_string(15);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), &[topition.clone()], None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(Some(&-1), offset_fetch.get(&topition));

    let groups = sc.list_groups(None).await?;
    assert_eq!(0, groups.len());

    Ok(())
}

pub async fn list_groups_none(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let groups = sc.list_groups(None).await?;
    assert_eq!(0, groups.len());

    Ok(())
}

mod pg {
    use super::*;

    fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        Url::parse("tcp://127.0.0.1/")
            .map_err(Into::into)
            .and_then(|advertised_listener| {
                common::storage_container(StorageType::Postgres, cluster, node, advertised_listener)
            })
    }

    #[tokio::test]
    async fn offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = thread_rng().gen_range(0..i32::MAX);

        super::offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn topic_delete_cascade_to_offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = thread_rng().gen_range(0..i32::MAX);

        super::topic_delete_cascade_to_offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn consumer_group_delete_cascade_to_offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = thread_rng().gen_range(0..i32::MAX);

        super::consumer_group_delete_cascade_to_offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_unknown_consumer_group() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = thread_rng().gen_range(0..i32::MAX);

        super::delete_unknown_consumer_group(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn offset_commit_unknown_topition() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = thread_rng().gen_range(0..i32::MAX);

        super::offset_commit_unknown_topition(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn offset_fetch_unknown_topition() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = thread_rng().gen_range(0..i32::MAX);

        super::offset_fetch_unknown_topition(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn list_groups_none() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = thread_rng().gen_range(0..i32::MAX);

        super::list_groups_none(
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
                common::storage_container(StorageType::InMemory, cluster, node, advertised_listener)
            })
    }

    #[tokio::test]
    async fn offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = thread_rng().gen_range(0..i32::MAX);

        super::offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn topic_delete_cascade_to_offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = thread_rng().gen_range(0..i32::MAX);

        super::topic_delete_cascade_to_offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn consumer_group_delete_cascade_to_offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = thread_rng().gen_range(0..i32::MAX);

        super::consumer_group_delete_cascade_to_offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[ignore]
    #[tokio::test]
    async fn delete_unknown_consumer_group() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = thread_rng().gen_range(0..i32::MAX);

        super::delete_unknown_consumer_group(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn offset_commit_unknown_topition() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = thread_rng().gen_range(0..i32::MAX);

        super::offset_commit_unknown_topition(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn offset_fetch_unknown_topition() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = thread_rng().gen_range(0..i32::MAX);

        super::offset_fetch_unknown_topition(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn list_groups_none() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = thread_rng().gen_range(0..i32::MAX);

        super::list_groups_none(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }
}
