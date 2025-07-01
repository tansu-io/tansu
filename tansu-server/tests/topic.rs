// Copyright ⓒ 2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use common::{alphanumeric_string, register_broker};
use tansu_sans_io::{
    ErrorCode, NULL_TOPIC_ID,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
};
use tansu_server::Result;
use tansu_storage::{Storage, StorageContainer, TopicId};
use tracing::debug;
use uuid::Uuid;

pub mod common;

pub async fn create_delete(
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

    assert_eq!(
        ErrorCode::None,
        sc.delete_topic(&TopicId::from(topic_id)).await?
    );

    Ok(())
}

pub async fn create_describe_topic_partitions_by_id(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 3;

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

    let responses = sc
        .describe_topic_partitions(Some(&[topic_id.into()]), 32123, None)
        .await?;

    assert_eq!(1, responses.len());

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(responses[0].error_code)?
    );
    assert_eq!(Some(topic_name), responses[0].name);
    assert_eq!(topic_id.as_bytes(), &responses[0].topic_id);
    assert!(!responses[0].is_internal);
    assert_eq!(
        num_partitions as usize,
        responses[0]
            .partitions
            .as_ref()
            .map(|partitions| partitions.len())
            .unwrap_or_default()
    );

    for partition in responses[0].partitions.as_deref().unwrap_or_default() {
        assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
        assert_eq!(broker_id, partition.leader_id);
        assert_eq!(-1, partition.leader_epoch);
        assert_eq!(
            Some(vec![broker_id; replication_factor as usize]),
            partition.replica_nodes
        );
        assert_eq!(
            Some(vec![broker_id; replication_factor as usize]),
            partition.isr_nodes
        );
        assert_eq!(Some(vec![]), partition.eligible_leader_replicas);
        assert_eq!(Some(vec![]), partition.last_known_elr);
        assert_eq!(Some(vec![]), partition.offline_replicas);
    }

    Ok(())
}

pub async fn create_describe_topic_partitions_by_name(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 3;

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

    let responses = sc
        .describe_topic_partitions(Some(&[topic_name.clone().into()]), 32123, None)
        .await?;

    assert_eq!(1, responses.len());

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(responses[0].error_code)?
    );
    assert_eq!(Some(topic_name), responses[0].name);
    // assert_eq!(topic_id.as_bytes(), &responses[0].topic_id);
    assert!(!responses[0].is_internal);
    assert_eq!(
        num_partitions as usize,
        responses[0]
            .partitions
            .as_ref()
            .map(|partitions| partitions.len())
            .unwrap_or_default()
    );

    for partition in responses[0].partitions.as_deref().unwrap_or_default() {
        assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
        assert_eq!(broker_id, partition.leader_id);
        assert_eq!(-1, partition.leader_epoch);
        assert_eq!(
            Some(vec![broker_id; replication_factor as usize]),
            partition.replica_nodes
        );
        assert_eq!(
            Some(vec![broker_id; replication_factor as usize]),
            partition.isr_nodes
        );
        assert_eq!(Some(vec![]), partition.eligible_leader_replicas);
        assert_eq!(Some(vec![]), partition.last_known_elr);
        assert_eq!(Some(vec![]), partition.offline_replicas);
    }

    Ok(())
}

pub async fn describe_non_existing_topic_partitions_by_name(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let responses = sc
        .describe_topic_partitions(Some(&[topic_name.clone().into()]), 32123, None)
        .await?;

    assert_eq!(1, responses.len());

    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(responses[0].error_code)?
    );
    assert_eq!(Some(topic_name), responses[0].name);
    assert_eq!(&NULL_TOPIC_ID, &responses[0].topic_id);
    assert!(!responses[0].is_internal);
    assert_eq!(
        0,
        responses[0]
            .partitions
            .as_ref()
            .map(|partitions| partitions.len())
            .unwrap_or_default()
    );

    Ok(())
}

pub async fn create_with_config_delete(
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
    let configs = Some(
        [CreatableTopicConfig {
            name: "xyz".into(),
            value: Some("12321".into()),
        }]
        .into(),
    );

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

    assert_eq!(
        ErrorCode::None,
        sc.delete_topic(&TopicId::from(topic_id)).await?
    );

    Ok(())
}

mod pg {
    use common::{StorageType, init_tracing};
    use rand::{prelude::*, rng};
    use url::Url;

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
    async fn create_describe_topic_partitions_by_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_describe_topic_partitions_by_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn create_describe_topic_partitions_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_describe_topic_partitions_by_name(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn describe_non_existing_topic_partitions_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::describe_non_existing_topic_partitions_by_name(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn create_delete() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_delete(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn create_with_config_delete() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_with_config_delete(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }
}

mod in_memory {
    use common::{StorageType, init_tracing};
    use rand::{prelude::*, rng};
    use url::Url;

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
    async fn create_delete() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_delete(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn create_describe_topic_partitions_by_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_describe_topic_partitions_by_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn create_describe_topic_partitions_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_describe_topic_partitions_by_name(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn describe_non_existing_topic_partitions_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::describe_non_existing_topic_partitions_by_name(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn create_with_config_delete() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_with_config_delete(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }
}
