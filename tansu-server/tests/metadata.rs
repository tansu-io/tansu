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

use common::{alphanumeric_string, register_broker};
use tansu_kafka_sans_io::{create_topics_request::CreatableTopic, ErrorCode};
use tansu_server::Result;
use tansu_storage::{Storage, StorageContainer, TopicId, NULL_TOPIC_ID};
use tracing::debug;
use url::Url;
use uuid::Uuid;

pub mod common;

pub async fn topics_none(
    cluster_id: Uuid,
    broker_id: i32,
    advertised_listener: Url,
    mut sc: StorageContainer,
) -> Result<()> {
    debug!(%cluster_id, broker_id, %advertised_listener);
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let id = sc
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
        .await
        .inspect(|topic_id| debug!(?topic_id))?;

    let metadata = sc
        .metadata(None)
        .await
        .inspect(|metadata| debug!(?metadata))?;

    assert_eq!(1, metadata.brokers().len());
    assert_eq!(broker_id, metadata.brokers()[0].node_id);
    assert_eq!(
        advertised_listener.host_str().unwrap_or("0.0.0.0"),
        metadata.brokers()[0].host
    );
    assert_eq!(
        advertised_listener.port().unwrap_or(9092) as i32,
        metadata.brokers()[0].port
    );

    assert_eq!(1, metadata.topics().len());
    assert_eq!(i16::from(ErrorCode::None), metadata.topics()[0].error_code);
    assert_eq!(Some(topic_name), metadata.topics()[0].name);
    assert_eq!(Some(id.as_bytes()), metadata.topics()[0].topic_id.as_ref());

    let partitions = metadata.topics()[0]
        .partitions
        .as_deref()
        .unwrap_or_default();

    assert_eq!(num_partitions, partitions.len() as i32);

    assert!(partitions
        .iter()
        .map(|partition| partition.leader_id)
        .inspect(|leader_id| debug!(leader_id))
        .all(|leader_id| leader_id == broker_id));

    assert!(partitions
        .iter()
        .map(|partition| partition.leader_epoch)
        .inspect(|leader_epoch| debug!(leader_epoch))
        .all(|leader_epoch| leader_epoch == Some(-1)));

    assert!(partitions
        .iter()
        .map(|partition| partition.offline_replicas.as_deref().unwrap_or_default())
        .inspect(|offline_replicas| debug!(?offline_replicas))
        .all(|offline_replicas| offline_replicas.is_empty()));

    assert!(partitions
        .iter()
        .map(|partition| partition.replica_nodes.as_deref().unwrap_or_default())
        .inspect(|replica_nodes| debug!(?replica_nodes))
        .all(
            |replica_nodes| (replica_nodes.len() as i16) == replication_factor
                && replica_nodes
                    .iter()
                    .all(|replica_node| *replica_node == broker_id)
        ));

    assert!(partitions
        .iter()
        .map(|partition| partition.isr_nodes.as_deref().unwrap_or_default())
        .inspect(|isr_nodes| debug!(?isr_nodes))
        .all(|isr_nodes| {
            (isr_nodes.len() as i16) == replication_factor
                && isr_nodes.iter().all(|isr_node| *isr_node == broker_id)
        }));

    Ok(())
}

pub async fn topics_some_empty(
    cluster_id: Uuid,
    broker_id: i32,
    advertised_listener: Url,
    mut sc: StorageContainer,
) -> Result<()> {
    debug!(%cluster_id, broker_id, %advertised_listener);
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let id = sc
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
        .await
        .inspect(|topic_id| debug!(?topic_id))?;

    let metadata = sc
        .metadata(Some([].as_slice()))
        .await
        .inspect(|metadata| debug!(?metadata))?;

    assert_eq!(1, metadata.brokers().len());
    assert_eq!(broker_id, metadata.brokers()[0].node_id);
    assert_eq!(
        advertised_listener.host_str().unwrap_or("0.0.0.0"),
        metadata.brokers()[0].host
    );
    assert_eq!(
        advertised_listener.port().unwrap_or(9092) as i32,
        metadata.brokers()[0].port
    );

    assert_eq!(1, metadata.topics().len());
    assert_eq!(i16::from(ErrorCode::None), metadata.topics()[0].error_code);
    assert_eq!(Some(topic_name), metadata.topics()[0].name);
    assert_eq!(Some(id.as_bytes()), metadata.topics()[0].topic_id.as_ref());

    let partitions = metadata.topics()[0]
        .partitions
        .as_deref()
        .unwrap_or_default();

    assert_eq!(num_partitions, partitions.len() as i32);

    assert!(partitions
        .iter()
        .map(|partition| partition.leader_id)
        .inspect(|leader_id| debug!(leader_id))
        .all(|leader_id| leader_id == broker_id));

    assert!(partitions
        .iter()
        .map(|partition| partition.leader_epoch)
        .inspect(|leader_epoch| debug!(leader_epoch))
        .all(|leader_epoch| leader_epoch == Some(-1)));

    assert!(partitions
        .iter()
        .map(|partition| partition.offline_replicas.as_deref().unwrap_or_default())
        .inspect(|offline_replicas| debug!(?offline_replicas))
        .all(|offline_replicas| offline_replicas.is_empty()));

    assert!(partitions
        .iter()
        .map(|partition| partition.replica_nodes.as_deref().unwrap_or_default())
        .inspect(|replica_nodes| debug!(?replica_nodes))
        .all(
            |replica_nodes| (replica_nodes.len() as i16) == replication_factor
                && replica_nodes
                    .iter()
                    .all(|replica_node| *replica_node == broker_id)
        ));

    assert!(partitions
        .iter()
        .map(|partition| partition.isr_nodes.as_deref().unwrap_or_default())
        .inspect(|isr_nodes| debug!(?isr_nodes))
        .all(|isr_nodes| {
            (isr_nodes.len() as i16) == replication_factor
                && isr_nodes.iter().all(|isr_node| *isr_node == broker_id)
        }));

    Ok(())
}

pub async fn topics_some_matching_by_name(
    cluster_id: Uuid,
    broker_id: i32,
    advertised_listener: Url,
    mut sc: StorageContainer,
) -> Result<()> {
    debug!(%cluster_id, broker_id, %advertised_listener);
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let id = sc
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
        .await
        .inspect(|topic_id| debug!(?topic_id))?;

    let metadata = sc
        .metadata(Some([TopicId::Name(topic_name.clone())].as_slice()))
        .await
        .inspect(|metadata| debug!(?metadata))?;

    assert_eq!(1, metadata.brokers().len());
    assert_eq!(broker_id, metadata.brokers()[0].node_id);
    assert_eq!(
        advertised_listener.host_str().unwrap_or("0.0.0.0"),
        metadata.brokers()[0].host
    );
    assert_eq!(
        advertised_listener.port().unwrap_or(9092) as i32,
        metadata.brokers()[0].port
    );

    assert_eq!(1, metadata.topics().len());
    assert_eq!(i16::from(ErrorCode::None), metadata.topics()[0].error_code);
    assert_eq!(Some(topic_name), metadata.topics()[0].name);
    assert_eq!(Some(id.as_bytes()), metadata.topics()[0].topic_id.as_ref());

    let partitions = metadata.topics()[0]
        .partitions
        .as_deref()
        .unwrap_or_default();

    assert_eq!(num_partitions, partitions.len() as i32);

    assert!(partitions
        .iter()
        .map(|partition| partition.leader_id)
        .inspect(|leader_id| debug!(leader_id))
        .all(|leader_id| leader_id == broker_id));

    assert!(partitions
        .iter()
        .map(|partition| partition.leader_epoch)
        .inspect(|leader_epoch| debug!(leader_epoch))
        .all(|leader_epoch| leader_epoch == Some(-1)));

    assert!(partitions
        .iter()
        .map(|partition| partition.offline_replicas.as_deref().unwrap_or_default())
        .inspect(|offline_replicas| debug!(?offline_replicas))
        .all(|offline_replicas| offline_replicas.is_empty()));

    assert!(partitions
        .iter()
        .map(|partition| partition.replica_nodes.as_deref().unwrap_or_default())
        .inspect(|replica_nodes| debug!(?replica_nodes))
        .all(
            |replica_nodes| (replica_nodes.len() as i16) == replication_factor
                && replica_nodes
                    .iter()
                    .all(|replica_node| *replica_node == broker_id)
        ));

    assert!(partitions
        .iter()
        .map(|partition| partition.isr_nodes.as_deref().unwrap_or_default())
        .inspect(|isr_nodes| debug!(?isr_nodes))
        .all(|isr_nodes| {
            (isr_nodes.len() as i16) == replication_factor
                && isr_nodes.iter().all(|isr_node| *isr_node == broker_id)
        }));

    Ok(())
}

pub async fn topics_some_not_matching_by_name(
    cluster_id: Uuid,
    broker_id: i32,
    advertised_listener: Url,
    mut sc: StorageContainer,
) -> Result<()> {
    debug!(%cluster_id, broker_id, %advertised_listener);
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let metadata = sc
        .metadata(Some([TopicId::Name(topic_name.clone())].as_slice()))
        .await
        .inspect(|metadata| debug!(?metadata))?;

    assert_eq!(1, metadata.brokers().len());
    assert_eq!(broker_id, metadata.brokers()[0].node_id);
    assert_eq!(
        advertised_listener.host_str().unwrap_or("0.0.0.0"),
        metadata.brokers()[0].host
    );
    assert_eq!(
        advertised_listener.port().unwrap_or(9092) as i32,
        metadata.brokers()[0].port
    );

    assert_eq!(1, metadata.topics().len());
    assert_eq!(
        i16::from(ErrorCode::UnknownTopicOrPartition),
        metadata.topics()[0].error_code
    );
    assert_eq!(Some(topic_name), metadata.topics()[0].name);
    assert_eq!(Some(&NULL_TOPIC_ID), metadata.topics()[0].topic_id.as_ref());

    Ok(())
}

pub async fn topics_some_matching_by_id(
    cluster_id: Uuid,
    broker_id: i32,
    advertised_listener: Url,
    mut sc: StorageContainer,
) -> Result<()> {
    debug!(%cluster_id, broker_id, %advertised_listener);
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let id = sc
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
        .await
        .inspect(|topic_id| debug!(?topic_id))?;

    let metadata = sc
        .metadata(Some([TopicId::Id(id)].as_slice()))
        .await
        .inspect(|metadata| debug!(?metadata))?;

    assert_eq!(1, metadata.brokers().len());
    assert_eq!(broker_id, metadata.brokers()[0].node_id);
    assert_eq!(
        advertised_listener.host_str().unwrap_or("0.0.0.0"),
        metadata.brokers()[0].host
    );
    assert_eq!(
        advertised_listener.port().unwrap_or(9092) as i32,
        metadata.brokers()[0].port
    );

    assert_eq!(1, metadata.topics().len());
    assert_eq!(i16::from(ErrorCode::None), metadata.topics()[0].error_code);
    assert_eq!(Some(topic_name), metadata.topics()[0].name);
    assert_eq!(Some(id.as_bytes()), metadata.topics()[0].topic_id.as_ref());

    let partitions = metadata.topics()[0]
        .partitions
        .as_deref()
        .unwrap_or_default();

    assert_eq!(num_partitions, partitions.len() as i32);

    assert!(partitions
        .iter()
        .map(|partition| partition.leader_id)
        .inspect(|leader_id| debug!(leader_id))
        .all(|leader_id| leader_id == broker_id));

    assert!(partitions
        .iter()
        .map(|partition| partition.leader_epoch)
        .inspect(|leader_epoch| debug!(leader_epoch))
        .all(|leader_epoch| leader_epoch == Some(-1)));

    assert!(partitions
        .iter()
        .map(|partition| partition.offline_replicas.as_deref().unwrap_or_default())
        .inspect(|offline_replicas| debug!(?offline_replicas))
        .all(|offline_replicas| offline_replicas.is_empty()));

    assert!(partitions
        .iter()
        .map(|partition| partition.replica_nodes.as_deref().unwrap_or_default())
        .inspect(|replica_nodes| debug!(?replica_nodes))
        .all(
            |replica_nodes| (replica_nodes.len() as i16) == replication_factor
                && replica_nodes
                    .iter()
                    .all(|replica_node| *replica_node == broker_id)
        ));

    assert!(partitions
        .iter()
        .map(|partition| partition.isr_nodes.as_deref().unwrap_or_default())
        .inspect(|isr_nodes| debug!(?isr_nodes))
        .all(|isr_nodes| {
            (isr_nodes.len() as i16) == replication_factor
                && isr_nodes.iter().all(|isr_node| *isr_node == broker_id)
        }));

    Ok(())
}

pub async fn topics_some_not_matching_by_id(
    cluster_id: Uuid,
    broker_id: i32,
    advertised_listener: Url,
    mut sc: StorageContainer,
) -> Result<()> {
    debug!(%cluster_id, broker_id, %advertised_listener);
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let id = Uuid::new_v4();

    let metadata = sc
        .metadata(Some([TopicId::Id(id)].as_slice()))
        .await
        .inspect(|metadata| debug!(?metadata))?;

    assert_eq!(1, metadata.brokers().len());
    assert_eq!(broker_id, metadata.brokers()[0].node_id);
    assert_eq!(
        advertised_listener.host_str().unwrap_or("0.0.0.0"),
        metadata.brokers()[0].host
    );
    assert_eq!(
        advertised_listener.port().unwrap_or(9092) as i32,
        metadata.brokers()[0].port
    );

    assert_eq!(1, metadata.topics().len());
    assert_eq!(
        i16::from(ErrorCode::UnknownTopicOrPartition),
        metadata.topics()[0].error_code
    );
    assert_eq!(None, metadata.topics()[0].name);
    assert_eq!(Some(id.as_bytes()), metadata.topics()[0].topic_id.as_ref());

    Ok(())
}

mod pg {
    use common::{init_tracing, StorageType};
    use rand::{prelude::*, thread_rng};

    use super::*;

    fn storage_container(
        cluster: impl Into<String>,
        node: i32,
        advertised_listener: Url,
    ) -> Result<StorageContainer> {
        common::storage_container(StorageType::Postgres, cluster, node, advertised_listener)
    }

    #[tokio::test]
    async fn topics_none() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = Uuid::now_v7();
        let node = thread_rng().gen_range(0..i32::MAX);
        let advertised_listener = Url::parse("tcp://example.com:9092/")?;

        super::topics_none(
            cluster,
            node,
            advertised_listener.clone(),
            storage_container(cluster, node, advertised_listener)?,
        )
        .await
    }

    #[tokio::test]
    async fn topics_some_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = Uuid::now_v7();
        let node = thread_rng().gen_range(0..i32::MAX);
        let advertised_listener = Url::parse("tcp://example.com:9092/")?;

        super::topics_some_empty(
            cluster,
            node,
            advertised_listener.clone(),
            storage_container(cluster, node, advertised_listener)?,
        )
        .await
    }

    #[tokio::test]
    async fn topics_some_matching_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = Uuid::now_v7();
        let node = thread_rng().gen_range(0..i32::MAX);
        let advertised_listener = Url::parse("tcp://example.com:9092/")?;

        super::topics_some_matching_by_name(
            cluster,
            node,
            advertised_listener.clone(),
            storage_container(cluster, node, advertised_listener)?,
        )
        .await
    }

    #[tokio::test]
    async fn topics_some_not_matching_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = Uuid::now_v7();
        let node = thread_rng().gen_range(0..i32::MAX);
        let advertised_listener = Url::parse("tcp://example.com:9092/")?;

        super::topics_some_not_matching_by_name(
            cluster,
            node,
            advertised_listener.clone(),
            storage_container(cluster, node, advertised_listener)?,
        )
        .await
    }

    #[tokio::test]
    async fn topics_some_matching_by_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = Uuid::now_v7();
        let node = thread_rng().gen_range(0..i32::MAX);
        let advertised_listener = Url::parse("tcp://example.com:9092/")?;

        super::topics_some_matching_by_id(
            cluster,
            node,
            advertised_listener.clone(),
            storage_container(cluster, node, advertised_listener)?,
        )
        .await
    }

    #[tokio::test]
    async fn topics_some_not_matching_by_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = Uuid::now_v7();
        let node = thread_rng().gen_range(0..i32::MAX);
        let advertised_listener = Url::parse("tcp://example.com:9092/")?;

        super::topics_some_not_matching_by_id(
            cluster,
            node,
            advertised_listener.clone(),
            storage_container(cluster, node, advertised_listener)?,
        )
        .await
    }
}
