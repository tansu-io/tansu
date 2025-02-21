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
use common::register_broker;
use rand::{prelude::*, rng};
use serde_json::json;
use tansu_kafka_sans_io::{
    ErrorCode,
    create_topics_request::CreatableTopic,
    record::{Record, inflated::Batch},
};
use tansu_server::Result;
use tansu_storage::{Error, Storage, StorageContainer, Topition};
use tracing::{debug, error};
use uuid::Uuid;

pub mod common;

pub async fn person_valid(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name = "person";
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic {
                name: topic_name.into(),
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
    let topition = Topition::new(topic_name.to_owned(), partition_index);

    let key = serde_json::to_vec(&json!("ABC-123")).map(Bytes::from)?;

    let value = serde_json::to_vec(&json!({
      "firstName": "John",
      "lastName": "Doe",
      "age": 21
    }))
    .map(Bytes::from)?;

    let batch = Batch::builder()
        .record(
            Record::builder()
                .key(key.clone().into())
                .value(value.clone().into()),
        )
        .build()
        .and_then(TryInto::try_into)
        .inspect(|deflated| debug!(?deflated))?;

    let _offset = sc
        .produce(None, &topition, batch)
        .await
        .inspect(|offset| debug!(?offset))?;

    Ok(())
}

pub async fn person_invalid(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic_name = "person";
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic {
                name: topic_name.into(),
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
    let topition = Topition::new(topic_name.to_owned(), partition_index);

    let key = serde_json::to_vec(&json!("ABC-123")).map(Bytes::from)?;

    let value = serde_json::to_vec(&json!({
      "firstName": "John",
      "lastName": "Doe",
      "age": -1,
    }))
    .map(Bytes::from)?;

    let batch = Batch::builder()
        .record(
            Record::builder()
                .key(key.clone().into())
                .value(value.clone().into()),
        )
        .build()
        .and_then(TryInto::try_into)
        .inspect(|deflated| debug!(?deflated))?;

    assert!(matches!(
        sc.produce(None, &topition, batch)
            .await
            .inspect(|offset| debug!(?offset))
            .inspect_err(|err| error!(?err)),
        Err(Error::Api(ErrorCode::InvalidRecord))
    ));

    Ok(())
}

mod pg {
    use std::env;

    use common::{StorageType, init_tracing};
    use tansu_schema_registry::Registry;
    use tansu_server::Error;
    use url::Url;

    use super::*;

    fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        let current_dir = env::current_dir()?;
        debug!(?current_dir);

        let schemas = Url::parse("file://../etc/schema")
            .map_err(Error::from)
            .and_then(|url| Registry::try_from(url).map_err(Into::into))
            .map(Some)?;

        Url::parse("tcp://127.0.0.1/")
            .map_err(Into::into)
            .and_then(|advertised_listener| {
                common::storage_container(
                    StorageType::Postgres,
                    cluster,
                    node,
                    advertised_listener,
                    schemas,
                )
            })
    }

    #[tokio::test]
    async fn person_valid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::person_valid(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn person_invalid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::person_invalid(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }
}

mod in_memory {
    use std::env;

    use common::{StorageType, init_tracing};
    use tansu_schema_registry::Registry;
    use tansu_server::Error;
    use url::Url;

    use super::*;

    fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        let current_dir = env::current_dir()?;
        debug!(?current_dir);

        let schemas = Url::parse("file://../etc/schema")
            .map_err(Error::from)
            .and_then(|url| Registry::try_from(url).map_err(Into::into))
            .map(Some)?;

        Url::parse("tcp://127.0.0.1/")
            .map_err(Into::into)
            .and_then(|advertised_listener| {
                common::storage_container(
                    StorageType::InMemory,
                    cluster,
                    node,
                    advertised_listener,
                    schemas,
                )
            })
    }

    #[tokio::test]
    async fn person_valid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::person_valid(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn person_invalid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::person_invalid(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }
}
