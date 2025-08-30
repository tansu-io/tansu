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

use bytes::Bytes;
use common::register_broker;
use rand::{prelude::*, rng};
use serde_json::json;
use tansu_broker::Result;
use tansu_sans_io::{
    ErrorCode,
    create_topics_request::CreatableTopic,
    record::{Record, inflated::Batch},
};
use tansu_storage::{Error, Storage, StorageContainer, Topition};
use tracing::{debug, error};
use uuid::Uuid;

pub mod common;

pub async fn person_valid(
    cluster_id: impl Into<String>,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &mut sc).await?;

    let topic_name = "person";
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.into())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await?;
    debug!(?topic_id);

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.to_owned(), partition_index);

    let key = serde_json::to_vec(&json!("345-67-6543")).map(Bytes::from)?;

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
    cluster_id: impl Into<String>,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &mut sc).await?;

    let topic_name = "person";
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.into())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await?;
    debug!(?topic_id);

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.to_owned(), partition_index);

    let key = serde_json::to_vec(&json!("345-67-6543")).map(Bytes::from)?;

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

#[cfg(feature = "postgres")]
mod pg {
    use std::env;

    use common::{StorageType, init_tracing};
    use tansu_broker::Error;
    use tansu_schema::Registry;
    use url::Url;

    use super::*;

    async fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        let current_dir = env::current_dir()?;
        debug!(?current_dir);

        let schemas = Url::parse("file://../etc/schema")
            .map_err(Error::from)
            .and_then(|url| {
                Registry::builder_try_from_url(&url)
                    .map(|builder| builder.build())
                    .map_err(Into::into)
            })
            .map(Some)?;

        common::storage_container(
            StorageType::Postgres,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            schemas,
        )
        .await
    }

    #[tokio::test]
    async fn person_valid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::person_valid(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
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
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

mod in_memory {
    use std::env;

    use common::{StorageType, init_tracing};
    use tansu_broker::Error;
    use tansu_schema::Registry;
    use url::Url;

    use super::*;

    async fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        let current_dir = env::current_dir()?;
        debug!(?current_dir);

        let schemas = Url::parse("file://../etc/schema")
            .map_err(Error::from)
            .and_then(|url| {
                Registry::builder_try_from_url(&url)
                    .map(|builder| builder.build())
                    .map_err(Into::into)
            })
            .map(Some)?;

        common::storage_container(
            StorageType::InMemory,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            schemas,
        )
        .await
    }

    #[tokio::test]
    async fn person_valid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::person_valid(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
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
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use std::env;

    use common::{StorageType, init_tracing};
    use tansu_broker::Error;
    use tansu_schema::Registry;
    use url::Url;

    use super::*;

    async fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        let current_dir = env::current_dir()?;
        debug!(?current_dir);

        let schemas = Url::parse("file://../etc/schema")
            .map_err(Error::from)
            .and_then(|url| {
                Registry::builder_try_from_url(&url)
                    .map(|builder| builder.build())
                    .map_err(Into::into)
            })
            .map(Some)?;

        common::storage_container(
            StorageType::Lite,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            schemas,
        )
        .await
    }

    #[tokio::test]
    async fn person_valid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::person_valid(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
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
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}
