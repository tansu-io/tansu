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

use common::{alphanumeric_string, register_broker};
use rand::{prelude::*, rng};
use tansu_broker::{Result, broker::describe_configs::DescribeConfigsRequest};
use tansu_sans_io::{
    ConfigResource, ConfigSource, ErrorCode, OpType,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    describe_configs_request::DescribeConfigsResource,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
};
use tansu_storage::{Storage, StorageContainer};
use tracing::debug;
use uuid::Uuid;
pub mod common;

pub async fn single_topic(
    cluster_id: impl Into<String>,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let cleanup_policy = "cleanup.policy";
    let compact = "compact";

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some(
        [CreatableTopicConfig::default()
            .name(cleanup_policy.into())
            .value(Some(compact.into()))]
        .into(),
    );

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

    let resources = [DescribeConfigsResource::default()
        .resource_type(ConfigResource::Topic.into())
        .resource_name(topic_name.clone())
        .configuration_keys(None)];

    let include_synonyms = Some(false);
    let include_documentation = Some(false);

    let results = DescribeConfigsRequest::with_storage(sc)
        .response(
            Some(&resources[..]),
            include_synonyms,
            include_documentation,
        )
        .await
        .inspect(|results| debug!(?results))?;

    assert_eq!(
        results,
        vec![
            DescribeConfigsResult::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some(ErrorCode::None.to_string()))
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic_name)
                .configs(Some(
                    [DescribeConfigsResourceResult::default()
                        .name(cleanup_policy.into())
                        .value(Some(compact.into()))
                        .read_only(false)
                        .is_default(None)
                        .config_source(Some(ConfigSource::DefaultConfig.into()))
                        .is_sensitive(false)
                        .synonyms(Some([].into()))
                        .config_type(Some(ConfigResource::Topic.into()))
                        .documentation(Some("".into()))]
                    .into()
                ))
        ],
    );

    debug!(?topic_id);
    Ok(())
}

pub async fn alter_single_topic(
    cluster_id: impl Into<String>,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &mut sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let cleanup_policy = "cleanup.policy";
    let compact = "compact";
    let delete = "delete";

    let num_partitions = 6;
    let replication_factor = 0;

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let resources = [DescribeConfigsResource::default()
        .resource_type(ConfigResource::Topic.into())
        .resource_name(topic_name.clone())
        .configuration_keys(None)];

    let include_synonyms = Some(false);
    let include_documentation = Some(false);

    let results = DescribeConfigsRequest::with_storage(sc.clone())
        .response(
            Some(&resources[..]),
            include_synonyms,
            include_documentation,
        )
        .await
        .inspect(|results| debug!(?results))?;

    let none = ErrorCode::None;

    assert_eq!(
        results,
        vec![
            DescribeConfigsResult::default()
                .error_code(none.into())
                .error_message(Some(none.to_string()))
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic_name.clone())
                .configs(Some([].into()))
        ],
    );

    let response = sc
        .incremental_alter_resource(
            AlterConfigsResource::default()
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic_name.clone())
                .configs(Some(vec![
                    AlterableConfig::default()
                        .name(cleanup_policy.into())
                        .config_operation(OpType::Set.into())
                        .value(Some(compact.into())),
                ])),
        )
        .await?;

    assert_eq!(i16::from(none), response.error_code);
    assert_eq!(i8::from(ConfigResource::Topic), response.resource_type);
    assert_eq!(topic_name, response.resource_name);

    let results = DescribeConfigsRequest::with_storage(sc.clone())
        .response(
            Some(&resources[..]),
            include_synonyms,
            include_documentation,
        )
        .await
        .inspect(|results| debug!(?results))?;

    assert_eq!(
        results,
        vec![
            DescribeConfigsResult::default()
                .error_code(none.into())
                .error_message(Some(none.to_string()))
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic_name.clone())
                .configs(Some(
                    [DescribeConfigsResourceResult::default()
                        .name(cleanup_policy.into())
                        .value(Some(compact.into()))
                        .read_only(false)
                        .is_default(None)
                        .config_source(Some(ConfigSource::DefaultConfig.into()))
                        .is_sensitive(false)
                        .synonyms(Some([].into()))
                        .config_type(Some(ConfigResource::Topic.into()))
                        .documentation(Some("".into()))]
                    .into(),
                )),
        ],
    );

    let response = sc
        .incremental_alter_resource(
            AlterConfigsResource::default()
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic_name.clone())
                .configs(Some(vec![
                    AlterableConfig::default()
                        .name(cleanup_policy.into())
                        .config_operation(OpType::Set.into())
                        .value(Some(delete.into())),
                ])),
        )
        .await?;

    assert_eq!(i16::from(none), response.error_code);
    assert_eq!(i8::from(ConfigResource::Topic), response.resource_type);
    assert_eq!(topic_name, response.resource_name);

    let results = DescribeConfigsRequest::with_storage(sc.clone())
        .response(
            Some(&resources[..]),
            include_synonyms,
            include_documentation,
        )
        .await
        .inspect(|results| debug!(?results))?;

    assert_eq!(
        results,
        vec![
            DescribeConfigsResult::default()
                .error_code(none.into())
                .error_message(Some(none.to_string()))
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic_name.clone())
                .configs(Some(
                    [DescribeConfigsResourceResult::default()
                        .name(cleanup_policy.into())
                        .value(Some(delete.into()))
                        .read_only(false)
                        .is_default(None)
                        .config_source(Some(ConfigSource::DefaultConfig.into()))
                        .is_sensitive(false)
                        .synonyms(Some([].into()))
                        .config_type(Some(ConfigResource::Topic.into()))
                        .documentation(Some("".into()))]
                    .into(),
                )),
        ],
    );

    let response = sc
        .incremental_alter_resource(
            AlterConfigsResource::default()
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic_name.clone())
                .configs(Some(vec![
                    AlterableConfig::default()
                        .name(cleanup_policy.into())
                        .config_operation(OpType::Delete.into())
                        .value(None),
                ])),
        )
        .await?;

    assert_eq!(i16::from(none), response.error_code);
    assert_eq!(i8::from(ConfigResource::Topic), response.resource_type);
    assert_eq!(topic_name, response.resource_name);

    let results = DescribeConfigsRequest::with_storage(sc.clone())
        .response(
            Some(&resources[..]),
            include_synonyms,
            include_documentation,
        )
        .await
        .inspect(|results| debug!(?results))?;

    assert_eq!(
        results,
        vec![
            DescribeConfigsResult::default()
                .error_code(none.into())
                .error_message(Some(none.to_string()))
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic_name.clone())
                .configs(Some([].into()))
        ],
    );

    debug!(?topic_id);
    Ok(())
}

#[cfg(feature = "postgres")]
mod pg {
    use common::{StorageType, init_tracing};
    use url::Url;

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
    async fn alter_single_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::alter_single_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn single_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

mod in_memory {
    use common::{StorageType, init_tracing};
    use url::Url;

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
    async fn alter_single_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::alter_single_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn single_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use common::{StorageType, init_tracing};
    use url::Url;

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
    async fn alter_single_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::alter_single_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn single_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}
