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
use rand::{prelude::*, rng};
use tansu_kafka_sans_io::{
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    describe_configs_request::DescribeConfigsResource,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
};
use tansu_server::{broker::describe_configs::DescribeConfigsRequest, Result};
use tansu_storage::{Storage, StorageContainer};
use tracing::debug;
use uuid::Uuid;
pub mod common;

pub async fn single_topic(
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
            name: "cleanup.policy".into(),
            value: Some("compact".into()),
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

    let resources = [DescribeConfigsResource {
        resource_type: 2,
        resource_name: topic_name.clone(),
        configuration_keys: None,
    }];

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
        vec![DescribeConfigsResult {
            error_code: 0,
            error_message: Some("No error.".into()),
            resource_type: 2,
            resource_name: topic_name,
            configs: Some(
                [DescribeConfigsResourceResult {
                    name: "cleanup.policy".into(),
                    value: Some("compact".into()),
                    read_only: false,
                    is_default: None,
                    config_source: Some(5),
                    is_sensitive: false,
                    synonyms: Some([].into()),
                    config_type: Some(2),
                    documentation: Some("".into()),
                }]
                .into(),
            ),
        }],
    );

    debug!(?topic_id);
    Ok(())
}

mod pg {
    use common::{init_tracing, StorageType};
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
    async fn single_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }
}

mod in_memory {
    use common::{init_tracing, StorageType};
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
    async fn single_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }
}
