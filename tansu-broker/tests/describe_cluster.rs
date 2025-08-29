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

use common::register_broker;
use rama::{Context, Service};
use tansu_broker::Result;
use tansu_sans_io::{
    DescribeClusterRequest, DescribeClusterResponse, ErrorCode,
    describe_cluster_response::DescribeClusterBroker,
};
use tansu_storage::{StorageContainer, service::DescribeClusterService};
use tracing::debug;
use url::Url;
use uuid::Uuid;

pub mod common;

pub async fn describe(
    cluster_id: Uuid,
    broker_id: i32,
    advertised_listener: Url,
    sc: StorageContainer,
) -> Result<()> {
    debug!(%cluster_id, broker_id, %advertised_listener);
    register_broker(&cluster_id, broker_id, &sc).await?;

    let include_cluster_authorized_operations = true;
    let endpoint_type = Some(6);

    let ctx = Context::with_state(sc);
    let service = DescribeClusterService;

    let response = service
        .serve(
            ctx,
            DescribeClusterRequest::default()
                .include_cluster_authorized_operations(include_cluster_authorized_operations)
                .endpoint_type(endpoint_type),
        )
        .await?;

    let host = advertised_listener.host_str().unwrap().to_string();
    let port = advertised_listener.port().unwrap() as i32;
    let rack = None;

    assert!(matches!(
        response,
        DescribeClusterResponse {
            throttle_time_ms: 0,
            error_code,
            error_message: None,
            controller_id: -1,
            brokers,
            cluster_authorized_operations: -2_147_483_648,
            ..
        } if error_code == i16::from(ErrorCode::None)
        && brokers == Some(vec![DescribeClusterBroker::default()
            .broker_id(broker_id)
            .host(host)
            .port(port)
            .rack(rack)
        ])
    ));

    Ok(())
}

mod pg {
    use common::{StorageType, init_tracing};
    use rand::{prelude::*, rng};

    use super::*;

    fn storage_container(
        cluster: impl Into<String>,
        node: i32,
        advertised_listener: Url,
    ) -> Result<StorageContainer> {
        common::storage_container(
            StorageType::Postgres,
            cluster,
            node,
            advertised_listener,
            None,
        )
    }

    #[tokio::test]
    async fn describe() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = Uuid::now_v7();
        let node = rng().random_range(0..i32::MAX);
        let advertised_listener = Url::parse("tcp://example.com:9092/")?;

        super::describe(
            cluster,
            node,
            advertised_listener.clone(),
            storage_container(cluster, node, advertised_listener)?,
        )
        .await
    }
}

mod in_memory {
    use common::{StorageType, init_tracing};
    use rand::{prelude::*, rng};

    use super::*;

    fn storage_container(
        cluster: impl Into<String>,
        node: i32,
        advertised_listener: Url,
    ) -> Result<StorageContainer> {
        common::storage_container(
            StorageType::InMemory,
            cluster,
            node,
            advertised_listener,
            None,
        )
    }

    #[tokio::test]
    async fn describe() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = Uuid::now_v7();
        let node = rng().random_range(0..i32::MAX);
        let advertised_listener = Url::parse("tcp://example.com:9092/")?;

        super::describe(
            cluster,
            node,
            advertised_listener.clone(),
            storage_container(cluster, node, advertised_listener)?,
        )
        .await
    }
}
