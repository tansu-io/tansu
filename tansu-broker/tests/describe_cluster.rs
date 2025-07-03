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

use common::register_broker;
use tansu_broker::{Result, broker::describe_cluster::DescribeClusterRequest};
use tansu_sans_io::{Body, ErrorCode, describe_cluster_response::DescribeClusterBroker};
use tansu_storage::StorageContainer;
use tracing::debug;
use url::Url;
use uuid::Uuid;

pub mod common;

pub async fn describe(
    cluster_id: Uuid,
    broker_id: i32,
    advertised_listener: Url,
    mut sc: StorageContainer,
) -> Result<()> {
    debug!(%cluster_id, broker_id, %advertised_listener);
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let mut dc = DescribeClusterRequest {
        cluster_id: cluster_id.to_string(),
        storage: sc,
    };

    let include_cluster_authorized_operations = true;
    let endpoint_type = Some(6);

    let response = dc
        .response(include_cluster_authorized_operations, endpoint_type)
        .await?;

    let host = advertised_listener.host_str().unwrap().to_string();
    let port = advertised_listener.port().unwrap() as i32;
    let rack = None;

    assert!(matches!(
        response,
        Body::DescribeClusterResponse {
            throttle_time_ms: 0,
            error_code,
            error_message: None,
            controller_id: -1,
            brokers,
            cluster_authorized_operations: -2_147_483_648,
            ..
        } if error_code == i16::from(ErrorCode::None)
        && brokers == Some(vec![DescribeClusterBroker {
            broker_id,
            host,
            port,
            rack,
        }])
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
