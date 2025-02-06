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
use common::{register_broker, StorageType, CLIENT_ID, COOPERATIVE_STICKY, PROTOCOL_TYPE, RANGE};
use rand::{prelude::*, rng};
use tansu_kafka_sans_io::{join_group_request::JoinGroupRequestProtocol, ErrorCode};
use tansu_server::{coordinator::group::administrator::Controller, Result};
use tansu_storage::StorageContainer;
use tracing::debug;
use url::Url;
use uuid::Uuid;

mod common;

pub async fn join_with_empty_member_id(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let mut controller = Controller::with_storage(sc.clone())?;

    let session_timeout_ms = 45_000;
    let rebalance_timeout_ms = Some(300_000);
    let group_instance_id = common::alphanumeric_string(6);
    let reason = None;

    let group_id: String = common::alphanumeric_string(15);
    debug!(?group_id);

    const CLIENT_ID: &str = "console-consumer";
    const RANGE: &str = "range";
    const COOPERATIVE_STICKY: &str = "cooperative-sticky";

    const PROTOCOL_TYPE: &str = "consumer";

    let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
    let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

    let protocols = [
        JoinGroupRequestProtocol {
            name: RANGE.into(),
            metadata: first_member_range_meta.clone(),
        },
        JoinGroupRequestProtocol {
            name: COOPERATIVE_STICKY.into(),
            metadata: first_member_sticky_meta,
        },
    ];

    // join static group without a member id
    //
    let join_response = common::join_group(
        &mut controller,
        Some(CLIENT_ID),
        group_id.as_str(),
        session_timeout_ms,
        rebalance_timeout_ms,
        "",
        Some(group_instance_id.as_str()),
        PROTOCOL_TYPE,
        Some(&protocols[..]),
        reason,
    )
    .await?;

    assert_eq!(ErrorCode::None, join_response.error_code);
    assert_eq!(Some(PROTOCOL_TYPE.into()), join_response.protocol_type);
    assert_eq!(Some(RANGE.into()), join_response.protocol_name);
    assert_eq!(join_response.member_id, join_response.leader);
    assert!(join_response
        .member_id
        .starts_with(group_instance_id.as_str()));
    assert_eq!(1, join_response.members.len());

    Ok(())
}

pub async fn rejoin_with_empty_member_id(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let mut controller = Controller::with_storage(sc.clone())?;

    let session_timeout_ms = 45_000;
    let rebalance_timeout_ms = Some(300_000);
    let group_instance_id = common::alphanumeric_string(6);
    let reason = None;

    let group_id: String = common::alphanumeric_string(15);
    debug!(?group_id);

    let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
    let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

    let protocols = [
        JoinGroupRequestProtocol {
            name: RANGE.into(),
            metadata: first_member_range_meta.clone(),
        },
        JoinGroupRequestProtocol {
            name: COOPERATIVE_STICKY.into(),
            metadata: first_member_sticky_meta,
        },
    ];

    // join static group without a member id
    //
    let join_response = common::join_group(
        &mut controller,
        Some(CLIENT_ID),
        group_id.as_str(),
        session_timeout_ms,
        rebalance_timeout_ms,
        "",
        Some(group_instance_id.as_str()),
        PROTOCOL_TYPE,
        Some(&protocols[..]),
        reason,
    )
    .await?;

    assert_eq!(ErrorCode::None, join_response.error_code);
    assert_eq!(Some(PROTOCOL_TYPE.into()), join_response.protocol_type);
    assert_eq!(Some(RANGE.into()), join_response.protocol_name);
    assert_eq!(join_response.member_id, join_response.leader);
    assert!(join_response
        .member_id
        .starts_with(group_instance_id.as_str()));
    assert_eq!(1, join_response.members.len());

    let rejoin_response = common::join_group(
        &mut controller,
        Some(CLIENT_ID),
        group_id.as_str(),
        session_timeout_ms,
        rebalance_timeout_ms,
        "",
        Some(group_instance_id.as_str()),
        PROTOCOL_TYPE,
        Some(&protocols[..]),
        reason,
    )
    .await?;

    assert_eq!(ErrorCode::None, rejoin_response.error_code);
    assert_eq!(Some(PROTOCOL_TYPE.into()), rejoin_response.protocol_type);
    assert_eq!(Some(RANGE.into()), rejoin_response.protocol_name);
    assert_eq!(rejoin_response.leader, join_response.leader);
    assert_eq!(rejoin_response.member_id, join_response.member_id);
    assert_eq!(rejoin_response.generation_id, join_response.generation_id);
    assert_eq!(1, rejoin_response.members.len());

    Ok(())
}

mod pg {
    use super::*;

    fn storage_container(cluster: Uuid, node: i32) -> Result<StorageContainer> {
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

    #[ignore]
    #[tokio::test]
    async fn join_with_empty_member_id() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::join_with_empty_member_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[ignore]
    #[tokio::test]
    async fn rejoin_with_empty_member_id() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::rejoin_with_empty_member_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }
}

mod in_memory {
    use super::*;

    fn storage_container(cluster: Uuid, node: i32) -> Result<StorageContainer> {
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

    #[ignore]
    #[tokio::test]
    async fn join_with_empty_member_id() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::join_with_empty_member_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[ignore]
    #[tokio::test]
    async fn rejoin_with_empty_member_id() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::rejoin_with_empty_member_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }
}
