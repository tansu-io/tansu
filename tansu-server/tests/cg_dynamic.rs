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
use common::{
    CLIENT_ID, COOPERATIVE_STICKY, HeartbeatResponse, PROTOCOL_TYPE, RANGE, StorageType,
    alphanumeric_string, heartbeat, join, join_group, register_broker, sync_group,
};
use rand::{prelude::*, rng};
use tansu_sans_io::{
    ErrorCode, join_group_request::JoinGroupRequestProtocol,
    sync_group_request::SyncGroupRequestAssignment,
};
use tansu_server::{Result, coordinator::group::administrator::Controller};
use tansu_storage::StorageContainer;
use tracing::debug;
use url::Url;
use uuid::Uuid;

pub mod common;

pub async fn reject_empty_member_id_on_join(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let mut controller = Controller::with_storage(sc.clone())?;

    let session_timeout_ms = 45_000;
    let rebalance_timeout_ms = Some(300_000);
    let group_instance_id = None;
    let reason = None;

    let group_id: String = alphanumeric_string(15);
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

    // join dynamic group without a member id
    //
    let member_id_required = join_group(
        &mut controller,
        Some(CLIENT_ID),
        group_id.as_str(),
        session_timeout_ms,
        rebalance_timeout_ms,
        "",
        group_instance_id,
        PROTOCOL_TYPE,
        Some(&protocols[..]),
        reason,
    )
    .await?;

    // join rejected as member id is required
    //
    assert_eq!(ErrorCode::MemberIdRequired, member_id_required.error_code);
    assert_eq!(Some(PROTOCOL_TYPE.into()), member_id_required.protocol_type);
    assert_eq!(Some("".into()), member_id_required.protocol_name);
    assert!(member_id_required.leader.is_empty());
    assert!(member_id_required.member_id.starts_with(CLIENT_ID));
    assert_eq!(0, member_id_required.members.len());

    Ok(())
}

pub async fn lifecycle(cluster_id: Uuid, broker_id: i32, mut sc: StorageContainer) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let mut controller = Controller::with_storage(sc.clone())?;

    let session_timeout_ms = 45_000;
    let rebalance_timeout_ms = Some(300_000);
    let group_instance_id = None;

    let group_id: String = alphanumeric_string(15);
    debug!(?group_id);

    // 1st member
    //
    let first_member = join(
        &mut controller,
        group_id.as_str(),
        None,
        None,
        None,
        session_timeout_ms,
        rebalance_timeout_ms,
    )
    .await?;
    debug!(?first_member);

    let first_member_assignment_01 = common::random_bytes(15);

    let assignments = [SyncGroupRequestAssignment {
        member_id: first_member.id().into(),
        assignment: first_member_assignment_01.clone(),
    }];

    // sync to form the group
    //
    let sync_response = sync_group(
        &mut controller,
        group_id.as_str(),
        first_member.generation(),
        first_member.id(),
        group_instance_id,
        PROTOCOL_TYPE,
        RANGE,
        &assignments,
    )
    .await?;
    assert_eq!(ErrorCode::None, sync_response.error_code);
    assert_eq!(PROTOCOL_TYPE, sync_response.protocol_type);
    assert_eq!(RANGE, sync_response.protocol_name);
    assert_eq!(first_member_assignment_01, sync_response.assignment);

    // heartbeat establishing leadership of current generation
    //
    assert_eq!(
        HeartbeatResponse {
            error_code: ErrorCode::None,
        },
        heartbeat(
            &mut controller,
            group_id.as_str(),
            first_member.generation(),
            first_member.id(),
            group_instance_id
        )
        .await?
    );

    // 2nd member joins
    //
    let second_member = join(
        &mut controller,
        group_id.as_str(),
        None,
        None,
        None,
        session_timeout_ms,
        rebalance_timeout_ms,
    )
    .await?;
    debug!(?second_member);

    // 2nd member on sync is told that the group is rebalancing
    //
    let sync_response = sync_group(
        &mut controller,
        group_id.as_str(),
        second_member.generation(),
        second_member.id(),
        group_instance_id,
        PROTOCOL_TYPE,
        RANGE,
        &assignments,
    )
    .await?;
    assert_eq!(ErrorCode::RebalanceInProgress, sync_response.error_code);
    assert_eq!(PROTOCOL_TYPE, sync_response.protocol_type);
    assert_eq!(RANGE, sync_response.protocol_name);

    // rebalance in progress on heartbeat from leader with previous generation
    //
    assert_eq!(
        HeartbeatResponse {
            error_code: ErrorCode::RebalanceInProgress,
        },
        heartbeat(
            &mut controller,
            group_id.as_str(),
            first_member.generation(),
            first_member.id(),
            group_instance_id
        )
        .await?
    );

    // 2nd member rejoins due to rebalance
    //
    let second_member = join(
        &mut controller,
        group_id.as_str(),
        Some(second_member.id()),
        None,
        Some(second_member.protocols().into()),
        session_timeout_ms,
        rebalance_timeout_ms,
    )
    .await?;
    debug!(?second_member);
    assert_eq!(first_member.id(), second_member.leader());

    // first member rejoins as leader
    //
    let first_member = join(
        &mut controller,
        group_id.as_str(),
        Some(first_member.id()),
        None,
        Some(first_member.protocols().into()),
        session_timeout_ms,
        rebalance_timeout_ms,
    )
    .await?;
    debug!(?first_member);
    assert!(first_member.is_leader());

    // both have joined the same generation
    //
    assert_eq!(first_member.generation(), second_member.generation());

    let first_member_assignment_02 = common::random_bytes(15);
    let second_member_assignment_02 = common::random_bytes(15);

    let assignments = [
        SyncGroupRequestAssignment {
            member_id: first_member.id().into(),
            assignment: first_member_assignment_02.clone(),
        },
        SyncGroupRequestAssignment {
            member_id: second_member.id().into(),
            assignment: second_member_assignment_02.clone(),
        },
    ];

    // 1st member leader sync to form and assign the group
    //
    let sync_response = sync_group(
        &mut controller,
        group_id.as_str(),
        first_member.generation(),
        first_member.id(),
        group_instance_id,
        PROTOCOL_TYPE,
        RANGE,
        &assignments,
    )
    .await?;
    assert_eq!(ErrorCode::None, sync_response.error_code);
    assert_eq!(PROTOCOL_TYPE, sync_response.protocol_type);
    assert_eq!(RANGE, sync_response.protocol_name);
    assert_eq!(first_member_assignment_02, sync_response.assignment);

    // 2st member receives group assignments
    //
    let sync_response = sync_group(
        &mut controller,
        group_id.as_str(),
        second_member.generation(),
        second_member.id(),
        group_instance_id,
        PROTOCOL_TYPE,
        RANGE,
        &assignments,
    )
    .await?;
    assert_eq!(ErrorCode::None, sync_response.error_code);
    assert_eq!(PROTOCOL_TYPE, sync_response.protocol_type);
    assert_eq!(RANGE, sync_response.protocol_name);
    assert_eq!(second_member_assignment_02, sync_response.assignment);

    // 1st member heartbeat
    //
    assert_eq!(
        HeartbeatResponse {
            error_code: ErrorCode::None,
        },
        heartbeat(
            &mut controller,
            group_id.as_str(),
            first_member.generation(),
            first_member.id(),
            group_instance_id
        )
        .await?
    );

    // 2nd member heartbeat
    //
    assert_eq!(
        HeartbeatResponse {
            error_code: ErrorCode::None,
        },
        heartbeat(
            &mut controller,
            group_id.as_str(),
            second_member.generation(),
            second_member.id(),
            group_instance_id
        )
        .await?
    );

    // 1st member leaves the group
    //
    let leave_response = common::leave(
        &mut controller,
        group_id.as_str(),
        first_member.id(),
        group_instance_id,
    )
    .await?;
    assert_eq!(ErrorCode::None, leave_response.error_code);

    // 2nd member heartbeat resulting in rebalance in progress
    //
    assert_eq!(
        HeartbeatResponse {
            error_code: ErrorCode::RebalanceInProgress,
        },
        heartbeat(
            &mut controller,
            group_id.as_str(),
            second_member.generation(),
            second_member.id(),
            group_instance_id
        )
        .await?
    );

    // 2nd member rejoins due to rebalance as leader
    //
    let second_member = join(
        &mut controller,
        group_id.as_str(),
        Some(second_member.id()),
        None,
        Some(second_member.protocols().into()),
        session_timeout_ms,
        rebalance_timeout_ms,
    )
    .await?;
    debug!(?second_member);
    assert!(second_member.is_leader());

    let second_member_assignment_03 = common::random_bytes(15);

    let assignments = [SyncGroupRequestAssignment {
        member_id: second_member.id().into(),
        assignment: second_member_assignment_03.clone(),
    }];

    // 2nd member leader sync to reform and assign the group
    //
    let sync_response = sync_group(
        &mut controller,
        group_id.as_str(),
        second_member.generation(),
        second_member.id(),
        group_instance_id,
        PROTOCOL_TYPE,
        RANGE,
        &assignments,
    )
    .await?;
    assert_eq!(ErrorCode::None, sync_response.error_code);
    assert_eq!(PROTOCOL_TYPE, sync_response.protocol_type);
    assert_eq!(RANGE, sync_response.protocol_name);
    assert_eq!(second_member_assignment_03, sync_response.assignment);

    // 2nd member heartbeat
    //
    assert_eq!(
        HeartbeatResponse {
            error_code: ErrorCode::None,
        },
        heartbeat(
            &mut controller,
            group_id.as_str(),
            second_member.generation(),
            second_member.id(),
            group_instance_id
        )
        .await?
    );

    Ok(())
}

mod pg {
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
    async fn reject_empty_member_id_on_join() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::reject_empty_member_id_on_join(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn lifecycle() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::lifecycle(
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
    async fn reject_empty_member_id_on_join() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::reject_empty_member_id_on_join(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }

    #[tokio::test]
    async fn lifecycle() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::lifecycle(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id)?,
        )
        .await
    }
}
