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

use std::{fmt::Debug, time::Instant};
use tansu_kafka_sans_io::{
    join_group_request::JoinGroupRequestProtocol,
    leave_group_request::MemberIdentity,
    offset_commit_request::OffsetCommitRequestTopic,
    offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopic},
    sync_group_request::SyncGroupRequestAssignment,
    Body,
};

pub(crate) trait Group: Debug + Send {
    type JoinState;
    type SyncState;
    type HeartbeatState;
    type LeaveState;
    type OffsetCommitState;
    type OffsetFetchState;

    fn join(
        self,
        now: Instant,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> (Self::JoinState, Body);

    fn sync(
        self,
        now: Instant,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Self::SyncState, Body);

    fn heartbeat(
        self,
        now: Instant,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Self::HeartbeatState, Body);

    fn leave(
        self,
        now: Instant,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Self::LeaveState, Body);

    fn offset_commit(
        self,
        now: Instant,
        group_id: &str,
        generation_id_or_member_epoch: Option<i32>,
        member_id: Option<&str>,
        group_instance_id: Option<&str>,
        retention_time_ms: Option<i64>,
        topics: Option<&[OffsetCommitRequestTopic]>,
    ) -> (Self::OffsetCommitState, Body);

    fn offset_fetch(
        self,
        now: Instant,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Self::OffsetFetchState, Body);
}
