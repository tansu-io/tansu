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

pub mod administrator;

use crate::Result;
use async_trait::async_trait;
use std::fmt::Debug;
use tansu_sans_io::{
    Body,
    join_group_request::JoinGroupRequestProtocol,
    leave_group_request::MemberIdentity,
    offset_commit_request::OffsetCommitRequestTopic,
    offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopic},
    sync_group_request::SyncGroupRequestAssignment,
};

#[derive(Debug)]
pub struct OffsetCommit<'a> {
    pub group_id: &'a str,
    pub generation_id_or_member_epoch: Option<i32>,
    pub member_id: Option<&'a str>,
    pub group_instance_id: Option<&'a str>,
    pub retention_time_ms: Option<i64>,
    pub topics: Option<&'a [OffsetCommitRequestTopic]>,
}

#[async_trait]
pub trait Coordinator: Clone + Debug + Send + Sync + 'static {
    #[allow(clippy::too_many_arguments)]
    async fn join(
        &mut self,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> Result<Body>;

    #[allow(clippy::too_many_arguments)]
    async fn sync(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> Result<Body>;

    async fn heartbeat(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<Body>;

    async fn leave(
        &mut self,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> Result<Body>;

    async fn offset_commit(&mut self, detail: OffsetCommit<'_>) -> Result<Body>;

    async fn offset_fetch(
        &mut self,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> Result<Body>;
}
