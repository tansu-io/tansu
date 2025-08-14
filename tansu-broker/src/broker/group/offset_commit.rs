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

use rama::{Context, Service};
use tansu_sans_io::{
    ApiKey, Body,
    offset_commit_request::{self, OffsetCommitRequestTopic},
};

use crate::{
    Error, Result,
    broker::group::Request,
    coordinator::group::{Coordinator, OffsetCommit},
};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OffsetCommitService;

impl ApiKey for OffsetCommitService {
    const KEY: i16 = offset_commit_request::OffsetCommitRequest::KEY;
}

impl<C, State> Service<State, Request<C>> for OffsetCommitService
where
    C: Coordinator,
    State: Clone + Send + Sync + 'static,
{
    type Response = Body;
    type Error = Error;

    async fn serve(
        &self,
        _ctx: Context<State>,
        mut request: Request<C>,
    ) -> Result<Self::Response, Self::Error> {
        let offset_commit =
            offset_commit_request::OffsetCommitRequest::try_from(request.frame.body)?;
        request
            .coordinator
            .offset_commit(OffsetCommit {
                group_id: offset_commit.group_id.as_str(),
                generation_id_or_member_epoch: offset_commit.generation_id_or_member_epoch,
                member_id: offset_commit.member_id.as_deref(),
                group_instance_id: offset_commit.group_instance_id.as_deref(),
                retention_time_ms: offset_commit.retention_time_ms,
                topics: offset_commit.topics.as_deref(),
            })
            .await
    }
}

#[derive(Debug)]
pub struct OffsetCommitRequest<C> {
    coordinator: C,
}

impl<C> OffsetCommitRequest<C>
where
    C: Coordinator,
{
    pub async fn response(
        &mut self,
        group_id: &str,
        generation_id_or_member_epoch: Option<i32>,
        member_id: Option<&str>,
        group_instance_id: Option<&str>,
        retention_time_ms: Option<i64>,
        topics: Option<&[OffsetCommitRequestTopic]>,
    ) -> Result<Body> {
        self.coordinator
            .offset_commit(OffsetCommit {
                group_id,
                generation_id_or_member_epoch,
                member_id,
                group_instance_id,
                retention_time_ms,
                topics,
            })
            .await
    }
}
