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
    sync_group_request::{self, SyncGroupRequestAssignment},
};

use crate::{Error, Result, broker::group::Request, coordinator::group::Coordinator};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SyncGroupService;

impl ApiKey for SyncGroupService {
    const KEY: i16 = sync_group_request::SyncGroupRequest::KEY;
}

impl<C, State> Service<State, Request<C>> for SyncGroupService
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
        let sync_group = sync_group_request::SyncGroupRequest::try_from(request.frame.body)?;
        request
            .coordinator
            .sync(
                sync_group.group_id.as_str(),
                sync_group.generation_id,
                sync_group.member_id.as_str(),
                sync_group.group_instance_id.as_deref(),
                sync_group.protocol_type.as_deref(),
                sync_group.protocol_name.as_deref(),
                sync_group.assignments.as_deref(),
            )
            .await
    }
}

#[derive(Debug)]
pub struct SyncGroupRequest<C> {
    coordinator: C,
}

impl<C> SyncGroupRequest<C>
where
    C: Coordinator,
{
    pub fn with_coordinator(coordinator: C) -> Self {
        Self { coordinator }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn response(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> Result<Body> {
        self.coordinator
            .sync(
                group_id,
                generation_id,
                member_id,
                group_instance_id,
                protocol_type,
                protocol_name,
                assignments,
            )
            .await
    }
}
