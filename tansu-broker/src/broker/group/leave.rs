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
    leave_group_request::{self, MemberIdentity},
};

use crate::{Error, Result, broker::group::Request, coordinator::group::Coordinator};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LeaveGroupService;

impl ApiKey for LeaveGroupService {
    const KEY: i16 = leave_group_request::LeaveGroupRequest::KEY;
}

impl<C, State> Service<State, Request<C>> for LeaveGroupService
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
        let leave = leave_group_request::LeaveGroupRequest::try_from(request.frame.body)?;
        request
            .coordinator
            .leave(
                leave.group_id.as_str(),
                leave.member_id.as_deref(),
                leave.members.as_deref(),
            )
            .await
    }
}

#[derive(Debug)]
pub struct LeaveRequest<C> {
    coordinator: C,
}

impl<C> LeaveRequest<C>
where
    C: Coordinator,
{
    pub async fn response(
        &mut self,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> Result<Body> {
        self.coordinator.leave(group_id, member_id, members).await
    }
}
