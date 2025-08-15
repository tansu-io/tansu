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
    join_group_request::{self, JoinGroupRequestProtocol},
};

use crate::{Error, Result, broker::group::Request, coordinator::group::Coordinator};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JoinGroupService;

impl ApiKey for JoinGroupService {
    const KEY: i16 = join_group_request::JoinGroupRequest::KEY;
}

impl<C, State> Service<State, Request<C>> for JoinGroupService
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
        let client_id = request
            .frame
            .client_id()
            .map(|client_id| client_id.map(|client_id| client_id.to_owned()))?;

        let join_group = join_group_request::JoinGroupRequest::try_from(request.frame.body)?;

        request
            .coordinator
            .join(
                client_id.as_deref(),
                join_group.group_id.as_str(),
                join_group.session_timeout_ms,
                join_group.rebalance_timeout_ms,
                join_group.member_id.as_str(),
                join_group.group_instance_id.as_deref(),
                join_group.protocol_type.as_str(),
                join_group.protocols.as_deref(),
                join_group.reason.as_deref(),
            )
            .await
    }
}

#[derive(Clone, Debug)]
pub struct JoinRequest<C> {
    coordinator: C,
}

impl<C> JoinRequest<C>
where
    C: Coordinator,
{
    pub fn with_coordinator(coordinator: C) -> Self {
        Self { coordinator }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn response(
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
    ) -> Result<Body> {
        self.coordinator
            .join(
                client_id,
                group_id,
                session_timeout_ms,
                rebalance_timeout_ms,
                member_id,
                group_instance_id,
                protocol_type,
                protocols,
                reason,
            )
            .await
    }
}
