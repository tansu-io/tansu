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
use tansu_sans_io::{ApiKey, Body, heartbeat_request};

use crate::{Error, Result, broker::group::Request, coordinator::group::Coordinator};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HeartbeatService;

impl ApiKey for HeartbeatService {
    const KEY: i16 = heartbeat_request::HeartbeatRequest::KEY;
}

impl<C, State> Service<State, Request<C>> for HeartbeatService
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
        let heartbeat = heartbeat_request::HeartbeatRequest::try_from(request.frame.body)?;

        request
            .coordinator
            .heartbeat(
                heartbeat.group_id.as_str(),
                heartbeat.generation_id,
                heartbeat.member_id.as_str(),
                heartbeat.group_instance_id.as_deref(),
            )
            .await
    }
}

#[derive(Debug)]
pub struct HeartbeatRequest<C> {
    coordinator: C,
}

impl<C> HeartbeatRequest<C> {
    pub fn with_coordinator(coordinator: C) -> Self
    where
        C: Coordinator,
    {
        Self { coordinator }
    }
}

impl<C> HeartbeatRequest<C>
where
    C: Coordinator,
{
    pub async fn response(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<Body> {
        self.coordinator
            .heartbeat(group_id, generation_id, member_id, group_instance_id)
            .await
    }
}
