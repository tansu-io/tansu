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
    offset_fetch_request::{self, OffsetFetchRequestGroup, OffsetFetchRequestTopic},
};

use crate::{Error, Result, broker::group::Request, coordinator::group::Coordinator};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OffsetFetchService;

impl ApiKey for OffsetFetchService {
    const KEY: i16 = offset_fetch_request::OffsetFetchRequest::KEY;
}

impl<C, State> Service<State, Request<C>> for OffsetFetchService
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
        let offset_fetch = offset_fetch_request::OffsetFetchRequest::try_from(request.frame.body)?;
        request
            .coordinator
            .offset_fetch(
                offset_fetch.group_id.as_deref(),
                offset_fetch.topics.as_deref(),
                offset_fetch.groups.as_deref(),
                offset_fetch.require_stable,
            )
            .await
    }
}

#[derive(Debug)]
pub struct OffsetFetchRequest<C> {
    coordinator: C,
}

impl<C> OffsetFetchRequest<C>
where
    C: Coordinator,
{
    pub fn with_coordinator(coordinator: C) -> Self {
        Self { coordinator }
    }

    pub async fn response(
        &mut self,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> Result<Body> {
        self.coordinator
            .offset_fetch(group_id, topics, groups, require_stable)
            .await
    }
}
