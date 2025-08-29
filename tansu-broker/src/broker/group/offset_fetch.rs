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
use tansu_sans_io::{ApiKey, Body, Frame, OffsetFetchRequest};

use crate::{Error, Result, coordinator::group::Coordinator};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OffsetFetchService;

impl ApiKey for OffsetFetchService {
    const KEY: i16 = OffsetFetchRequest::KEY;
}

impl<C> Service<C, Frame> for OffsetFetchService
where
    C: Coordinator,
{
    type Response = Body;
    type Error = Error;

    async fn serve(&self, mut ctx: Context<C>, req: Frame) -> Result<Self::Response, Self::Error> {
        let coordinator = ctx.state_mut();
        let offset_fetch = OffsetFetchRequest::try_from(req.body)?;

        coordinator
            .offset_fetch(
                offset_fetch.group_id.as_deref(),
                offset_fetch.topics.as_deref(),
                offset_fetch.groups.as_deref(),
                offset_fetch.require_stable,
            )
            .await
    }
}
