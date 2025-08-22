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
use tansu_sans_io::{ApiKey, ErrorCode, ListGroupsRequest, ListGroupsResponse};

use crate::{Error, Result, Storage};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ListGroupsService;

impl ApiKey for ListGroupsService {
    const KEY: i16 = ListGroupsRequest::KEY;
}

impl<G> Service<G, ListGroupsRequest> for ListGroupsService
where
    G: Storage,
{
    type Response = ListGroupsResponse;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<G>,
        req: ListGroupsRequest,
    ) -> Result<Self::Response, Self::Error> {
        ctx.state()
            .list_groups(req.states_filter.as_deref())
            .await
            .map(Some)
            .map(|groups| {
                ListGroupsResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
                    .groups(groups)
            })
    }
}
