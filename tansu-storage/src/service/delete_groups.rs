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
use tansu_sans_io::{ApiKey, DeleteGroupsRequest, DeleteGroupsResponse};

use crate::{Error, Result, Storage};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DeleteGroupsService;

impl ApiKey for DeleteGroupsService {
    const KEY: i16 = DeleteGroupsRequest::KEY;
}

impl<G> Service<G, DeleteGroupsRequest> for DeleteGroupsService
where
    G: Storage,
{
    type Response = DeleteGroupsResponse;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<G>,
        req: DeleteGroupsRequest,
    ) -> Result<Self::Response, Self::Error> {
        ctx.state()
            .delete_groups(req.groups_names.as_deref())
            .await
            .map(Some)
            .map(|results| {
                DeleteGroupsResponse::default()
                    .throttle_time_ms(0)
                    .results(results)
            })
    }
}
