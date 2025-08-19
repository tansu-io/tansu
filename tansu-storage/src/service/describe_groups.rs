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
    ApiKey, DescribeGroupsRequest, DescribeGroupsResponse, describe_groups_response::DescribedGroup,
};

use crate::{Error, Result, Storage};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DescribeGroupsService;

impl ApiKey for DescribeGroupsService {
    const KEY: i16 = DescribeGroupsRequest::KEY;
}

impl<G> Service<G, DescribeGroupsRequest> for DescribeGroupsService
where
    G: Storage,
{
    type Response = DescribeGroupsResponse;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<G>,
        req: DescribeGroupsRequest,
    ) -> Result<Self::Response, Self::Error> {
        ctx.state()
            .describe_groups(
                req.groups.as_deref(),
                req.include_authorized_operations.unwrap_or(false),
            )
            .await
            .map(|described| {
                described
                    .iter()
                    .map(DescribedGroup::from)
                    .collect::<Vec<_>>()
            })
            .map(Some)
            .map(|groups| {
                DescribeGroupsResponse::default()
                    .throttle_time_ms(Some(0))
                    .groups(groups)
            })
    }
}
