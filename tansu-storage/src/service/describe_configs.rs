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
use tansu_sans_io::{ApiKey, ConfigResource, DescribeConfigsRequest, DescribeConfigsResponse};
use tracing::error;

use crate::{Error, Result, Storage};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DescribeConfigsService;

impl ApiKey for DescribeConfigsService {
    const KEY: i16 = DescribeConfigsRequest::KEY;
}

impl<G> Service<G, DescribeConfigsRequest> for DescribeConfigsService
where
    G: Storage,
{
    type Response = DescribeConfigsResponse;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<G>,
        req: DescribeConfigsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let mut results = vec![];

        for resource in req.resources.unwrap_or_default() {
            results.push(
                ctx.state()
                    .describe_config(
                        resource.resource_name.as_str(),
                        ConfigResource::from(resource.resource_type),
                        resource.configuration_keys.as_deref(),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?,
            );
        }

        Ok(DescribeConfigsResponse::default().results(Some(results)))
    }
}
