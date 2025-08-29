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
use tansu_sans_io::{ApiKey, IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse};

use crate::{Error, Result, Storage};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IncrementalAlterConfigsService;

impl ApiKey for IncrementalAlterConfigsService {
    const KEY: i16 = IncrementalAlterConfigsRequest::KEY;
}

impl<G> Service<G, IncrementalAlterConfigsRequest> for IncrementalAlterConfigsService
where
    G: Storage,
{
    type Response = IncrementalAlterConfigsResponse;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<G>,
        req: IncrementalAlterConfigsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let mut responses = vec![];

        for resource in req.resources.unwrap_or_default() {
            responses.push(ctx.state().incremental_alter_resource(resource).await?);
        }

        Ok(IncrementalAlterConfigsResponse::default()
            .throttle_time_ms(0)
            .responses(Some(responses)))
    }
}
