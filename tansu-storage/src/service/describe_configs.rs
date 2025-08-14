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
    ApiKey, Body, ConfigResource, DescribeConfigsRequest, DescribeConfigsResponse,
};
use tracing::error;

use crate::{Error, Result, Storage};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DescribeConfigsService<S> {
    storage: S,
}

impl<S> ApiKey for DescribeConfigsService<S> {
    const KEY: i16 = DescribeConfigsRequest::KEY;
}

impl<S> DescribeConfigsService<S>
where
    S: Storage,
{
    pub fn new(storage: S) -> Self {
        Self { storage }
    }
}

impl<S, State, Q> Service<State, Q> for DescribeConfigsService<S>
where
    S: Storage,
    State: Clone + Send + Sync + 'static,
    Q: Into<Body> + Send + Sync + 'static,
{
    type Response = Body;
    type Error = Error;

    async fn serve(&self, _ctx: Context<State>, request: Q) -> Result<Self::Response, Self::Error> {
        let describe_configs = DescribeConfigsRequest::try_from(request.into())?;

        let mut results = vec![];

        for resource in describe_configs.resources.unwrap_or_default() {
            results.push(
                self.storage
                    .describe_config(
                        resource.resource_name.as_str(),
                        ConfigResource::from(resource.resource_type),
                        resource.configuration_keys.as_deref(),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?,
            );
        }

        Ok(DescribeConfigsResponse::default()
            .results(Some(results))
            .into())
    }
}
