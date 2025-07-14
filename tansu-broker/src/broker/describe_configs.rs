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

use crate::Result;
use tansu_sans_io::{
    ConfigResource, describe_configs_request::DescribeConfigsResource,
    describe_configs_response::DescribeConfigsResult,
};
use tansu_storage::Storage;
use tracing::{debug, error};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DescribeConfigsRequest<S> {
    storage: S,
}

impl<S> DescribeConfigsRequest<S>
where
    S: Storage,
{
    pub fn with_storage(storage: S) -> Self {
        Self { storage }
    }

    pub async fn response(
        &mut self,
        resources: Option<&[DescribeConfigsResource]>,
        include_synonyms: Option<bool>,
        include_documentation: Option<bool>,
    ) -> Result<Vec<DescribeConfigsResult>> {
        debug!(?resources, ?include_synonyms, ?include_documentation);

        let mut results = vec![];

        if let Some(resources) = resources {
            for resource in resources {
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
        }

        Ok(results)
    }
}
