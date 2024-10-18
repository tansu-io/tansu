// Copyright ⓒ 2024 Peter Morgan <peter.james.morgan@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use crate::Result;
use tansu_kafka_sans_io::{
    describe_configs_request::DescribeConfigsResource, Body, ConfigResource,
};
use tansu_storage::Storage;
use tracing::error;

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
    ) -> Result<Body> {
        let _ = include_synonyms;
        let _ = include_documentation;

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

        Ok(Body::DescribeConfigsResponse {
            throttle_time_ms: 0,
            results: Some(results),
        })
    }
}
