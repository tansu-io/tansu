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
use tansu_sans_io::{ApiKey, Body, DescribeClusterRequest, DescribeClusterResponse, ErrorCode};
use tracing::debug;

use crate::{Error, Result, Storage};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DescribeClusterService<S> {
    storage: S,
}

impl<S> ApiKey for DescribeClusterService<S> {
    const KEY: i16 = DescribeClusterRequest::KEY;
}

impl<S> DescribeClusterService<S>
where
    S: Storage,
{
    pub fn new(storage: S) -> Self {
        Self { storage }
    }
}

impl<S, State, Q> Service<State, Q> for DescribeClusterService<S>
where
    S: Storage,
    State: Clone + Send + Sync + 'static,
    Q: Into<Body> + Send + Sync + 'static,
{
    type Response = Body;
    type Error = Error;

    async fn serve(&self, _ctx: Context<State>, request: Q) -> Result<Self::Response, Self::Error> {
        let describe_cluster = DescribeClusterRequest::try_from(request.into())?;

        let brokers = self.storage.brokers().await?;
        debug!(?brokers);

        let cluster_id = self.storage.cluster_id()?;

        Ok(DescribeClusterResponse::default()
            .throttle_time_ms(0)
            .error_code(ErrorCode::None.into())
            .error_message(None)
            .endpoint_type(describe_cluster.endpoint_type)
            .controller_id(-1)
            .cluster_id(cluster_id.to_owned())
            .brokers(Some(brokers))
            .cluster_authorized_operations(-2_147_483_648)
            .into())
    }
}
