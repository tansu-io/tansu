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
use tansu_sans_io::{Body, ErrorCode, describe_cluster_response::DescribeClusterResponse};
use tansu_storage::Storage;
use tracing::debug;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DescribeClusterRequest<S> {
    pub cluster_id: String,
    pub storage: S,
}

impl<S> DescribeClusterRequest<S>
where
    S: Storage,
{
    pub async fn response(
        &mut self,
        include_cluster_authorized_operations: bool,
        endpoint_type: Option<i8>,
    ) -> Result<Body> {
        debug!(?include_cluster_authorized_operations, ?endpoint_type);

        let brokers = self.storage.brokers().await?;
        debug!(?brokers);

        Ok(DescribeClusterResponse::default()
            .throttle_time_ms(0)
            .error_code(ErrorCode::None.into())
            .error_message(None)
            .endpoint_type(endpoint_type)
            .cluster_id(self.cluster_id.clone())
            .controller_id(-1)
            .brokers(Some(brokers))
            .cluster_authorized_operations(-2_147_483_648)
            .into())
    }
}
