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
use tansu_kafka_sans_io::{Body, ErrorCode};
use tansu_storage::Storage;

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
        let _ = include_cluster_authorized_operations;

        let brokers = self.storage.brokers().await?;

        Ok(Body::DescribeClusterResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None.into(),
            error_message: None,
            endpoint_type,
            cluster_id: self.cluster_id.clone(),
            controller_id: -1,
            brokers: Some(brokers),
            cluster_authorized_operations: -2_147_483_648,
        })
    }
}
