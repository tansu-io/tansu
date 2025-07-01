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

use tansu_sans_io::{Body, ErrorCode};
use tansu_storage::{Storage, TxnAddPartitionsRequest, TxnAddPartitionsResponse};
use tracing::debug;

use crate::Result;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AddPartitions<S> {
    storage: S,
}

impl<S> AddPartitions<S>
where
    S: Storage,
{
    pub fn with_storage(storage: S) -> Self {
        Self { storage }
    }

    pub async fn response(&mut self, partitions: TxnAddPartitionsRequest) -> Result<Body> {
        debug!(?partitions);
        match self.storage.txn_add_partitions(partitions).await? {
            TxnAddPartitionsResponse::VersionZeroToThree(results_by_topic_v_3_and_below) => {
                Ok(Body::AddPartitionsToTxnResponse {
                    throttle_time_ms: 0,
                    error_code: Some(ErrorCode::None.into()),
                    results_by_transaction: Some([].into()),
                    results_by_topic_v_3_and_below: Some(results_by_topic_v_3_and_below),
                })
            }

            TxnAddPartitionsResponse::VersionFourPlus(results_by_transaction) => {
                Ok(Body::AddPartitionsToTxnResponse {
                    throttle_time_ms: 0,
                    error_code: Some(ErrorCode::None.into()),
                    results_by_transaction: Some(results_by_transaction),
                    results_by_topic_v_3_and_below: Some([].into()),
                })
            }
        }
    }
}
