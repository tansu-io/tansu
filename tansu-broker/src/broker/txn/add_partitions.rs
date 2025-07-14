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

use tansu_sans_io::{Body, ErrorCode, add_partitions_to_txn_response::AddPartitionsToTxnResponse};
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
                Ok(AddPartitionsToTxnResponse::default()
                    .throttle_time_ms(0)
                    .error_code(Some(ErrorCode::None.into()))
                    .results_by_transaction(Some([].into()))
                    .results_by_topic_v_3_and_below(Some(results_by_topic_v_3_and_below))
                    .into())
            }

            TxnAddPartitionsResponse::VersionFourPlus(results_by_transaction) => {
                Ok(AddPartitionsToTxnResponse::default()
                    .throttle_time_ms(0)
                    .error_code(Some(ErrorCode::None.into()))
                    .results_by_transaction(Some(results_by_transaction))
                    .results_by_topic_v_3_and_below(Some([].into()))
                    .into())
            }
        }
    }
}
