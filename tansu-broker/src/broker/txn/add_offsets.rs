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

use tansu_sans_io::Body;
use tansu_sans_io::add_offsets_to_txn_response::AddOffsetsToTxnResponse;
use tansu_storage::Storage;
use tracing::debug;

use crate::Result;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AddOffsets<S> {
    storage: S,
}

impl<S> AddOffsets<S>
where
    S: Storage,
{
    pub fn with_storage(storage: S) -> Self {
        Self { storage }
    }

    pub async fn response(
        &mut self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<Body> {
        debug!(?transaction_id, ?producer_id, ?producer_epoch, ?group_id);

        self.storage
            .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
            .map_err(Into::into)
            .map(|error_code| {
                AddOffsetsToTxnResponse::default()
                    .throttle_time_ms(0)
                    .error_code(error_code.into())
                    .into()
            })
    }
}
