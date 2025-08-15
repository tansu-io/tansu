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
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, ApiKey, Body, ErrorCode,
};

use crate::{Error, Result, Storage, TxnAddPartitionsRequest, TxnAddPartitionsResponse};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AddPartitionService<S> {
    storage: S,
}

impl<S> ApiKey for AddPartitionService<S> {
    const KEY: i16 = AddPartitionsToTxnRequest::KEY;
}

impl<S> AddPartitionService<S>
where
    S: Storage,
{
    pub fn new(storage: S) -> Self {
        Self { storage }
    }
}

impl<S, State, Q> Service<State, Q> for AddPartitionService<S>
where
    S: Storage,
    State: Clone + Send + Sync + 'static,
    Q: Into<Body> + Send + Sync + 'static,
{
    type Response = Body;
    type Error = Error;

    async fn serve(&self, _ctx: Context<State>, request: Q) -> Result<Self::Response, Self::Error> {
        let partitions = TxnAddPartitionsRequest::try_from(request.into())?;
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
