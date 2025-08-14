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
use tansu_sans_io::{ApiKey, Body, TxnOffsetCommitResponse, txn_offset_commit_request};

use crate::{Error, Result, Storage};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OffsetCommitService<S> {
    storage: S,
}

impl<S> ApiKey for OffsetCommitService<S> {
    const KEY: i16 = txn_offset_commit_request::TxnOffsetCommitRequest::KEY;
}

impl<S> OffsetCommitService<S>
where
    S: Storage,
{
    pub fn new(storage: S) -> Self {
        Self { storage }
    }
}

impl<S, State, Q> Service<State, Q> for OffsetCommitService<S>
where
    S: Storage,
    State: Clone + Send + Sync + 'static,
    Q: Into<Body> + Send + Sync + 'static,
{
    type Response = Body;
    type Error = Error;

    async fn serve(&self, _ctx: Context<State>, request: Q) -> Result<Self::Response, Self::Error> {
        let txn = txn_offset_commit_request::TxnOffsetCommitRequest::try_from(request.into())?;
        let responses = self
            .storage
            .txn_offset_commit(crate::TxnOffsetCommitRequest {
                transaction_id: txn.transactional_id.to_owned(),
                group_id: txn.group_id.to_owned(),
                producer_id: txn.producer_id,
                producer_epoch: txn.producer_epoch,
                generation_id: txn.generation_id,
                member_id: txn.member_id,
                group_instance_id: txn.group_instance_id,
                topics: txn.topics.unwrap_or_default(),
            })
            .await?;

        Ok(TxnOffsetCommitResponse::default()
            .throttle_time_ms(0)
            .topics(Some(responses))
            .into())
    }
}
