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

use tansu_sans_io::{
    Body, txn_offset_commit_request::TxnOffsetCommitRequestTopic,
    txn_offset_commit_response::TxnOffsetCommitResponse,
};
use tansu_storage::{Storage, TxnOffsetCommitRequest};

use crate::Result;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OffsetCommit<S> {
    storage: S,
}

impl<S> OffsetCommit<S>
where
    S: Storage,
{
    pub fn with_storage(storage: S) -> Self {
        Self { storage }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn response(
        &mut self,
        transactional_id: &str,
        group_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        generation_id: Option<i32>,
        member_id: Option<String>,
        group_instance_id: Option<String>,
        topics: Option<Vec<TxnOffsetCommitRequestTopic>>,
    ) -> Result<Body> {
        let responses = self
            .storage
            .txn_offset_commit(TxnOffsetCommitRequest {
                transaction_id: transactional_id.to_owned(),
                group_id: group_id.to_owned(),
                producer_id,
                producer_epoch,
                generation_id,
                member_id,
                group_instance_id,
                topics: topics.unwrap_or_default(),
            })
            .await?;

        Ok(TxnOffsetCommitResponse::default()
            .throttle_time_ms(0)
            .topics(Some(responses))
            .into())
    }
}
