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

use tansu_kafka_sans_io::{txn_offset_commit_request::TxnOffsetCommitRequestTopic, Body};
use tansu_storage::Storage;

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

    pub async fn response(
        &self,
        transactional_id: &str,
        group_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        generation_id: Option<i32>,
        member_id: Option<String>,
        group_instance_id: Option<String>,
        topics: Option<Vec<TxnOffsetCommitRequestTopic>>,
    ) -> Result<Body> {
        let _ = transactional_id;
        let _ = group_id;
        let _ = producer_id;
        let _ = producer_epoch;
        let _ = generation_id;
        let _ = member_id;
        let _ = group_instance_id;
        let _ = topics;

        todo!()
    }
}
