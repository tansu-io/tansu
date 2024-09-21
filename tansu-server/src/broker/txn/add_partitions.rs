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

use tansu_kafka_sans_io::{
    add_partitions_to_txn_request::{AddPartitionsToTxnTopic, AddPartitionsToTxnTransaction},
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
    Body, ErrorCode,
};
use tansu_storage::Storage;
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

    pub async fn response(
        &self,
        transactions: Option<Vec<AddPartitionsToTxnTransaction>>,
        v_3_and_below_transactional_id: Option<String>,
        v_3_and_below_producer_id: Option<i64>,
        v_3_and_below_producer_epoch: Option<i16>,
        v_3_and_below_topics: Option<Vec<AddPartitionsToTxnTopic>>,
    ) -> Result<Body> {
        debug!(
            ?transactions,
            ?v_3_and_below_transactional_id,
            ?v_3_and_below_producer_id,
            ?v_3_and_below_producer_epoch,
            ?v_3_and_below_topics
        );

        match (
            transactions,
            v_3_and_below_transactional_id,
            v_3_and_below_producer_id,
            v_3_and_below_producer_epoch,
            v_3_and_below_topics,
        ) {
            (
                None,
                Some(v_3_and_below_transactional_id),
                Some(v_3_and_below_producer_id),
                Some(v_3_and_below_producer_epoch),
                Some(v_3_and_below_topics),
            ) => {
                let _ = v_3_and_below_producer_id;
                let _ = v_3_and_below_producer_epoch;

                let results_by_topic_v_3_and_below = Some(
                    v_3_and_below_topics
                        .iter()
                        .map(|topic| AddPartitionsToTxnTopicResult {
                            name: v_3_and_below_transactional_id.clone(),
                            results_by_partition: topic.partitions.as_ref().map(|partitions| {
                                partitions
                                    .iter()
                                    .map(|partition| AddPartitionsToTxnPartitionResult {
                                        partition_index: *partition,
                                        partition_error_code: ErrorCode::None.into(),
                                    })
                                    .collect()
                            }),
                        })
                        .collect(),
                );

                Ok(Body::AddPartitionsToTxnResponse {
                    throttle_time_ms: 0,
                    error_code: Some(ErrorCode::None.into()),
                    results_by_transaction: Some([].into()),
                    results_by_topic_v_3_and_below,
                })
            }

            (Some(_transactions), _, _, _, _) => Ok(Body::AddPartitionsToTxnResponse {
                throttle_time_ms: 0,
                error_code: Some(ErrorCode::UnknownServerError.into()),
                results_by_transaction: Some([].into()),
                results_by_topic_v_3_and_below: Some([].into()),
            }),

            (_, _, _, _, _) => Ok(Body::AddPartitionsToTxnResponse {
                throttle_time_ms: 0,
                error_code: Some(ErrorCode::UnknownServerError.into()),
                results_by_transaction: Some([].into()),
                results_by_topic_v_3_and_below: Some([].into()),
            }),
        }
    }
}
