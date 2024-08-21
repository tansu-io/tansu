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

use crate::State;
use std::sync::MutexGuard;
use tansu_kafka_sans_io::{
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    Body, ErrorCode,
};
use tansu_storage::{Storage, Topition};

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProduceRequest;

impl ProduceRequest {
    fn error(index: i32, error_code: ErrorCode) -> PartitionProduceResponse {
        PartitionProduceResponse {
            index,
            error_code: error_code.into(),
            base_offset: -1,
            log_append_time_ms: Some(-1),
            log_start_offset: Some(0),
            record_errors: Some([].into()),
            error_message: None,
            current_leader: None,
        }
    }

    fn partition<'a>(
        name: &'a str,
        partition: PartitionProduceData,
        manager: &'a mut MutexGuard<'_, Storage>,
    ) -> PartitionProduceResponse {
        match partition.records {
            Some(mut records) if records.batches.len() == 1 => {
                let batch = records.batches.remove(0);

                let tp = Topition::new(name.into(), partition.index);

                let base_offset = manager.produce(&tp, batch).unwrap();

                PartitionProduceResponse {
                    index: partition.index,
                    error_code: ErrorCode::None.into(),
                    base_offset,
                    log_append_time_ms: Some(-1),
                    log_start_offset: Some(0),
                    record_errors: Some([].into()),
                    error_message: None,
                    current_leader: None,
                }
            }

            _otherwise => Self::error(partition.index, ErrorCode::UnknownServerError),
        }
    }

    fn topic<'a>(
        topic: TopicProduceData,
        manager: &'a mut MutexGuard<'_, Storage>,
        state: &State,
    ) -> TopicProduceResponse {
        let partition_responses = if state.topics().contains_key(&topic.name) {
            topic.partition_data.map(|partitions| {
                partitions
                    .into_iter()
                    .map(|partition| Self::partition(&topic.name, partition, manager))
                    .collect()
            })
        } else {
            topic.partition_data.map(|partitions| {
                partitions
                    .into_iter()
                    .map(|partition| {
                        Self::error(partition.index, ErrorCode::UnknownTopicOrPartition)
                    })
                    .collect()
            })
        };

        TopicProduceResponse {
            name: topic.name,
            partition_responses,
        }
    }

    pub(crate) fn response<'a>(
        _transactional_id: Option<String>,
        _acks: i16,
        _timeout_ms: i32,
        topic_data: Option<Vec<TopicProduceData>>,
        manager: &'a mut MutexGuard<'_, Storage>,
        state: &State,
    ) -> Body {
        let responses = topic_data.map(|topics| {
            topics
                .into_iter()
                .map(|topic| Self::topic(topic, manager, state))
                .collect()
        });

        Body::ProduceResponse {
            responses,
            throttle_time_ms: Some(0),
            node_endpoints: None,
        }
    }
}
