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
use tansu_kafka_sans_io::{
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    Body, ErrorCode,
};
use tansu_storage::{Storage, Topition};
use tracing::debug;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProduceRequest<S> {
    storage: S,
}

impl<S> ProduceRequest<S>
where
    S: Storage,
{
    pub fn with_storage(storage: S) -> Self {
        Self { storage }
    }

    fn error(&self, index: i32, error_code: ErrorCode) -> PartitionProduceResponse {
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

    async fn partition(
        &self,
        name: &str,
        partition: PartitionProduceData,
    ) -> PartitionProduceResponse {
        match partition.records {
            Some(mut records) if records.batches.len() == 1 => {
                let batch = records.batches.remove(0);

                let tp = Topition::new(name, partition.index);

                if let Ok(base_offset) = self.storage.produce(&tp, batch).await {
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
                } else {
                    self.error(partition.index, ErrorCode::UnknownServerError)
                }
            }

            _otherwise => self.error(partition.index, ErrorCode::UnknownServerError),
        }
    }

    async fn topic(&self, topic: TopicProduceData) -> TopicProduceResponse {
        let mut partitions = vec![];

        if let Some(partition_data) = topic.partition_data {
            for partition in partition_data {
                partitions.push(self.partition(&topic.name, partition).await)
            }
        }

        TopicProduceResponse {
            name: topic.name,
            partition_responses: Some(partitions),
        }
    }

    pub async fn response(
        &self,
        _transactional_id: Option<String>,
        _acks: i16,
        _timeout_ms: i32,
        topic_data: Option<Vec<TopicProduceData>>,
    ) -> Result<Body> {
        let mut responses =
            Vec::with_capacity(topic_data.as_ref().map_or(0, |topic_data| topic_data.len()));

        if let Some(topics) = topic_data {
            for topic in topics {
                debug!(?topic);

                responses.push(self.topic(topic).await)
            }
        }

        Ok(Body::ProduceResponse {
            responses: Some(responses),
            throttle_time_ms: Some(0),
            node_endpoints: None,
        })
    }
}
