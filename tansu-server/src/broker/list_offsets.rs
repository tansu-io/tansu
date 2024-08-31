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
    list_offsets_request::ListOffsetsTopic,
    list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
    Body, ErrorCode,
};

use crate::State;

#[derive(Clone, Copy, Debug)]
pub struct ListOffsetsRequest;

impl ListOffsetsRequest {
    pub fn response(
        &self,
        replica_id: i32,
        isolation_level: Option<i8>,
        topics: Option<&[ListOffsetsTopic]>,
        state: &State,
    ) -> Body {
        let _ = replica_id;
        let _ = isolation_level;
        let _ = state;

        let throttle_time_ms = Some(0);
        let topics = topics.map(|topics| {
            topics
                .iter()
                .map(|topic| ListOffsetsTopicResponse {
                    name: topic.name.clone(),
                    partitions: topic.partitions.as_ref().map(|partitions| {
                        partitions
                            .iter()
                            .map(|partition| ListOffsetsPartitionResponse {
                                partition_index: partition.partition_index,
                                error_code: ErrorCode::None.into(),
                                old_style_offsets: None,
                                timestamp: Some(-1),
                                offset: Some(0),
                                leader_epoch: Some(0),
                            })
                            .collect()
                    }),
                })
                .collect()
        });

        Body::ListOffsetsResponse {
            throttle_time_ms,
            topics,
        }
    }
}
