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
    list_partition_reassignments_request::ListPartitionReassignmentsTopics,
    list_partition_reassignments_response::{
        OngoingPartitionReassignment, OngoingTopicReassignment,
    },
    Body, ErrorCode,
};

use crate::State;

pub(crate) struct ListPartitionReassignmentsRequest;

impl ListPartitionReassignmentsRequest {
    pub(crate) fn response(
        &self,
        topics: Option<&[ListPartitionReassignmentsTopics]>,
        state: &State,
    ) -> Body {
        Body::ListPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None.into(),
            error_message: None,
            topics: topics.map(|topics| {
                topics
                    .iter()
                    .map(|topic| OngoingTopicReassignment {
                        name: topic.name.clone(),
                        partitions: state.topic(topic.into()).map(|detail| {
                            detail.creatable_topic.assignments.clone().map_or(
                                Vec::new(),
                                |assignments| {
                                    assignments
                                        .iter()
                                        .map(|assignment| OngoingPartitionReassignment {
                                            partition_index: assignment.partition_index,
                                            replicas: Some(
                                                assignment
                                                    .broker_ids
                                                    .as_ref()
                                                    .map_or(vec![], |broker_ids| {
                                                        broker_ids.clone()
                                                    }),
                                            ),
                                            adding_replicas: Some(vec![]),
                                            removing_replicas: Some(vec![]),
                                        })
                                        .collect()
                                },
                            )
                        }),
                    })
                    .collect()
            }),
        }
    }
}
