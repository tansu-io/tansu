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
    list_partition_reassignments_request::ListPartitionReassignmentsTopics,
    list_partition_reassignments_response::{
        OngoingPartitionReassignment, OngoingTopicReassignment,
    },
    Body, ErrorCode,
};
use tansu_storage::{Storage, TopicId};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ListPartitionReassignmentsRequest<S> {
    storage: S,
}

impl<S> ListPartitionReassignmentsRequest<S>
where
    S: Storage,
{
    pub fn with_storage(storage: S) -> Self {
        Self { storage }
    }

    pub async fn response(
        &mut self,
        topics: Option<&[ListPartitionReassignmentsTopics]>,
    ) -> Result<Body> {
        let topics = topics.map(|topics| {
            topics
                .iter()
                .map(|topic| TopicId::Name(topic.name.as_str().into()))
                .collect::<Vec<_>>()
        });

        let metadata = self.storage.metadata(topics.as_deref()).await?;

        let mut ongoing = vec![];

        for topic in metadata.topics() {
            let Some(ref name) = topic.name else { continue };

            ongoing.push(OngoingTopicReassignment {
                name: name.into(),
                partitions: topic.partitions.as_ref().map(|partitions| {
                    partitions
                        .iter()
                        .map(|partition| OngoingPartitionReassignment {
                            partition_index: partition.partition_index,
                            replicas: partition.replica_nodes.clone(),
                            adding_replicas: Some(vec![]),
                            removing_replicas: Some(vec![]),
                        })
                        .collect::<Vec<_>>()
                }),
            });
        }

        Ok(Body::ListPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None.into(),
            error_message: None,
            topics: Some(ongoing),
        })
    }
}
