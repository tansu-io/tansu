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

use crate::Result;
use tansu_sans_io::{
    Body, ErrorCode,
    list_partition_reassignments_request::ListPartitionReassignmentsTopics,
    list_partition_reassignments_response::{
        ListPartitionReassignmentsResponse, OngoingPartitionReassignment, OngoingTopicReassignment,
    },
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

            ongoing.push(
                OngoingTopicReassignment::default()
                    .name(name.into())
                    .partitions(topic.partitions.as_ref().map(|partitions| {
                        partitions
                            .iter()
                            .map(|partition| {
                                OngoingPartitionReassignment::default()
                                    .partition_index(partition.partition_index)
                                    .replicas(partition.replica_nodes.clone())
                                    .adding_replicas(Some(vec![]))
                                    .removing_replicas(Some(vec![]))
                            })
                            .collect::<Vec<_>>()
                    })),
            );
        }

        Ok(ListPartitionReassignmentsResponse::default()
            .throttle_time_ms(0)
            .error_code(ErrorCode::None.into())
            .error_message(None)
            .topics(Some(ongoing))
            .into())
    }
}
