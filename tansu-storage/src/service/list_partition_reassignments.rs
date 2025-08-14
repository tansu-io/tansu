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
use tansu_sans_io::{
    ApiKey, Body, ErrorCode, ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse,
    list_partition_reassignments_response::{
        OngoingPartitionReassignment, OngoingTopicReassignment,
    },
};

use crate::{Error, Result, Storage, TopicId};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ListPartitionReassignmentsService<S> {
    storage: S,
}

impl<S> ApiKey for ListPartitionReassignmentsService<S> {
    const KEY: i16 = ListPartitionReassignmentsRequest::KEY;
}

impl<S> ListPartitionReassignmentsService<S>
where
    S: Storage,
{
    pub fn new(storage: S) -> Self {
        Self { storage }
    }
}

impl<S, State, Q> Service<State, Q> for ListPartitionReassignmentsService<S>
where
    S: Storage,
    State: Clone + Send + Sync + 'static,
    Q: Into<Body> + Send + Sync + 'static,
{
    type Response = Body;
    type Error = Error;

    async fn serve(&self, _ctx: Context<State>, request: Q) -> Result<Self::Response, Self::Error> {
        let list_partition_reassignments =
            ListPartitionReassignmentsRequest::try_from(request.into())?;

        let topics = list_partition_reassignments.topics.map(|topics| {
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
