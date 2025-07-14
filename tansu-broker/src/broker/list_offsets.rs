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

use std::{collections::BTreeSet, ops::Deref};

use tansu_sans_io::{
    Body, IsolationLevel,
    list_offsets_request::ListOffsetsTopic,
    list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
    },
};
use tansu_storage::{ListOffsetRequest, Storage, Topition};
use tracing::{debug, error};

use crate::Result;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ListOffsetsRequest<S> {
    storage: S,
}

impl<S> ListOffsetsRequest<S>
where
    S: Storage,
{
    pub fn with_storage(storage: S) -> Self {
        Self { storage }
    }

    pub async fn response(
        &mut self,
        replica_id: i32,
        isolation_level: IsolationLevel,
        topics: Option<&[ListOffsetsTopic]>,
    ) -> Result<Body> {
        debug!(?replica_id, ?isolation_level, ?topics);

        let throttle_time_ms = Some(0);

        let topics = if let Some(topics) = topics {
            let mut offsets = vec![];

            for topic in topics {
                if let Some(ref partitions) = topic.partitions {
                    for partition in partitions {
                        let tp = Topition::new(topic.name.clone(), partition.partition_index);
                        let offset = ListOffsetRequest::try_from(partition.timestamp)?;

                        offsets.push((tp, offset));
                    }
                }
            }

            Some(
                self.storage
                    .list_offsets(isolation_level, offsets.deref())
                    .await
                    .inspect(|r| debug!(?r, ?offsets))
                    .inspect_err(|err| error!(?err, ?offsets))
                    .map(|offsets| {
                        offsets
                            .iter()
                            .fold(BTreeSet::new(), |mut topics, (topition, _)| {
                                _ = topics.insert(topition.topic());
                                topics
                            })
                            .iter()
                            .map(|topic_name| {
                                ListOffsetsTopicResponse::default()
                                    .name((*topic_name).into())
                                    .partitions(Some(
                                        offsets
                                            .iter()
                                            .filter_map(|(topition, offset)| {
                                                if topition.topic() == *topic_name {
                                                    Some(
                                                        ListOffsetsPartitionResponse::default()
                                                            .partition_index(topition.partition())
                                                            .error_code(offset.error_code().into())
                                                            .old_style_offsets(None)
                                                            .timestamp(
                                                                offset
                                                                    .timestamp()
                                                                    .unwrap_or(Some(-1))
                                                                    .or(Some(-1)),
                                                            )
                                                            .offset(offset.offset().or(Some(0)))
                                                            .leader_epoch(Some(0)),
                                                    )
                                                } else {
                                                    None
                                                }
                                            })
                                            .collect(),
                                    ))
                            })
                            .collect()
                    })?,
            )
        } else {
            None
        };

        Ok(ListOffsetsResponse::default()
            .throttle_time_ms(throttle_time_ms)
            .topics(topics)
            .into())
        .inspect(|r| debug!(?r))
    }
}
