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

use std::{collections::BTreeSet, ops::Deref};

use tansu_kafka_sans_io::{
    list_offsets_request::ListOffsetsTopic,
    list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
    Body,
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
        isolation_level: Option<i8>,
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
                    .list_offsets(offsets.deref())
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
                            .map(|topic_name| ListOffsetsTopicResponse {
                                name: (*topic_name).into(),
                                partitions: Some(
                                    offsets
                                        .iter()
                                        .filter_map(|(topition, offset)| {
                                            if topition.topic() == *topic_name {
                                                Some(ListOffsetsPartitionResponse {
                                                    partition_index: topition.partition(),
                                                    error_code: offset.error_code().into(),
                                                    old_style_offsets: None,
                                                    timestamp: offset
                                                        .timestamp()
                                                        .unwrap_or(Some(0))
                                                        .or(Some(0)),
                                                    offset: offset.offset().or(Some(0)),
                                                    leader_epoch: Some(0),
                                                })
                                            } else {
                                                None
                                            }
                                        })
                                        .collect(),
                                ),
                            })
                            .collect()
                    })?,
            )
        } else {
            None
        };

        Ok(Body::ListOffsetsResponse {
            throttle_time_ms,
            topics,
        })
        .inspect(|r| debug!(?r))
    }
}
