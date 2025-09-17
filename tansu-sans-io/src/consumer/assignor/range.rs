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

use std::collections::BTreeSet;

use bytes::Bytes;

use crate::{
    Error, MetadataResponse,
    consumer::{
        ConsumerAssignor, ConsumerProtocolAssignment, MemberAssignment, MemberMetadata,
        TopicPartition,
    },
    join_group_response::JoinGroupResponseMember,
    sync_group_request::SyncGroupRequestAssignment,
};

fn partitions(metadata: &MetadataResponse) -> BTreeSet<i32> {
    metadata
        .topics
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter(|topic| topic.name.is_some())
        .flat_map(|topic| {
            topic
                .partitions
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|partition| partition.partition_index)
                .collect::<Vec<_>>()
        })
        .collect()
}

#[derive(Clone, Default, Eq, Hash, Debug, Ord, PartialEq, PartialOrd)]
pub struct RangeAssignor;

impl ConsumerAssignor for RangeAssignor {
    fn assign(
        &self,
        members: &[JoinGroupResponseMember],
        metadata: &MetadataResponse,
    ) -> Result<Vec<SyncGroupRequestAssignment>, Error> {
        let (allocation, mut remainder, mut partition) = {
            let partitions = partitions(metadata);

            let allocation = partitions.len() / members.len();
            let remainder = 0..(partitions.len() % members.len());

            (allocation, remainder, partitions.into_iter())
        };

        members
            .iter()
            .map(|member| {
                MemberMetadata::try_from(member.metadata.clone()).and_then(|member_metadata| {
                    let partitions: Vec<_> = if remainder.next().is_some() {
                        (0..=allocation)
                            .map(|_| partition.next().expect("partition"))
                            .collect()
                    } else {
                        (0..allocation)
                            .map(|_| partition.next().expect("partition"))
                            .collect()
                    };

                    Bytes::try_from(&MemberAssignment::default().version(3).assignment(
                        ConsumerProtocolAssignment::default().assigned_partitions(
                            member_metadata.subscription.topics.iter().map(|topic| {
                                TopicPartition {
                                    topic: topic.into(),
                                    partitions: partitions.clone(),
                                }
                            }),
                        ),
                    ))
                    .map(|assignment| {
                        SyncGroupRequestAssignment::default()
                            .member_id(member.member_id.clone())
                            .assignment(assignment)
                    })
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use bytes::Bytes;

    use crate::{
        consumer::{ConsumerProtocolSubscription, MemberMetadata},
        metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
    };

    use super::*;

    fn metadata_response_topic(
        topic: impl Into<String>,
        partitions: Range<i32>,
    ) -> MetadataResponseTopic {
        MetadataResponseTopic::default()
            .name(Some(topic.into()))
            .partitions(Some(
                partitions
                    .into_iter()
                    .map(|partition| {
                        MetadataResponsePartition::default().partition_index(partition)
                    })
                    .collect(),
            ))
    }

    #[test]
    fn even_partitions() -> Result<(), Error> {
        const T0: &str = "t0";
        const T1: &str = "t1";

        let metadata = MetadataResponse::default().topics(Some(
            [
                metadata_response_topic(T0, 0..3),
                metadata_response_topic(T1, 0..3),
            ]
            .into(),
        ));

        let partitions = partitions(&metadata);
        assert_eq!(3, partitions.len());
        assert!(partitions.contains(&0));
        assert!(partitions.contains(&1));
        assert!(partitions.contains(&2));

        Ok(())
    }

    #[test]
    fn evenly_divided() -> Result<(), Error> {
        const C0: &str = "C0";
        const C1: &str = "C1";
        const C2: &str = "C2";

        const T0: &str = "t0";
        const T1: &str = "t1";

        let member_metadata =
            Bytes::try_from(&MemberMetadata::default().version(3).subscription(
                ConsumerProtocolSubscription::default().topics([T0.into(), T1.into()]),
            ))?;

        let members = [
            JoinGroupResponseMember::default()
                .member_id(C0.into())
                .metadata(member_metadata.clone()),
            JoinGroupResponseMember::default()
                .member_id(C1.into())
                .metadata(member_metadata.clone()),
            JoinGroupResponseMember::default()
                .member_id(C2.into())
                .metadata(member_metadata),
        ];

        let assignor = RangeAssignor;

        let metadata = MetadataResponse::default().topics(Some(
            [
                metadata_response_topic(T0, 0..3),
                metadata_response_topic(T1, 0..3),
            ]
            .into(),
        ));

        let assignments = assignor.assign(&members[..], &metadata)?;
        assert_eq!(3, assignments.len());

        let index = 0;
        assert_eq!(C0, assignments[index].member_id);
        assert_eq!(
            [T0, T1]
                .into_iter()
                .map(|topic| TopicPartition {
                    topic: topic.into(),
                    partitions: [index as i32].into(),
                })
                .collect::<Vec<_>>(),
            MemberAssignment::try_from(assignments[index].clone().assignment)?
                .assignment
                .assigned_partitions
        );

        let index = 1;
        assert_eq!(C1, assignments[index].member_id);
        assert_eq!(
            [T0, T1]
                .into_iter()
                .map(|topic| TopicPartition {
                    topic: topic.into(),
                    partitions: [index as i32].into(),
                })
                .collect::<Vec<_>>(),
            MemberAssignment::try_from(assignments[index].clone().assignment)?
                .assignment
                .assigned_partitions
        );

        let index = 2;
        assert_eq!(C2, assignments[index].member_id);
        assert_eq!(
            [T0, T1]
                .into_iter()
                .map(|topic| TopicPartition {
                    topic: topic.into(),
                    partitions: [index as i32].into(),
                })
                .collect::<Vec<_>>(),
            MemberAssignment::try_from(assignments[index].clone().assignment)?
                .assignment
                .assigned_partitions
        );

        Ok(())
    }

    #[test]
    fn remainder() -> Result<(), Error> {
        const C0: &str = "C0";
        const C1: &str = "C1";

        const T0: &str = "t0";
        const T1: &str = "t1";

        let member_metadata =
            Bytes::try_from(&MemberMetadata::default().version(3).subscription(
                ConsumerProtocolSubscription::default().topics([T0.into(), T1.into()]),
            ))?;

        let members = [
            JoinGroupResponseMember::default()
                .member_id(C0.into())
                .metadata(member_metadata.clone()),
            JoinGroupResponseMember::default()
                .member_id(C1.into())
                .metadata(member_metadata.clone()),
        ];

        let assignor = RangeAssignor;

        let metadata = MetadataResponse::default().topics(Some(
            [
                metadata_response_topic(T0, 0..3),
                metadata_response_topic(T1, 0..3),
            ]
            .into(),
        ));

        let assignments = assignor.assign(&members[..], &metadata)?;
        assert_eq!(2, assignments.len());

        let index = 0;
        assert_eq!(C0, assignments[index].member_id);
        assert_eq!(
            [T0, T1]
                .into_iter()
                .map(|topic| TopicPartition {
                    topic: topic.into(),
                    partitions: [0, 1].into(),
                })
                .collect::<Vec<_>>(),
            MemberAssignment::try_from(assignments[index].clone().assignment)?
                .assignment
                .assigned_partitions
        );

        let index = 1;
        assert_eq!(C1, assignments[index].member_id);
        assert_eq!(
            [T0, T1]
                .into_iter()
                .map(|topic| TopicPartition {
                    topic: topic.into(),
                    partitions: [2].into(),
                })
                .collect::<Vec<_>>(),
            MemberAssignment::try_from(assignments[index].clone().assignment)?
                .assignment
                .assigned_partitions
        );

        Ok(())
    }
}
