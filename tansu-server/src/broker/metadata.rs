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
    create_topics_request::CreatableReplicaAssignment,
    metadata_request::MetadataRequestTopic,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    Body, ErrorCode,
};

use crate::{State, TopicDetail};

#[derive(Clone, Copy, Debug)]
pub(crate) struct MetadataRequest;

impl MetadataRequest {
    fn replica_assignment(
        &self,
        assignment: &CreatableReplicaAssignment,
    ) -> MetadataResponsePartition {
        MetadataResponsePartition {
            error_code: ErrorCode::None.into(),
            partition_index: assignment.partition_index,
            leader_id: assignment
                .broker_ids
                .as_ref()
                .map_or(-1, |broker_ids| broker_ids[0]),
            leader_epoch: Some(-1),
            replica_nodes: Some(
                assignment
                    .broker_ids
                    .as_ref()
                    .map_or(vec![], |broker_ids| broker_ids.clone()),
            ),
            isr_nodes: Some(vec![]),
            offline_replicas: Some(vec![]),
        }
    }

    fn replica_assignments(
        &self,
        assignments: Option<&[CreatableReplicaAssignment]>,
    ) -> Option<Vec<MetadataResponsePartition>> {
        assignments.map(|assignments| {
            assignments
                .iter()
                .map(|assignment| self.replica_assignment(assignment))
                .collect()
        })
    }

    fn with_name_detail(&self, name: &str, detail: &TopicDetail) -> MetadataResponseTopic {
        let partitions = self.replica_assignments(detail.replica_assignments());

        MetadataResponseTopic {
            error_code: ErrorCode::None.into(),
            name: Some(name.into()),
            topic_id: Some(detail.id),
            is_internal: Some(false),
            partitions,
            topic_authorized_operations: Some(-2147483648),
        }
    }

    fn topic(&self, topic: &MetadataRequestTopic, state: &State) -> MetadataResponseTopic {
        topic.name.as_ref().map_or(
            MetadataResponseTopic {
                error_code: ErrorCode::UnknownTopicOrPartition.into(),
                name: None,
                topic_id: None,
                is_internal: None,
                partitions: None,
                topic_authorized_operations: None,
            },
            |name| {
                state.topics.get(name).map_or(
                    MetadataResponseTopic {
                        error_code: ErrorCode::UnknownTopicOrPartition.into(),
                        name: Some(name.clone()),
                        topic_id: None,
                        is_internal: None,
                        partitions: None,
                        topic_authorized_operations: None,
                    },
                    |detail| {
                        let partitions = self.replica_assignments(detail.replica_assignments());

                        MetadataResponseTopic {
                            error_code: ErrorCode::None.into(),
                            name: Some(name.into()),
                            topic_id: Some(detail.id),
                            is_internal: Some(false),
                            partitions,
                            topic_authorized_operations: Some(-2147483648),
                        }
                    },
                )
            },
        )
    }

    fn topics(
        &self,
        topics: Option<&[MetadataRequestTopic]>,
        state: &State,
    ) -> Vec<MetadataResponseTopic> {
        topics.map_or(
            state
                .topics()
                .iter()
                .map(|(name, detail)| self.with_name_detail(name, detail))
                .collect(),
            |topics| {
                topics
                    .iter()
                    .map(|topic| self.topic(topic, state))
                    .collect()
            },
        )
    }

    fn brokers(&self, state: &State) -> Vec<MetadataResponseBroker> {
        state
            .brokers
            .iter()
            .map(|(node_id, broker)| {
                broker
                    .listeners
                    .as_ref()
                    .map(|listeners| {
                        listeners
                            .iter()
                            .map(|listener| {
                                let node_id = *node_id;
                                let host = listener.host.clone();
                                let port = i32::from(listener.port);
                                let rack = broker.rack.clone();

                                MetadataResponseBroker {
                                    node_id,
                                    host,
                                    port,
                                    rack,
                                }
                            })
                            .collect()
                    })
                    .unwrap_or(vec![])
            })
            .flatten()
            .collect()
    }

    pub(crate) fn response(
        &self,
        controller_id: Option<i32>,
        topics: Option<&[MetadataRequestTopic]>,
        state: &State,
    ) -> Body {
        let topics = Some(self.topics(topics, state));
        let brokers = Some(self.brokers(state));
        let throttle_time_ms = Some(0);
        let cluster_id = state.cluster_id.clone();
        let cluster_authorized_operations = None;

        Body::MetadataResponse {
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
            cluster_authorized_operations,
        }
    }
}
