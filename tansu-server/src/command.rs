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

use crate::{BrokerDetail, Error, State, TopicDetail};
use bytes::{Buf, Bytes};
use core::fmt::Debug;
use rand::{prelude::*, thread_rng};
use serde::{Deserialize, Serialize};
use tansu_kafka_sans_io::{
    create_topics_request::{CreatableReplicaAssignment, CreatableTopic},
    delete_topics_request::DeleteTopicState,
    Body, ErrorCode,
};
use tracing::debug;
use uuid::Uuid;

#[typetag::serde(tag = "type")]
pub trait Command: Debug + Send + Sync {
    fn apply(&self, state: &mut State) -> Body;

    fn id(&self) -> Option<Uuid> {
        None
    }
}

impl TryFrom<&Bytes> for Box<dyn Command> {
    type Error = Error;

    fn try_from(value: &Bytes) -> Result<Self, Self::Error> {
        serde_json::from_slice(&value.slice(..)).map_err(|error| error.into())
    }
}

impl TryFrom<&mut dyn Buf> for Box<dyn Command> {
    type Error = Error;

    fn try_from(value: &mut dyn Buf) -> Result<Self, Self::Error> {
        serde_json::from_reader(value.reader()).map_err(|error| error.into())
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Request {
    id: Uuid,
    command: Box<dyn Command>,
}

impl Request {
    pub(crate) fn new(id: Uuid, body: Body) -> Self {
        Self {
            id,
            command: Box::new(body) as Box<dyn Command>,
        }
    }
}

#[typetag::serde]
impl Command for Request {
    fn apply(&self, state: &mut State) -> Body {
        debug!(?self.id);
        self.command.apply(state)
    }

    fn id(&self) -> Option<Uuid> {
        Some(self.id)
    }
}

#[typetag::serde]
impl Command for Body {
    fn apply(&self, state: &mut State) -> Body {
        match self {
            Self::BrokerRegistrationRequest {
                broker_id,
                cluster_id,
                incarnation_id,
                listeners,
                rack,
                ..
            } => match state.cluster_id {
                None => {
                    _ = state.cluster_id.replace(cluster_id.to_owned());

                    _ = state.brokers.insert(
                        *broker_id,
                        BrokerDetail {
                            incarnation_id: uuid::Builder::from_bytes(*incarnation_id).into_uuid(),
                            listeners: listeners.clone(),
                            rack: rack.clone(),
                        },
                    );

                    Body::BrokerRegistrationResponse {
                        throttle_time_ms: 0,
                        error_code: ErrorCode::None.into(),
                        broker_epoch: -1,
                    }
                }

                Some(ref id) if id == cluster_id => {
                    _ = state.brokers.insert(
                        *broker_id,
                        BrokerDetail {
                            incarnation_id: uuid::Builder::from_bytes(*incarnation_id).into_uuid(),
                            listeners: listeners.clone(),
                            rack: rack.clone(),
                        },
                    );

                    Body::BrokerRegistrationResponse {
                        throttle_time_ms: 0,
                        error_code: ErrorCode::None.into(),
                        broker_epoch: -1,
                    }
                }

                _ => Body::BrokerRegistrationResponse {
                    throttle_time_ms: 0,
                    error_code: ErrorCode::InconsistentClusterId.into(),
                    broker_epoch: -1,
                },
            },

            Self::CreateTopicsRequest {
                topics: Some(topics),
                validate_only: Some(false),
                ..
            } => Body::CreateTopicsResponse {
                throttle_time_ms: Some(0),
                topics: Some(
                    topics
                        .iter()
                        .inspect(|topic| debug!(?topic))
                        .map(|topic| {
                            let mut rng = thread_rng();
                            let mut broker_ids: Vec<i32> = state.brokers.keys().cloned().collect();
                            broker_ids.shuffle(&mut rng);

                            let mut brokers = broker_ids.into_iter().cycle();
                            assign_brokers(topic.clone(), &mut brokers).response_for(state)
                        })
                        .collect(),
                ),
            },

            Self::DeleteTopicsRequest {
                topics: Some(topics),
                topic_names: None,
                ..
            } => Body::DeleteTopicsResponse {
                throttle_time_ms: Some(0),
                responses: Some(
                    topics
                        .iter()
                        .map(|topic| topic.response_for(state))
                        .collect(),
                ),
            },

            _ => unimplemented!(),
        }
    }
}

fn assign_brokers(
    mut topic: CreatableTopic,
    brokers: &mut impl Iterator<Item = i32>,
) -> CreatableTopic {
    if topic.assignments.is_none()
        || topic
            .assignments
            .as_ref()
            .is_some_and(|assignments| assignments.is_empty())
    {
        _ = topic.assignments.replace(creatable_replica_assignments(
            topic.num_partitions,
            topic.replication_factor,
            brokers,
        ));
    }

    topic
}

fn creatable_replica_assignments(
    partitions: i32,
    replication_factor: i16,
    brokers: &mut impl Iterator<Item = i32>,
) -> Vec<CreatableReplicaAssignment> {
    (0..partitions)
        .map(|partition_index| {
            let broker_ids: Vec<i32> = (0..replication_factor)
                .map(|_replica| brokers.next().unwrap())
                .collect();

            debug!(?partition_index, ?broker_ids);

            CreatableReplicaAssignment {
                partition_index,
                broker_ids: Some(broker_ids),
            }
        })
        .collect()
}

pub(crate) trait Response: Debug {
    type Output;
    fn response_for(&self, state: &mut State) -> Self::Output;
}

impl Response for CreatableTopic {
    type Output = tansu_kafka_sans_io::create_topics_response::CreatableTopicResult;

    fn response_for(&self, state: &mut State) -> Self::Output {
        use tansu_kafka_sans_io::create_topics_response::{
            CreatableTopicConfigs, CreatableTopicResult,
        };

        if let Some(topic_detail) = state.topic(self.into()) {
            CreatableTopicResult {
                name: self.name.clone(),
                topic_id: Some(topic_detail.id),
                error_code: ErrorCode::TopicAlreadyExists.into(),
                error_message: None,
                topic_config_error_code: None,
                num_partitions: Some(topic_detail.creatable_topic.num_partitions),
                replication_factor: Some(topic_detail.creatable_topic.replication_factor),
                configs: topic_detail
                    .creatable_topic
                    .configs
                    .as_ref()
                    .map(|configs| {
                        configs
                            .iter()
                            .map(|config| CreatableTopicConfigs {
                                name: config.name.clone(),
                                value: config.value.clone(),
                                read_only: false,
                                config_source: 1,
                                is_sensitive: false,
                            })
                            .collect()
                    }),
            }
        } else {
            let id = Uuid::new_v4();

            _ = state.topics.insert(
                self.name.as_str().into(),
                TopicDetail {
                    id: id.as_bytes().clone(),
                    creatable_topic: self.clone(),
                },
            );

            _ = state
                .topic_uuid_to_name
                .insert(id.clone(), self.name.as_str().into());

            CreatableTopicResult {
                name: self.name.clone(),
                topic_id: Some(id.as_bytes().clone()),
                error_code: ErrorCode::None.into(),
                error_message: None,
                topic_config_error_code: None,
                num_partitions: Some(self.num_partitions),
                replication_factor: Some(self.replication_factor),
                configs: self.configs.as_ref().map(|configs| {
                    configs
                        .iter()
                        .map(|config| CreatableTopicConfigs {
                            name: config.name.clone(),
                            value: config.value.clone(),
                            read_only: false,
                            config_source: 1,
                            is_sensitive: false,
                        })
                        .collect()
                }),
            }
        }
    }
}

impl Response for DeleteTopicState {
    type Output = tansu_kafka_sans_io::delete_topics_response::DeletableTopicResult;

    fn response_for(&self, state: &mut State) -> Self::Output {
        use tansu_kafka_sans_io::delete_topics_response::DeletableTopicResult;

        if let Some(ref name) = self.name {
            if let Some(topic_detail) = state.topics.remove(name) {
                _ = state
                    .topic_uuid_to_name
                    .remove(&Uuid::from_bytes(topic_detail.id));

                DeletableTopicResult {
                    name: Some(name.into()),
                    topic_id: Some(topic_detail.id),
                    error_code: ErrorCode::None.into(),
                    error_message: None,
                }
            } else {
                DeletableTopicResult {
                    name: Some(name.into()),
                    topic_id: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0].into(),
                    error_code: ErrorCode::UnknownTopicId.into(),
                    error_message: None,
                }
            }
        } else {
            DeletableTopicResult {
                name: None,
                topic_id: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0].into(),
                error_code: ErrorCode::None.into(),
                error_message: None,
            }
        }
    }
}
