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

use crate::{Error, Result};
use tansu_kafka_sans_io::{
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{NodeEndpoint, PartitionProduceResponse, TopicProduceResponse},
    ErrorCode,
};
use tansu_storage::{Storage, Topition};
use tracing::{debug, error};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProduceRequest<S> {
    storage: S,
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProduceResponse {
    pub responses: Option<Vec<TopicProduceResponse>>,
    pub throttle_time_ms: Option<i32>,
    pub node_endpoints: Option<Vec<NodeEndpoint>>,
}

impl<S> ProduceRequest<S>
where
    S: Storage,
{
    pub fn with_storage(storage: S) -> Self {
        Self { storage }
    }

    fn error(&self, index: i32, error_code: ErrorCode) -> PartitionProduceResponse {
        PartitionProduceResponse {
            index,
            error_code: error_code.into(),
            base_offset: -1,
            log_append_time_ms: Some(-1),
            log_start_offset: Some(0),
            record_errors: Some([].into()),
            error_message: None,
            current_leader: None,
        }
    }

    async fn partition(
        &mut self,
        name: &str,
        partition: PartitionProduceData,
    ) -> PartitionProduceResponse {
        match partition.records {
            Some(mut records) if records.batches.len() == 1 => {
                let batch = records.batches.remove(0);

                let tp = Topition::new(name, partition.index);

                match self
                    .storage
                    .produce(&tp, batch)
                    .await
                    .map_err(Into::into)
                    .inspect_err(|err| error!(?err))
                {
                    Ok(base_offset) => PartitionProduceResponse {
                        index: partition.index,
                        error_code: ErrorCode::None.into(),
                        base_offset,
                        log_append_time_ms: Some(-1),
                        log_start_offset: Some(0),
                        record_errors: Some([].into()),
                        error_message: None,
                        current_leader: None,
                    },

                    Err(Error::Storage(tansu_storage::Error::Api(error_code))) => {
                        debug!(?self, ?error_code);
                        self.error(partition.index, error_code)
                    }

                    Err(_) => self.error(partition.index, ErrorCode::UnknownServerError),
                }
            }

            _otherwise => self.error(partition.index, ErrorCode::UnknownServerError),
        }
    }

    async fn topic(&mut self, topic: TopicProduceData) -> TopicProduceResponse {
        let mut partitions = vec![];

        if let Some(partition_data) = topic.partition_data {
            for partition in partition_data {
                partitions.push(self.partition(&topic.name, partition).await)
            }
        }

        TopicProduceResponse {
            name: topic.name,
            partition_responses: Some(partitions),
        }
    }

    pub async fn response(
        &mut self,
        _transactional_id: Option<String>,
        _acks: i16,
        _timeout_ms: i32,
        topic_data: Option<Vec<TopicProduceData>>,
    ) -> Result<ProduceResponse> {
        let mut responses =
            Vec::with_capacity(topic_data.as_ref().map_or(0, |topic_data| topic_data.len()));

        if let Some(topics) = topic_data {
            for topic in topics {
                debug!(?topic);

                responses.push(self.topic(topic).await)
            }
        }

        Ok(ProduceResponse {
            responses: Some(responses),
            throttle_time_ms: Some(0),
            node_endpoints: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{broker::init_producer_id::InitProducerIdRequest, Error};
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use tansu_kafka_sans_io::{
        record::{
            deflated::{self, Frame},
            inflated, Record,
        },
        ErrorCode,
    };
    use tansu_storage::dynostore::DynoStore;
    use tracing::subscriber::DefaultGuard;

    #[cfg(miri)]
    fn init_tracing() -> Result<()> {
        Ok(())
    }

    #[cfg(not(miri))]
    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        use tracing::Level;
        use tracing_subscriber::fmt::format::FmtSpan;

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_max_level(Level::DEBUG)
                .with_span_events(FmtSpan::ACTIVE)
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Custom(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME")))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    fn topic_data(
        topic: &str,
        index: i32,
        builder: inflated::Builder,
    ) -> Result<Option<Vec<TopicProduceData>>> {
        builder
            .build()
            .and_then(deflated::Batch::try_from)
            .map(|deflated| {
                let partition_data = PartitionProduceData {
                    index,
                    records: Some(Frame {
                        batches: vec![deflated],
                    }),
                };

                Some(vec![TopicProduceData {
                    name: topic.into(),
                    partition_data: Some(vec![partition_data]),
                }])
            })
            .map_err(Into::into)
    }

    #[tokio::test]
    async fn non_txn_idempotent_unknown_producer_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = "abc";
        let node = 12321;
        let topic = "pqr";
        let index = 0;

        let storage = DynoStore::new(cluster, node, InMemory::new());

        let transactional_id = None;
        let acks = 0;
        let timeout_ms = 0;

        assert_eq!(
            ProduceResponse {
                responses: Some(vec![TopicProduceResponse {
                    name: topic.into(),
                    partition_responses: Some(vec![PartitionProduceResponse {
                        index,
                        error_code: ErrorCode::UnknownProducerId.into(),
                        base_offset: -1,
                        log_append_time_ms: Some(-1),
                        log_start_offset: Some(0),
                        record_errors: Some(vec![]),
                        error_message: None,
                        current_leader: None,
                    }],),
                }]),
                throttle_time_ms: Some(0),
                node_endpoints: None
            },
            ProduceRequest::with_storage(storage)
                .response(
                    transactional_id,
                    acks,
                    timeout_ms,
                    topic_data(
                        topic,
                        index,
                        inflated::Batch::builder()
                            .record(Record::builder().value(Bytes::from_static(b"lorem").into()))
                            .producer_id(54345)
                    )?
                )
                .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn non_txn_idempotent() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = "abc";
        let node = 12321;
        let topic = "pqr";
        let index = 0;

        let storage = DynoStore::new(cluster, node, InMemory::new());

        let producer = InitProducerIdRequest::with_storage(storage.clone())
            .response(None, 0, Some(-1), Some(-1))
            .await?;

        let mut request = ProduceRequest::with_storage(storage.clone());

        let transactional_id = None;
        let acks = 0;
        let timeout_ms = 0;

        assert_eq!(
            ProduceResponse {
                responses: Some(vec![TopicProduceResponse {
                    name: topic.into(),
                    partition_responses: Some(vec![PartitionProduceResponse {
                        index,
                        error_code: ErrorCode::None.into(),
                        base_offset: 0,
                        log_append_time_ms: Some(-1),
                        log_start_offset: Some(0),
                        record_errors: Some(vec![]),
                        error_message: None,
                        current_leader: None,
                    }],),
                }]),
                throttle_time_ms: Some(0),
                node_endpoints: None
            },
            request
                .response(
                    transactional_id.clone(),
                    acks,
                    timeout_ms,
                    topic_data(
                        topic,
                        index,
                        inflated::Batch::builder()
                            .record(
                                Record::builder().value(
                                    Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
                                )
                            )
                            .producer_id(producer.id)
                    )?
                )
                .await?
        );

        assert_eq!(
            ProduceResponse {
                responses: Some(vec![TopicProduceResponse {
                    name: topic.into(),
                    partition_responses: Some(vec![PartitionProduceResponse {
                        index,
                        error_code: ErrorCode::None.into(),
                        base_offset: 1,
                        log_append_time_ms: Some(-1),
                        log_start_offset: Some(0),
                        record_errors: Some(vec![]),
                        error_message: None,
                        current_leader: None,
                    }],),
                }]),
                throttle_time_ms: Some(0),
                node_endpoints: None
            },
            request
                .response(
                    transactional_id.clone(),
                    acks,
                    timeout_ms,
                    topic_data(
                        topic,
                        index,
                        inflated::Batch::builder()
                            .record(
                                Record::builder().value(
                                    Bytes::from_static(b"consectetur adipiscing elit").into()
                                )
                            )
                            .record(
                                Record::builder()
                                    .value(Bytes::from_static(b"sed do eiusmod tempor").into())
                            )
                            .base_sequence(1)
                            .last_offset_delta(1)
                            .producer_id(producer.id)
                    )?
                )
                .await?
        );

        assert_eq!(
            ProduceResponse {
                responses: Some(vec![TopicProduceResponse {
                    name: topic.into(),
                    partition_responses: Some(vec![PartitionProduceResponse {
                        index,
                        error_code: ErrorCode::None.into(),
                        base_offset: 3,
                        log_append_time_ms: Some(-1),
                        log_start_offset: Some(0),
                        record_errors: Some(vec![]),
                        error_message: None,
                        current_leader: None,
                    }],),
                }]),
                throttle_time_ms: Some(0),
                node_endpoints: None
            },
            request
                .response(
                    transactional_id.clone(),
                    acks,
                    timeout_ms,
                    topic_data(
                        topic,
                        index,
                        inflated::Batch::builder()
                            .record(
                                Record::builder()
                                    .value(Bytes::from_static(b"incididunt ut labore").into())
                            )
                            .base_sequence(3)
                            .producer_id(producer.id)
                    )?
                )
                .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn non_txn_idempotent_duplicate_sequence() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = "abc";
        let node = 12321;
        let topic = "pqr";
        let index = 0;

        let storage = DynoStore::new(cluster, node, InMemory::new());

        let producer = InitProducerIdRequest::with_storage(storage.clone())
            .response(None, 0, Some(-1), Some(-1))
            .await?;

        let mut request = ProduceRequest::with_storage(storage.clone());

        let transactional_id = None;
        let acks = 0;
        let timeout_ms = 0;

        assert_eq!(
            ProduceResponse {
                responses: Some(vec![TopicProduceResponse {
                    name: topic.into(),
                    partition_responses: Some(vec![PartitionProduceResponse {
                        index,
                        error_code: ErrorCode::None.into(),
                        base_offset: 0,
                        log_append_time_ms: Some(-1),
                        log_start_offset: Some(0),
                        record_errors: Some(vec![]),
                        error_message: None,
                        current_leader: None,
                    }],),
                }]),
                throttle_time_ms: Some(0),
                node_endpoints: None
            },
            request
                .response(
                    transactional_id.clone(),
                    acks,
                    timeout_ms,
                    topic_data(
                        topic,
                        index,
                        inflated::Batch::builder()
                            .record(
                                Record::builder().value(
                                    Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
                                )
                            )
                            .producer_id(producer.id)
                    )?
                )
                .await?
        );

        assert_eq!(
            ProduceResponse {
                responses: Some(vec![TopicProduceResponse {
                    name: topic.into(),
                    partition_responses: Some(vec![PartitionProduceResponse {
                        index,
                        error_code: ErrorCode::DuplicateSequenceNumber.into(),
                        base_offset: -1,
                        log_append_time_ms: Some(-1),
                        log_start_offset: Some(0),
                        record_errors: Some(vec![]),
                        error_message: None,
                        current_leader: None,
                    }],),
                }]),
                throttle_time_ms: Some(0),
                node_endpoints: None
            },
            request
                .response(
                    transactional_id,
                    acks,
                    timeout_ms,
                    topic_data(
                        topic,
                        index,
                        inflated::Batch::builder()
                            .record(
                                Record::builder().value(
                                    Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
                                )
                            )
                            .producer_id(producer.id)
                    )?
                )
                .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn non_txn_idempotent_sequence_out_of_order() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = "abc";
        let node = 12321;
        let topic = "pqr";
        let index = 0;

        let storage = DynoStore::new(cluster, node, InMemory::new());

        let producer = InitProducerIdRequest::with_storage(storage.clone())
            .response(None, 0, Some(-1), Some(-1))
            .await?;

        let mut request = ProduceRequest::with_storage(storage.clone());

        let transactional_id = None;
        let acks = 0;
        let timeout_ms = 0;

        assert_eq!(
            ProduceResponse {
                responses: Some(vec![TopicProduceResponse {
                    name: topic.into(),
                    partition_responses: Some(vec![PartitionProduceResponse {
                        index,
                        error_code: ErrorCode::None.into(),
                        base_offset: 0,
                        log_append_time_ms: Some(-1),
                        log_start_offset: Some(0),
                        record_errors: Some(vec![]),
                        error_message: None,
                        current_leader: None,
                    }],),
                }]),
                throttle_time_ms: Some(0),
                node_endpoints: None
            },
            request
                .response(
                    transactional_id.clone(),
                    acks,
                    timeout_ms,
                    topic_data(
                        topic,
                        index,
                        inflated::Batch::builder()
                            .record(
                                Record::builder().value(
                                    Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
                                )
                            )
                            .producer_id(producer.id)
                    )?
                )
                .await?
        );

        assert_eq!(
            ProduceResponse {
                responses: Some(vec![TopicProduceResponse {
                    name: topic.into(),
                    partition_responses: Some(vec![PartitionProduceResponse {
                        index,
                        error_code: ErrorCode::OutOfOrderSequenceNumber.into(),
                        base_offset: -1,
                        log_append_time_ms: Some(-1),
                        log_start_offset: Some(0),
                        record_errors: Some(vec![]),
                        error_message: None,
                        current_leader: None,
                    }],),
                }]),
                throttle_time_ms: Some(0),
                node_endpoints: None
            },
            request
                .response(
                    transactional_id,
                    acks,
                    timeout_ms,
                    topic_data(
                        topic,
                        index,
                        inflated::Batch::builder()
                            .record(
                                Record::builder().value(
                                    Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
                                )
                            )
                            .base_sequence(2)
                            .producer_id(producer.id)
                    )?
                )
                .await?
        );

        Ok(())
    }
}
