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
    ApiKey, ErrorCode, ProduceRequest, ProduceResponse,
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
};
use tracing::{debug, error, warn};

use crate::{Error, Result, Storage, Topition};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProduceService;

impl ApiKey for ProduceService {
    const KEY: i16 = ProduceRequest::KEY;
}

impl ProduceService {
    fn error(&self, index: i32, error_code: ErrorCode) -> PartitionProduceResponse {
        PartitionProduceResponse::default()
            .index(index)
            .error_code(error_code.into())
            .base_offset(-1)
            .log_append_time_ms(Some(-1))
            .log_start_offset(Some(0))
            .record_errors(Some([].into()))
            .error_message(None)
            .current_leader(None)
    }

    async fn partition<G>(
        &self,
        ctx: Context<G>,
        transaction_id: Option<&str>,
        name: &str,
        partition: PartitionProduceData,
    ) -> PartitionProduceResponse
    where
        G: Storage,
    {
        if let Some(records) = partition.records {
            let mut base_offset = None;

            for batch in records.batches {
                let tp = Topition::new(name, partition.index);
                debug!(
                    record_count = batch.record_count,
                    record_bytes = batch.record_data.len(),
                    ?tp
                );

                match ctx
                    .state()
                    .produce(transaction_id, &tp, batch)
                    .await
                    .inspect_err(|err| match err {
                        storage_api @ Error::Api(_) => {
                            warn!(?storage_api)
                        }
                        otherwise => error!(?otherwise),
                    }) {
                    Ok(offset) => _ = base_offset.get_or_insert(offset),

                    Err(Error::Api(error_code)) => {
                        debug!(?self, ?error_code);
                        return self.error(partition.index, error_code);
                    }

                    Err(otherwise) => {
                        warn!(?otherwise);
                        let error = self.error(partition.index, ErrorCode::UnknownServerError);
                        return error;
                    }
                }
            }

            if let Some(base_offset) = base_offset {
                PartitionProduceResponse::default()
                    .index(partition.index)
                    .error_code(ErrorCode::None.into())
                    .base_offset(base_offset)
                    .log_append_time_ms(Some(-1))
                    .log_start_offset(Some(0))
                    .record_errors(Some([].into()))
                    .error_message(None)
                    .current_leader(None)
            } else {
                self.error(partition.index, ErrorCode::UnknownServerError)
            }
        } else {
            self.error(partition.index, ErrorCode::UnknownServerError)
        }
    }

    async fn topic<G>(
        &self,
        ctx: Context<G>,
        transaction_id: Option<&str>,
        topic: TopicProduceData,
    ) -> TopicProduceResponse
    where
        G: Storage,
    {
        let mut partitions = vec![];

        if let Some(partition_data) = topic.partition_data {
            for partition in partition_data {
                partitions.push(
                    self.partition(ctx.clone(), transaction_id, &topic.name, partition)
                        .await,
                )
            }
        }

        TopicProduceResponse::default()
            .name(topic.name)
            .partition_responses(Some(partitions))
    }
}

impl<G> Service<G, ProduceRequest> for ProduceService
where
    G: Storage,
{
    type Response = ProduceResponse;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<G>,
        req: ProduceRequest,
    ) -> Result<Self::Response, Self::Error> {
        let mut responses = Vec::with_capacity(
            req.topic_data
                .as_ref()
                .map_or(0, |topic_data| topic_data.len()),
        );

        if let Some(topics) = req.topic_data {
            for topic in topics {
                debug!(?topic);

                responses.push(
                    self.topic(ctx.clone(), req.transactional_id.as_deref(), topic)
                        .await,
                )
            }
        }

        Ok(ProduceResponse::default()
            .responses(Some(responses))
            .throttle_time_ms(Some(0))
            .node_endpoints(None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Error, dynostore::DynoStore, service::init_producer_id::InitProducerIdService};
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use rama::Context;
    use tansu_sans_io::{
        ErrorCode, InitProducerIdRequest,
        record::{
            Record,
            deflated::{self, Frame},
            inflated,
        },
    };
    use tracing::subscriber::DefaultGuard;

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
                        .ok_or(Error::Message(String::from("unnamed thread")))
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
                let partition_data =
                    PartitionProduceData::default()
                        .index(index)
                        .records(Some(Frame {
                            batches: vec![deflated],
                        }));

                Some(vec![
                    TopicProduceData::default()
                        .name(topic.into())
                        .partition_data(Some(vec![partition_data])),
                ])
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

        let transactional_id = None;
        let acks = 0;
        let timeout_ms = 0;

        let storage = DynoStore::new(cluster, node, InMemory::new());
        let ctx = Context::with_state(storage);
        let service = ProduceService;

        assert_eq!(
            ProduceResponse::default()
                .responses(Some(vec![
                    TopicProduceResponse::default()
                        .name(topic.into())
                        .partition_responses(Some(vec![
                            PartitionProduceResponse::default()
                                .index(index)
                                .error_code(ErrorCode::UnknownProducerId.into())
                                .base_offset(-1)
                                .log_append_time_ms(Some(-1))
                                .log_start_offset(Some(0))
                                .record_errors(Some(vec![]))
                                .error_message(None)
                                .current_leader(None)
                        ]))
                ]))
                .throttle_time_ms(Some(0))
                .node_endpoints(None),
            service
                .serve(
                    ctx,
                    ProduceRequest::default()
                        .transactional_id(transactional_id)
                        .acks(acks)
                        .timeout_ms(timeout_ms)
                        .topic_data(topic_data(
                            topic,
                            index,
                            inflated::Batch::builder()
                                .record(
                                    Record::builder().value(Bytes::from_static(b"lorem").into())
                                )
                                .producer_id(54345)
                        )?)
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
        let ctx = Context::with_state(storage);

        let init_producer_id = InitProducerIdService;

        let producer = init_producer_id
            .serve(
                ctx.clone(),
                InitProducerIdRequest::default()
                    .transactional_id(None)
                    .transaction_timeout_ms(0)
                    .producer_id(Some(-1))
                    .producer_epoch(Some(-1)),
            )
            .await?;

        let request = ProduceService;

        let transactional_id = None;
        let acks = 0;
        let timeout_ms = 0;

        assert_eq!(
            ProduceResponse::default()
                .responses(Some(vec![
                    TopicProduceResponse::default()
                        .name(topic.into())
                        .partition_responses(Some(vec![
                            PartitionProduceResponse::default()
                                .index(index)
                                .error_code(ErrorCode::None.into())
                                .base_offset(0)
                                .log_append_time_ms(Some(-1))
                                .log_start_offset(Some(0))
                                .record_errors(Some(vec![]))
                                .error_message(None)
                                .current_leader(None)
                        ]))
                ]))
                .throttle_time_ms(Some(0))
                .node_endpoints(None),
            request
                .serve(
                    ctx.clone(),
                    ProduceRequest::default()
                        .transactional_id(transactional_id.clone())
                        .acks(acks)
                        .timeout_ms(timeout_ms)
                        .topic_data(topic_data(
                            topic,
                            index,
                            inflated::Batch::builder()
                                .record(Record::builder().value(
                                    Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
                                ))
                                .producer_id(producer.producer_id)
                        )?)
                )
                .await?
        );

        assert_eq!(
            ProduceResponse::default()
                .responses(Some(vec![
                    TopicProduceResponse::default()
                        .name(topic.into())
                        .partition_responses(Some(vec![
                            PartitionProduceResponse::default()
                                .index(index)
                                .error_code(ErrorCode::None.into())
                                .base_offset(1)
                                .log_append_time_ms(Some(-1))
                                .log_start_offset(Some(0))
                                .record_errors(Some(vec![]))
                                .error_message(None)
                                .current_leader(None)
                        ]))
                ]))
                .throttle_time_ms(Some(0))
                .node_endpoints(None),
            request
                .serve(
                    ctx.clone(),
                    ProduceRequest::default()
                        .transactional_id(transactional_id.clone())
                        .acks(acks)
                        .timeout_ms(timeout_ms)
                        .topic_data(topic_data(
                            topic,
                            index,
                            inflated::Batch::builder()
                                .record(Record::builder().value(
                                    Bytes::from_static(b"consectetur adipiscing elit").into()
                                ))
                                .record(
                                    Record::builder()
                                        .value(Bytes::from_static(b"sed do eiusmod tempor").into())
                                )
                                .base_sequence(1)
                                .last_offset_delta(1)
                                .producer_id(producer.producer_id)
                        )?)
                )
                .await?
        );

        assert_eq!(
            ProduceResponse::default()
                .responses(Some(vec![
                    TopicProduceResponse::default()
                        .name(topic.into())
                        .partition_responses(Some(vec![
                            PartitionProduceResponse::default()
                                .index(index)
                                .error_code(ErrorCode::None.into())
                                .base_offset(3)
                                .log_append_time_ms(Some(-1))
                                .log_start_offset(Some(0))
                                .record_errors(Some(vec![]))
                                .error_message(None)
                                .current_leader(None)
                        ]))
                ]))
                .throttle_time_ms(Some(0))
                .node_endpoints(None),
            request
                .serve(
                    ctx,
                    ProduceRequest::default()
                        .transactional_id(transactional_id.clone())
                        .acks(acks)
                        .timeout_ms(timeout_ms)
                        .topic_data(topic_data(
                            topic,
                            index,
                            inflated::Batch::builder()
                                .record(
                                    Record::builder()
                                        .value(Bytes::from_static(b"incididunt ut labore").into())
                                )
                                .base_sequence(3)
                                .producer_id(producer.producer_id)
                        )?)
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
        let ctx = Context::with_state(storage);

        let init_producer_id = InitProducerIdService;

        let producer = init_producer_id
            .serve(
                ctx.clone(),
                InitProducerIdRequest::default()
                    .transactional_id(None)
                    .transaction_timeout_ms(0)
                    .producer_id(Some(-1))
                    .producer_epoch(Some(-1)),
            )
            .await?;

        let request = ProduceService;

        let transactional_id = None;
        let acks = 0;
        let timeout_ms = 0;

        assert_eq!(
            ProduceResponse::default()
                .responses(Some(vec![
                    TopicProduceResponse::default()
                        .name(topic.into())
                        .partition_responses(Some(vec![
                            PartitionProduceResponse::default()
                                .index(index)
                                .error_code(ErrorCode::None.into())
                                .base_offset(0)
                                .log_append_time_ms(Some(-1))
                                .log_start_offset(Some(0))
                                .record_errors(Some(vec![]))
                                .error_message(None)
                                .current_leader(None)
                        ]))
                ]))
                .throttle_time_ms(Some(0))
                .node_endpoints(None),
            request
                .serve(
                    ctx.clone(),
                    ProduceRequest::default()
                        .transactional_id(transactional_id.clone())
                        .acks(acks)
                        .timeout_ms(timeout_ms)
                        .topic_data(topic_data(
                            topic,
                            index,
                            inflated::Batch::builder()
                                .record(Record::builder().value(
                                    Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
                                ))
                                .producer_id(producer.producer_id)
                        )?)
                )
                .await?
        );

        assert_eq!(
            ProduceResponse::default()
                .responses(Some(vec![
                    TopicProduceResponse::default()
                        .name(topic.into())
                        .partition_responses(Some(vec![
                            PartitionProduceResponse::default()
                                .index(index)
                                .error_code(ErrorCode::DuplicateSequenceNumber.into())
                                .base_offset(-1)
                                .log_append_time_ms(Some(-1))
                                .log_start_offset(Some(0))
                                .record_errors(Some(vec![]))
                                .error_message(None)
                                .current_leader(None)
                        ]))
                ]))
                .throttle_time_ms(Some(0))
                .node_endpoints(None),
            request
                .serve(
                    ctx,
                    ProduceRequest::default()
                        .transactional_id(transactional_id)
                        .acks(acks)
                        .timeout_ms(timeout_ms)
                        .topic_data(topic_data(
                            topic,
                            index,
                            inflated::Batch::builder()
                                .record(Record::builder().value(
                                    Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
                                ))
                                .producer_id(producer.producer_id)
                        )?)
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
        let ctx = Context::with_state(storage);

        let init_producer_id = InitProducerIdService;

        let producer = init_producer_id
            .serve(
                ctx.clone(),
                InitProducerIdRequest::default()
                    .transactional_id(None)
                    .transaction_timeout_ms(0)
                    .producer_id(Some(-1))
                    .producer_epoch(Some(-1)),
            )
            .await?;

        let request = ProduceService;

        let transactional_id = None;
        let acks = 0;
        let timeout_ms = 0;

        assert_eq!(
            ProduceResponse::default()
                .responses(Some(vec![
                    TopicProduceResponse::default()
                        .name(topic.into())
                        .partition_responses(Some(vec![
                            PartitionProduceResponse::default()
                                .index(index)
                                .error_code(ErrorCode::None.into())
                                .base_offset(0)
                                .log_append_time_ms(Some(-1))
                                .log_start_offset(Some(0))
                                .record_errors(Some(vec![]))
                                .error_message(None)
                                .current_leader(None)
                        ]))
                ]))
                .throttle_time_ms(Some(0))
                .node_endpoints(None),
            request
                .serve(
                    ctx.clone(),
                    ProduceRequest::default()
                        .transactional_id(transactional_id.clone())
                        .acks(acks)
                        .timeout_ms(timeout_ms)
                        .topic_data(topic_data(
                            topic,
                            index,
                            inflated::Batch::builder()
                                .record(Record::builder().value(
                                    Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
                                ))
                                .producer_id(producer.producer_id)
                        )?)
                )
                .await?
        );

        assert_eq!(
            ProduceResponse::default()
                .responses(Some(vec![
                    TopicProduceResponse::default()
                        .name(topic.into())
                        .partition_responses(Some(vec![
                            PartitionProduceResponse::default()
                                .index(index)
                                .error_code(ErrorCode::OutOfOrderSequenceNumber.into())
                                .base_offset(-1)
                                .log_append_time_ms(Some(-1))
                                .log_start_offset(Some(0))
                                .record_errors(Some(vec![]))
                                .error_message(None)
                                .current_leader(None)
                        ]))
                ]))
                .throttle_time_ms(Some(0))
                .node_endpoints(None),
            request
                .serve(
                    ctx,
                    ProduceRequest::default()
                        .transactional_id(transactional_id)
                        .acks(acks)
                        .timeout_ms(timeout_ms)
                        .topic_data(topic_data(
                            topic,
                            index,
                            inflated::Batch::builder()
                                .record(Record::builder().value(
                                    Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
                                ))
                                .base_sequence(2)
                                .producer_id(producer.producer_id)
                        )?)
                )
                .await?
        );

        Ok(())
    }
}
