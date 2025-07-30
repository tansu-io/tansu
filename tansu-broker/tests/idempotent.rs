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

use bytes::Bytes;
use common::{StorageType, alphanumeric_string, register_broker};
use rand::{prelude::*, rng};
use tansu_broker::{
    Result,
    broker::{
        init_producer_id::InitProducerIdRequest,
        produce::{ProduceRequest, ProduceResponse},
    },
};
use tansu_sans_io::{
    ErrorCode,
    create_topics_request::CreatableTopic,
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    record::{
        Record,
        deflated::{self, Frame},
        inflated,
    },
};
use tansu_storage::{Storage, StorageContainer};
use url::Url;
use uuid::Uuid;

pub mod common;

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

async fn non_txn_idempotent_unknown_producer_id(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic = alphanumeric_string(10);
    let index = 0;

    _ = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic.clone())
                .num_partitions(3)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let transactional_id = None;
    let acks = 0;
    let timeout_ms = 0;

    assert_eq!(
        ProduceResponse {
            responses: Some(vec![
                TopicProduceResponse::default()
                    .name(topic.clone())
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
            ]),
            throttle_time_ms: Some(0),
            node_endpoints: None
        },
        ProduceRequest::with_storage(sc)
            .response(
                transactional_id,
                acks,
                timeout_ms,
                topic_data(
                    topic.as_str(),
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

async fn non_txn_idempotent(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic = alphanumeric_string(10);
    let index = 0;

    _ = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic.clone())
                .num_partitions(3)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let producer = InitProducerIdRequest::with_storage(sc.clone())
        .response(None, 0, Some(-1), Some(-1))
        .await?;

    let mut request = ProduceRequest::with_storage(sc.clone());

    let transactional_id = None;
    let acks = 0;
    let timeout_ms = 0;

    assert_eq!(
        ProduceResponse {
            responses: Some(vec![
                TopicProduceResponse::default()
                    .name(topic.clone())
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
            ]),
            throttle_time_ms: Some(0),
            node_endpoints: None
        },
        request
            .response(
                transactional_id.clone(),
                acks,
                timeout_ms,
                topic_data(
                    topic.as_str(),
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into())
                        )
                        .producer_id(producer.id)
                        .producer_epoch(producer.epoch)
                )?
            )
            .await?
    );

    assert_eq!(
        ProduceResponse {
            responses: Some(vec![
                TopicProduceResponse::default()
                    .name(topic.clone())
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
            ]),
            throttle_time_ms: Some(0),
            node_endpoints: None
        },
        request
            .response(
                transactional_id.clone(),
                acks,
                timeout_ms,
                topic_data(
                    topic.as_str(),
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"consectetur adipiscing elit").into())
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
            responses: Some(vec![
                TopicProduceResponse::default()
                    .name(topic.clone())
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
            ]),
            throttle_time_ms: Some(0),
            node_endpoints: None
        },
        request
            .response(
                transactional_id.clone(),
                acks,
                timeout_ms,
                topic_data(
                    topic.as_str(),
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

async fn non_txn_idempotent_duplicate_sequence(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic = alphanumeric_string(10);
    let index = 0;

    _ = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic.clone())
                .num_partitions(3)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let producer = InitProducerIdRequest::with_storage(sc.clone())
        .response(None, 0, Some(-1), Some(-1))
        .await?;

    let mut request = ProduceRequest::with_storage(sc.clone());

    let transactional_id = None;
    let acks = 0;
    let timeout_ms = 0;

    assert_eq!(
        ProduceResponse {
            responses: Some(vec![
                TopicProduceResponse::default()
                    .name(topic.clone())
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
            ]),
            throttle_time_ms: Some(0),
            node_endpoints: None
        },
        request
            .response(
                transactional_id.clone(),
                acks,
                timeout_ms,
                topic_data(
                    topic.as_str(),
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into())
                        )
                        .producer_id(producer.id)
                )?
            )
            .await?
    );

    assert_eq!(
        ProduceResponse {
            responses: Some(vec![
                TopicProduceResponse::default()
                    .name(topic.clone())
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
            ]),
            throttle_time_ms: Some(0),
            node_endpoints: None
        },
        request
            .response(
                transactional_id,
                acks,
                timeout_ms,
                topic_data(
                    topic.as_str(),
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into())
                        )
                        .producer_id(producer.id)
                )?
            )
            .await?
    );

    Ok(())
}

async fn non_txn_idempotent_sequence_out_of_order(
    cluster_id: Uuid,
    broker_id: i32,
    mut sc: StorageContainer,
) -> Result<()> {
    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let topic = alphanumeric_string(10);
    let index = 0;

    _ = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic.clone())
                .num_partitions(3)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let producer = InitProducerIdRequest::with_storage(sc.clone())
        .response(None, 0, Some(-1), Some(-1))
        .await?;

    let mut request = ProduceRequest::with_storage(sc.clone());

    let transactional_id = None;
    let acks = 0;
    let timeout_ms = 0;

    assert_eq!(
        ProduceResponse {
            responses: Some(vec![
                TopicProduceResponse::default()
                    .name(topic.clone())
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
            ]),
            throttle_time_ms: Some(0),
            node_endpoints: None
        },
        request
            .response(
                transactional_id.clone(),
                acks,
                timeout_ms,
                topic_data(
                    topic.as_str(),
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into())
                        )
                        .producer_id(producer.id)
                )?
            )
            .await?
    );

    assert_eq!(
        ProduceResponse {
            responses: Some(vec![
                TopicProduceResponse::default()
                    .name(topic.clone())
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
            ]),
            throttle_time_ms: Some(0),
            node_endpoints: None
        },
        request
            .response(
                transactional_id,
                acks,
                timeout_ms,
                topic_data(
                    topic.as_str(),
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into())
                        )
                        .base_sequence(2)
                        .producer_id(producer.id)
                )?
            )
            .await?
    );

    Ok(())
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    async fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        common::storage_container(
            StorageType::Postgres,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn non_txn_idempotent_unknown_producer_id() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::non_txn_idempotent_unknown_producer_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn non_txn_idempotent() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::non_txn_idempotent(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn non_txn_idempotent_duplicate_sequence() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::non_txn_idempotent_duplicate_sequence(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn non_txn_idempotent_sequence_out_of_order() -> Result<()> {
        let _guard = common::init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::non_txn_idempotent_sequence_out_of_order(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

mod in_memory {
    use super::*;

    async fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        common::storage_container(
            StorageType::InMemory,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn non_txn_idempotent_unknown_producer_id() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::non_txn_idempotent_unknown_producer_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn non_txn_idempotent() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::non_txn_idempotent(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn non_txn_idempotent_duplicate_sequence() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::non_txn_idempotent_duplicate_sequence(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn non_txn_idempotent_sequence_out_of_order() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::non_txn_idempotent_sequence_out_of_order(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;

    async fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        common::storage_container(
            StorageType::Lite,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn non_txn_idempotent_unknown_producer_id() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::non_txn_idempotent_unknown_producer_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn non_txn_idempotent() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::non_txn_idempotent(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn non_txn_idempotent_duplicate_sequence() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::non_txn_idempotent_duplicate_sequence(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn non_txn_idempotent_sequence_out_of_order() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::non_txn_idempotent_sequence_out_of_order(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}
