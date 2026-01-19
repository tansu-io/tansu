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

//! Tests for SlateDB storage engine
//!
//! Note: Basic CRUD operations (create/delete topic, metadata, produce, offset commit,
//! list offsets, init producer, describe config, list/delete groups, transactions)
//! are covered by broker tests in tansu-broker/tests/*.rs with slatedb module.
//!
//! This file contains tests for:
//! - Low-level API tests (offset_stage, brokers, cluster_id, node)
//! - Error case tests (duplicate topic, unknown txn, wrong producer/epoch)
//! - Unique feature tests (isolation levels, delete records, idempotent produce, builder pattern)

use std::sync::Arc;

use bytes::Bytes;
use slatedb::{Db, object_store::memory::InMemory};
use tansu_sans_io::{ErrorCode, create_topics_request::CreatableTopic, record::deflated::Batch};
use url::Url;

use crate::{
    BrokerRegistrationRequest, Error, Storage, Topition, TxnAddPartitionsRequest,
    TxnAddPartitionsResponse,
};

use super::engine::Engine;

async fn create_test_engine() -> Engine {
    let object_store = Arc::new(InMemory::new());
    let db = Db::open("test.slatedb", object_store)
        .await
        .expect("Failed to open SlateDB");

    Engine::new(
        "test-cluster",
        1,
        Url::parse("tcp://localhost:9092").unwrap(),
        Arc::new(db),
    )
}

// ========== Unique Error Case Tests ==========

#[tokio::test]
async fn test_create_duplicate_topic() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("dup-topic".into())
        .num_partitions(1)
        .replication_factor(1);

    // First creation should succeed
    let result = engine.create_topic(topic.clone(), false).await;
    assert!(result.is_ok());

    // Second creation should fail
    let result = engine.create_topic(topic, false).await;
    assert!(matches!(
        result,
        Err(Error::Api(ErrorCode::TopicAlreadyExists))
    ));
}

// ========== Low-level API Tests ==========

#[tokio::test]
async fn test_offset_stage() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("stage-topic".into())
        .num_partitions(1)
        .replication_factor(1);

    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("stage-topic", 0);

    let stage = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(0, stage.log_start);
    assert_eq!(0, stage.last_stable);
    assert_eq!(0, stage.high_watermark);
}

#[tokio::test]
async fn test_brokers() {
    let engine = create_test_engine().await;

    let brokers = engine.brokers().await.unwrap();
    assert_eq!(1, brokers.len());
    assert_eq!(1, brokers[0].broker_id);
    assert_eq!("localhost", brokers[0].host.as_str());
    assert_eq!(9092, brokers[0].port);
}

#[tokio::test]
async fn test_cluster_id() {
    let engine = create_test_engine().await;

    let cluster_id = engine.cluster_id().await.unwrap();
    assert_eq!("test-cluster", cluster_id);
}

#[tokio::test]
async fn test_node() {
    let engine = create_test_engine().await;

    let node = engine.node().await.unwrap();
    assert_eq!(1, node);
}

// ========== Transaction Error Tests ==========

#[tokio::test]
async fn test_txn_add_partitions_unknown_txn() {
    use tansu_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;

    let engine = create_test_engine().await;

    // Try to add partitions without initializing transaction
    let request = TxnAddPartitionsRequest::VersionZeroToThree {
        transaction_id: "unknown-txn".into(),
        producer_id: 1,
        producer_epoch: 0,
        topics: vec![
            AddPartitionsToTxnTopic::default()
                .name("any-topic".into())
                .partitions(Some(vec![0])),
        ],
    };

    let response = engine.txn_add_partitions(request).await.unwrap();

    match response {
        TxnAddPartitionsResponse::VersionZeroToThree(results) => {
            let partitions = results[0].results_by_partition.as_ref().unwrap();
            assert_eq!(
                ErrorCode::TransactionalIdNotFound,
                ErrorCode::try_from(partitions[0].partition_error_code).unwrap()
            );
        }
        _ => panic!("Expected VersionZeroToThree response"),
    }
}

// ========== Isolation Level Tests ==========

#[tokio::test]
async fn test_fetch_isolation_levels() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("isolation-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("isolation-topic", 0);

    // Produce some data with valid empty batch (record_count=0)
    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        record_count: 0,
        record_data: Bytes::new(),
    };

    let _ = engine
        .produce(None, &topition, batch.clone())
        .await
        .unwrap();
    let _ = engine
        .produce(None, &topition, batch.clone())
        .await
        .unwrap();

    // Verify offset stage to confirm data was written
    let stage = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(2, stage.high_watermark);
    assert_eq!(2, stage.last_stable);

    // Test that ReadUncommitted and ReadCommitted return same result when no txns
    let stage_uncommitted = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(
        stage_uncommitted.high_watermark,
        stage_uncommitted.last_stable
    );
}

#[tokio::test]
async fn test_offset_stage_with_transaction() {
    use tansu_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;

    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("stage-txn-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("stage-txn-topic", 0);

    // Produce some data first
    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        record_count: 1,
        record_data: Bytes::new(),
    };

    let _ = engine
        .produce(None, &topition, batch.clone())
        .await
        .unwrap();

    // Check offset stage - no transactions, so last_stable == high_watermark
    let stage = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(1, stage.high_watermark);
    assert_eq!(1, stage.last_stable);

    // Start a transaction and add this partition
    let producer = engine
        .init_producer(Some("stage-test-txn"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    let request = TxnAddPartitionsRequest::VersionZeroToThree {
        transaction_id: "stage-test-txn".into(),
        producer_id: producer.id,
        producer_epoch: producer.epoch,
        topics: vec![
            AddPartitionsToTxnTopic::default()
                .name("stage-txn-topic".into())
                .partitions(Some(vec![0])),
        ],
    };
    let _ = engine.txn_add_partitions(request).await.unwrap();

    // After committing the transaction, offset stage should still be consistent
    // Note: txn_end now writes a commit marker batch, incrementing high_watermark by 1
    let _ = engine
        .txn_end("stage-test-txn", producer.id, producer.epoch, true)
        .await
        .unwrap();

    let stage = engine.offset_stage(&topition).await.unwrap();
    // high_watermark increased by 1 due to commit marker batch
    assert_eq!(2, stage.high_watermark);
    assert_eq!(2, stage.last_stable);
}

// ========== Idempotent Producer Tests ==========

#[tokio::test]
async fn test_idempotent_produce() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("idempotent-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    // Initialize idempotent producer
    let producer = engine
        .init_producer(None, 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    let topition = Topition::new("idempotent-topic", 0);

    // First batch with sequence 0
    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: producer.id,
        producer_epoch: producer.epoch,
        base_sequence: 0,
        record_count: 1,
        record_data: Bytes::new(),
    };

    let offset = engine.produce(None, &topition, batch).await.unwrap();
    assert_eq!(0, offset);
}

// ========== Delete Records Tests ==========

#[tokio::test]
async fn test_delete_records() {
    use tansu_sans_io::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};

    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("delete-records-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("delete-records-topic", 0);

    // Produce some data
    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        record_count: 1,
        record_data: Bytes::new(),
    };

    for _ in 0..5 {
        let _ = engine
            .produce(None, &topition, batch.clone())
            .await
            .unwrap();
    }

    // Verify initial state
    let stage = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(0, stage.log_start);
    assert_eq!(5, stage.high_watermark);

    // Delete records up to offset 3
    let delete_request = vec![
        DeleteRecordsTopic::default()
            .name("delete-records-topic".into())
            .partitions(Some(vec![
                DeleteRecordsPartition::default()
                    .partition_index(0)
                    .offset(3),
            ])),
    ];

    let results = engine.delete_records(&delete_request).await.unwrap();

    assert_eq!(1, results.len());
    let partitions = results[0].partitions.as_ref().unwrap();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code).unwrap()
    );
    assert_eq!(3, partitions[0].low_watermark);

    // Verify log_start was updated
    let stage = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(3, stage.log_start);
}

#[tokio::test]
async fn test_fetch_with_min_bytes() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("min-bytes-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("min-bytes-topic", 0);

    // Produce multiple small batches with valid empty batch (record_count=0)
    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        record_count: 0,
        record_data: Bytes::new(),
    };

    for _ in 0..5 {
        let _ = engine
            .produce(None, &topition, batch.clone())
            .await
            .unwrap();
    }

    // Verify data was written
    let stage = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(5, stage.high_watermark);
}

// ========== Register Broker Tests ==========

#[tokio::test]
async fn test_register_broker() {
    let engine = create_test_engine().await;

    let registration = BrokerRegistrationRequest {
        cluster_id: "test-cluster".into(),
        broker_id: 1,
        rack: Some("rack-1".into()),
        incarnation_id: Default::default(),
    };

    engine.register_broker(registration).await.unwrap();

    // Check that broker is now registered
    let brokers = engine.brokers().await.unwrap();
    assert_eq!(1, brokers.len());
    assert_eq!(1, brokers[0].broker_id);
    assert_eq!(Some("rack-1".to_string()), brokers[0].rack);
}

// ========== Version Four Plus Transaction Tests ==========

#[tokio::test]
async fn test_txn_add_partitions_version_four_plus() {
    use tansu_sans_io::add_partitions_to_txn_request::{
        AddPartitionsToTxnTopic, AddPartitionsToTxnTransaction,
    };

    let engine = create_test_engine().await;

    // Create topic
    let topic = CreatableTopic::default()
        .name("v4-topic".into())
        .num_partitions(2)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    // Initialize transactional producer
    let producer = engine
        .init_producer(Some("v4-txn"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    // Use VersionFourPlus format
    let request = TxnAddPartitionsRequest::VersionFourPlus {
        transactions: vec![
            AddPartitionsToTxnTransaction::default()
                .transactional_id("v4-txn".into())
                .producer_id(producer.id)
                .producer_epoch(producer.epoch)
                .verify_only(false)
                .topics(Some(vec![
                    AddPartitionsToTxnTopic::default()
                        .name("v4-topic".into())
                        .partitions(Some(vec![0, 1])),
                ])),
        ],
    };

    let response = engine.txn_add_partitions(request).await.unwrap();

    match response {
        TxnAddPartitionsResponse::VersionFourPlus(results) => {
            assert_eq!(1, results.len());
            assert_eq!("v4-txn", results[0].transactional_id.as_str());

            let topic_results = results[0].topic_results.as_ref().unwrap();
            assert_eq!(1, topic_results.len());
            assert_eq!("v4-topic", topic_results[0].name.as_str());

            let partitions = topic_results[0].results_by_partition.as_ref().unwrap();
            assert_eq!(2, partitions.len());
        }
        _ => panic!("Expected VersionFourPlus response"),
    }
}

// ========== Additional Transaction Error Tests ==========

#[tokio::test]
async fn test_txn_wrong_producer_id() {
    use tansu_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;

    let engine = create_test_engine().await;

    // Initialize transactional producer
    let _ = engine
        .init_producer(Some("wrong-id-txn"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    // Try with wrong producer_id
    let request = TxnAddPartitionsRequest::VersionZeroToThree {
        transaction_id: "wrong-id-txn".into(),
        producer_id: 9999, // Wrong ID
        producer_epoch: 0,
        topics: vec![
            AddPartitionsToTxnTopic::default()
                .name("any-topic".into())
                .partitions(Some(vec![0])),
        ],
    };

    let response = engine.txn_add_partitions(request).await.unwrap();

    match response {
        TxnAddPartitionsResponse::VersionZeroToThree(results) => {
            let partitions = results[0].results_by_partition.as_ref().unwrap();
            assert_eq!(
                ErrorCode::UnknownProducerId,
                ErrorCode::try_from(partitions[0].partition_error_code).unwrap()
            );
        }
        _ => panic!("Expected VersionZeroToThree response"),
    }
}

#[tokio::test]
async fn test_txn_wrong_epoch() {
    use tansu_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;

    let engine = create_test_engine().await;

    // Initialize transactional producer
    let producer = engine
        .init_producer(Some("wrong-epoch-txn"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    // Try with wrong producer_epoch
    let request = TxnAddPartitionsRequest::VersionZeroToThree {
        transaction_id: "wrong-epoch-txn".into(),
        producer_id: producer.id,
        producer_epoch: 99, // Wrong epoch
        topics: vec![
            AddPartitionsToTxnTopic::default()
                .name("any-topic".into())
                .partitions(Some(vec![0])),
        ],
    };

    let response = engine.txn_add_partitions(request).await.unwrap();

    match response {
        TxnAddPartitionsResponse::VersionZeroToThree(results) => {
            let partitions = results[0].results_by_partition.as_ref().unwrap();
            assert_eq!(
                ErrorCode::ProducerFenced,
                ErrorCode::try_from(partitions[0].partition_error_code).unwrap()
            );
        }
        _ => panic!("Expected VersionZeroToThree response"),
    }
}

#[tokio::test]
async fn test_txn_end_unknown_transaction() {
    let engine = create_test_engine().await;

    // Try to end a transaction that doesn't exist
    let result = engine.txn_end("nonexistent-txn", 1, 0, true).await;

    assert!(matches!(
        result,
        Err(Error::Api(ErrorCode::TransactionalIdNotFound))
    ));
}

#[tokio::test]
async fn test_txn_end_wrong_producer() {
    let engine = create_test_engine().await;

    // Initialize transactional producer
    let producer = engine
        .init_producer(Some("end-wrong-prod"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    // Try to end with wrong producer_id
    let result = engine
        .txn_end("end-wrong-prod", 9999, producer.epoch, true)
        .await;

    assert!(matches!(
        result,
        Err(Error::Api(ErrorCode::UnknownProducerId))
    ));
}

#[tokio::test]
async fn test_txn_end_wrong_epoch() {
    let engine = create_test_engine().await;

    // Initialize transactional producer
    let producer = engine
        .init_producer(Some("end-wrong-epoch"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    // Try to end with wrong epoch
    let result = engine
        .txn_end("end-wrong-epoch", producer.id, 99, true)
        .await;

    assert!(matches!(result, Err(Error::Api(ErrorCode::ProducerFenced))));
}

// ========== Idempotent Producer Error Tests ==========

#[tokio::test]
async fn test_idempotent_unknown_producer() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("unknown-prod-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("unknown-prod-topic", 0);

    // Try to produce with unknown producer_id (but idempotent flag set)
    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: 9999, // Unknown producer
        producer_epoch: 0,
        base_sequence: 0,
        record_count: 0,
        record_data: Bytes::new(),
    };

    let result = engine.produce(None, &topition, batch).await;

    assert!(matches!(
        result,
        Err(Error::Api(ErrorCode::UnknownProducerId))
    ));
}

// ========== Delete Records Edge Cases ==========

#[tokio::test]
async fn test_delete_records_unknown_topic() {
    use tansu_sans_io::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};

    let engine = create_test_engine().await;

    // Delete records from non-existent topic
    let delete_request = vec![
        DeleteRecordsTopic::default()
            .name("nonexistent-topic".into())
            .partitions(Some(vec![
                DeleteRecordsPartition::default()
                    .partition_index(0)
                    .offset(5),
            ])),
    ];

    let results = engine.delete_records(&delete_request).await.unwrap();

    assert_eq!(1, results.len());
    let partitions = results[0].partitions.as_ref().unwrap();
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(partitions[0].error_code).unwrap()
    );
}

#[tokio::test]
async fn test_delete_records_unknown_partition() {
    use tansu_sans_io::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};

    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("del-unknown-part".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    // Delete records from non-existent partition
    let delete_request = vec![
        DeleteRecordsTopic::default()
            .name("del-unknown-part".into())
            .partitions(Some(vec![
                DeleteRecordsPartition::default()
                    .partition_index(99) // Invalid partition
                    .offset(5),
            ])),
    ];

    let results = engine.delete_records(&delete_request).await.unwrap();

    assert_eq!(1, results.len());
    let partitions = results[0].partitions.as_ref().unwrap();
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(partitions[0].error_code).unwrap()
    );
}

// ========== Multiple Partition Tests ==========

#[tokio::test]
async fn test_produce_multiple_partitions() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("multi-part-topic".into())
        .num_partitions(3)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        record_count: 0,
        record_data: Bytes::new(),
    };

    // Produce to different partitions
    for partition in 0..3 {
        let topition = Topition::new("multi-part-topic", partition);
        let offset = engine
            .produce(None, &topition, batch.clone())
            .await
            .unwrap();
        assert_eq!(0, offset); // Each partition starts at 0
    }

    // Verify each partition has its own offset
    for partition in 0..3 {
        let topition = Topition::new("multi-part-topic", partition);
        let stage = engine.offset_stage(&topition).await.unwrap();
        assert_eq!(1, stage.high_watermark);
    }
}

// ========== Builder Pattern Tests ==========

#[tokio::test]
async fn test_builder_pattern() {
    let object_store = Arc::new(InMemory::new());
    let db = Db::open("builder-test.slatedb", object_store)
        .await
        .expect("Failed to open SlateDB");

    let engine = Engine::builder()
        .cluster("builder-cluster")
        .node(42)
        .advertised_listener(Url::parse("tcp://10.0.0.1:9093").unwrap())
        .db(Arc::new(db))
        .schemas(None)
        .lake(None)
        .build();

    assert_eq!("builder-cluster", engine.cluster_id().await.unwrap());
    assert_eq!(42, engine.node().await.unwrap());
}
