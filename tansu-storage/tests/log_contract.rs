// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

mod common;

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use tansu_sans_io::{
    ErrorCode, IsolationLevel, ListOffset,
    record::{
        Record,
        deflated::{self},
        inflated,
    },
};
use tansu_storage::{
    ListOffsetResponse, Storage, StorageCertification, StorageEngine, StorageFeature, Topition,
};
use tokio::time::sleep;
use url::Url;

use crate::common::{Error, build_storage, create_topic, init_tracing, register_broker};

const CLUSTER_ID: &str = "phase06-storage-log-contract";
const NODE_ID: i32 = 111;
const PARTITIONS: i32 = 2;

fn topic_name(name: &str) -> String {
    format!("phase06-{name}")
}

fn topition(topic: &str, partition: i32) -> Topition {
    Topition::new(topic, partition)
}

fn single_record_batch(value: &'static [u8]) -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .record(Record::builder().value(Bytes::from_static(value).into()))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn prepare_storage(storage_url: Url, topic: &str) -> Result<Arc<Box<dyn Storage>>, Error> {
    let storage = build_storage(CLUSTER_ID, NODE_ID, storage_url).await?;

    register_broker(&storage, CLUSTER_ID, NODE_ID).await?;
    _ = create_topic(&storage, topic, PARTITIONS).await?;

    Ok(storage)
}

async fn list_offset(
    storage: &impl Storage,
    topition: &Topition,
    timestamp: ListOffset,
) -> Result<ListOffsetResponse, Error> {
    let response = storage
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), timestamp)],
        )
        .await?;

    Ok(response
        .into_iter()
        .find(|(candidate, _)| candidate == topition)
        .map(|(_, response)| response)
        .ok_or_else(|| Error::Message("missing list offset response".into()))?)
}

async fn fetch_record_count(
    storage: &impl Storage,
    topition: &Topition,
    offset: i64,
) -> Result<usize, Error> {
    let batches = storage
        .fetch(
            topition,
            offset,
            0,
            u32::MAX,
            IsolationLevel::ReadUncommitted,
        )
        .await?;

    Ok(batches
        .into_iter()
        .map(|batch| batch.record_count as usize)
        .sum())
}

async fn produce_record(
    storage: &impl Storage,
    topition: &Topition,
    value: &'static [u8],
) -> Result<i64, Error> {
    storage
        .produce(None, topition, single_record_batch(value)?)
        .await
        .map_err(Into::into)
}

async fn assert_empty_offsets(storage: &impl Storage, topic: &str) -> Result<(), Error> {
    for partition in 0..PARTITIONS {
        let topition = topition(topic, partition);
        let stage = storage.offset_stage(&topition).await?;

        assert_eq!(0, stage.log_start());
        assert_eq!(0, stage.high_watermark());
        assert_eq!(0, stage.last_stable());

        let earliest = list_offset(storage, &topition, ListOffset::Earliest).await?;
        assert_eq!(ErrorCode::None, earliest.error_code);
        assert_eq!(Some(0), earliest.offset);
        assert_eq!(None, earliest.timestamp);

        let latest = list_offset(storage, &topition, ListOffset::Latest).await?;
        assert_eq!(ErrorCode::None, latest.error_code);
        assert_eq!(Some(0), latest.offset);
        assert_eq!(None, latest.timestamp);
    }

    Ok(())
}

async fn assert_contiguous_offsets(storage: &impl Storage, topic: &str) -> Result<(), Error> {
    let topition = topition(topic, 0);
    let offsets = [
        produce_record(storage, &topition, b"one").await?,
        produce_record(storage, &topition, b"two").await?,
        produce_record(storage, &topition, b"three").await?,
    ];

    assert_eq!([0, 1, 2], offsets);

    let stage = storage.offset_stage(&topition).await?;
    assert_eq!(0, stage.log_start());
    assert_eq!(3, stage.high_watermark());
    assert_eq!(3, stage.last_stable());

    Ok(())
}

async fn assert_fetch_counts(storage: &impl Storage, topic: &str) -> Result<(), Error> {
    let topition = topition(topic, 0);

    let offsets = [
        produce_record(storage, &topition, b"one").await?,
        produce_record(storage, &topition, b"two").await?,
        produce_record(storage, &topition, b"three").await?,
    ];

    assert_eq!([0, 1, 2], offsets);
    assert_eq!(3, fetch_record_count(storage, &topition, 0).await?);
    assert_eq!(2, fetch_record_count(storage, &topition, 1).await?);

    Ok(())
}

async fn assert_timestamp_lookup(storage: &impl Storage, topic: &str) -> Result<(), Error> {
    let topition = topition(topic, 0);
    let before = SystemTime::now();

    sleep(Duration::from_millis(25)).await;
    let _ = produce_record(storage, &topition, b"one").await?;

    let response = list_offset(storage, &topition, ListOffset::Timestamp(before)).await?;

    assert_eq!(ErrorCode::None, response.error_code);
    assert_eq!(Some(0), response.offset);

    Ok(())
}

async fn backend_contract<F>(mut make_storage_url: F, label: &str) -> Result<(), Error>
where
    F: FnMut() -> Result<(Option<tempfile::TempDir>, Url), Error>,
{
    let mut keep_alive = Vec::new();

    let (tempdir, storage_url) = make_storage_url()?;
    if let Some(tempdir) = tempdir {
        keep_alive.push(tempdir);
    }

    let topic = topic_name(&format!("{label}-empty"));
    let storage = prepare_storage(storage_url, &topic).await?;
    assert_empty_offsets(&*storage, &topic).await?;

    let (tempdir, storage_url) = make_storage_url()?;
    if let Some(tempdir) = tempdir {
        keep_alive.push(tempdir);
    }

    let topic = topic_name(&format!("{label}-produce"));
    let storage = prepare_storage(storage_url, &topic).await?;
    assert_contiguous_offsets(&*storage, &topic).await?;

    let (tempdir, storage_url) = make_storage_url()?;
    if let Some(tempdir) = tempdir {
        keep_alive.push(tempdir);
    }

    let topic = topic_name(&format!("{label}-fetch"));
    let storage = prepare_storage(storage_url, &topic).await?;
    assert_fetch_counts(&*storage, &topic).await?;

    let (tempdir, storage_url) = make_storage_url()?;
    if let Some(tempdir) = tempdir {
        keep_alive.push(tempdir);
    }

    let topic = topic_name(&format!("{label}-timestamp"));
    let storage = prepare_storage(storage_url, &topic).await?;
    assert_timestamp_lookup(&*storage, &topic).await?;

    drop(keep_alive);

    Ok(())
}

#[tokio::test]
async fn empty_partition_offsets_are_zero() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let topic = topic_name("empty-partition-offsets");
    let storage = prepare_storage(
        Url::parse("memory://phase06-storage-log-contract/")?,
        &topic,
    )
    .await?;

    assert_empty_offsets(&*storage, &topic).await
}

#[tokio::test]
async fn produce_assigns_contiguous_offsets() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let topic = topic_name("produce-contiguous-offsets");
    let storage = prepare_storage(
        Url::parse("memory://phase06-storage-log-contract/")?,
        &topic,
    )
    .await?;

    assert_contiguous_offsets(&*storage, &topic).await
}

#[tokio::test]
async fn fetch_reads_written_records() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let topic = topic_name("fetch-written-records");
    let storage = prepare_storage(
        Url::parse("memory://phase06-storage-log-contract/")?,
        &topic,
    )
    .await?;

    assert_fetch_counts(&*storage, &topic).await
}

#[tokio::test]
async fn timestamp_lookup_before_first_record_returns_first_offset() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let topic = topic_name("timestamp-lookup");
    let storage = prepare_storage(
        Url::parse("memory://phase06-storage-log-contract/")?,
        &topic,
    )
    .await?;

    assert_timestamp_lookup(&*storage, &topic).await
}

#[tokio::test]
async fn null_storage_reports_log_features_unsupported() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = build_storage(CLUSTER_ID, NODE_ID, Url::parse("null://phase06/")?).await?;

    let capabilities = storage.capabilities();
    assert_eq!(StorageEngine::Null, capabilities.engine);

    for feature in [
        StorageFeature::ContiguousOffsets,
        StorageFeature::EmptyPartitionOffsets,
        StorageFeature::FetchVisibility,
        StorageFeature::ListOffsetsEarliestLatest,
        StorageFeature::TimestampLookup,
        StorageFeature::LogStartOffset,
        StorageFeature::HighWatermark,
        StorageFeature::LastStableOffset,
        StorageFeature::BatchValidation,
        StorageFeature::CompressionCodecs,
        StorageFeature::ProducerState,
        StorageFeature::Transactions,
        StorageFeature::DeleteRecords,
        StorageFeature::Retention,
        StorageFeature::Compaction,
        StorageFeature::CrashRecovery,
    ] {
        assert_eq!(
            StorageCertification::Unsupported,
            capabilities.certification(feature)
        );
        assert!(!capabilities.supports(feature));
    }

    let topic = topition("phase06-null-storage", 0);
    let response = list_offset(&*storage, &topic, ListOffset::Latest).await?;

    assert_eq!(ErrorCode::KafkaStorageError, response.error_code);
    assert_eq!(None, response.offset);
    assert_eq!(None, response.timestamp);

    Ok(())
}

#[cfg(feature = "dynostore")]
#[tokio::test]
async fn dynostore_log_contract() -> Result<(), Error> {
    let _guard = init_tracing()?;

    backend_contract(
        || Ok((None, Url::parse("memory://phase06-storage-log-contract/")?)),
        "dynostore",
    )
    .await
}

#[cfg(feature = "libsql")]
#[tokio::test]
async fn libsql_log_contract() -> Result<(), Error> {
    let _guard = init_tracing()?;

    backend_contract(
        || {
            let storage_path = "phase06-storage-log-contract-libsql.db";
            let _ = std::fs::remove_file(storage_path);
            let storage_url = Url::parse(&format!("sqlite://{storage_path}"))?;

            Ok((None, storage_url))
        },
        "libsql",
    )
    .await
}

#[cfg(feature = "slatedb")]
#[tokio::test]
async fn slatedb_log_contract() -> Result<(), Error> {
    let _guard = init_tracing()?;

    backend_contract(|| Ok((None, Url::parse("slatedb://memory")?)), "slatedb").await
}

#[cfg(feature = "postgres")]
// Run after `just db-up` then:
// `rtk cargo test -p tansu-storage log_contract_postgres --features postgres -- --ignored --nocapture`
#[ignore]
#[tokio::test]
async fn log_contract_postgres() -> Result<(), Error> {
    let _guard = init_tracing()?;

    backend_contract(
        || Ok((None, Url::parse("postgres://postgres:postgres@localhost")?)),
        "postgres",
    )
    .await
}
