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

//! `cleanup.policy=compact` for the dynostore (object store) backend.

use std::{sync::Arc, time::Duration};

use crate::common::{Error, init_tracing};
use bytes::Bytes;
use tansu_sans_io::{
    IsolationLevel,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    record::{Record, deflated, inflated},
};
use tansu_storage::{Storage, StorageContainer, Topition};
use url::Url;

mod common;

type Sc = Arc<Box<dyn Storage>>;

async fn memory_storage() -> Result<Sc, Error> {
    StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://tansu/")?)
        .build()
        .await
        .map_err(Into::into)
}

async fn create_topic(
    storage: &Sc,
    name: &str,
    configs: Vec<CreatableTopicConfig>,
) -> Result<(), Error> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some(configs)),
            false,
        )
        .await?;

    Ok(())
}

fn cleanup_compact() -> Vec<CreatableTopicConfig> {
    vec![
        CreatableTopicConfig::default()
            .name("cleanup.policy".into())
            .value(Some("compact".into())),
    ]
}

fn keyed_batch(key: &'static [u8], value: &'static [u8]) -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .record(
            Record::builder()
                .key(Some(Bytes::from_static(key)))
                .value(Some(Bytes::from_static(value))),
        )
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// (key, value, absolute offset) of every record currently fetchable.
async fn fetched_records(
    storage: &Sc,
    topition: &Topition,
) -> Result<Vec<(Option<Bytes>, Option<Bytes>, i64)>, Error> {
    let batches = storage
        .fetch(
            topition,
            0,
            1,
            64 * 1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(500),
        )
        .await?;

    let mut records = vec![];

    for deflated in &batches {
        let inflated = inflated::Batch::try_from(deflated)?;

        for record in &inflated.records {
            records.push((
                record.key.clone(),
                record.value.clone(),
                inflated.base_offset + i64::from(record.offset_delta),
            ));
        }
    }

    Ok(records)
}

#[tokio::test]
async fn keeps_latest_per_key() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = memory_storage().await?;

    create_topic(&storage, "kv", cleanup_compact()).await?;

    let topition = Topition::new("kv", 0);

    // three single-record batches, all the same key (offsets 0, 1, 2)
    for value in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
        _ = storage
            .produce(None, &topition, keyed_batch(b"alpha", value)?)
            .await?;
    }

    assert_eq!(3, fetched_records(&storage, &topition).await?.len());

    storage.maintain(std::time::SystemTime::now()).await?;

    // only the latest record for the key survives, at its original offset
    let records = fetched_records(&storage, &topition).await?;
    assert_eq!(
        vec![(
            Some(Bytes::from_static(b"alpha")),
            Some(Bytes::from_static(b"three")),
            2,
        )],
        records
    );

    Ok(())
}

#[tokio::test]
async fn distinct_keys_are_retained() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = memory_storage().await?;

    create_topic(&storage, "kv", cleanup_compact()).await?;

    let topition = Topition::new("kv", 0);

    _ = storage
        .produce(None, &topition, keyed_batch(b"a", b"1")?)
        .await?;
    _ = storage
        .produce(None, &topition, keyed_batch(b"b", b"2")?)
        .await?;

    storage.maintain(std::time::SystemTime::now()).await?;

    // different keys: nothing is superseded
    assert_eq!(2, fetched_records(&storage, &topition).await?.len());

    Ok(())
}

#[tokio::test]
async fn compacts_within_a_multi_record_batch() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = memory_storage().await?;

    create_topic(&storage, "kv", cleanup_compact()).await?;

    let topition = Topition::new("kv", 0);

    // a single batch holding k1@0, k2@1, k1@2 — k1@0 is superseded by k1@2
    let batch = inflated::Batch::builder()
        .last_offset_delta(2)
        .record(
            Record::builder()
                .offset_delta(0)
                .key(Some(Bytes::from_static(b"k1")))
                .value(Some(Bytes::from_static(b"v1"))),
        )
        .record(
            Record::builder()
                .offset_delta(1)
                .key(Some(Bytes::from_static(b"k2")))
                .value(Some(Bytes::from_static(b"v2"))),
        )
        .record(
            Record::builder()
                .offset_delta(2)
                .key(Some(Bytes::from_static(b"k1")))
                .value(Some(Bytes::from_static(b"v3"))),
        )
        .build()
        .and_then(deflated::Batch::try_from)?;

    _ = storage.produce(None, &topition, batch).await?;

    assert_eq!(3, fetched_records(&storage, &topition).await?.len());

    storage.maintain(std::time::SystemTime::now()).await?;

    // the batch is rewritten in place: k2@1 and k1@3 survive, offsets preserved
    let records = fetched_records(&storage, &topition).await?;
    assert_eq!(
        vec![
            (
                Some(Bytes::from_static(b"k2")),
                Some(Bytes::from_static(b"v2")),
                1,
            ),
            (
                Some(Bytes::from_static(b"k1")),
                Some(Bytes::from_static(b"v3")),
                2,
            ),
        ],
        records
    );

    Ok(())
}

#[tokio::test]
async fn without_compact_policy_nothing_is_removed() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = memory_storage().await?;

    create_topic(&storage, "plain", vec![]).await?;

    let topition = Topition::new("plain", 0);

    for value in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
        _ = storage
            .produce(None, &topition, keyed_batch(b"alpha", value)?)
            .await?;
    }

    storage.maintain(std::time::SystemTime::now()).await?;

    assert_eq!(3, fetched_records(&storage, &topition).await?.len());

    Ok(())
}
