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

//! `cleanup.policy=delete` (retention) for the dynostore (object store) backend.

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

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

fn batch(value: &'static [u8]) -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(value))))
        .build()
        .and_then(deflated::Batch::try_from)
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

async fn fetch_len(storage: &Sc, topition: &Topition) -> Result<usize, Error> {
    storage
        .fetch(
            topition,
            0,
            1,
            64 * 1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(500),
        )
        .await
        .map(|batches| batches.len())
        .map_err(Into::into)
}

async fn produce_three(storage: &Sc, topition: &Topition) -> Result<(), Error> {
    for value in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
        _ = storage.produce(None, topition, batch(value)?).await?;
    }

    Ok(())
}

#[tokio::test]
async fn retention_ms_expires_old_records() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = memory_storage().await?;

    let retention = Duration::from_mins(30);

    create_topic(
        &storage,
        "retained",
        vec![
            CreatableTopicConfig::default()
                .name("cleanup.policy".into())
                .value(Some("delete".into())),
            CreatableTopicConfig::default()
                .name("retention.ms".into())
                .value(Some(retention.as_millis().to_string())),
        ],
    )
    .await?;

    let topition = Topition::new("retained", 0);

    produce_three(&storage, &topition).await?;

    // the three batches are present
    assert_eq!(3, fetch_len(&storage, &topition).await?);

    // a maintenance pass now keeps everything: nothing is older than 30m
    storage.maintain(SystemTime::now()).await?;
    assert_eq!(3, fetch_len(&storage, &topition).await?);

    // a maintenance pass an hour into the future ages every batch out
    let later = SystemTime::now()
        .checked_add(retention + Duration::from_mins(30))
        .expect("an hour ahead");
    storage.maintain(later).await?;

    assert_eq!(0, fetch_len(&storage, &topition).await?);

    // the partition reports an empty log
    let stage = storage.offset_stage(&topition).await?;
    assert_eq!(0, stage.log_start());

    Ok(())
}

#[tokio::test]
async fn default_retention_keeps_recent_records() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = memory_storage().await?;

    // cleanup.policy=delete with no retention.ms falls back to the 7 day default
    create_topic(
        &storage,
        "defaulted",
        vec![
            CreatableTopicConfig::default()
                .name("cleanup.policy".into())
                .value(Some("delete".into())),
        ],
    )
    .await?;

    let topition = Topition::new("defaulted", 0);

    produce_three(&storage, &topition).await?;

    assert_eq!(3, fetch_len(&storage, &topition).await?);

    // an hour into the future is still well inside the 7 day window
    let later = SystemTime::now()
        .checked_add(Duration::from_hours(1))
        .expect("an hour ahead");
    storage.maintain(later).await?;

    assert_eq!(3, fetch_len(&storage, &topition).await?);

    // eight days into the future is past the default retention
    let later = SystemTime::now()
        .checked_add(Duration::from_hours(8 * 24))
        .expect("eight days ahead");
    storage.maintain(later).await?;

    assert_eq!(0, fetch_len(&storage, &topition).await?);

    Ok(())
}

#[tokio::test]
async fn without_delete_policy_records_are_retained() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = memory_storage().await?;

    // no cleanup.policy: maintenance must never drop records
    create_topic(&storage, "forever", vec![]).await?;

    let topition = Topition::new("forever", 0);

    produce_three(&storage, &topition).await?;

    assert_eq!(3, fetch_len(&storage, &topition).await?);

    let later = SystemTime::now()
        .checked_add(Duration::from_hours(100 * 24))
        .expect("a hundred days ahead");
    storage.maintain(later).await?;

    assert_eq!(3, fetch_len(&storage, &topition).await?);

    Ok(())
}
