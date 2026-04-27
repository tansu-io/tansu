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

//! Regression tests for the storage backends that decompose record
//! batches into individual rows on produce and reconstruct them on
//! fetch (libsql/`lite.rs`, turso/`limbo.rs`, postgres/`pg.rs`).
//!
//! Reproduces the `UnexpectedType("Snappy")` failure: each backend used
//! to copy the original batch attributes, including the Snappy
//! compression bits, into the rebuilt batch even though the record data
//! had already been decompressed during produce. The subsequent
//! `inflated -> deflated` conversion then exploded because
//! `tansu_sans_io::record::deflated::into_record_data` has no Snappy
//! arm.
//!
//! `dynostore` (S3/memory) and `slatedb` store batches as opaque blobs
//! and are not affected.

mod common;

use std::{sync::Arc, thread};

use bytes::Bytes;
use rand::{distr::Alphanumeric, prelude::*, rng};
use tansu_sans_io::{
    BatchAttribute, Compression, Encode as _, IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use tansu_storage::{
    BrokerRegistrationRequest, Storage, StorageContainer, Topition,
};
use tracing::debug;
use url::Url;
use uuid::Uuid;

use crate::common::{Error, init_tracing};

/// Build a Snappy-compressed `deflated::Batch` directly.
///
/// `tansu-sans-io` currently supports inflating Snappy (read path) but
/// not deflating to Snappy (`into_record_data` has no Snappy arm), so
/// we hand-roll the framing here rather than rely on
/// `inflated::Batch::try_into`.
fn snappy_batch(values: &[&[u8]]) -> Result<deflated::Batch, Error> {
    let mut builder = inflated::Batch::builder();
    for (delta, value) in values.iter().enumerate() {
        builder = builder.record(
            Record::builder()
                .offset_delta(delta as i32)
                .value(Bytes::copy_from_slice(value).into()),
        );
    }
    if values.len() > 1 {
        builder = builder.last_offset_delta((values.len() - 1) as i32);
    }
    let inflated = builder.build()?;

    let uncompressed = inflated.records.as_slice().encode()?;

    let snappy_compressed: Bytes = snap::raw::Encoder::new()
        .compress_vec(&uncompressed)
        .map_err(|err| Error::Message(format!("snap encode: {err:?}")))?
        .into();

    let attributes: i16 = BatchAttribute::default()
        .compression(Compression::Snappy)
        .into();

    let record_count = u32::try_from(inflated.records.len())
        .map_err(|err| Error::Message(format!("record_count: {err:?}")))?;

    Ok(deflated::Batch {
        base_offset: inflated.base_offset,
        // The storage layer's produce path inflates via
        // `Vec<Record>::try_from` and does not validate `batch_length`
        // or `crc`, so leaving them zero here keeps the test free of
        // a crc-fast dev-dep.
        batch_length: 0,
        partition_leader_epoch: inflated.partition_leader_epoch,
        magic: inflated.magic,
        crc: 0,
        attributes,
        last_offset_delta: inflated.last_offset_delta,
        base_timestamp: inflated.base_timestamp,
        max_timestamp: inflated.max_timestamp,
        producer_id: inflated.producer_id,
        producer_epoch: inflated.producer_epoch,
        base_sequence: inflated.base_sequence,
        record_count,
        record_data: snappy_compressed,
    })
}

fn storage_url(scheme: &str) -> Result<Url, Error> {
    thread::current()
        .name()
        .ok_or_else(|| Error::Message("unnamed thread".into()))
        .map(|name| {
            format!(
                "{scheme}://../logs/{}/{}::{name}.db",
                env!("CARGO_PKG_NAME"),
                env!("CARGO_CRATE_NAME"),
            )
        })
        .and_then(|url| Url::parse(&url).map_err(Into::into))
}

async fn build_storage(
    url: Url,
    cluster: &str,
    node: i32,
) -> Result<Arc<Box<dyn Storage>>, Error> {
    StorageContainer::builder()
        .cluster_id(cluster)
        .node_id(node)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
        .storage(url)
        .build()
        .await
        .map_err(Into::into)
}

async fn produce_fetch_round_trip(url: Url) -> Result<(), Error> {
    let _guard = init_tracing()?;

    let cluster_id = Uuid::now_v7().to_string();
    let node_id = rng().random_range(0..i32::MAX);

    let storage = build_storage(url, &cluster_id, node_id).await?;

    storage
        .register_broker(BrokerRegistrationRequest {
            broker_id: node_id,
            cluster_id: cluster_id.clone(),
            incarnation_id: Uuid::new_v4(),
            rack: None,
        })
        .await?;

    let topic_name: String = rng()
        .sample_iter(&Alphanumeric)
        .take(15)
        .map(char::from)
        .collect();

    let _topic_id = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await
        .inspect(|topic_id| debug!(?topic_id))?;

    let topition = Topition::new(topic_name.clone(), 0);

    let payload = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit";
    let batch = snappy_batch(&[payload])?;

    let base_offset = storage.produce(None, &topition, batch).await?;
    assert_eq!(0, base_offset);

    // Pre-fix this would explode with `UnexpectedType("Snappy")`: the
    // fetch path propagates the original Snappy attribute bits onto a
    // freshly built batch whose record data has already been
    // decompressed by produce.
    let batches = storage
        .fetch(
            &topition,
            0,
            1,
            1024 * 1024,
            IsolationLevel::ReadUncommitted,
        )
        .await?;

    assert_eq!(1, batches.len(), "expected exactly one batch from fetch");

    // Whatever compression the fetched batch claims, it must decode
    // back to the value we produced.
    let inflated = inflated::Batch::try_from(batches[0].clone())?;
    assert_eq!(1, inflated.records.len());
    assert_eq!(
        Some(Bytes::copy_from_slice(payload)),
        inflated.records[0].value.clone(),
    );

    Ok(())
}

/// Sanity check: `snappy_batch` produces a batch that round-trips
/// through `inflated::Batch::try_from` (i.e. the framing is valid).
#[tokio::test]
async fn snappy_batch_helper_round_trips() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let batch = snappy_batch(&[b"Lorem ipsum dolor sit amet"])?;
    let inflated = inflated::Batch::try_from(batch)?;
    assert_eq!(1, inflated.records.len());
    assert_eq!(
        Some(Bytes::from_static(b"Lorem ipsum dolor sit amet")),
        inflated.records[0].value.clone(),
    );
    Ok(())
}

#[cfg(feature = "libsql")]
#[tokio::test]
async fn libsql_snappy_produce_then_fetch_round_trip() -> Result<(), Error> {
    produce_fetch_round_trip(storage_url("sqlite")?).await
}

// The turso end-to-end harness is unstable (existing turso tests in
// `tansu-broker` are all `#[ignore]`); run by hand with
// `cargo test --features turso -- --ignored` once the harness lands.
#[cfg(feature = "turso")]
#[ignore]
#[tokio::test]
async fn turso_snappy_produce_then_fetch_round_trip() -> Result<(), Error> {
    produce_fetch_round_trip(storage_url("turso")?).await
}

// Postgres requires `postgres://postgres:postgres@localhost`; skipped
// by default to match how the postgres tests in tansu-broker are run.
#[cfg(feature = "postgres")]
#[ignore]
#[tokio::test]
async fn postgres_snappy_produce_then_fetch_round_trip() -> Result<(), Error> {
    produce_fetch_round_trip(Url::parse(
        "postgres://postgres:postgres@localhost",
    )?)
    .await
}
