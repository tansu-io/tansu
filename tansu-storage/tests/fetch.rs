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

mod common;

mod doctest_template {
    use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
    use tansu_sans_io::{
        CreateTopicsRequest, ErrorCode, FetchRequest,
        create_topics_request::CreatableTopic,
        fetch_request::{FetchPartition, FetchTopic},
    };
    use tansu_storage::{CreateTopicsService, FetchService, StorageContainer};
    use url::Url;

    use crate::common::{Error, init_tracing};

    #[tokio::test]
    async fn req() -> Result<(), Error> {
        let _guard = init_tracing()?;

        const CLUSTER_ID: &str = "tansu";
        const NODE_ID: i32 = 111;
        const HOST: &str = "localhost";
        const PORT: i32 = 9092;

        let storage = StorageContainer::builder()
            .cluster_id(CLUSTER_ID)
            .node_id(NODE_ID)
            .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
            .storage(Url::parse("memory://tansu/")?)
            .build()
            .await?;

        let create_topic = {
            let storage = storage.clone();
            MapStateLayer::new(|_| storage).into_layer(CreateTopicsService)
        };

        let name = "abcba";

        let response = create_topic
            .serve(
                Context::default(),
                CreateTopicsRequest::default()
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name(name.into())
                            .num_partitions(5)
                            .replication_factor(3)
                            .assignments(Some([].into()))
                            .configs(Some([].into())),
                    ]))
                    .validate_only(Some(false)),
            )
            .await?;

        let topics = response.topics.unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

        let fetch = {
            let storage = storage.clone();
            MapStateLayer::new(|_| storage).into_layer(FetchService)
        };

        let partition = 0;

        let response = fetch
            .serve(
                Context::default(),
                FetchRequest::default()
                    .topics(Some(
                        [FetchTopic::default()
                            .topic(Some(name.into()))
                            .partitions(Some(
                                [FetchPartition::default().partition(partition)].into(),
                            ))]
                        .into(),
                    ))
                    .max_bytes(Some(0))
                    .max_wait_ms(5_000),
            )
            .await?;

        let topics = response.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        let partitions = topics[0].partitions.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );

        Ok(())
    }
}

/// Exercises the `start-after` based fetch on the `dynostore` (`memory://`) engine: fetch must
/// return the contiguous run of batches at or after the requested offset, bounded by `max_bytes`,
/// without depending on the partition's total history.
mod start_after {
    use std::{sync::Arc, time::Duration};

    use bytes::Bytes;
    use tansu_sans_io::{
        IsolationLevel,
        record::{Record, deflated, inflated},
    };
    use tansu_storage::{Storage, StorageContainer, Topition};
    use url::Url;

    use crate::common::{Error, init_tracing};

    const CLUSTER_ID: &str = "tansu";
    const NODE_ID: i32 = 111;
    const MAX_WAIT: Duration = Duration::from_secs(5);

    async fn in_memory_storage() -> Result<Arc<Box<dyn Storage>>, Error> {
        StorageContainer::builder()
            .cluster_id(CLUSTER_ID)
            .node_id(NODE_ID)
            .advertised_listener(Url::parse("tcp://localhost:9092")?)
            .storage(Url::parse("memory://tansu/")?)
            .build()
            .await
            .map_err(Into::into)
    }

    fn single_record_batch(value: &'static [u8]) -> Result<deflated::Batch, Error> {
        inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(value).into()))
            .build()
            .and_then(deflated::Batch::try_from)
            .map_err(Into::into)
    }

    /// Produce `count` single-record batches; each occupies one offset, so base offsets are
    /// `0..count` and the high watermark ends at `count`.
    async fn produce_batches(
        storage: &Arc<Box<dyn Storage>>,
        topition: &Topition,
        count: i64,
    ) -> Result<(), Error> {
        for offset in 0..count {
            let assigned = storage
                .produce(None, topition, single_record_batch(b"lorem ipsum")?)
                .await?;
            assert_eq!(offset, assigned);
        }

        Ok(())
    }

    #[tokio::test]
    async fn fetch_from_middle_returns_contiguous_tail() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let storage = in_memory_storage().await?;
        let topition = Topition::new("abcba", 0);

        produce_batches(&storage, &topition, 10).await?;

        let batches = storage
            .fetch(
                &topition,
                4,
                0,
                1024 * 1024,
                IsolationLevel::ReadUncommitted,
                MAX_WAIT,
            )
            .await?;

        let base_offsets = batches.iter().map(|b| b.base_offset).collect::<Vec<_>>();
        assert_eq!(vec![4, 5, 6, 7, 8, 9], base_offsets);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_from_start_returns_all() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let storage = in_memory_storage().await?;
        let topition = Topition::new("abcba", 0);

        produce_batches(&storage, &topition, 10).await?;

        let batches = storage
            .fetch(
                &topition,
                0,
                0,
                1024 * 1024,
                IsolationLevel::ReadUncommitted,
                MAX_WAIT,
            )
            .await?;

        let base_offsets = batches.iter().map(|b| b.base_offset).collect::<Vec<_>>();
        assert_eq!((0..10).collect::<Vec<_>>(), base_offsets);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_is_bounded_by_max_bytes() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let storage = in_memory_storage().await?;
        let topition = Topition::new("abcba", 0);

        produce_batches(&storage, &topition, 10).await?;

        // A single byte budget always yields at least one batch (Kafka semantics) but never the
        // whole partition.
        let batches = storage
            .fetch(
                &topition,
                0,
                0,
                1,
                IsolationLevel::ReadUncommitted,
                MAX_WAIT,
            )
            .await?;

        assert_eq!(1, batches.len());
        assert_eq!(0, batches[0].base_offset);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_at_or_beyond_high_watermark_is_empty() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let storage = in_memory_storage().await?;
        let topition = Topition::new("abcba", 0);

        produce_batches(&storage, &topition, 10).await?;

        for offset in [10, 11, 100] {
            let batches = storage
                .fetch(
                    &topition,
                    offset,
                    0,
                    1024 * 1024,
                    IsolationLevel::ReadUncommitted,
                    MAX_WAIT,
                )
                .await?;
            assert!(batches.is_empty(), "offset {offset} should be empty");
        }

        Ok(())
    }
}
