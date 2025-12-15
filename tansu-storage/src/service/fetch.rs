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

use std::time::{Duration, Instant};

use rama::{Context, Service};
use tansu_sans_io::{
    ApiKey, ErrorCode, FetchRequest, FetchResponse, IsolationLevel,
    fetch_request::{FetchPartition, FetchTopic},
    fetch_response::{
        EpochEndOffset, FetchableTopicResponse, LeaderIdAndEpoch, PartitionData, SnapshotId,
    },
    metadata_response::MetadataResponseTopic,
    record::deflated::{Batch, Frame},
};
use tokio::time::sleep;
use tracing::{debug, error, instrument};

use crate::{Error, Result, Storage, Topition};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`FetchRequest`] returning [`FetchResponse`].
/// ```
/// use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
/// use tansu_sans_io::{
///     CreateTopicsRequest, ErrorCode, FetchRequest,
///     create_topics_request::CreatableTopic,
///     fetch_request::{FetchPartition, FetchTopic},
/// };
/// use tansu_storage::{CreateTopicsService, Error, FetchService, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// const CLUSTER_ID: &str = "tansu";
/// const NODE_ID: i32 = 111;
/// const HOST: &str = "localhost";
/// const PORT: i32 = 9092;
///
/// let storage = StorageContainer::builder()
///     .cluster_id(CLUSTER_ID)
///     .node_id(NODE_ID)
///     .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
///     .storage(Url::parse("memory://tansu/")?)
///     .build()
///     .await?;
///
/// let create_topic = {
///     let storage = storage.clone();
///     MapStateLayer::new(|_| storage).into_layer(CreateTopicsService)
/// };
///
/// let name = "abcba";
///
/// let response = create_topic
///     .serve(
///         Context::default(),
///         CreateTopicsRequest::default()
///             .topics(Some(vec![
///                 CreatableTopic::default()
///                     .name(name.into())
///                     .num_partitions(5)
///                     .replication_factor(3)
///                     .assignments(Some([].into()))
///                     .configs(Some([].into())),
///             ]))
///             .validate_only(Some(false)),
///     )
///     .await?;
///
/// let topics = response.topics.unwrap_or_default();
/// assert_eq!(1, topics.len());
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
///
/// let fetch = {
///     let storage = storage.clone();
///     MapStateLayer::new(|_| storage).into_layer(FetchService)
/// };
///
/// let partition = 0;
///
/// let response = fetch
///     .serve(
///         Context::default(),
///         FetchRequest::default()
///             .topics(Some(
///                 [FetchTopic::default()
///                     .topic(Some(name.into()))
///                     .partitions(Some(
///                         [FetchPartition::default().partition(partition)].into(),
///                     ))]
///                 .into(),
///             ))
///             .max_bytes(Some(0))
///             .max_wait_ms(5_000),
///     )
///     .await?;
///
/// let topics = response.responses.as_deref().unwrap_or_default();
/// assert_eq!(1, topics.len());
/// let partitions = topics[0].partitions.as_deref().unwrap_or_default();
/// assert_eq!(1, partitions.len());
/// assert_eq!(
///     ErrorCode::None,
///     ErrorCode::try_from(partitions[0].error_code)?
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FetchService;

impl ApiKey for FetchService {
    const KEY: i16 = FetchRequest::KEY;
}

impl FetchService {
    #[allow(clippy::too_many_arguments)]
    async fn fetch_partition<G>(
        &self,
        ctx: Context<G>,
        max_wait_ms: Duration,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        topic: &str,
        fetch_partition: &FetchPartition,
    ) -> Result<PartitionData>
    where
        G: Storage,
    {
        debug!(
            ?max_wait_ms,
            ?min_bytes,
            ?max_bytes,
            ?isolation,
            ?fetch_partition
        );

        let partition_index = fetch_partition.partition;
        let tp = Topition::new(topic, partition_index);

        let mut batches = Vec::new();

        let mut offset = fetch_partition.fetch_offset;

        loop {
            if *max_bytes == 0 {
                break;
            }

            debug!(offset);

            let mut fetched = ctx
                .state()
                .fetch(&tp, offset, min_bytes, *max_bytes, isolation)
                .await
                .inspect(|r| debug!(?tp, ?offset, ?r))
                .inspect_err(|error| error!(?tp, ?error))?;

            *max_bytes =
                u32::try_from(fetched.byte_size()).map(|bytes| max_bytes.saturating_sub(bytes))?;

            offset += fetched
                .iter()
                .map(|batch| batch.record_count as i64)
                .sum::<i64>();

            debug!(?offset, ?fetched);

            if fetched.is_empty() || fetched.first().is_some_and(|batch| batch.record_count == 0) {
                break;
            } else {
                batches.append(&mut fetched);
            }
        }

        let offset_stage = ctx
            .state()
            .offset_stage(&tp)
            .await
            .inspect_err(|error| error!(?error, ?tp))?;

        Ok(PartitionData::default()
            .partition_index(partition_index)
            .error_code(ErrorCode::None.into())
            .high_watermark(offset_stage.high_watermark())
            .last_stable_offset(Some(offset_stage.last_stable()))
            .log_start_offset(Some(offset_stage.log_start()))
            .diverging_epoch(None)
            .current_leader(None)
            .snapshot_id(None)
            .aborted_transactions(Some([].into()))
            .preferred_read_replica(Some(-1))
            .records(if batches.is_empty() {
                None
            } else {
                Some(Frame { batches })
            }))
        .inspect(|r| debug!(?r))
    }

    fn unknown_topic_response(&self, fetch: &FetchTopic) -> Result<FetchableTopicResponse> {
        Ok(FetchableTopicResponse::default()
            .topic(fetch.topic.clone())
            .topic_id(Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]))
            .partitions(fetch.partitions.as_ref().map(|partitions| {
                partitions
                    .iter()
                    .map(|partition| {
                        PartitionData::default()
                            .partition_index(partition.partition)
                            .error_code(ErrorCode::UnknownTopicOrPartition.into())
                            .high_watermark(0)
                            .last_stable_offset(Some(0))
                            .log_start_offset(Some(-1))
                            .diverging_epoch(Some(
                                EpochEndOffset::default().epoch(-1).end_offset(-1),
                            ))
                            .current_leader(Some(
                                LeaderIdAndEpoch::default().leader_id(0).leader_epoch(0),
                            ))
                            .snapshot_id(Some(SnapshotId::default().end_offset(-1).epoch(-1)))
                            .aborted_transactions(Some([].into()))
                            .preferred_read_replica(Some(-1))
                            .records(None)
                    })
                    .collect()
            })))
    }

    #[allow(clippy::too_many_arguments)]
    async fn fetch_topic<G>(
        &self,
        ctx: Context<G>,
        max_wait_ms: Duration,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        fetch: &FetchTopic,
        _is_first: bool,
    ) -> Result<FetchableTopicResponse>
    where
        G: Storage,
    {
        debug!(?max_wait_ms, ?min_bytes, ?isolation, ?fetch);

        let metadata = ctx.state().metadata(Some(&[fetch.into()])).await?;

        if let Some(MetadataResponseTopic {
            topic_id,
            name: Some(name),
            ..
        }) = metadata.topics().first()
        {
            let mut partitions = Vec::new();

            for fetch_partition in fetch.partitions.as_ref().unwrap_or(&Vec::new()) {
                let partition = self
                    .fetch_partition(
                        ctx.clone(),
                        max_wait_ms,
                        min_bytes,
                        max_bytes,
                        isolation,
                        name,
                        fetch_partition,
                    )
                    .await?;

                partitions.push(partition);
            }

            Ok(FetchableTopicResponse::default()
                .topic(fetch.topic.to_owned())
                .topic_id(topic_id.to_owned())
                .partitions(Some(partitions)))
        } else {
            self.unknown_topic_response(fetch)
        }
    }

    pub(crate) async fn fetch<G>(
        &self,
        ctx: Context<G>,
        max_wait: Duration,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        topics: &[FetchTopic],
    ) -> Result<Vec<FetchableTopicResponse>>
    where
        G: Storage,
    {
        debug!(?max_wait, ?min_bytes, ?isolation, ?topics);

        if topics.is_empty() {
            Ok(vec![])
        } else {
            let start = Instant::now();
            let mut responses = vec![];
            let mut iteration = 0;
            let mut elapsed = Duration::from_millis(0);
            let mut bytes = 0;

            while elapsed <= max_wait && bytes <= min_bytes {
                debug!(?elapsed, ?max_wait, ?bytes, ?min_bytes);

                let enumerate = topics.iter().enumerate();
                responses.clear();

                for (i, fetch) in enumerate {
                    let fetch_response = self
                        .fetch_topic(
                            ctx.clone(),
                            max_wait,
                            min_bytes,
                            max_bytes,
                            isolation,
                            fetch,
                            i == 0,
                        )
                        .await?;

                    responses.push(fetch_response);
                }

                bytes += u32::try_from(responses.byte_size())?;

                let now = Instant::now();
                elapsed = now.duration_since(start);
                let remaining = max_wait.saturating_sub(elapsed);

                debug!(
                    ?iteration,
                    ?max_wait,
                    ?elapsed,
                    ?remaining,
                    ?bytes,
                    ?min_bytes
                );

                sleep(if remaining.as_millis() >= 250 {
                    remaining / 2
                } else {
                    remaining
                })
                .await;

                iteration += 1;
            }

            Ok(responses)
        }
    }
}

impl<G> Service<G, FetchRequest> for FetchService
where
    G: Storage,
{
    type Response = FetchResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: FetchRequest,
    ) -> Result<Self::Response, Self::Error> {
        debug!(?req);

        let responses = Some(if let Some(topics) = req.topics {
            let isolation_level = req
                .isolation_level
                .map_or(Ok(IsolationLevel::ReadUncommitted), |isolation| {
                    IsolationLevel::try_from(isolation)
                })?;

            let max_wait_ms = u64::try_from(req.max_wait_ms).map(Duration::from_millis)?;

            let min_bytes = u32::try_from(req.min_bytes)?;

            const DEFAULT_MAX_BYTES: u32 = 5 * 1024 * 1024;

            let mut max_bytes = req.max_bytes.map_or(Ok(DEFAULT_MAX_BYTES), |max_bytes| {
                u32::try_from(max_bytes).map(|max_bytes| max_bytes.min(DEFAULT_MAX_BYTES))
            })?;

            self.fetch(
                ctx,
                max_wait_ms,
                min_bytes,
                &mut max_bytes,
                isolation_level,
                topics.as_ref(),
            )
            .await?
        } else {
            vec![]
        });

        Ok(FetchResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(Some(ErrorCode::None.into()))
            .session_id(Some(0))
            .node_endpoints(Some([].into()))
            .responses(responses))
        .inspect(|r| debug!(?r))
    }
}

trait ByteSize {
    fn byte_size(&self) -> u64;
}

impl<T> ByteSize for Vec<T>
where
    T: ByteSize,
{
    fn byte_size(&self) -> u64 {
        self.iter().map(|item| item.byte_size()).sum()
    }
}

impl<T> ByteSize for Option<T>
where
    T: ByteSize,
{
    fn byte_size(&self) -> u64 {
        self.as_ref().map_or(0, |some| some.byte_size())
    }
}

impl ByteSize for Batch {
    fn byte_size(&self) -> u64 {
        self.record_data.len() as u64
    }
}

impl ByteSize for Frame {
    fn byte_size(&self) -> u64 {
        self.batches.byte_size()
    }
}

impl ByteSize for PartitionData {
    fn byte_size(&self) -> u64 {
        self.records.byte_size()
    }
}

impl ByteSize for FetchableTopicResponse {
    fn byte_size(&self) -> u64 {
        self.partitions.byte_size()
    }
}
