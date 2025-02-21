// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::time::{Duration, Instant};

use tansu_kafka_sans_io::{
    Body, ErrorCode, IsolationLevel,
    fetch_request::{FetchPartition, FetchTopic},
    fetch_response::{
        EpochEndOffset, FetchableTopicResponse, LeaderIdAndEpoch, PartitionData, SnapshotId,
    },
    metadata_response::MetadataResponseTopic,
    record::{deflated::Batch, deflated::Frame},
};
use tansu_storage::{Storage, Topition};
use tokio::time::sleep;
use tracing::{debug, error};

use crate::Result;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FetchRequest<S> {
    storage: S,
}

impl<S> FetchRequest<S>
where
    S: Storage,
{
    pub fn with_storage(storage: S) -> Self {
        Self { storage }
    }

    async fn fetch_partition(
        &mut self,
        max_wait_ms: Duration,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        topic: &str,
        fetch_partition: &FetchPartition,
    ) -> Result<PartitionData> {
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

            let mut fetched = self
                .storage
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

        let offset_stage = self
            .storage
            .offset_stage(&tp)
            .await
            .inspect_err(|error| error!(?error, ?tp))?;

        Ok(PartitionData {
            partition_index,
            error_code: ErrorCode::None.into(),
            high_watermark: offset_stage.high_watermark(),
            last_stable_offset: Some(offset_stage.last_stable()),
            log_start_offset: Some(offset_stage.log_start()),
            diverging_epoch: None,
            current_leader: None,
            snapshot_id: None,
            aborted_transactions: Some([].into()),
            preferred_read_replica: Some(-1),
            records: if batches.is_empty() {
                None
            } else {
                Some(Frame { batches })
            },
        })
        .inspect(|r| debug!(?r))
    }

    fn unknown_topic_response(&self, fetch: &FetchTopic) -> Result<FetchableTopicResponse> {
        Ok(FetchableTopicResponse {
            topic: fetch.topic.clone(),
            topic_id: Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
            partitions: fetch.partitions.as_ref().map(|partitions| {
                partitions
                    .iter()
                    .map(|partition| PartitionData {
                        partition_index: partition.partition,
                        error_code: ErrorCode::UnknownTopicOrPartition.into(),
                        high_watermark: 0,
                        last_stable_offset: Some(0),
                        log_start_offset: Some(-1),
                        diverging_epoch: Some(EpochEndOffset {
                            epoch: -1,
                            end_offset: -1,
                        }),
                        current_leader: Some(LeaderIdAndEpoch {
                            leader_id: 0,
                            leader_epoch: 0,
                        }),
                        snapshot_id: Some(SnapshotId {
                            end_offset: -1,
                            epoch: -1,
                        }),
                        aborted_transactions: Some([].into()),
                        preferred_read_replica: Some(-1),
                        records: None,
                    })
                    .collect()
            }),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn fetch_topic(
        &mut self,
        max_wait_ms: Duration,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        fetch: &FetchTopic,
        _is_first: bool,
    ) -> Result<FetchableTopicResponse> {
        debug!(?max_wait_ms, ?min_bytes, ?isolation, ?fetch);

        let metadata = self.storage.metadata(Some(&[fetch.into()])).await?;

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

            Ok(FetchableTopicResponse {
                topic: fetch.topic.to_owned(),
                topic_id: topic_id.to_owned(),
                partitions: Some(partitions),
            })
        } else {
            self.unknown_topic_response(fetch)
        }
    }

    pub(crate) async fn fetch(
        &mut self,
        max_wait: Duration,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        topics: &[FetchTopic],
    ) -> Result<Vec<FetchableTopicResponse>> {
        debug!(?max_wait, ?min_bytes, ?isolation, ?topics);

        if topics.is_empty() {
            Ok(vec![])
        } else {
            let start = Instant::now();
            let mut responses = vec![];
            let mut iteration = 0;
            let mut elapsed = Duration::from_millis(0);
            let mut bytes = 0;

            while elapsed < max_wait && bytes < min_bytes {
                debug!(?elapsed, ?max_wait, ?bytes, ?min_bytes);

                let enumerate = topics.iter().enumerate();
                responses.clear();

                for (i, fetch) in enumerate {
                    let fetch_response = self
                        .fetch_topic(max_wait, min_bytes, max_bytes, isolation, fetch, i == 0)
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

    pub async fn response(
        &mut self,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: Option<i32>,
        isolation_level: Option<i8>,
        topics: Option<&[FetchTopic]>,
    ) -> Result<Body> {
        debug!(?max_wait_ms, ?min_bytes, ?max_bytes, ?topics);

        let responses = Some(if let Some(topics) = topics {
            let isolation_level = isolation_level
                .map_or(Ok(IsolationLevel::ReadUncommitted), |isolation| {
                    IsolationLevel::try_from(isolation)
                })?;

            let max_wait_ms = u64::try_from(max_wait_ms).map(Duration::from_millis)?;

            let min_bytes = u32::try_from(min_bytes)?;

            const DEFAULT_MAX_BYTES: u32 = 5 * 1024 * 1024;

            let mut max_bytes = max_bytes.map_or(Ok(DEFAULT_MAX_BYTES), |max_bytes| {
                u32::try_from(max_bytes).map(|max_bytes| max_bytes.min(DEFAULT_MAX_BYTES))
            })?;

            self.fetch(
                max_wait_ms,
                min_bytes,
                &mut max_bytes,
                isolation_level,
                topics,
            )
            .await?
        } else {
            vec![]
        });

        Ok(Body::FetchResponse {
            throttle_time_ms: Some(0),
            error_code: Some(ErrorCode::None.into()),
            session_id: Some(0),
            node_endpoints: Some([].into()),
            responses,
        })
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
