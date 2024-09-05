// Copyright ⓒ 2024 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use tansu_kafka_sans_io::{
    fetch_request::{FetchPartition, FetchTopic},
    fetch_response::{
        EpochEndOffset, FetchableTopicResponse, LeaderIdAndEpoch, PartitionData, SnapshotId,
    },
    record::{deflated::Batch, deflated::Frame},
    Body, ErrorCode, IsolationLevel,
};
use tansu_storage::{Storage, Topition};
use tokio::time::sleep;
use tracing::debug;

use crate::{Result, State};

#[derive(Clone, Debug)]
pub(crate) struct FetchRequest {
    pub storage: Arc<Mutex<Storage>>,
}

impl FetchRequest {
    pub(crate) fn storage_lock(&self) -> Result<MutexGuard<'_, Storage>> {
        self.storage.lock().map_err(|error| error.into())
    }

    async fn fetch_partition(
        &mut self,
        max_wait_ms: Duration,
        min_bytes: u64,
        max_bytes: Option<u64>,
        isolation: Option<IsolationLevel>,
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

        for offset in fetch_partition.fetch_offset.. {
            let mut fetched = self
                .storage_lock()
                .and_then(|mut storage| storage.fetch(&tp, offset).map_err(Into::into))
                .map_or(Vec::new(), |batch| vec![batch]);

            debug!(?offset, ?fetched);

            if fetched.is_empty() {
                break;
            } else {
                batches.append(&mut fetched);
            }
        }

        self.storage_lock().and_then(|storage| {
            let last_stable_offset = storage.last_stable_offset(&tp)?;
            let high_watermark = storage.high_watermark(&tp)?;

            Ok(PartitionData {
                partition_index,
                error_code: ErrorCode::None.into(),
                high_watermark,
                last_stable_offset: Some(last_stable_offset),
                log_start_offset: Some(-1),
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
        })
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
    async fn fetch_topic<'a>(
        &mut self,
        state: &'a State,
        max_wait_ms: Duration,
        min_bytes: u64,
        max_bytes: Option<u64>,
        isolation: Option<IsolationLevel>,
        fetch: &'a FetchTopic,
        _is_first: bool,
    ) -> Result<FetchableTopicResponse> {
        debug!(?state, ?max_wait_ms, ?min_bytes, ?isolation, ?fetch);

        match state.topic(fetch.into()) {
            None => self.unknown_topic_response(fetch),

            Some(detail) => {
                let mut partitions = Vec::new();

                for fetch_partition in fetch.partitions.as_ref().unwrap_or(&Vec::new()) {
                    let partition = self
                        .fetch_partition(
                            max_wait_ms,
                            min_bytes,
                            max_bytes,
                            isolation,
                            detail.name(),
                            fetch_partition,
                        )
                        .await?;

                    partitions.push(partition);
                }

                Ok(FetchableTopicResponse {
                    topic: fetch.topic.to_owned(),
                    topic_id: Some(detail.id),
                    partitions: Some(partitions),
                })
            }
        }
    }

    pub(crate) async fn fetch<'a>(
        &mut self,
        state: &'a State,
        max_wait: Duration,
        min_bytes: u64,
        max_bytes: Option<u64>,
        isolation: Option<IsolationLevel>,
        topics: &'a [FetchTopic],
    ) -> Result<Vec<FetchableTopicResponse>> {
        debug!(?state, ?max_wait, ?min_bytes, ?isolation, ?topics);

        if topics.is_empty() {
            Ok(vec![])
        } else {
            let start = Instant::now();
            let mut responses = vec![];
            let mut iteration = 0;
            let mut elapsed = Duration::from_millis(0);
            let mut bytes = 0;

            while elapsed < max_wait && bytes < min_bytes {
                let enumerate = topics.iter().enumerate();
                responses.clear();

                for (i, fetch) in enumerate {
                    let fetch_response = self
                        .fetch_topic(
                            state,
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

                bytes = responses.byte_size();

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

    pub(crate) async fn response(
        &mut self,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: Option<i32>,
        isolation_level: Option<i8>,
        topics: Option<&[FetchTopic]>,
        state: &State,
    ) -> Result<Body> {
        let responses = Some(if let Some(topics) = topics {
            let isolation_level = isolation_level.map_or(Ok(None), |isolation| {
                IsolationLevel::try_from(isolation).map(Some)
            })?;

            let max_wait_ms = u64::try_from(max_wait_ms).map(Duration::from_millis)?;

            let min_bytes = u64::try_from(min_bytes)?;

            let max_bytes =
                max_bytes.map_or(Ok(None), |max_bytes| u64::try_from(max_bytes).map(Some))?;

            self.fetch(
                state,
                max_wait_ms,
                min_bytes,
                max_bytes,
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
