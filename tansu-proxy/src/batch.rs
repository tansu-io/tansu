// Copyright ⓒ 2025 Peter Morgan <peter.james.morgan@gmail.com>
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
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    ops::DerefMut,
    pin::Pin,
    sync::{Arc, LazyLock, Mutex},
    task::{Context, Poll, Waker},
    time::{Duration, SystemTime},
    vec::IntoIter,
};

use opentelemetry::metrics::{Counter, Histogram};
use rama::{Layer, Service, error::BoxError};
use tansu_sans_io::{
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    record::{
        Record,
        deflated::{self, Frame},
        inflated,
    },
};
use tansu_service::api::{
    ApiKey, ApiVersion, CorrelationId,
    describe_config::ResourceConfig,
    produce::{ProduceRequest, ProduceResponse},
};
use tokio::time::sleep;
use tracing::debug;
use uuid::Uuid;

use crate::{METER, Result};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Topition {
    topic: String,
    partition: i32,
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct BatchRequest {
    id: Uuid,
    request: ProduceRequest,
}
impl BatchRequest {
    fn number_of_records(&self) -> usize {
        self.request.number_of_records()
    }
}

#[derive(Clone, Debug)]
enum BatchResponse {
    Waker(Waker),
    Response(ProduceResponse),
}

#[derive(Clone, Debug)]
pub struct BatchProduceService<S: Clone + Debug> {
    service: S,
    requests: Arc<Mutex<Vec<BatchRequest>>>,
    responses: Arc<Mutex<BTreeMap<Uuid, BatchResponse>>>,
    resource_config: ResourceConfig,
}

static SEND_PENDING_BATCH_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("send_pending_batch")
        .with_description("The number times we send a pending batch")
        .build()
});

static SEND_PENDING_EMPTY_BATCH_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("send_pending_empty_batch")
        .with_description("The number times we tried to send an empty pending batch")
        .build()
});

static SEND_PENDING_PRODUCE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("send_pending_produce_duration")
        .with_unit("ms")
        .with_description("The send pending produce latency in milliseconds")
        .build()
});

static SEND_PENDING_FOUND_OWNER_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("send_pending_found_owner")
        .with_description("The number of owners found after sending a pending batch")
        .build()
});

static SEND_PENDING_OWNER_WAKE_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("send_pending_owner_wake")
        .with_description(
            "The number of owners we have called wake on after sending a pending batch",
        )
        .build()
});

impl<S> BatchProduceService<S>
where
    S: Clone + Debug,
{
    async fn send_pending_batch<State>(
        &self,
        id: &Uuid,
        ctx: rama::Context<State>,
    ) -> Result<Option<ProduceResponse>>
    where
        S: Service<State, ProduceRequest, Response = ProduceResponse> + Clone + Debug,
        S::Error: Into<BoxError> + Send + Debug + 'static,
        State: Send + Sync + 'static,
    {
        debug!(%id);

        SEND_PENDING_BATCH_COUNTER.add(1, &[]);

        let requests = self
            .requests
            .lock()
            .map(|mut guard| std::mem::take(guard.deref_mut()))
            .inspect(|request| debug!(?request))
            .inspect_err(|err| debug!(?err))
            .expect("poison");

        if requests.is_empty() {
            SEND_PENDING_EMPTY_BATCH_COUNTER.add(1, &[]);
            return Ok(None);
        }

        let owners = Owner::from(&requests[..]);
        let produce_request = produce_request(requests);

        let produce_response = {
            let start = SystemTime::now();

            self.service
                .serve(ctx, produce_request)
                .await
                .inspect(|response| debug!(?response))
                .inspect_err(|err| debug!(?err))
                .map_err(Into::into)
                .inspect(|_| {
                    SEND_PENDING_PRODUCE_DURATION.record(
                        start
                            .elapsed()
                            .map_or(0, |duration| duration.as_millis() as u64),
                        &[],
                    )
                })?
        };

        let mut responses = self
            .responses
            .lock()
            .inspect_err(|err| debug!(?err))
            .expect("poison");

        let mut owners = owners.split(produce_response);
        debug!(?owners);
        SEND_PENDING_FOUND_OWNER_COUNTER.add(owners.len() as u64, &[]);

        if let Some(produce_response) = owners
            .remove(id)
            .inspect(|produce_response| debug!(%id, ?produce_response))
        {
            _ = responses
                .remove(id)
                .inspect(|response| debug!(%id, ?response));

            for (owner, response) in owners {
                debug!(?owner, ?response);

                if let Some(BatchResponse::Waker(waker)) =
                    responses.insert(owner, BatchResponse::Response(response))
                {
                    SEND_PENDING_OWNER_WAKE_COUNTER.add(1, &[]);
                    waker.wake();
                }
            }

            Ok(Some(produce_response))
        } else {
            for (owner, response) in owners {
                debug!(%owner, ?response);
                if let Some(BatchResponse::Waker(waker)) =
                    responses.insert(owner, BatchResponse::Response(response))
                {
                    SEND_PENDING_OWNER_WAKE_COUNTER.add(1, &[]);
                    waker.wake();
                }
            }

            Ok(None)
        }
    }
}

static BATCH_OVERFLOW_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_record_overflow")
        .with_description("The number of overflows while producing a batch")
        .build()
});

static TIMEOUT_EXPIRED_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_timeout_expired")
        .with_description("The timeouts while preparing a batch")
        .build()
});

static TICKET_READY_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_ticket_ready")
        .with_description("The ticket was ready")
        .build()
});

impl<S, State> Service<State, ProduceRequest> for BatchProduceService<S>
where
    S: Service<State, ProduceRequest, Response = ProduceResponse> + Clone + Debug,
    S::Error: Into<BoxError> + Send + Debug + 'static,
    State: Clone + Send + Sync + 'static,
{
    type Response = ProduceResponse;

    type Error = BoxError;

    async fn serve(
        &self,
        ctx: rama::Context<State>,
        request: ProduceRequest,
    ) -> Result<Self::Response, Self::Error> {
        debug!(?request);

        let batch_timeout_ms = Duration::from_millis(
            request
                .topic_names()
                .iter()
                .filter_map(|topic_name| {
                    self.resource_config
                        .get(topic_name, "tansu.batch.timeout_ms")
                        .and_then(|value| value.as_u64())
                })
                .min()
                .inspect(|batch_timeout_ms| debug!(?batch_timeout_ms))
                .unwrap_or(10_000),
        );

        let max_records = request
            .topic_names()
            .iter()
            .filter_map(|topic_name| {
                self.resource_config
                    .get(topic_name, "tansu.batch.max_records")
                    .and_then(|value| value.as_usize())
            })
            .min()
            .inspect(|max_records| debug!(?max_records))
            .unwrap_or(1_000);

        let ticket = self
            .requests
            .lock()
            .map(|mut requests| {
                let ticket = Ticket::new(self.clone());
                requests.push(BatchRequest {
                    id: ticket.id,
                    request,
                });
                ticket
            })
            .inspect(|ticket| debug!(?ticket))
            .expect("poison");

        loop {
            let id = ticket.id;
            let ctx = ctx.clone();

            if self
                .requests
                .lock()
                .map(|requests| {
                    requests
                        .iter()
                        .map(|batch_request| batch_request.number_of_records())
                        .sum::<usize>()
                })
                .inspect(|num_records| debug!(num_records, max_records))
                .expect("poison")
                >= max_records
            {
                BATCH_OVERFLOW_COUNTER.add(1, &[]);

                if let Ok(Some(produce_response)) = self
                    .send_pending_batch(&id, ctx.clone())
                    .await
                    .inspect(|r| debug!(?r))
                {
                    return Ok(produce_response);
                }
            }

            let patience = sleep(batch_timeout_ms);
            let ticket = ticket.clone();
            debug!(?patience);

            tokio::select! {
                response = ticket  => {
                    TICKET_READY_COUNTER.add(1, &[]);

                    return response
                    .inspect(|response|debug!(?response))
                    .inspect_err(|err|debug!(?err))
                }

                expired = patience => {
                    debug!(?expired);

                    TIMEOUT_EXPIRED_COUNTER.add(1, &[]);

                    if let Ok(Some(produce_response)) = self.send_pending_batch(&id, ctx).await.inspect(|r|debug!(?r)) {
                        return Ok(produce_response)
                    }
                }

            }
        }
    }
}

#[allow(dead_code)]
impl<S> BatchProduceService<S>
where
    S: Service<(), ProduceRequest, Response = ProduceResponse> + Clone + Debug,
{
    fn new(resource_config: ResourceConfig, service: S) -> Self {
        Self {
            service,
            requests: Arc::new(Mutex::new(Vec::new())),
            responses: Arc::new(Mutex::new(BTreeMap::new())),
            resource_config,
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
pub struct BatchProduceLayer {
    resource_config: ResourceConfig,
}

#[allow(dead_code)]
impl BatchProduceLayer {
    pub fn new(resource_config: ResourceConfig) -> Self {
        Self { resource_config }
    }
}

impl<S> Layer<S> for BatchProduceLayer
where
    S: Debug + Clone,
{
    type Service = BatchProduceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BatchProduceService {
            service: inner,
            requests: Arc::new(Mutex::new(Vec::new())),
            responses: Arc::new(Mutex::new(BTreeMap::new())),
            resource_config: self.resource_config.clone(),
        }
    }
}

type Topic = String;
type Partition = i32;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct BatchProduction {
    transaction_id: Option<String>,
    acks: i16,
    timeout_ms: i32,
    run: TopicPartitionBatch,
    owners: BTreeMap<Uuid, BTreeSet<Topition>>,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ProduceMetaResponse {
    api_key: ApiKey,
    api_version: ApiVersion,
    correlation_id: CorrelationId,
}

struct Owner {
    topition: BTreeMap<Topition, BTreeSet<Uuid>>,
    meta: BTreeMap<Uuid, ProduceMetaResponse>,
}

impl From<&[BatchRequest]> for Owner {
    fn from(requests: &[BatchRequest]) -> Self {
        debug!(?requests);

        let mut topition = BTreeMap::<Topition, BTreeSet<Uuid>>::new();
        let mut meta = BTreeMap::new();

        for request in requests {
            debug!(?request);

            meta.entry(request.id).or_insert(ProduceMetaResponse {
                api_key: request.request.api_key,
                api_version: request.request.api_version,
                correlation_id: request.request.correlation_id,
            });

            for topic in request.request.topic_data.as_deref().unwrap_or_default() {
                debug!(?topic);

                for partition in topic.partition_data.as_deref().unwrap_or_default() {
                    debug!(?partition);

                    topition
                        .entry(Topition {
                            topic: topic.name.clone(),
                            partition: partition.index,
                        })
                        .or_default()
                        .insert(request.id);
                }
            }
        }

        Self { topition, meta }
    }
}

impl Owner {
    fn split(&self, produce_response: ProduceResponse) -> BTreeMap<Uuid, ProduceResponse> {
        let mut responses = BTreeMap::<Uuid, BatchTopicProduceResponse>::new();

        for topic in produce_response.responses.unwrap_or_default() {
            debug!(?topic);

            for partition in topic.partition_responses.unwrap_or_default() {
                let topition = Topition {
                    topic: topic.name.clone(),
                    partition: partition.index,
                };

                debug!(?topition);

                for owner in self.topition.get(&topition).cloned().unwrap_or_default() {
                    debug!(?owner);

                    _ = responses
                        .entry(owner)
                        .or_default()
                        .0
                        .entry(topic.name.clone())
                        .or_default()
                        .0
                        .insert(partition.index, partition.clone())
                        .inspect(|partition_produce_response| debug!(?partition_produce_response));
                }
            }
        }

        responses
            .into_iter()
            .inspect(|(owner, batch_topic_produce_response)| {
                debug!(?owner, ?batch_topic_produce_response)
            })
            .map(|(owner, batch_topic_produce_response)| {
                let meta = self.meta.get(&owner).copied().unwrap_or_default();

                (
                    owner,
                    ProduceResponse {
                        api_key: meta.api_key,
                        api_version: meta.api_version,
                        correlation_id: meta.correlation_id,
                        responses: Some(batch_topic_produce_response.into_iter().collect()),
                        throttle_time_ms: produce_response.throttle_time_ms,
                        node_endpoints: produce_response.node_endpoints.clone(),
                    },
                )
            })
            .inspect(|(owner, produce_response)| debug!(?owner, ?produce_response))
            .collect()
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct BatchTopicProduceResponse(BTreeMap<Topic, BatchPartitionProduceResponse>);

impl AsRef<BTreeMap<Topic, BatchPartitionProduceResponse>> for BatchTopicProduceResponse {
    fn as_ref(&self) -> &BTreeMap<Topic, BatchPartitionProduceResponse> {
        &self.0
    }
}

impl IntoIterator for BatchTopicProduceResponse {
    type Item = TopicProduceResponse;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0
            .into_iter()
            .map(|(name, partitions)| TopicProduceResponse {
                name,
                partition_responses: Some(partitions.into_iter().collect()),
            })
            .collect::<Vec<_>>()
            .into_iter()
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct BatchPartitionProduceResponse(BTreeMap<Partition, PartitionProduceResponse>);

impl AsRef<BTreeMap<Partition, PartitionProduceResponse>> for BatchPartitionProduceResponse {
    fn as_ref(&self) -> &BTreeMap<Partition, PartitionProduceResponse> {
        &self.0
    }
}

impl IntoIterator for BatchPartitionProduceResponse {
    type Item = PartitionProduceResponse;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_values().collect::<Vec<_>>().into_iter()
    }
}

fn produce_request(requests: Vec<BatchRequest>) -> ProduceRequest {
    debug!(?requests);

    let (api_key, api_version) = requests
        .first()
        .map(|request| (request.request.api_key, request.request.api_version))
        .unwrap_or_default();

    let client_id = requests
        .first()
        .and_then(|request| request.request.client_id.clone());

    let mut run = TopicPartitionBatch::default();
    for request in requests {
        debug!(?request);

        for topic in request.request.topic_data.unwrap_or_default() {
            debug!(?topic);

            for partition in topic.partition_data.unwrap_or_default() {
                debug!(?partition);

                if let Some(mut records) = partition.records {
                    run.topics
                        .entry(topic.name.clone())
                        .or_default()
                        .partitions
                        .entry(partition.index)
                        .or_default()
                        .append(&mut records.batches);
                }
            }
        }
    }

    ProduceRequest {
        api_key,
        api_version,
        client_id,
        topic_data: Some(run.into_iter().collect::<Vec<_>>()),
        ..Default::default()
    }
}

impl From<Vec<BatchRequest>> for BatchProduction {
    fn from(requests: Vec<BatchRequest>) -> Self {
        debug!(?requests);

        let mut run = TopicPartitionBatch::default();
        let mut owners = BTreeMap::<Uuid, BTreeSet<Topition>>::new();

        for request in requests {
            debug!(?request);

            for topic in request.request.topic_data.unwrap_or_default() {
                debug!(?topic);

                for partition in topic.partition_data.unwrap_or_default() {
                    debug!(?partition);

                    owners.entry(request.id).or_default().insert(Topition {
                        topic: topic.name.clone(),
                        partition: partition.index,
                    });

                    if let Some(mut records) = partition.records {
                        run.topics
                            .entry(topic.name.clone())
                            .or_default()
                            .partitions
                            .entry(partition.index)
                            .or_default()
                            .append(&mut records.batches);
                    }
                }
            }
        }

        debug!(?run, ?owners);

        Self {
            run,
            owners,
            ..Default::default()
        }
    }
}

impl From<BatchProduction> for ProduceRequest {
    fn from(batch_production: BatchProduction) -> Self {
        Self {
            transactional_id: batch_production.transaction_id,
            acks: batch_production.acks,
            timeout_ms: batch_production.timeout_ms,
            topic_data: Some(batch_production.run.into_iter().collect::<Vec<_>>()),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TopicPartitionBatch {
    topics: BTreeMap<Topic, PartitionBatch>,
}

impl IntoIterator for TopicPartitionBatch {
    type Item = TopicProduceData;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.topics
            .into_iter()
            .map(|(name, partitions)| TopicProduceData {
                name,
                partition_data: Some(partitions.into_iter().collect()),
            })
            .collect::<Vec<_>>()
            .into_iter()
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct PartitionBatch {
    partitions: BTreeMap<Partition, Vec<deflated::Batch>>,
}

impl IntoIterator for PartitionBatch {
    type Item = PartitionProduceData;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.partitions
            .into_iter()
            .map(|(index, batches)| PartitionProduceData {
                index,
                records: Some(Frame {
                    batches: combine(batches).unwrap(),
                }),
            })
            .collect::<Vec<_>>()
            .into_iter()
    }
}

fn combine(batches: Vec<deflated::Batch>) -> Result<Vec<deflated::Batch>> {
    debug!(len = ?batches.len());

    if batches.len() >= 2 {
        let mut i = batches.into_iter();

        if let Some(first) = i.next() {
            let mut sink = inflated::Batch::try_from(first)?;

            for batch in i {
                let batch = inflated::Batch::try_from(batch)?;

                sink.records.append(
                    &mut batch
                        .records
                        .into_iter()
                        .map(|record| Record {
                            offset_delta: record.offset_delta + sink.last_offset_delta,
                            timestamp_delta: record.timestamp_delta
                                + (sink.base_timestamp - batch.base_timestamp),
                            ..record
                        })
                        .collect::<Vec<_>>(),
                );

                sink.last_offset_delta += batch.last_offset_delta;
                sink.max_timestamp = sink.max_timestamp.max(batch.max_timestamp);
            }

            deflated::Batch::try_from(sink)
                .map(|batch| vec![batch])
                .map_err(Into::into)
        } else {
            unreachable!()
        }
    } else {
        Ok(batches)
    }
}

#[derive(Clone, Debug)]
struct Ticket<S: Clone + Debug> {
    id: Uuid,
    batch: BatchProduceService<S>,
}

impl<S> Ticket<S>
where
    S: Clone + Debug,
{
    fn new(batch: BatchProduceService<S>) -> Self {
        Self {
            id: Uuid::now_v7(),
            batch,
        }
    }
}

impl<S> AsRef<Uuid> for Ticket<S>
where
    S: Clone + Debug,
{
    fn as_ref(&self) -> &Uuid {
        &self.id
    }
}

impl<S> Future for Ticket<S>
where
    S: Clone + Debug,
{
    type Output = Result<ProduceResponse, BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self
            .batch
            .responses
            .lock()
            .inspect(|responses| debug!(?responses))
        {
            Ok(ref mut responses) => match responses
                .remove(&self.id)
                .inspect(|response| debug!(id = ?self.id, ?response))
            {
                Some(BatchResponse::Response(response)) => Poll::Ready(Ok(response)),
                Some(_) => Poll::Pending,
                None => {
                    _ = responses.insert(self.id, BatchResponse::Waker(cx.waker().clone()));
                    Poll::Pending
                }
            },

            Err(error) => panic!("{error:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, thread};

    use bytes::Bytes;
    use rama::error::OpaqueError;
    use tansu_sans_io::{
        ErrorCode,
        produce_response::{LeaderIdAndEpoch, PartitionProduceResponse, TopicProduceResponse},
        record,
    };
    use tokio::{task::yield_now, time::advance};
    use tracing::{Instrument, Level, debug, span, subscriber::DefaultGuard};
    use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan};

    use super::*;

    #[derive(Clone, Default, Debug)]
    struct MockProduceService {
        reqs: Arc<Mutex<Vec<ProduceRequest>>>,
    }

    impl<State> Service<State, ProduceRequest> for MockProduceService
    where
        State: Send + Sync + 'static,
    {
        type Response = ProduceResponse;
        type Error = BoxError;

        async fn serve(
            &self,
            ctx: rama::Context<State>,
            req: ProduceRequest,
        ) -> Result<Self::Response, Self::Error> {
            let _ = ctx;
            debug!(?req);

            let produce_response = ProduceResponse {
                api_key: req.api_key,
                api_version: req.api_version,
                correlation_id: req.correlation_id,
                responses: req.topic_data.as_ref().map(|topic_data| {
                    topic_data
                        .iter()
                        .map(|topic_produce_data| TopicProduceResponse {
                            name: topic_produce_data.name.clone(),
                            partition_responses: topic_produce_data.partition_data.as_ref().map(
                                |partition_produce_data| {
                                    partition_produce_data
                                        .iter()
                                        .map(|partition_produce| PartitionProduceResponse {
                                            index: partition_produce.index,
                                            error_code: ErrorCode::None.into(),
                                            base_offset: 65456,
                                            log_append_time_ms: Some(0),
                                            log_start_offset: Some(0),
                                            record_errors: Some([].into()),
                                            error_message: Some("none".into()),
                                            current_leader: Some(LeaderIdAndEpoch {
                                                leader_id: 12321,
                                                leader_epoch: 23432,
                                            }),
                                        })
                                        .collect()
                                },
                            ),
                        })
                        .collect()
                }),
                throttle_time_ms: Some(5_000),
                node_endpoints: Some([].into()),
            };

            let mut guard = self.reqs.lock().expect("poison");
            guard.push(req);

            Ok(produce_response)
        }
    }

    fn init_tracing() -> Result<DefaultGuard> {
        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_ids(false)
                .with_span_events(FmtSpan::NONE)
                .with_env_filter(
                    EnvFilter::from_default_env()
                        .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
                )
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(OpaqueError::from_display("unnamed thread").into_boxed())
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    fn record_data_batch(value: &'static [u8]) -> Result<deflated::Batch> {
        inflated::Batch::builder()
            .base_timestamp(1_234_567_890 * 1_000)
            .max_timestamp(1_234_567_890 * 1_000)
            .record(record::Record::builder().value(Bytes::from_static(value).into()))
            .build()
            .and_then(deflated::Batch::try_from)
            .map_err(Into::into)
    }

    fn produce_request(
        topic: &str,
        correlation_id: CorrelationId,
        record_data: &'static [u8],
    ) -> Result<ProduceRequest> {
        record_data_batch(record_data).map(|batch| ProduceRequest {
            correlation_id,
            topic_data: Some(
                [TopicProduceData {
                    name: String::from(topic),
                    partition_data: Some(
                        [PartitionProduceData {
                            index: 0,
                            records: Some(Frame {
                                batches: vec![batch],
                            }),
                        }]
                        .into(),
                    ),
                }]
                .into(),
            ),
            ..Default::default()
        })
    }

    #[tokio::test(start_paused = true)]
    async fn single_request_in_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let mock = MockProduceService::default();

        let configuration = ResourceConfig::default();

        let topic_a = "a";
        configuration.put(topic_a, "tansu.batch", "true")?;
        configuration.put(topic_a, "tansu.batch.timeout_ms", "5000")?;

        let bp = (BatchProduceLayer::new(configuration)).into_layer(mock.clone());

        let correlation_id_a = 67876;

        let batch_a = {
            let bp = bp.clone();
            debug!(?bp);

            tokio::spawn(async move {
                let span = span!(Level::DEBUG, "batch", topic_a);

                async move {
                    bp.serve(
                        rama::Context::default(),
                        produce_request(topic_a, correlation_id_a, b"foo")?,
                    )
                    .await
                }
                .instrument(span)
                .await
            })
        };

        yield_now().await;
        advance(Duration::from_secs(1)).await;
        yield_now().await;

        let len = bp.requests.lock().map(|guard| guard.len()).expect("poison");
        assert_eq!(1, len);

        let len = mock.reqs.lock().map(|guard| guard.len()).expect("poison");
        assert_eq!(0, len);

        yield_now().await;
        advance(Duration::from_secs(5)).await;
        yield_now().await;

        let requests = mock.reqs.lock().map(|guard| guard.clone()).expect("poison");
        assert_eq!(
            vec![ProduceRequest {
                api_key: ApiKey(0),
                api_version: 11,
                correlation_id: 0,
                client_id: Some("tansu".into()),
                transactional_id: None,
                acks: 0,
                timeout_ms: 5000,
                topic_data: Some(
                    [TopicProduceData {
                        name: "a".into(),
                        partition_data: Some(
                            [PartitionProduceData {
                                index: 0,
                                records: Some(Frame {
                                    batches: [deflated::Batch {
                                        base_offset: 0,
                                        batch_length: 59,
                                        partition_leader_epoch: -1,
                                        magic: 2,
                                        crc: 1920191007,
                                        attributes: 0,
                                        last_offset_delta: 0,
                                        base_timestamp: 1234567890000,
                                        max_timestamp: 1234567890000,
                                        producer_id: -1,
                                        producer_epoch: 0,
                                        base_sequence: 0,
                                        record_count: 1,
                                        record_data: Bytes::from_static(b"\x12\0\0\0\x01\x06foo\0")
                                    }]
                                    .into()
                                })
                            }]
                            .into()
                        )
                    }]
                    .into()
                )
            }],
            requests
        );

        let response_a = batch_a
            .await
            .expect("a_await")
            .inspect(|produce_response| debug!(?produce_response))?;

        assert_eq!(
            ProduceResponse {
                api_key: ApiKey(0),
                api_version: 11,
                correlation_id: correlation_id_a,
                throttle_time_ms: Some(5_000),
                node_endpoints: Some([].into()),
                responses: Some(
                    [TopicProduceResponse {
                        name: "a".into(),
                        partition_responses: Some(
                            [PartitionProduceResponse {
                                index: 0,
                                error_code: 0,
                                base_offset: 65456,
                                log_append_time_ms: Some(0),
                                log_start_offset: Some(0),
                                record_errors: Some([].into()),
                                error_message: Some("none".into()),
                                current_leader: Some(LeaderIdAndEpoch {
                                    leader_id: 12321,
                                    leader_epoch: 23432
                                })
                            }]
                            .into()
                        )
                    }]
                    .into()
                ),
                ..Default::default()
            },
            response_a
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn two_requests_in_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let mock = MockProduceService::default();

        let configuration = ResourceConfig::default();

        let topic_a = "a";
        let topic_b = "b";
        configuration.put(topic_a, "tansu.batch", "true")?;
        configuration.put(topic_a, "tansu.batch.timeout_ms", "5000")?;

        let bp = (BatchProduceLayer::new(configuration)).into_layer(mock.clone());

        let correlation_id_a = 67876;

        let batch_a = {
            let bp = bp.clone();
            debug!(?bp);

            tokio::spawn(async move {
                let span = span!(Level::DEBUG, "batch", topic_a);

                async move {
                    bp.serve(
                        rama::Context::default(),
                        produce_request(topic_a, correlation_id_a, b"foo")?,
                    )
                    .await
                }
                .instrument(span)
                .await
            })
        };

        advance(Duration::from_secs(1)).await;
        yield_now().await;

        let len = bp.requests.lock().map(|guard| guard.len()).expect("poison");
        assert_eq!(1, len);

        let len = mock.reqs.lock().map(|guard| guard.len()).expect("poison");
        assert_eq!(0, len);

        let correlation_id_b = 78987;

        let batch_b = {
            let bp = bp.clone();
            debug!(?bp);

            tokio::spawn(async move {
                let span = span!(Level::DEBUG, "batch", topic_b);

                async move {
                    bp.serve(
                        rama::Context::default(),
                        produce_request(topic_b, correlation_id_b, b"bar")?,
                    )
                    .await
                }
                .instrument(span)
                .await
            })
        };

        advance(Duration::from_secs(1)).await;
        yield_now().await;

        let len = bp.requests.lock().map(|guard| guard.len()).expect("poison");
        assert_eq!(2, len);

        let len = mock.reqs.lock().map(|guard| guard.len()).expect("poison");
        assert_eq!(0, len);

        advance(Duration::from_secs(5)).await;
        yield_now().await;

        let requests = mock.reqs.lock().map(|guard| guard.clone()).expect("poison");
        assert_eq!(
            vec![ProduceRequest {
                api_key: ApiKey(0),
                api_version: 11,
                correlation_id: 0,
                client_id: Some("tansu".into()),
                transactional_id: None,
                acks: 0,
                timeout_ms: 5000,
                topic_data: Some(
                    [
                        TopicProduceData {
                            name: "a".into(),
                            partition_data: Some(
                                [PartitionProduceData {
                                    index: 0,
                                    records: Some(Frame {
                                        batches: [deflated::Batch {
                                            base_offset: 0,
                                            batch_length: 59,
                                            partition_leader_epoch: -1,
                                            magic: 2,
                                            crc: 1920191007,
                                            attributes: 0,
                                            last_offset_delta: 0,
                                            base_timestamp: 1234567890000,
                                            max_timestamp: 1234567890000,
                                            producer_id: -1,
                                            producer_epoch: 0,
                                            base_sequence: 0,
                                            record_count: 1,
                                            record_data: Bytes::from_static(
                                                b"\x12\0\0\0\x01\x06foo\0"
                                            )
                                        }]
                                        .into()
                                    })
                                }]
                                .into()
                            )
                        },
                        TopicProduceData {
                            name: "b".into(),
                            partition_data: Some(
                                [PartitionProduceData {
                                    index: 0,
                                    records: Some(Frame {
                                        batches: [deflated::Batch {
                                            base_offset: 0,
                                            batch_length: 59,
                                            partition_leader_epoch: -1,
                                            magic: 2,
                                            crc: 524875948,
                                            attributes: 0,
                                            last_offset_delta: 0,
                                            base_timestamp: 1234567890000,
                                            max_timestamp: 1234567890000,
                                            producer_id: -1,
                                            producer_epoch: 0,
                                            base_sequence: 0,
                                            record_count: 1,
                                            record_data: Bytes::from_static(
                                                b"\x12\0\0\0\x01\x06bar\0"
                                            )
                                        }]
                                        .into()
                                    })
                                }]
                                .into()
                            )
                        }
                    ]
                    .into()
                )
            }],
            requests
        );

        let response_a = batch_a
            .await?
            .inspect(|produce_response| debug!(?produce_response))?;

        assert_eq!(
            ProduceResponse {
                correlation_id: correlation_id_a,
                throttle_time_ms: Some(5_000),
                node_endpoints: Some([].into()),
                responses: Some(
                    [TopicProduceResponse {
                        name: "a".into(),
                        partition_responses: Some(
                            [PartitionProduceResponse {
                                index: 0,
                                error_code: 0,
                                base_offset: 65456,
                                log_append_time_ms: Some(0),
                                log_start_offset: Some(0),
                                record_errors: Some([].into()),
                                error_message: Some("none".into()),
                                current_leader: Some(LeaderIdAndEpoch {
                                    leader_id: 12321,
                                    leader_epoch: 23432
                                })
                            }]
                            .into()
                        )
                    }]
                    .into()
                ),
                api_version: 11,
                ..Default::default()
            },
            response_a
        );

        let response_b = batch_b
            .await?
            .inspect(|produce_response| debug!(?produce_response))?;

        assert_eq!(
            ProduceResponse {
                api_key: ApiKey(0),
                api_version: 11,
                correlation_id: correlation_id_b,
                throttle_time_ms: Some(5_000),
                node_endpoints: Some([].into()),
                responses: Some(
                    [TopicProduceResponse {
                        name: "b".into(),
                        partition_responses: Some(
                            [PartitionProduceResponse {
                                index: 0,
                                error_code: 0,
                                base_offset: 65456,
                                log_append_time_ms: Some(0),
                                log_start_offset: Some(0),
                                record_errors: Some([].into()),
                                error_message: Some("none".into()),
                                current_leader: Some(LeaderIdAndEpoch {
                                    leader_id: 12321,
                                    leader_epoch: 23432
                                })
                            }]
                            .into()
                        )
                    }]
                    .into()
                ),
                ..Default::default()
            },
            response_b
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn two_requests_into_single_topition() -> Result<()> {
        let _guard = init_tracing()?;

        let produce_service = MockProduceService::default();

        let configuration = ResourceConfig::default();

        let topic_a = "a";
        configuration.put(topic_a, "tansu.batch", "true")?;
        configuration.put(topic_a, "tansu.batch.timeout_ms", "5000")?;

        let bp = (BatchProduceLayer::new(configuration)).into_layer(produce_service.clone());

        let correlation_id_a = 67876;

        let batch_a = {
            let bp = bp.clone();
            debug!(?bp);

            tokio::spawn(async move {
                let span = span!(Level::DEBUG, "batch", topic_a);

                async move {
                    bp.serve(
                        rama::Context::default(),
                        produce_request(topic_a, correlation_id_a, b"foo")?,
                    )
                    .await
                }
                .instrument(span)
                .await
            })
        };

        advance(Duration::from_secs(1)).await;
        yield_now().await;

        let len = bp.requests.lock().map(|guard| guard.len()).expect("poison");
        assert_eq!(1, len);

        let len = produce_service
            .reqs
            .lock()
            .map(|guard| guard.len())
            .expect("poison");
        assert_eq!(0, len);

        let correlation_id_b = 78987;

        let batch_b = {
            let bp = bp.clone();
            debug!(?bp);

            tokio::spawn(async move {
                let span = span!(Level::DEBUG, "batch", topic_a);

                async move {
                    bp.serve(
                        rama::Context::default(),
                        produce_request(topic_a, correlation_id_b, b"bar")?,
                    )
                    .await
                }
                .instrument(span)
                .await
            })
        };

        advance(Duration::from_secs(1)).await;
        yield_now().await;

        let len = bp.requests.lock().map(|guard| guard.len()).expect("poison");
        assert_eq!(2, len);

        let len = produce_service
            .reqs
            .lock()
            .map(|guard| guard.len())
            .expect("poison");
        assert_eq!(0, len);

        advance(Duration::from_secs(5)).await;
        yield_now().await;

        let requests = produce_service
            .reqs
            .lock()
            .map(|guard| guard.clone())
            .expect("poison");
        assert_eq!(
            vec![ProduceRequest {
                api_key: ApiKey(0),
                api_version: 11,
                correlation_id: 0,
                client_id: Some("tansu".into()),
                transactional_id: None,
                acks: 0,
                timeout_ms: 5000,
                topic_data: Some(
                    [TopicProduceData {
                        name: "a".into(),
                        partition_data: Some(
                            [PartitionProduceData {
                                index: 0,
                                records: Some(Frame {
                                    batches: [deflated::Batch {
                                        base_offset: 0,
                                        batch_length: 69,
                                        partition_leader_epoch: -1,
                                        magic: 2,
                                        crc: 2619797409,
                                        attributes: 0,
                                        last_offset_delta: 0,
                                        base_timestamp: 1234567890000,
                                        max_timestamp: 1234567890000,
                                        producer_id: -1,
                                        producer_epoch: 0,
                                        base_sequence: 0,
                                        record_count: 2,
                                        record_data: Bytes::from_static(
                                            b"\x12\0\0\0\x01\x06foo\0\x12\0\0\0\x01\x06bar\0"
                                        )
                                    }]
                                    .into()
                                })
                            }]
                            .into()
                        )
                    }]
                    .into()
                )
            }],
            requests
        );

        let response_a = batch_a
            .await?
            .inspect(|produce_response| debug!(?produce_response))?;

        assert_eq!(
            ProduceResponse {
                api_version: 11,
                correlation_id: correlation_id_a,
                throttle_time_ms: Some(5_000),
                node_endpoints: Some([].into()),
                responses: Some(
                    [TopicProduceResponse {
                        name: "a".into(),
                        partition_responses: Some(
                            [PartitionProduceResponse {
                                index: 0,
                                error_code: 0,
                                base_offset: 65456,
                                log_append_time_ms: Some(0),
                                log_start_offset: Some(0),
                                record_errors: Some([].into()),
                                error_message: Some("none".into()),
                                current_leader: Some(LeaderIdAndEpoch {
                                    leader_id: 12321,
                                    leader_epoch: 23432
                                })
                            }]
                            .into()
                        )
                    }]
                    .into()
                ),
                ..Default::default()
            },
            response_a
        );

        let response_b = batch_b
            .await?
            .inspect(|produce_response| debug!(?produce_response))?;

        assert_eq!(
            ProduceResponse {
                api_version: 11,
                correlation_id: correlation_id_b,
                throttle_time_ms: Some(5_000),
                node_endpoints: Some([].into()),
                responses: Some(
                    [TopicProduceResponse {
                        name: "a".into(),
                        partition_responses: Some(
                            [PartitionProduceResponse {
                                index: 0,
                                error_code: 0,
                                base_offset: 65456,
                                log_append_time_ms: Some(0),
                                log_start_offset: Some(0),
                                record_errors: Some([].into()),
                                error_message: Some("none".into()),
                                current_leader: Some(LeaderIdAndEpoch {
                                    leader_id: 12321,
                                    leader_epoch: 23432
                                })
                            }]
                            .into()
                        )
                    }]
                    .into()
                ),
                ..Default::default()
            },
            response_b
        );

        Ok(())
    }
}
