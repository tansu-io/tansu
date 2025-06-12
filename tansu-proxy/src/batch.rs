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
    result::Result,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
    vec::IntoIter,
};

use rama::{Layer, Service};
use tansu_kafka_sans_io::{
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    record::deflated::{Batch, Frame},
};
use tokio::time::sleep;
use tracing::debug;
use uuid::Uuid;

use crate::{
    Error,
    api::{
        ApiKey, ApiVersion, CorrelationId,
        produce::{ProduceRequest, ProduceResponse},
    },
};

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
    duration: Duration,
}

impl<S> BatchProduceService<S>
where
    S: Clone + Debug,
{
    async fn send_pending_batch<State>(
        &self,
        id: &Uuid,
        ctx: rama::Context<State>,
    ) -> Result<Option<ProduceResponse>, Error>
    where
        S: Service<State, ProduceRequest, Response = ProduceResponse> + Clone + Debug,
        S::Error: From<Error> + Send + Debug + 'static,
        State: Send + Sync + 'static,
    {
        let requests = self
            .requests
            .lock()
            .inspect_err(|err| debug!(?err))
            .map(|mut guard| std::mem::take(guard.deref_mut()))
            .inspect(|request| debug!(?request))?;

        if requests.is_empty() {
            return Ok(None);
        }

        let owners = Owner::from(&requests[..]);
        let produce_request = ProduceRequest::from(requests);

        let produce_response = self
            .service
            .serve(ctx, produce_request)
            .await
            .expect("produce_request");

        let mut responses = self.responses.lock().inspect_err(|err| debug!(?err))?;

        let mut owners = owners.split(produce_response);

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
                    waker.wake();
                }
            }

            Ok(None)
        }
    }
}

impl<S, State> Service<State, ProduceRequest> for BatchProduceService<S>
where
    S: Service<State, ProduceRequest, Response = ProduceResponse> + Clone + Debug,
    S::Error: From<Error> + Send + Debug + 'static,
    State: Clone + Send + Sync + 'static,
{
    type Response = ProduceResponse;

    type Error = S::Error;

    async fn serve(
        &self,
        ctx: rama::Context<State>,
        request: ProduceRequest,
    ) -> Result<Self::Response, Self::Error> {
        let ticket = self
            .requests
            .lock()
            .map_err(Into::into)
            .map(|mut requests| {
                let ticket = Ticket::new(self.clone());
                requests.push(BatchRequest {
                    id: ticket.id,
                    request,
                });
                ticket
            })
            .inspect(|ticket| debug!(?ticket))?;

        loop {
            let patience = sleep(self.duration);
            let ticket = ticket.clone();
            let id = ticket.id;
            let ctx = ctx.clone();
            debug!(?patience);

            tokio::select! {
                response = ticket  => {
                    return response
                    .inspect(|response|debug!(?response))
                    .inspect_err(|err|debug!(?err))
                    .map_err(Into::into)
                }

                expired = patience => {
                    debug!(?expired);
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
    fn new(duration: Duration, service: S) -> Self {
        Self {
            service,
            requests: Arc::new(Mutex::new(Vec::new())),
            responses: Arc::new(Mutex::new(BTreeMap::new())),
            duration,
        }
    }
}

#[allow(dead_code)]
#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BatchProduceLayer {
    duration: Duration,
}

#[allow(dead_code)]
impl BatchProduceLayer {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
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
            duration: self.duration,
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
            for partition in topic.partition_responses.unwrap_or_default() {
                let topition = Topition {
                    topic: topic.name.clone(),
                    partition: partition.index,
                };

                for owner in self.topition.get(&topition).cloned().unwrap_or_default() {
                    _ = responses
                        .entry(owner)
                        .or_default()
                        .0
                        .entry(topic.name.clone())
                        .or_default()
                        .0
                        .insert(partition.index, partition.clone());
                }
            }
        }

        responses
            .into_iter()
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

impl From<Vec<BatchRequest>> for ProduceRequest {
    fn from(requests: Vec<BatchRequest>) -> Self {
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

        Self {
            topic_data: Some(run.into_iter().collect::<Vec<_>>()),
            ..Default::default()
        }
    }
}

impl From<Vec<BatchRequest>> for BatchProduction {
    fn from(requests: Vec<BatchRequest>) -> Self {
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
    partitions: BTreeMap<Partition, Vec<Batch>>,
}

impl IntoIterator for PartitionBatch {
    type Item = PartitionProduceData;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.partitions
            .into_iter()
            .map(|(index, batches)| PartitionProduceData {
                index,
                records: Some(Frame { batches }),
            })
            .collect::<Vec<_>>()
            .into_iter()
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
    type Output = Result<ProduceResponse, Error>;

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

            Err(error) => Poll::Ready(Err(error.into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, thread};

    use bytes::Bytes;
    use tansu_kafka_sans_io::{
        ErrorCode,
        produce_response::{LeaderIdAndEpoch, PartitionProduceResponse, TopicProduceResponse},
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
        type Error = Error;

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

            let mut guard = self.reqs.lock()?;
            guard.push(req);

            Ok(produce_response)
        }
    }

    fn init_tracing() -> Result<DefaultGuard, Error> {
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
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    fn produce_request(
        topic: &str,
        correlation_id: CorrelationId,
        record_data: &'static [u8],
    ) -> ProduceRequest {
        ProduceRequest {
            correlation_id,
            topic_data: Some(
                [TopicProduceData {
                    name: String::from(topic),
                    partition_data: Some(
                        [PartitionProduceData {
                            index: 0,
                            records: Some(Frame {
                                batches: vec![Batch {
                                    record_data: Bytes::from_static(record_data),
                                    ..Batch::default()
                                }],
                            }),
                        }]
                        .into(),
                    ),
                }]
                .into(),
            ),
            ..Default::default()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn single_request_in_batch() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let mock = MockProduceService::default();

        let bp = (BatchProduceLayer::new(Duration::from_secs(5))).into_layer(mock.clone());

        let correlation_id_a = 67876;

        let batch_a = {
            let bp = bp.clone();
            debug!(?bp);

            tokio::spawn(async move {
                let span = span!(Level::DEBUG, "batch", "a");

                async move {
                    bp.serve(
                        rama::Context::default(),
                        produce_request("a", correlation_id_a, b"foo"),
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

        let len = bp.requests.lock().map(|guard| guard.len())?;
        assert_eq!(1, len);

        let len = mock.reqs.lock().map(|guard| guard.len())?;
        assert_eq!(0, len);

        yield_now().await;
        advance(Duration::from_secs(5)).await;
        yield_now().await;

        let requests = mock.reqs.lock().map(|guard| guard.clone())?;
        assert_eq!(
            vec![ProduceRequest {
                topic_data: Some(
                    [TopicProduceData {
                        name: "a".into(),
                        partition_data: Some(
                            [PartitionProduceData {
                                index: 0,
                                records: Some(Frame {
                                    batches: [Batch {
                                        base_offset: 0,
                                        batch_length: 0,
                                        partition_leader_epoch: 0,
                                        magic: 0,
                                        crc: 0,
                                        attributes: 0,
                                        last_offset_delta: 0,
                                        base_timestamp: 0,
                                        max_timestamp: 0,
                                        producer_id: 0,
                                        producer_epoch: 0,
                                        base_sequence: 0,
                                        record_count: 0,
                                        record_data: Bytes::from_static(b"foo")
                                    }]
                                    .into()
                                })
                            }]
                            .into()
                        )
                    }]
                    .into()
                ),
                ..Default::default()
            }],
            requests
        );

        let response_a = batch_a
            .await
            .expect("a_await")
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
                ..Default::default()
            },
            response_a
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn two_requests_in_batch() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let mock = MockProduceService::default();

        let bp = (BatchProduceLayer::new(Duration::from_secs(5))).into_layer(mock.clone());

        let correlation_id_a = 67876;

        let pr = |name, correlation_id, record_data| ProduceRequest {
            correlation_id,
            topic_data: Some(
                [TopicProduceData {
                    name: String::from(name),
                    partition_data: Some(
                        [PartitionProduceData {
                            index: 0,
                            records: Some(Frame {
                                batches: vec![Batch {
                                    record_data: Bytes::from_static(record_data),
                                    ..Batch::default()
                                }],
                            }),
                        }]
                        .into(),
                    ),
                }]
                .into(),
            ),
            ..Default::default()
        };

        let batch_a = {
            let bp = bp.clone();
            debug!(?bp);

            tokio::spawn(async move {
                let span = span!(Level::DEBUG, "batch", "a");

                async move {
                    bp.serve(rama::Context::default(), pr("a", correlation_id_a, b"foo"))
                        .await
                }
                .instrument(span)
                .await
            })
        };

        advance(Duration::from_secs(1)).await;
        yield_now().await;

        let len = bp.requests.lock().map(|guard| guard.len())?;
        assert_eq!(1, len);

        let len = mock.reqs.lock().map(|guard| guard.len())?;
        assert_eq!(0, len);

        let correlation_id_b = 78987;

        let batch_b = {
            let bp = bp.clone();
            debug!(?bp);

            tokio::spawn(async move {
                let span = span!(Level::DEBUG, "batch", "b");

                async move {
                    bp.serve(rama::Context::default(), pr("b", correlation_id_b, b"bar"))
                        .await
                }
                .instrument(span)
                .await
            })
        };

        advance(Duration::from_secs(1)).await;
        yield_now().await;

        let len = bp.requests.lock().map(|guard| guard.len())?;
        assert_eq!(2, len);

        let len = mock.reqs.lock().map(|guard| guard.len())?;
        assert_eq!(0, len);

        advance(Duration::from_secs(5)).await;
        yield_now().await;

        let requests = mock.reqs.lock().map(|guard| guard.clone())?;
        assert_eq!(
            vec![ProduceRequest {
                topic_data: Some(
                    [
                        TopicProduceData {
                            name: "a".into(),
                            partition_data: Some(
                                [PartitionProduceData {
                                    index: 0,
                                    records: Some(Frame {
                                        batches: [Batch {
                                            base_offset: 0,
                                            batch_length: 0,
                                            partition_leader_epoch: 0,
                                            magic: 0,
                                            crc: 0,
                                            attributes: 0,
                                            last_offset_delta: 0,
                                            base_timestamp: 0,
                                            max_timestamp: 0,
                                            producer_id: 0,
                                            producer_epoch: 0,
                                            base_sequence: 0,
                                            record_count: 0,
                                            record_data: Bytes::from_static(b"foo")
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
                                        batches: [Batch {
                                            base_offset: 0,
                                            batch_length: 0,
                                            partition_leader_epoch: 0,
                                            magic: 0,
                                            crc: 0,
                                            attributes: 0,
                                            last_offset_delta: 0,
                                            base_timestamp: 0,
                                            max_timestamp: 0,
                                            producer_id: 0,
                                            producer_epoch: 0,
                                            base_sequence: 0,
                                            record_count: 0,
                                            record_data: Bytes::from_static(b"bar")
                                        }]
                                        .into()
                                    })
                                }]
                                .into()
                            )
                        }
                    ]
                    .into()
                ),
                ..Default::default()
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
                ..Default::default()
            },
            response_a
        );

        let response_b = batch_b
            .await?
            .inspect(|produce_response| debug!(?produce_response))?;

        assert_eq!(
            ProduceResponse {
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
}
