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

use core::{
    fmt::{self, Display},
    result,
};
use std::{
    fmt::Debug,
    io,
    marker::PhantomData,
    num::NonZeroU32,
    pin::Pin,
    sync::{Arc, LazyLock, PoisonError},
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use governor::{InsufficientCapacity, Jitter, Quota, RateLimiter};
use nonzero_ext::nonzero;
use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    metrics::{Counter, Meter},
};
use opentelemetry_otlp::ExporterBuildError;
use opentelemetry_sdk::{
    error::{OTelSdkError, OTelSdkResult},
    metrics::{
        SdkMeterProvider, Temporality,
        data::{AggregatedMetrics, Histogram, Metric, MetricData, ResourceMetrics},
        exporter::PushMetricExporter,
    },
};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use tansu_client::{Client, ConnectionManager};
use tansu_sans_io::{
    ErrorCode, ProduceRequest,
    primitive::ByteSize as _,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated, inflated},
};
use tokio::{
    signal::unix::{SignalKind, signal},
    task::JoinSet,
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument as _, Level, debug, instrument, span, warn};
use url::Url;

pub type Result<T, E = Error> = result::Result<T, E>;

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

#[derive(thiserror::Error, Debug)]
pub enum Error {
    Api(ErrorCode),
    Client(#[from] tansu_client::Error),
    ExporterBuild(#[from] ExporterBuildError),
    InsufficientCapacity(#[from] InsufficientCapacity),
    Io(Arc<io::Error>),
    OtelSdk(#[from] OTelSdkError),
    Random(#[from] getrandom::Error),
    Poison,
    Protocol(#[from] tansu_sans_io::Error),
    UnknownHost(String),
    Url(#[from] url::ParseError),
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CancelKind {
    Interrupt,
    Terminate,
    Timeout,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Perf {
    broker: Url,
    topic: String,
    partition: i32,
    batch_size: u32,
    record_size: usize,
    per_second: Option<u32>,
    throughput: Option<u32>,
    producers: u32,
    duration: Option<Duration>,
    otlp_endpoint_url: Option<Url>,
}

#[derive(Clone, Debug)]
pub struct Builder<B, T> {
    broker: B,
    topic: T,
    partition: i32,
    batch_size: u32,
    record_size: usize,
    per_second: Option<u32>,
    throughput: Option<u32>,
    producers: u32,
    duration: Option<Duration>,
    otlp_endpoint_url: Option<Url>,
}

impl Default for Builder<PhantomData<Url>, PhantomData<String>> {
    fn default() -> Self {
        Self {
            broker: Default::default(),
            topic: Default::default(),
            partition: Default::default(),
            batch_size: 1,
            record_size: 1024,
            per_second: None,
            throughput: None,
            producers: 1,
            duration: None,
            otlp_endpoint_url: None,
        }
    }
}

impl<B, T> Builder<B, T> {
    pub fn broker(self, broker: impl Into<Url>) -> Builder<Url, T> {
        Builder {
            broker: broker.into(),
            topic: self.topic,
            partition: self.partition,
            batch_size: self.batch_size,
            record_size: self.record_size,
            per_second: self.per_second,
            throughput: self.throughput,
            producers: self.producers,
            duration: self.duration,
            otlp_endpoint_url: self.otlp_endpoint_url,
        }
    }

    pub fn topic(self, topic: impl Into<String>) -> Builder<B, String> {
        Builder {
            broker: self.broker,
            topic: topic.into(),
            partition: self.partition,
            batch_size: self.batch_size,
            record_size: self.record_size,
            per_second: self.per_second,
            throughput: self.throughput,
            producers: self.producers,
            duration: self.duration,
            otlp_endpoint_url: self.otlp_endpoint_url,
        }
    }

    pub fn partition(self, partition: i32) -> Builder<B, T> {
        Self { partition, ..self }
    }

    pub fn batch_size(self, batch_size: u32) -> Self {
        Self { batch_size, ..self }
    }

    pub fn record_size(self, record_size: usize) -> Self {
        Self {
            record_size,
            ..self
        }
    }

    pub fn per_second(self, per_second: Option<u32>) -> Self {
        Self { per_second, ..self }
    }

    pub fn throughput(self, throughput: Option<u32>) -> Self {
        Self { throughput, ..self }
    }

    pub fn producers(self, producers: u32) -> Self {
        Self { producers, ..self }
    }

    pub fn duration(self, duration: Option<Duration>) -> Self {
        Self { duration, ..self }
    }

    pub fn otlp_endpoint_url(self, otlp_endpoint_url: Option<Url>) -> Self {
        Self {
            otlp_endpoint_url,
            ..self
        }
    }
}

impl Builder<Url, String> {
    pub fn build(self) -> Perf {
        Perf {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            batch_size: self.batch_size,
            record_size: self.record_size,
            per_second: self.per_second,
            throughput: self.throughput,
            producers: self.producers,
            duration: self.duration,
            otlp_endpoint_url: self.otlp_endpoint_url,
        }
    }
}

static RATE_LIMIT_DURATION: LazyLock<opentelemetry::metrics::Histogram<u64>> =
    LazyLock::new(|| {
        METER
            .u64_histogram("rate_limit_duration")
            .with_unit("ms")
            .with_description("Rate limit latencies in milliseconds")
            .build()
    });

static PRODUCE_RECORD_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("produce_record_count")
        .with_description("Produced record count")
        .build()
});

static PRODUCE_API_DURATION: LazyLock<opentelemetry::metrics::Histogram<u64>> =
    LazyLock::new(|| {
        METER
            .u64_histogram("produce_duration")
            .with_unit("ms")
            .with_description("Produce API latencies in milliseconds")
            .build()
    });

#[instrument(skip(record_data), fields(record_data_len = record_data.len()))]
fn frame(batch_size: i32, record_data: Bytes) -> Result<deflated::Frame> {
    let mut batch = inflated::Batch::builder();
    let offset_deltas = 0..batch_size;

    for offset_delta in offset_deltas {
        batch = batch.record(
            Record::builder()
                .value(Some(record_data.clone()))
                .offset_delta(offset_delta),
        )
    }

    batch
        .last_offset_delta(batch_size)
        .build()
        .map(|batch| inflated::Frame {
            batches: vec![batch],
        })
        .and_then(deflated::Frame::try_from)
        .map_err(Into::into)
}

#[instrument(skip(client, frame))]
pub async fn produce(
    client: Client,
    name: String,
    index: i32,
    batch_size: i32,
    frame: deflated::Frame,
) -> Result<()> {
    let req = ProduceRequest::default().topic_data(Some(
        [TopicProduceData::default().name(name).partition_data(Some(
            [PartitionProduceData::default()
                .index(index)
                .records(Some(frame))]
            .into(),
        ))]
        .into(),
    ));

    let response = client.call(req).await?;

    assert!(
        response
            .responses
            .unwrap_or_default()
            .into_iter()
            .all(|topic| {
                topic
                    .partition_responses
                    .unwrap_or_default()
                    .iter()
                    .all(|partition| partition.error_code == i16::from(ErrorCode::None))
            })
    );

    Ok(())
}

impl Perf {
    pub fn builder() -> Builder<PhantomData<Url>, PhantomData<String>> {
        Builder::default()
    }

    pub async fn main(self) -> Result<ErrorCode> {
        let meter_provider = {
            let exporter = MetricExporter::default();
            let meter_provider = SdkMeterProvider::builder()
                .with_periodic_exporter(exporter)
                .build();
            global::set_meter_provider(meter_provider.clone());

            meter_provider
        };

        let mut interrupt_signal = signal(SignalKind::interrupt()).unwrap();
        debug!(?interrupt_signal);

        let mut terminate_signal = signal(SignalKind::terminate()).unwrap();
        debug!(?terminate_signal);

        let rate_limiter = self
            .per_second
            .or(self.throughput)
            .inspect(|limit| warn!(?limit))
            .and_then(NonZeroU32::new)
            .map(Quota::per_second)
            .map(RateLimiter::direct)
            .map(Arc::new)
            .inspect(|rate_limiter| debug!(?rate_limiter));

        let record_data = {
            let mut data = vec![0u8; self.record_size];
            getrandom::fill(&mut data)?;
            Bytes::from(data)
        };

        let batch_size = NonZeroU32::new(self.batch_size)
            .inspect(|batch_size| debug!(batch_size = batch_size.get()))
            .unwrap_or(nonzero!(10u32));

        let mut set = JoinSet::new();

        let token = CancellationToken::new();

        let client = ConnectionManager::builder(self.broker)
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .build()
            .await
            .inspect(|pool| debug!(?pool))
            .map(Client::new)?;

        for producer in 0..self.producers {
            let rate_limiter = rate_limiter.clone();
            let topic = self.topic.clone();
            let partition = self.partition;
            let record_data = record_data.clone();
            let token = token.clone();
            let client = client.clone();

            _ = set.spawn(async move {
                    let span = span!(Level::DEBUG, "producer", producer);

                    async move {
                        let attributes = [KeyValue::new("producer", producer.to_string())];

                        loop {
                           let Ok(frame) = frame(batch_size.get() as i32, record_data.clone()) else {
                               break
                           };

                           if let Some(ref rate_limiter) = rate_limiter {
                                let rate_limit_start = SystemTime::now();


                                let cells = self.throughput.and(frame.size_in_bytes().ok().and_then(|bytes|NonZeroU32::new(bytes as u32))).unwrap_or(batch_size);


                                tokio::select! {
                                    cancelled = token.cancelled() => {
                                        debug!(?cancelled);
                                        break
                                    },

                                    Ok(_) = rate_limiter.until_n_ready_with_jitter(cells, Jitter::up_to(Duration::from_millis(50))) => {
                                        RATE_LIMIT_DURATION.record(
                                        rate_limit_start
                                            .elapsed()
                                            .inspect(|duration|debug!(rate_limit_duration_ms = duration.as_millis()))
                                            .map_or(0, |duration| duration.as_millis() as u64),
                                            &attributes)

                                    },
                                }
                            }

                            let produce_start = SystemTime::now();

                            tokio::select! {
                                cancelled = token.cancelled() => {
                                    debug!(?cancelled);
                                    break
                                },

                                Ok(_) = produce(client.clone(), topic.clone(), partition,  batch_size.get() as i32, frame) => {
                                    PRODUCE_RECORD_COUNT.add(batch_size.get() as u64, &attributes);
                                    PRODUCE_API_DURATION.record(produce_start.elapsed().inspect(|duration|debug!(produce_duration_ms = duration.as_millis())).map_or(0, |duration| duration.as_millis() as u64), &attributes);
                                },
                            }
                        }

                    }.instrument(span).await;


                });
        }

        let join_all = async {
            while !set.is_empty() {
                debug!(len = set.len());
                _ = set.join_next().await;
            }
        };

        let duration = self
            .duration
            .map(sleep)
            .map(Box::pin)
            .map(|pinned| pinned as Pin<Box<dyn Future<Output = ()>>>)
            .unwrap_or(Box::pin(std::future::pending()) as Pin<Box<dyn Future<Output = ()>>>);

        let cancellation = tokio::select! {

            timeout = duration => {
                debug!(?timeout);
                token.cancel();
                Some(CancelKind::Timeout)
            }

            completed = join_all => {
                debug!(?completed);
                None
            }

            interrupt = interrupt_signal.recv() => {
                debug!(?interrupt);
                Some(CancelKind::Interrupt)
            }

            terminate = terminate_signal.recv() => {
                debug!(?terminate);
                Some(CancelKind::Terminate)
            }

        };

        debug!(?cancellation);

        meter_provider
            .force_flush()
            .inspect(|force_flush| debug!(?force_flush))?;

        meter_provider
            .shutdown()
            .inspect(|shutdown| debug!(?shutdown))?;

        if let Some(CancelKind::Timeout) = cancellation {
            sleep(Duration::from_secs(5)).await;
        }

        debug!(abort = set.len());
        set.abort_all();

        while !set.is_empty() {
            _ = set.join_next().await;
        }

        Ok(ErrorCode::None)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Info {
    started_at: SystemTime,
    bytes_sent: u64,
    record_count: u64,
    latency: Latency,
}

impl Info {
    fn elapsed_secs(&self) -> u64 {
        self.started_at
            .elapsed()
            .map_or(0, |duration| duration.as_secs())
    }

    fn bandwidth(&self) -> u64 {
        self.bytes_sent
            .checked_div(1024 * 1024 * self.elapsed_secs())
            .unwrap_or_default()
    }
}

impl Display for Info {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} records sent, {} bytes sent, ({} MB/sec), latency: {}ms min, {}ms avg, {}ms max",
            self.record_count,
            self.bytes_sent,
            self.bandwidth(),
            self.latency.min.expect("minimum"),
            self.latency.mean.expect("mean"),
            self.latency.max.expect("max")
        )
    }
}

impl Info {
    fn new(started_at: SystemTime) -> Self {
        Self {
            started_at,
            bytes_sent: Default::default(),
            record_count: Default::default(),
            latency: Default::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Latency {
    min: Option<u64>,
    max: Option<u64>,
    mean: Option<u64>,
}

impl From<&Histogram<u64>> for Latency {
    fn from(histogram: &Histogram<u64>) -> Self {
        let min = histogram.data_points().filter_map(|dp| dp.min()).min();
        let max = histogram.data_points().filter_map(|dp| dp.max()).max();

        let sum = histogram.data_points().map(|dp| dp.sum()).sum::<u64>();
        let count = histogram.data_points().map(|dp| dp.count()).sum::<u64>();

        let mean = sum.checked_div(count);

        Self { min, max, mean }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct MetricExporter {
    started_at: SystemTime,
    temporality: Temporality,
}

impl Default for MetricExporter {
    fn default() -> Self {
        Self {
            started_at: SystemTime::now(),
            temporality: Default::default(),
        }
    }
}

impl MetricExporter {
    #[instrument(skip_all, fields(scope = scope.name(), metric = metric.name()))]
    fn info(&self, scope: &InstrumentationScope, metric: &Metric, info: &mut Info) {
        match (scope.name(), metric.name(), metric.data()) {
            ("tansu-client", "tcp_bytes_sent", AggregatedMetrics::U64(MetricData::Sum(sum))) => {
                for (point, data) in sum.data_points().enumerate() {
                    debug!(point, value = ?data.value());
                }

                info.bytes_sent = sum.data_points().map(|sum| sum.value()).sum::<u64>();
            }

            (
                "tansu-perf",
                "produce_record_count",
                AggregatedMetrics::U64(MetricData::Sum(sum)),
            ) => {
                for (point, data) in sum.data_points().enumerate() {
                    debug!(point, value = ?data.value());
                }

                info.record_count = sum.data_points().map(|sum| sum.value()).sum::<u64>();
            }

            (
                "tansu-perf",
                "produce_duration",
                AggregatedMetrics::U64(MetricData::Histogram(histogram)),
            ) => {
                info.latency = Latency::from(histogram);
            }

            _ => (),
        }
    }
}

impl PushMetricExporter for MetricExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let mut info = Info::new(self.started_at);

        for scope in metrics.scope_metrics() {
            debug!(scope = scope.scope().name());

            for metric in scope.metrics() {
                debug!(scope = scope.scope().name(), metric = metric.name());

                self.info(scope.scope(), metric, &mut info);
            }
        }

        println!("{info}");

        Ok(())
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    #[instrument]
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        self.temporality
    }
}
