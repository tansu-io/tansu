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
    fmt, io,
    marker::PhantomData,
    net::SocketAddr,
    num::NonZeroU32,
    pin::Pin,
    result,
    sync::{Arc, LazyLock, PoisonError},
    time::{Duration, SystemTime},
};

use governor::{InsufficientCapacity, Jitter, Quota, RateLimiter};
use nonzero_ext::nonzero;
use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    metrics::{Counter, Histogram, Meter},
};
use opentelemetry_otlp::ExporterBuildError;
use opentelemetry_sdk::error::OTelSdkError;
use opentelemetry_semantic_conventions::SCHEMA_URL;
use rama::{
    Context, Service,
    error::{BoxError, OpaqueError},
};
use tansu_otel::meter_provider;
use tansu_sans_io::{
    ErrorCode,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{deflated, inflated},
};
use tansu_schema::{Generator as _, Registry, Schema};
use tansu_service::{
    api::produce::{ProduceRequest, ProduceResponse},
    service::ApiClient,
};
use tokio::{
    net::lookup_host,
    signal::unix::{SignalKind, signal},
    task::JoinSet,
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Level, debug, span};
use url::Url;

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    Api(ErrorCode),
    Box(#[from] BoxError),
    ExporterBuild(#[from] ExporterBuildError),
    InsufficientCapacity(#[from] InsufficientCapacity),
    Io(Arc<io::Error>),
    Opaque(#[from] OpaqueError),
    Otel(#[from] tansu_otel::Error),
    OtelSdk(#[from] OTelSdkError),
    Poison,
    Protocol(#[from] tansu_sans_io::Error),
    Schema(Box<tansu_schema::Error>),
    SchemaNotFoundForTopic(String),
    UnknownHost(String),
    Url(#[from] url::ParseError),
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl From<tansu_schema::Error> for Error {
    fn from(error: tansu_schema::Error) -> Self {
        Self::Schema(Box::new(error))
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl fmt::Display for Error {
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
pub struct Configuration {
    broker: Url,
    topic: String,
    partition: i32,
    schema_registry: Url,
    batch_size: u32,
    per_second: Option<u32>,
    producers: u32,
    duration: Option<Duration>,
    otlp_endpoint_url: Option<Url>,
}

#[derive(Clone, Debug)]
pub struct Generate {
    configuration: Configuration,
    registry: Registry,
}

impl TryFrom<Configuration> for Generate {
    type Error = Error;

    fn try_from(configuration: Configuration) -> Result<Self, Self::Error> {
        Registry::try_from(&configuration.schema_registry)
            .map(|registry| Self {
                configuration,
                registry,
            })
            .map_err(Into::into)
    }
}

static DNS_LOOKUP_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("dns_lookup_duration")
        .with_unit("ms")
        .with_description("DNS lookup latencies")
        .build()
});

async fn host_port(url: &Url) -> Result<SocketAddr> {
    if let Some(host) = url.host_str()
        && let Some(port) = url.port()
    {
        let attributes = [KeyValue::new("url", url.to_string())];
        let start = SystemTime::now();

        let mut addresses = lookup_host(format!("{host}:{port}"))
            .await
            .inspect(|_| {
                DNS_LOOKUP_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &attributes,
                )
            })?
            .filter(|socket_addr| matches!(socket_addr, SocketAddr::V4(_)));

        if let Some(socket_addr) = addresses.next().inspect(|socket_addr| debug!(?socket_addr)) {
            return Ok(socket_addr);
        }
    }

    Err(Error::UnknownHost(url.to_string()))
}

static GENERATE_PRODUCE_BATCH_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("generate_produce_batch_duration")
        .with_unit("ms")
        .with_description("Generate a produce batch in milliseconds")
        .build()
});

static PRODUCE_REQUEST_RESPONSE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("produce_request_response_duration")
        .with_unit("ms")
        .with_description("Latency of receiving an produce response in milliseconds")
        .build()
});

pub async fn produce(
    broker: Url,
    name: String,
    index: i32,
    schema: Schema,
    batch_size: i32,
) -> Result<()> {
    debug!(%broker, %name, index, batch_size);

    let attributes = [
        KeyValue::new("topic", name.clone()),
        KeyValue::new("partition", index.to_string()),
        KeyValue::new("batch_size", batch_size.to_string()),
    ];

    let frame = {
        let start = SystemTime::now();

        let mut batch = inflated::Batch::builder();
        let offset_deltas = 0..batch_size;

        for offset_delta in offset_deltas {
            batch = schema
                .generate()
                .map(|record| record.offset_delta(offset_delta))
                .map(|record| batch.record(record))?;
        }

        batch
            .last_offset_delta(batch_size)
            .build()
            .map(|batch| inflated::Frame {
                batches: vec![batch],
            })
            .and_then(deflated::Frame::try_from)
            .inspect(|_| {
                GENERATE_PRODUCE_BATCH_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &attributes,
                )
            })?
    };

    let request = ProduceRequest {
        topic_data: Some(
            [TopicProduceData {
                name,
                partition_data: Some(
                    [PartitionProduceData {
                        index,
                        records: Some(frame),
                    }]
                    .into(),
                ),
            }]
            .into(),
        ),
        ..Default::default()
    };

    let origin = host_port(&broker)
        .await
        .map(Into::into)
        .map(ApiClient::new)
        .inspect(|client| debug!(?client))?;

    let response = {
        let start = SystemTime::now();

        let attributes = [];

        origin
            .serve(Context::default(), request.into())
            .await
            .and_then(|response| ProduceResponse::try_from(response).map_err(Into::into))
            .inspect(|response| debug!(?response))
            .inspect(|_| {
                PRODUCE_REQUEST_RESPONSE_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &attributes,
                )
            })
    }?;

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
                    .inspect(|partition| debug!(topic = %topic.name, ?partition))
                    .all(|partition| partition.error_code == i16::from(ErrorCode::None))
            })
    );

    Ok(())
}

static RATE_LIMIT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
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

static PRODUCE_API_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("produce_duration")
        .with_unit("ms")
        .with_description("Produce API latencies in milliseconds")
        .build()
});

impl Generate {
    pub async fn main(self) -> Result<ErrorCode> {
        let meter_provider = self
            .configuration
            .otlp_endpoint_url
            .map_or(Ok(None), |otlp_endpoint_url| {
                meter_provider(otlp_endpoint_url, env!("CARGO_PKG_NAME")).map(Some)
            })?;

        let Some(schema) = self.registry.schema(&self.configuration.topic).await? else {
            return Err(Error::SchemaNotFoundForTopic(
                self.configuration.topic.clone(),
            ));
        };

        let mut interrupt_signal = signal(SignalKind::interrupt()).unwrap();
        debug!(?interrupt_signal);

        let mut terminate_signal = signal(SignalKind::terminate()).unwrap();
        debug!(?terminate_signal);

        let rate_limiter = self
            .configuration
            .per_second
            .and_then(NonZeroU32::new)
            .map(Quota::per_second)
            .map(RateLimiter::direct)
            .map(Arc::new)
            .inspect(|rate_limiter| debug!(?rate_limiter));

        let batch_size = NonZeroU32::new(self.configuration.batch_size)
            .inspect(|batch_size| debug!(batch_size = batch_size.get()))
            .unwrap_or(nonzero!(10u32));

        let mut set = JoinSet::new();

        let token = CancellationToken::new();

        for producer in 0..self.configuration.producers {
            let rate_limiter = rate_limiter.clone();
            let schema = schema.clone();
            let broker = self.configuration.broker.clone();
            let topic = self.configuration.topic.clone();
            let partition = self.configuration.partition;
            let token = token.clone();

            _ = set.spawn(async move {
                    let span = span!(Level::DEBUG, "producer", producer);


                    async move {
                        let attributes = [KeyValue::new("producer", producer.to_string())];

                        loop {
                            debug!(%broker, %topic, partition);

                            if let Some(ref rate_limiter) = rate_limiter {
                                let rate_limit_start = SystemTime::now();





                                tokio::select! {
                                    cancelled = token.cancelled() => {
                                        debug!(?cancelled);
                                        break
                                    },

                                    Ok(_) = rate_limiter.until_n_ready_with_jitter(batch_size, Jitter::up_to(Duration::from_millis(50))) => {
                                        RATE_LIMIT_DURATION.record(
                                        rate_limit_start
                                            .elapsed()
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

                                Ok(_) = produce(broker.clone(), topic.clone(), partition, schema.clone(), batch_size.get() as i32) => {
                                    PRODUCE_RECORD_COUNT.add(batch_size.get() as u64, &attributes);
                                    PRODUCE_API_DURATION.record(produce_start.elapsed().map_or(0, |duration| duration.as_millis() as u64), &attributes);
                                },
                            }
                        }

                    }.instrument(span).await

                });
        }

        let join_all = async {
            while !set.is_empty() {
                debug!(len = set.len());
                set.join_next().await;
            }
        };

        let duration = self
            .configuration
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

        if let Some(meter_provider) = meter_provider {
            meter_provider
                .force_flush()
                .inspect(|force_flush| debug!(?force_flush))?;

            meter_provider
                .shutdown()
                .inspect(|shutdown| debug!(?shutdown))?;
        }

        if let Some(CancelKind::Timeout) = cancellation {
            sleep(Duration::from_secs(5)).await;
        }

        debug!(abort = set.len());
        set.abort_all();

        while !set.is_empty() {
            set.join_next().await;
        }

        Ok(ErrorCode::None)
    }

    pub fn builder()
    -> Builder<PhantomData<Url>, PhantomData<String>, PhantomData<i32>, PhantomData<Url>> {
        Builder::default()
    }
}

#[derive(Clone)]
pub struct Builder<B, T, P, S> {
    broker: B,
    topic: T,
    partition: P,
    schema_registry: S,
    batch_size: u32,
    per_second: Option<u32>,
    producers: u32,
    duration: Option<Duration>,
    otlp_endpoint_url: Option<Url>,
}

impl Default
    for Builder<PhantomData<Url>, PhantomData<String>, PhantomData<i32>, PhantomData<Url>>
{
    fn default() -> Self {
        Self {
            broker: Default::default(),
            topic: Default::default(),
            partition: Default::default(),
            schema_registry: Default::default(),
            batch_size: 1,
            per_second: None,
            producers: 1,
            duration: None,
            otlp_endpoint_url: None,
        }
    }
}

impl<B, T, P, S> Builder<B, T, P, S> {
    pub fn broker(self, broker: impl Into<Url>) -> Builder<Url, T, P, S> {
        Builder {
            broker: broker.into(),
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            batch_size: self.batch_size,
            per_second: self.per_second,
            producers: self.producers,
            duration: self.duration,
            otlp_endpoint_url: self.otlp_endpoint_url,
        }
    }

    pub fn topic(self, topic: impl Into<String>) -> Builder<B, String, P, S> {
        Builder {
            broker: self.broker,
            topic: topic.into(),
            partition: self.partition,
            schema_registry: self.schema_registry,
            batch_size: self.batch_size,
            per_second: self.per_second,
            producers: self.producers,
            duration: self.duration,
            otlp_endpoint_url: self.otlp_endpoint_url,
        }
    }

    pub fn partition(self, partition: i32) -> Builder<B, T, i32, S> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition,
            schema_registry: self.schema_registry,
            batch_size: self.batch_size,
            per_second: self.per_second,
            producers: self.producers,
            duration: self.duration,
            otlp_endpoint_url: self.otlp_endpoint_url,
        }
    }

    pub fn schema_registry(self, schema_registry: Url) -> Builder<B, T, P, Url> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry,
            batch_size: self.batch_size,
            per_second: self.per_second,
            producers: self.producers,
            duration: self.duration,
            otlp_endpoint_url: self.otlp_endpoint_url,
        }
    }

    pub fn batch_size(self, batch_size: u32) -> Builder<B, T, P, S> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            batch_size,
            per_second: self.per_second,
            producers: self.producers,
            duration: self.duration,
            otlp_endpoint_url: self.otlp_endpoint_url,
        }
    }

    pub fn per_second(self, per_second: Option<u32>) -> Builder<B, T, P, S> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            batch_size: self.batch_size,
            per_second,
            producers: self.producers,
            duration: self.duration,
            otlp_endpoint_url: self.otlp_endpoint_url,
        }
    }

    pub fn producers(self, producers: u32) -> Builder<B, T, P, S> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            batch_size: self.batch_size,
            per_second: self.per_second,
            producers,
            duration: self.duration,
            otlp_endpoint_url: self.otlp_endpoint_url,
        }
    }

    pub fn duration(self, duration: Option<Duration>) -> Builder<B, T, P, S> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            batch_size: self.batch_size,
            per_second: self.per_second,
            producers: self.producers,
            duration,
            otlp_endpoint_url: self.otlp_endpoint_url,
        }
    }

    pub fn otlp_endpoint_url(self, otlp_endpoint_url: Option<Url>) -> Builder<B, T, P, S> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            batch_size: self.batch_size,
            per_second: self.per_second,
            producers: self.producers,
            duration: self.duration,
            otlp_endpoint_url,
        }
    }
}

impl Builder<Url, String, i32, Url> {
    pub fn build(self) -> Result<Generate> {
        Generate::try_from(Configuration {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            batch_size: self.batch_size,
            per_second: self.per_second,
            producers: self.producers,
            duration: self.duration,
            otlp_endpoint_url: self.otlp_endpoint_url,
        })
    }
}
