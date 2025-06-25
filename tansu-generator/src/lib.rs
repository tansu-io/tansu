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
    num::NonZeroU32,
    pin::Pin,
    result,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use governor::{InsufficientCapacity, Quota, RateLimiter};
use nonzero_ext::nonzero;
use opentelemetry::{InstrumentationScope, global, metrics::Meter};
use opentelemetry_sdk::{
    error::OTelSdkResult,
    metrics::{
        PeriodicReaderBuilder, SdkMeterProvider, Temporality, data::ResourceMetrics,
        exporter::PushMetricExporter,
    },
};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use rama::{
    Context, Service,
    error::{BoxError, OpaqueError},
};
use tansu_kafka_sans_io::{
    ErrorCode,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{deflated, inflated},
};
use tansu_schema_registry::{Generator as _, Registry, Schema};
use tansu_service::{
    api::produce::{ProduceRequest, ProduceResponse},
    service::ApiClient,
};
use tokio::{
    signal::unix::{SignalKind, signal},
    task::JoinSet,
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Level, debug, info, span};
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
    InsufficientCapacity(#[from] InsufficientCapacity),
    Io(Arc<io::Error>),
    Opaque(#[from] OpaqueError),
    Protocol(#[from] tansu_kafka_sans_io::Error),
    Schema(Box<tansu_schema_registry::Error>),
    SchemaNotFoundForTopic(String),
}

impl From<tansu_schema_registry::Error> for Error {
    fn from(error: tansu_schema_registry::Error) -> Self {
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
        write!(f, "{:?}", self)
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

fn host_port(url: &Url) -> String {
    format!("{}:{}", url.host_str().unwrap(), url.port().unwrap())
}

pub async fn produce(
    broker: Url,
    name: String,
    index: i32,
    schema: Schema,
    batch_size: i32,
) -> Result<()> {
    debug!(%broker, %name, index, batch_size);

    let frame = {
        let mut batch = inflated::Batch::builder();
        let offset_deltas = 0..batch_size;

        for offset_delta in offset_deltas {
            batch = batch.record(
                schema
                    .generate()
                    .map(|record| record.offset_delta(offset_delta))?,
            );
        }

        batch
            .last_offset_delta(batch_size)
            .build()
            .map(|batch| inflated::Frame {
                batches: vec![batch],
            })
            .and_then(deflated::Frame::try_from)?
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
        .parse()
        .map(ApiClient::new)
        .inspect(|client| debug!(?client))?;

    let response = origin
        .serve(Context::default(), request.into())
        .await
        .and_then(|response| ProduceResponse::try_from(response).map_err(Into::into))
        .inspect(|response| debug!(?response))?;

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

impl Generate {
    pub async fn main(self) -> Result<ErrorCode> {
        global::set_meter_provider(SdkMeterProvider::default());

        let Some(schema) = self.registry.schema(&self.configuration.topic).await? else {
            return Err(Error::SchemaNotFoundForTopic(
                self.configuration.topic.clone(),
            ));
        };

        let response_count = Arc::new(AtomicU64::new(0));
        let records_sent = Arc::new(AtomicU64::new(0));

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

        let generator_start = SystemTime::now();

        for producer in 0..self.configuration.producers {
            let records_sent = records_sent.clone();
            let rate_limiter = rate_limiter.clone();
            let schema = schema.clone();
            let broker = self.configuration.broker.clone();
            let topic = self.configuration.topic.clone();
            let partition = self.configuration.partition;
            let token = token.clone();

            _ = set.spawn(async move {
                    let span = span!(Level::DEBUG, "producer", producer);

                    async move {
                        loop {
                            debug!(%broker, %topic, partition);

                            if let Some(ref rate_limiter) = rate_limiter {
                                let rate_limit_start = SystemTime::now();

                                tokio::select! {
                                    cancelled = token.cancelled() => {
                                        debug!(?cancelled);
                                        break
                                    },

                                    Ok(_) = rate_limiter.until_n_ready(batch_size) => {
                                        info!(rate_limit_duration_ms = rate_limit_start
                                            .elapsed()
                                            .map_or(0, |duration| duration.as_millis() as u64));

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
                                    records_sent.fetch_add(batch_size.get() as u64, Ordering::Relaxed);

                                    info!(produce_duration_ms = produce_start
                                    .elapsed()
                                    .map_or(0, |duration| duration.as_millis() as u64));
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

        let generator_duration_ms = generator_start
            .elapsed()
            .map_or(0, |duration| duration.as_millis() as u64);

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
        })
    }
}
