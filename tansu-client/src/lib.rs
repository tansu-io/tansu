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

//! Tansu Client
//!
//! Tansu API client.

use std::{
    collections::BTreeMap,
    error, fmt, io,
    sync::{Arc, LazyLock},
    time::SystemTime,
};

use bytes::Bytes;
use deadpool::managed::{self, BuildError, Object, PoolError};
use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    metrics::{Counter, Histogram, Meter},
};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use rama::{Context, Layer, Service};
use tansu_sans_io::{ApiKey, ApiVersionsRequest, Body, Frame, Header, Request};
use tansu_service::{FrameBytesLayer, FrameBytesService, host_port};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
};
use tracing::{Instrument, Level, debug, error, span};
use tracing_subscriber::filter::ParseError;
use url::Url;

#[derive(thiserror::Error, Clone, Debug)]
pub enum Error {
    DeadPoolBuild(#[from] BuildError),
    Io(Arc<io::Error>),
    Message(String),
    ParseFilter(Arc<ParseError>),
    ParseUrl(#[from] url::ParseError),
    Pool(Arc<Box<dyn error::Error + Send + Sync>>),
    Protocol(#[from] tansu_sans_io::Error),
    Service(#[from] tansu_service::Error),
    UnknownApiKey(i16),
    UnknownHost(Url),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl<E> From<PoolError<E>> for Error
where
    E: error::Error + Send + Sync + 'static,
{
    fn from(value: PoolError<E>) -> Self {
        Self::Pool(Arc::new(Box::new(value)))
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        Self::ParseFilter(Arc::new(value))
    }
}

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

///  Broker connection with a correlation id
#[derive(Debug)]
pub struct Connection {
    stream: TcpStream,
    correlation_id: i32,
}

/// Manager of supported API versions for a broker
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Manager {
    broker: Url,
    client_id: Option<String>,
    versions: BTreeMap<i16, i16>,
}

impl Manager {
    /// Build a manager with a broker endpoint
    pub fn builder(broker: Url) -> Builder {
        Builder::broker(broker)
    }

    /// Client id used in requests to the broker
    pub fn client_id(&self) -> Option<String> {
        self.client_id.clone()
    }

    /// The version supported by the broker for a given api key
    pub fn api_version(&self, api_key: i16) -> Result<i16, Error> {
        self.versions
            .get(&api_key)
            .copied()
            .ok_or(Error::UnknownApiKey(api_key))
    }
}

impl managed::Manager for Manager {
    type Type = Connection;
    type Error = Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        debug!(%self.broker);

        let attributes = [KeyValue::new("broker", self.broker.to_string())];
        let start = SystemTime::now();

        TcpStream::connect(host_port(self.broker.clone()).await?)
            .await
            .inspect(|_| {
                TCP_CONNECT_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &attributes,
                )
            })
            .inspect_err(|err| {
                error!(broker = %self.broker, ?err);
                TCP_CONNECT_ERRORS.add(1, &attributes);
            })
            .map(|stream| Connection {
                stream,
                correlation_id: 0,
            })
            .map_err(Into::into)
    }

    async fn recycle(
        &self,
        obj: &mut Self::Type,
        metrics: &managed::Metrics,
    ) -> managed::RecycleResult<Self::Error> {
        debug!(?obj, ?metrics);
        Ok(())
    }
}

/// a managed pool of broker connections
pub type Pool = managed::Pool<Manager>;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Builder {
    broker: Url,
    client_id: Option<String>,
}

impl Builder {
    /// broker url
    pub fn broker(broker: Url) -> Self {
        Self {
            broker,
            client_id: None,
        }
    }

    /// client id used when making requests to the broker
    pub fn client_id(self, client_id: Option<String>) -> Self {
        Self { client_id, ..self }
    }

    /// inquire with the broker supported api versions
    async fn bootstrap(&self) -> Result<BTreeMap<i16, i16>, Error> {
        let versions = BTreeMap::from([(ApiVersionsRequest::KEY, 0)]);

        let req = ApiVersionsRequest::default()
            .client_software_name(Some(env!("CARGO_PKG_NAME").into()))
            .client_software_version(Some(env!("CARGO_PKG_VERSION").into()));

        let client = Pool::builder(Manager {
            broker: self.broker.clone(),
            client_id: self.client_id.clone(),
            versions,
        })
        .build()
        .map(Client::new)?;

        client.call(req).await.map(|response| {
            response
                .api_keys
                .unwrap_or_default()
                .into_iter()
                .map(|api| (api.api_key, api.max_version))
                .collect()
        })
    }

    /// establish the api versions supported by the broker
    pub async fn build(self) -> Result<Pool, Error> {
        self.bootstrap().await.and_then(|versions| {
            Pool::builder(Manager {
                broker: self.broker,
                client_id: self.client_id,
                versions,
            })
            .build()
            .map_err(Into::into)
        })
    }
}

/// inject the pool into the service context of this frame layer
#[derive(Clone, Debug)]
pub struct FramePoolLayer {
    pool: Pool,
}

impl FramePoolLayer {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

impl<S> Layer<S> for FramePoolLayer {
    type Service = FramePoolService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        FramePoolService {
            pool: self.pool.clone(),
            inner,
        }
    }
}

/// inject the pool into the service context of this request layer
#[derive(Clone, Debug)]
pub struct RequestPoolLayer {
    pool: Pool,
}

impl RequestPoolLayer {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

impl<S> Layer<S> for RequestPoolLayer {
    type Service = RequestPoolService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestPoolService {
            pool: self.pool.clone(),
            inner,
        }
    }
}

/// inject the pool into the inner service context
#[derive(Clone, Debug)]
pub struct RequestPoolService<S> {
    pool: Pool,
    inner: S,
}

impl<State, S, Q> Service<State, Q> for RequestPoolService<S>
where
    Q: Request,
    S: Service<Pool, Q>,
    State: Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    /// serve the request, injecting the pool into the context of the inner service
    async fn serve(&self, ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        let (ctx, _) = ctx.swap_state(self.pool.clone());
        self.inner.serve(ctx, req).await
    }
}

#[derive(Clone, Debug)]
pub struct FramePoolService<S> {
    pool: Pool,
    inner: S,
}

impl<State, S> Service<State, Frame> for FramePoolService<S>
where
    S: Service<Pool, Frame, Response = Frame>,
    State: Send + Sync + 'static,
{
    type Response = Frame;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let (ctx, _) = ctx.swap_state(self.pool.clone());
        self.inner.serve(ctx, req).await
    }
}

/// API client using a connection pool
#[derive(Clone, Debug)]
pub struct Client {
    service:
        RequestPoolService<RequestConnectionService<FrameBytesService<BytesConnectionService>>>,
}

impl Client {
    /// Create a new client using the supplied pool
    pub fn new(pool: Pool) -> Self {
        let service = (
            RequestPoolLayer::new(pool),
            RequestConnectionLayer,
            FrameBytesLayer,
        )
            .into_layer(BytesConnectionService);

        Self { service }
    }

    /// Make an API request using the connection from the pool
    pub async fn call<Q>(&self, req: Q) -> Result<Q::Response, Error>
    where
        Q: Request,
        Error: From<<<Q as Request>::Response as TryFrom<Body>>::Error>,
    {
        self.service.serve(Context::default(), req).await
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameConnectionLayer;

impl<S> Layer<S> for FrameConnectionLayer {
    type Service = FrameConnectionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameConnectionService<S> {
    inner: S,
}

impl<S> Service<Pool, Frame> for FrameConnectionService<S>
where
    S: Service<Object<Manager>, Frame, Response = Frame>,
    S::Error: From<Error> + From<PoolError<Error>> + From<tansu_sans_io::Error>,
{
    type Response = Frame;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<Pool>, req: Frame) -> Result<Self::Response, Self::Error> {
        debug!(?ctx, ?req);

        let api_key = req.api_key()?;
        let api_version = req.api_version()?;
        let client_id = req
            .client_id()
            .map(|client_id| client_id.map(|client_id| client_id.to_string()))?;

        let connection = ctx.state().get().await?;
        let correlation_id = connection.correlation_id;

        let frame = Frame {
            size: 0,
            header: Header::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
            },
            body: req.body,
        };

        let (ctx, _) = ctx.swap_state(connection);

        self.inner.serve(ctx, frame).await
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestConnectionLayer;

impl<S> Layer<S> for RequestConnectionLayer {
    type Service = RequestConnectionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestConnectionService<S> {
    inner: S,
}

impl<Q, S> Service<Pool, Q> for RequestConnectionService<S>
where
    Q: Request,
    S: Service<Object<Manager>, Frame, Response = Frame>,
    S::Error: From<Error>
        + From<PoolError<Error>>
        + From<tansu_sans_io::Error>
        + From<<Q::Response as TryFrom<Body>>::Error>,
{
    type Response = Q::Response;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<Pool>, req: Q) -> Result<Self::Response, Self::Error> {
        debug!(?ctx, ?req);
        let pool = ctx.state();
        let api_key = Q::KEY;
        let api_version = pool.manager().api_version(api_key)?;
        let client_id = pool.manager().client_id();
        let connection = pool.get().await?;
        let correlation_id = connection.correlation_id;

        let frame = Frame {
            size: 0,
            header: Header::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
            },
            body: req.into(),
        };

        let (ctx, _) = ctx.swap_state(connection);

        let frame = self.inner.serve(ctx, frame).await?;

        Q::Response::try_from(frame.body).map_err(Into::into)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesConnectionService;

impl BytesConnectionService {
    async fn write(
        &self,
        stream: &mut TcpStream,
        frame: Bytes,
        attributes: &[KeyValue],
    ) -> Result<(), Error> {
        debug!(?frame);

        let start = SystemTime::now();

        stream
            .write_all(&frame[..])
            .await
            .inspect(|_| {
                TCP_SEND_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes,
                );

                TCP_BYTES_SENT.add(frame.len() as u64, attributes);
            })
            .inspect_err(|_| {
                TCP_SEND_ERRORS.add(1, attributes);
            })
            .map_err(Into::into)
    }

    async fn read(&self, stream: &mut TcpStream, attributes: &[KeyValue]) -> Result<Bytes, Error> {
        let start = SystemTime::now();

        let mut size = [0u8; 4];
        _ = stream.read_exact(&mut size).await?;

        let mut buffer: Vec<u8> = vec![0u8; frame_length(size)];
        buffer[0..size.len()].copy_from_slice(&size[..]);
        _ = stream
            .read_exact(&mut buffer[4..])
            .await
            .inspect(|_| {
                TCP_RECEIVE_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes,
                );

                TCP_BYTES_RECEIVED.add(buffer.len() as u64, attributes);
            })
            .inspect_err(|_| {
                TCP_RECEIVE_ERRORS.add(1, attributes);
            })?;

        Ok(Bytes::from(buffer)).inspect(|frame| debug!(?frame))
    }
}

impl Service<Object<Manager>, Bytes> for BytesConnectionService {
    type Response = Bytes;
    type Error = Error;

    async fn serve(
        &self,
        mut ctx: Context<Object<Manager>>,
        req: Bytes,
    ) -> Result<Self::Response, Self::Error> {
        let c = ctx.state_mut();

        let local = c.stream.local_addr()?;
        let peer = c.stream.peer_addr()?;

        let attributes = [
            KeyValue::new("correlation_id", c.correlation_id.to_string()),
            KeyValue::new("peer", peer.to_string()),
        ];

        let span = span!(Level::DEBUG, "client", local = %local, peer = %peer);

        async move {
            self.write(&mut c.stream, req, &attributes).await?;

            c.correlation_id += 1;

            self.read(&mut c.stream, &attributes).await
        }
        .instrument(span)
        .await
    }
}

fn frame_length(encoded: [u8; 4]) -> usize {
    i32::from_be_bytes(encoded) as usize + encoded.len()
}

static TCP_CONNECT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tcp_connect_duration")
        .with_unit("ms")
        .with_description("The TCP connect latencies in milliseconds")
        .build()
});

static TCP_CONNECT_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_connect_errors")
        .with_description("TCP connect errors")
        .build()
});

static TCP_SEND_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tcp_send_duration")
        .with_unit("ms")
        .with_description("The TCP send latencies in milliseconds")
        .build()
});

static TCP_SEND_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_send_errors")
        .with_description("TCP send errors")
        .build()
});

static TCP_RECEIVE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tcp_receive_duration")
        .with_unit("ms")
        .with_description("The TCP receive latencies in milliseconds")
        .build()
});

static TCP_RECEIVE_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_receive_errors")
        .with_description("TCP receive errors")
        .build()
});

static TCP_BYTES_SENT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_bytes_sent")
        .with_description("TCP bytes sent")
        .build()
});

static TCP_BYTES_RECEIVED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_bytes_received")
        .with_description("TCP bytes received")
        .build()
});

#[cfg(test)]
mod tests {
    use std::{fs::File, thread};

    use tansu_sans_io::{MetadataRequest, MetadataResponse};
    use tansu_service::{
        BytesFrameLayer, FrameRouteService, RequestLayer, ResponseService, TcpBytesLayer,
        TcpContextLayer, TcpListenerLayer,
    };
    use tokio::{net::TcpListener, task::JoinSet};
    use tokio_util::sync::CancellationToken;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use super::*;

    fn init_tracing() -> Result<DefaultGuard, Error> {
        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
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

    async fn server(cancellation: CancellationToken, listener: TcpListener) -> Result<(), Error> {
        let server = (
            TcpListenerLayer::new(cancellation),
            TcpContextLayer::default(),
            TcpBytesLayer::default(),
            BytesFrameLayer,
        )
            .into_layer(
                FrameRouteService::builder()
                    .with_service(RequestLayer::<MetadataRequest>::new().into_layer(
                        ResponseService::new(|_ctx: Context<()>, _req: MetadataRequest| {
                            Ok::<_, Error>(
                                MetadataResponse::default()
                                    .brokers(Some([].into()))
                                    .topics(Some([].into()))
                                    .cluster_id(Some("abc".into()))
                                    .controller_id(Some(111))
                                    .throttle_time_ms(Some(0))
                                    .cluster_authorized_operations(Some(-1)),
                            )
                        }),
                    ))
                    .and_then(|builder| builder.build())?,
            );

        server
            .serve(Context::default(), listener)
            .await
            .map_err(Into::into)
    }

    #[tokio::test]
    async fn tcp_client_server() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let cancellation = CancellationToken::new();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;

        let mut join = JoinSet::new();

        let _server = {
            let cancellation = cancellation.clone();
            join.spawn(async move { server(cancellation, listener).await })
        };

        let origin = (
            RequestPoolLayer::new(
                Manager::builder(
                    Url::parse(&format!("tcp://{local_addr}")).inspect(|url| debug!(%url))?,
                )
                .client_id(Some(env!("CARGO_PKG_NAME").into()))
                .build()
                .await
                .inspect(|pool| debug!(?pool))?,
            ),
            RequestConnectionLayer,
            FrameBytesLayer,
        )
            .into_layer(BytesConnectionService);

        let response = origin
            .serve(
                Context::default(),
                MetadataRequest::default()
                    .topics(Some([].into()))
                    .allow_auto_topic_creation(Some(false))
                    .include_cluster_authorized_operations(Some(false))
                    .include_topic_authorized_operations(Some(false)),
            )
            .await?;

        assert_eq!(Some("abc"), response.cluster_id.as_deref());
        assert_eq!(Some(111), response.controller_id);

        cancellation.cancel();

        let joined = join.join_all().await;
        debug!(?joined);

        Ok(())
    }
}
