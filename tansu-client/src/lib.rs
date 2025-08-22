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
    fmt, io,
    net::SocketAddr,
    sync::{Arc, LazyLock},
    time::SystemTime,
};

use bytes::Bytes;
use deadpool::managed::{self, BuildError, PoolError};
use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    metrics::{Counter, Histogram, Meter},
};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use rama::{Context, Layer, Service};
use tansu_sans_io::{ApiKey, ApiVersionsRequest, Body, Frame, Header, Request};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpStream, lookup_host},
};
use tracing::{Instrument, Level, debug, error, span};
use url::Url;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    DeadPoolBuild(#[from] BuildError),
    Io(Arc<io::Error>),
    KafkaProtocol(#[from] tansu_sans_io::Error),
    Message(String),
    Otherwise,
    Pool(Box<dyn std::error::Error + Send + Sync>),
    UnknownApiKey(i16),
    UnknownHost(Url),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(msg) => write!(f, "{msg}"),
            error => write!(f, "{error:?}"),
        }
    }
}

impl<E> From<PoolError<E>> for Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(value: PoolError<E>) -> Self {
        Self::Pool(Box::new(value))
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
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

///  broker connection with a correlation id
#[derive(Debug)]
pub struct Connection {
    stream: TcpStream,
    correlation_id: i32,
}

impl Connection {
    async fn request_response<Q>(
        &mut self,
        req: Q,
        api_version: i16,
        client_id: Option<String>,
    ) -> Result<Q::Response, Error>
    where
        Q: Request,
        <Q::Response as TryFrom<Body>>::Error: Into<Error>,
    {
        let api_key = Q::KEY;
        let local = self.stream.local_addr().inspect(|local| debug!(%local))?;
        let peer = self.stream.peer_addr().inspect(|peer| debug!(%peer))?;

        let attributes = [
            KeyValue::new("api_key", api_key.to_string()),
            KeyValue::new("peer", peer.to_string()),
        ];

        let span = span!(Level::DEBUG, "client", local = %local, peer = %peer);

        async move {
            self.request(req, api_version, client_id, &attributes)
                .await?;

            self.correlation_id += 1;

            self.response::<Q>(api_version, &attributes).await
        }
        .instrument(span)
        .await
    }

    /// send a request to the broker
    async fn request<Q>(
        &mut self,
        req: Q,
        api_version: i16,
        client_id: Option<String>,
        attributes: &[KeyValue],
    ) -> Result<(), Error>
    where
        Q: Request,
    {
        let api_key = Q::KEY;

        let payload = Frame::request(
            Header::Request {
                api_key,
                api_version,
                correlation_id: self.correlation_id,
                client_id,
            },
            req.into(),
        )?;

        let start = SystemTime::now();

        self.stream
            .write_all(&payload[..])
            .await
            .inspect(|_| {
                TCP_SEND_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes,
                );

                TCP_BYTES_SENT.add(payload.len() as u64, attributes);
            })
            .inspect_err(|_| {
                TCP_SEND_ERRORS.add(1, attributes);
            })
            .map_err(Into::into)
    }

    /// demarshall a versioned response frame from the broker
    async fn response<Q>(
        &mut self,
        api_version: i16,
        attributes: &[KeyValue],
    ) -> Result<Q::Response, Error>
    where
        Q: Request,
        <Q::Response as TryFrom<Body>>::Error: Into<Error>,
    {
        self.read_frame(attributes)
            .await
            .and_then(|response| {
                Frame::response_from_bytes(response, Q::KEY, api_version).map_err(Into::into)
            })
            .map(|frame| frame.body)
            .and_then(|body| Q::Response::try_from(body).map_err(Into::into))
            .inspect(|response| debug!(?response))
    }

    /// marshall a request frame to the broker
    async fn read_frame(&mut self, attributes: &[KeyValue]) -> Result<Bytes, Error> {
        let start = SystemTime::now();

        let mut size = [0u8; 4];
        _ = self.stream.read_exact(&mut size).await?;

        let mut buffer: Vec<u8> = vec![0u8; frame_length(size)];
        buffer[0..size.len()].copy_from_slice(&size[..]);
        _ = self
            .stream
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

        Ok(Bytes::from(buffer))
    }
}

/// manager of supported API versions for a broker
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Manager {
    broker: Url,
    client_id: Option<String>,
    versions: BTreeMap<i16, i16>,
}

impl Manager {
    /// build a manager with a broker endpoint
    pub fn builder(broker: Url) -> Builder {
        Builder::broker(broker)
    }

    /// client id used in requests to the broker
    pub fn client_id(&self) -> Option<String> {
        self.client_id.clone()
    }

    /// the version supported by the broker for a given api key
    pub fn api_version(&self, api_key: i16) -> Result<i16, Error> {
        self.versions
            .get(&api_key)
            .copied()
            .ok_or(Error::UnknownApiKey(api_key))
    }

    /// resolve a host into an IP socket address
    async fn host_port(&self) -> Result<SocketAddr, Error> {
        if let Some(host) = self.broker.host_str()
            && let Some(port) = self.broker.port()
        {
            let attributes = [KeyValue::new("url", self.broker.to_string())];
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

            if let Some(socket_addr) = addresses.next().inspect(|socket_addr| debug!(?socket_addr))
            {
                return Ok(socket_addr);
            }
        }

        Err(Error::UnknownHost(self.broker.clone()))
    }
}

impl managed::Manager for Manager {
    type Type = Connection;
    type Error = Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        debug!(%self.broker);

        let attributes = [KeyValue::new("broker", self.broker.to_string())];
        let start = SystemTime::now();

        TcpStream::connect(self.host_port().await?)
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

/// inject the pool into the service context of this layer
#[derive(Clone, Debug)]
pub(crate) struct PoolLayer {
    pool: Pool,
}

impl PoolLayer {
    fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

impl<S> Layer<S> for PoolLayer {
    type Service = PoolService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        PoolService {
            pool: self.pool.clone(),
            inner,
        }
    }
}

/// inject the pool into the inner service context
#[derive(Clone, Debug)]
pub(crate) struct PoolService<S> {
    pool: Pool,
    inner: S,
}

impl<State, S, Q> Service<State, Q> for PoolService<S>
where
    Q: Request,
    S: Service<Pool, Q>,
    S::Error: Into<Error>,
    State: Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = Error;

    /// serve the request, injecting the pool into the context of the inner service
    async fn serve(&self, _: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        let ctx = Context::with_state(self.pool.clone());
        self.inner.serve(ctx, req).await.map_err(Into::into)
    }
}

/// API client using a connection pool
#[derive(Clone, Debug)]
pub struct Client {
    service: PoolService<ConnectionService>,
}

impl Client {
    /// create a new client using the supplied pool
    pub fn new(pool: Pool) -> Self {
        Self {
            service: PoolLayer::new(pool).into_layer(ConnectionService),
        }
    }

    /// make an API request using the connection from the pool
    pub async fn call<Q>(&self, req: Q) -> Result<Q::Response, Error>
    where
        Q: Request,
        <Q::Response as TryFrom<Body>>::Error: Into<Error>,
    {
        self.service.serve(Context::default(), req).await
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ConnectionService;

impl<Q> Service<Pool, Q> for ConnectionService
where
    Q: Request,
    <Q::Response as TryFrom<Body>>::Error: Into<Error>,
{
    type Response = Q::Response;
    type Error = Error;

    async fn serve(&self, ctx: Context<Pool>, req: Q) -> Result<Self::Response, Self::Error> {
        debug!(?ctx, ?req);
        let pool = ctx.state();
        let api_key = Q::KEY;
        let api_version = pool.manager().api_version(api_key)?;
        let client_id = pool.manager().client_id();
        let mut connection = pool.get().await?;

        connection
            .request_response(req, api_version, client_id)
            .await
    }
}

fn frame_length(encoded: [u8; 4]) -> usize {
    i32::from_be_bytes(encoded) as usize + encoded.len()
}

static DNS_LOOKUP_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("dns_lookup_duration")
        .with_unit("ms")
        .with_description("DNS lookup latencies")
        .build()
});

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
