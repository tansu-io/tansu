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

use std::{
    collections::BTreeMap,
    error,
    fmt::{self, Debug},
    io,
    marker::PhantomData,
    net::SocketAddr,
    sync::{Arc, LazyLock},
    time::SystemTime,
};

use bytes::Bytes;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use rama::{Context, Layer, Service, context::Extensions, matcher::Matcher, service::BoxService};
use tansu_sans_io::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, Body, ErrorCode, Frame, Header, Request,
    Response, RootMessageMeta, api_versions_response::ApiVersion,
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream, lookup_host},
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument as _, Level, debug, error, span};
use tracing_subscriber::filter::ParseError;
use url::Url;

use crate::{METER, frame_length};

#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    FrameTooBig(usize),
    Io(Arc<io::Error>),
    Message(String),
    ParseFilter(Arc<ParseError>),
    Protocol(#[from] tansu_sans_io::Error),
    UnknownServiceFrame(Box<Frame>),
    DuplicateRoute(i16),
    UnableToSend(Box<Frame>),
    OneshotRecv(oneshot::error::RecvError),
    UnknownServiceBody(Box<Body>),
    UnknownHost(Url),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
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

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestApiKeyMatcher(pub i16);

impl<State, Q> Matcher<State, Q> for RequestApiKeyMatcher
where
    Q: Request,
    State: Clone + Debug,
{
    fn matches(&self, ext: Option<&mut Extensions>, ctx: &Context<State>, req: &Q) -> bool {
        debug!(?ext, ?ctx, ?req);
        Q::KEY == self.0
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameApiKeyMatcher(pub i16);

impl<State> Matcher<State, Frame> for FrameApiKeyMatcher
where
    State: Clone + Debug,
{
    fn matches(&self, ext: Option<&mut Extensions>, ctx: &Context<State>, req: &Frame) -> bool {
        debug!(?ext, ?ctx, ?req);
        req.api_key().is_ok_and(|api_key| api_key == self.0)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestLayer<Q> {
    request: PhantomData<Q>,
}

impl<Q> RequestLayer<Q> {
    pub fn new() -> Self {
        Self {
            request: PhantomData,
        }
    }
}

impl<S, Q> Layer<S> for RequestLayer<Q> {
    type Service = RequestService<S, Q>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            request: PhantomData,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestService<S, Q> {
    inner: S,
    request: PhantomData<Q>,
}

impl<State, S, Q> Service<State, Q> for RequestService<S, Q>
where
    S: Service<State, Q>,
    Q: Request,
    S::Error: From<<Q as TryFrom<Body>>::Error> + From<<S as Service<State, Q>>::Error>,
    S::Response: Response,
    Body: From<<S as Service<State, Q>>::Response>,
    State: Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        debug!(?req);
        self.inner
            .serve(ctx, req)
            .await
            .inspect(|response| debug!(?response))
    }
}

impl<S, Q> ApiKey for RequestService<S, Q>
where
    Q: Request,
{
    const KEY: i16 = Q::KEY;
}

pub async fn host_port(url: Url) -> Result<SocketAddr, Error> {
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

    Err(Error::UnknownHost(url))
}

static DNS_LOOKUP_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("dns_lookup_duration")
        .with_unit("ms")
        .with_description("DNS lookup latencies")
        .build()
});

#[derive(Clone, Debug, Default)]
pub struct TcpListenerLayer {
    cancellation: CancellationToken,
}

impl TcpListenerLayer {
    pub fn new(cancellation: CancellationToken) -> Self {
        Self { cancellation }
    }
}

impl<S> Layer<S> for TcpListenerLayer {
    type Service = TcpListenerService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            cancellation: self.cancellation.clone(),
            inner,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TcpListenerService<S> {
    cancellation: CancellationToken,
    inner: S,
}

impl<State, S> Service<State, TcpListener> for TcpListenerService<S>
where
    S: Service<State, TcpStream> + Clone,
    S::Response: Debug,
    S::Error: error::Error,
    State: Clone + Send + Sync + 'static,
{
    type Response = ();
    type Error = S::Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        req: TcpListener,
    ) -> Result<Self::Response, Self::Error> {
        let mut set = JoinSet::new();

        loop {
            tokio::select! {
                Ok((stream, addr)) = req.accept() => {
                    debug!(?req, ?stream, %addr);

                    let service = self.inner.clone();
                    let ctx = ctx.clone();

                    let handle = set.spawn(async move {
                            match service.serve(ctx, stream).await {
                                Err(error) => {
                                    debug!(%error);
                                },

                                Ok(response) => {
                                    debug!(?response)
                                }
                        }
                    });

                    debug!(?handle);
                    continue;
                }

                v = set.join_next(), if !set.is_empty() => {
                    debug!(?v);
                }

                cancelled = self.cancellation.cancelled() => {
                    debug!(?cancelled);
                    break;
                }
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpContext {
    cluster_id: Option<String>,
    maximum_frame_size: Option<usize>,
}

impl TcpContext {
    pub fn cluster_id(self, cluster_id: Option<String>) -> Self {
        Self { cluster_id, ..self }
    }

    pub fn maximum_frame_size(self, maximum_frame_size: Option<usize>) -> Self {
        Self {
            maximum_frame_size,
            ..self
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpContextLayer {
    state: TcpContext,
}

impl TcpContextLayer {
    pub fn new(state: TcpContext) -> Self {
        Self { state }
    }
}

impl<S> Layer<S> for TcpContextLayer {
    type Service = TcpContextService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            state: self.state.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpContextService<S> {
    inner: S,
    state: TcpContext,
}

impl<State, S> Service<State, TcpStream> for TcpContextService<S>
where
    S: Service<TcpContext, TcpStream>,
    State: Clone + Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        req: TcpStream,
    ) -> Result<Self::Response, Self::Error> {
        debug!(?req);
        let (ctx, _) = ctx.swap_state(self.state.clone());
        self.inner.serve(ctx, req).await
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesTcpService;

impl Service<TcpStream, Bytes> for BytesTcpService {
    type Response = Bytes;
    type Error = Error;

    async fn serve(
        &self,
        mut ctx: Context<TcpStream>,
        req: Bytes,
    ) -> Result<Self::Response, Self::Error> {
        let stream = ctx.state_mut();

        stream.write_all(&req[..]).await?;

        let mut size = [0u8; 4];
        _ = stream.read_exact(&mut size).await?;

        let mut buffer: Vec<u8> = vec![0u8; frame_length(size)];
        buffer[0..size.len()].copy_from_slice(&size[..]);
        _ = stream.read_exact(&mut buffer[4..]).await?;

        Ok(Bytes::from(buffer))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpBytesLayer<State = ()> {
    _state: PhantomData<State>,
}

impl<S, State> Layer<S> for TcpBytesLayer<State> {
    type Service = TcpBytesService<S, State>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            _state: PhantomData,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpBytesService<S, State> {
    inner: S,
    _state: PhantomData<State>,
}

impl<S, State> Service<TcpContext, TcpStream> for TcpBytesService<S, State>
where
    S: Service<State, Bytes, Response = Bytes>,
    S::Error: From<Error> + From<io::Error> + Debug,
    State: Clone + Default + Send + Sync + 'static,
{
    type Response = ();

    type Error = S::Error;

    async fn serve(
        &self,
        ctx: Context<TcpContext>,
        mut req: TcpStream,
    ) -> Result<Self::Response, Self::Error> {
        let peer = req.peer_addr()?;

        let span = span!(
            Level::DEBUG,
            "tcp",
            %peer,
        );

        let mut size = [0u8; 4];

        let attributes = {
            let state = ctx.state();

            let mut attributes = vec![KeyValue::new(
                "local_addr",
                req.local_addr().map(|local_addr| local_addr.to_string())?,
            )];

            if let Some(cluster_id) = state.cluster_id.clone() {
                attributes.push(KeyValue::new("cluster_id", cluster_id))
            }

            attributes
        };

        async move {
            loop {
                let ctx = ctx.clone();

                _ = req.read_exact(&mut size).await?;

                if ctx
                    .state()
                    .maximum_frame_size
                    .is_some_and(|maximum_frame_size| maximum_frame_size > frame_length(size))
                {
                    return Err(Into::into(Error::FrameTooBig(frame_length(size))));
                }

                let mut request: Vec<u8> = vec![0u8; frame_length(size)];
                request[0..size.len()].copy_from_slice(&size[..]);
                _ = req.read_exact(&mut request[4..]).await?;

                REQUEST_SIZE.record(request.len() as u64, &attributes);

                let (ctx, _) = ctx.swap_state(State::default());
                let request_start = SystemTime::now();

                let response = self
                    .inner
                    .serve(ctx, Bytes::from(request))
                    .await
                    .inspect_err(|err| error!(?err))
                    .inspect(|response| {
                        RESPONSE_SIZE.record(response.len() as u64, &attributes);

                        REQUEST_DURATION.record(
                            request_start
                                .elapsed()
                                .map_or(0, |duration| duration.as_millis() as u64),
                            &attributes,
                        );
                    })?;

                req.write_all(&response).await?
            }
        }
        .instrument(span)
        .await
    }
}

static REQUEST_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tansu_request_size")
        .with_unit("By")
        .with_description("The API request size in bytes")
        .build()
});

static RESPONSE_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tansu_response_size")
        .with_unit("By")
        .with_description("The API response size in bytes")
        .build()
});

static REQUEST_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tansu_request_duration")
        .with_unit("ms")
        .with_description("The API request latencies in milliseconds")
        .build()
});

// #[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
// pub struct FramingService<S> {
//     inner: S,
// }

// impl<S> FramingService<S> {
//     pub fn new(inner: S) -> Self {
//         Self { inner }
//     }
// }

// impl<S, State> Service<State, Bytes> for FramingService<S>
// where
//     S: Service<State, Frame>,
//     S::Response: Into<Body>,
//     S::Error: From<tansu_sans_io::Error>,
//     State: Send + Sync + 'static,
// {
//     type Response = Bytes;
//     type Error = S::Error;

//     async fn serve(
//         &self,
//         ctx: Context<State>,
//         request: Bytes,
//     ) -> Result<Self::Response, Self::Error> {
//         let request = Frame::request_from_bytes(&request[..])?;

//         let api_key = request.api_key()?;
//         let api_version = request.api_version()?;
//         let correlation_id = request.correlation_id()?;

//         let span = span!(
//             Level::DEBUG,
//             "frame",
//             api_name = request.api_name(),
//             api_version,
//             correlation_id
//         );

//         debug!(?request);

//         async move {
//             let body = self.inner.serve(ctx, request).await.map(Into::into)?;

//             let attributes = vec![
//                 KeyValue::new("api_key", api_key as i64),
//                 KeyValue::new("api_version", api_version as i64),
//             ];

//             Frame::response(
//                 Header::Response { correlation_id },
//                 body,
//                 api_key,
//                 api_version,
//             )
//             .inspect(|response| {
//                 debug!(?response);
//                 API_REQUESTS.add(1, &attributes);
//             })
//             .inspect_err(|err| {
//                 error!(api_key, api_version, ?err);
//                 API_ERRORS.add(1, &attributes);
//             })
//             .map_err(Into::into)
//         }
//         .instrument(span)
//         .await
//     }
// }

// #[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
// pub struct FramingLayer;

// impl<S> Layer<S> for FramingLayer {
//     type Service = FramingService<S>;

//     fn layer(&self, inner: S) -> Self::Service {
//         Self::Service { inner }
//     }
// }

static API_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_api_requests")
        .with_description("The number of API requests made")
        .build()
});

static API_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_api_errors")
        .with_description("The number of API errors")
        .build()
});

/// Route frames to a service based via the API key
#[derive(Clone, Debug, Default)]
pub struct FrameRouteService<State = (), E = Error> {
    routes: Arc<BTreeMap<i16, BoxService<State, Frame, Frame, E>>>,
}

impl<State, E> FrameRouteService<State, E>
where
    State: Clone + Send + Sync + 'static,
    E: error::Error + From<tansu_sans_io::Error> + From<Error> + Send + Sync + 'static,
{
    pub fn new(routes: Arc<BTreeMap<i16, BoxService<State, Frame, Frame, E>>>) -> Self {
        Self { routes }
    }

    pub fn builder() -> FrameRouteBuilder<State, E> {
        FrameRouteBuilder::<State, E>::new()
    }
}

impl<State, E> Service<State, Frame> for FrameRouteService<State, E>
where
    State: Clone + Send + Sync + 'static,
    E: error::Error + From<tansu_sans_io::Error> + From<Error> + Send + Sync + 'static,
{
    type Response = Frame;
    type Error = E;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        debug!(?req);

        let api_key = req.api_key()?;

        if let Some(service) = self.routes.get(&api_key) {
            service.serve(ctx, req).await
        } else {
            Err(E::from(Error::UnknownServiceFrame(Box::new(req))))
        }
    }
}

/// A frame route builder providing an API versions response with the available routes
#[derive(Debug)]
pub struct FrameRouteBuilder<State, E> {
    routes: BTreeMap<i16, BoxService<State, Frame, Frame, E>>,
}

impl<State, E> FrameRouteBuilder<State, E>
where
    State: Clone + Send + Sync + 'static,
    E: error::Error + From<tansu_sans_io::Error> + Send + Sync + 'static,
{
    fn new() -> Self {
        Self {
            routes: BTreeMap::new(),
        }
    }

    pub fn with_service<S>(self, service: S) -> Result<Self, Error>
    where
        S: Into<BoxService<State, Frame, Frame, E>> + ApiKey,
    {
        self.with_route(S::KEY, service.into())
    }

    pub fn with_route(
        mut self,
        api_key: i16,
        service: BoxService<State, Frame, Frame, E>,
    ) -> Result<Self, Error> {
        self.routes
            .insert(api_key, service)
            .map_or(Ok(self), |_existing| Err(Error::DuplicateRoute(api_key)))
    }

    pub fn build(self) -> Result<FrameRouteService<State, E>, Error> {
        let api_key = ApiVersionsRequest::KEY;
        let mut supported = self.routes.keys().copied().collect::<Vec<_>>();
        supported.push(api_key);

        self.with_route(
            api_key,
            ApiVersionsService {
                supported,
                error: PhantomData,
            }
            .boxed(),
        )
        .map(|builder| FrameRouteService {
            routes: Arc::new(builder.routes),
        })
    }
}

// An versions service with a supported set of APIs
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiVersionsService<E> {
    supported: Vec<i16>,
    error: PhantomData<E>,
}

impl<State, E> Service<State, ApiVersionsRequest> for ApiVersionsService<E>
where
    State: Clone + Send + Sync + 'static,
    E: error::Error + Send + Sync + 'static,
{
    type Response = ApiVersionsResponse;
    type Error = E;

    async fn serve(
        &self,
        _ctx: Context<State>,
        _req: ApiVersionsRequest,
    ) -> Result<Self::Response, Self::Error> {
        Ok::<_, E>(
            ApiVersionsResponse::default()
                .finalized_features(Some([].into()))
                .finalized_features_epoch(Some(-1))
                .supported_features(Some([].into()))
                .zk_migration_ready(Some(false))
                .error_code(ErrorCode::None.into())
                .api_keys(Some(
                    RootMessageMeta::messages()
                        .requests()
                        .iter()
                        .filter(|(api_key, _)| self.supported.contains(api_key))
                        .map(|(_, meta)| {
                            ApiVersion::default()
                                .api_key(meta.api_key)
                                .min_version(meta.version.valid.start)
                                .max_version(meta.version.valid.end)
                        })
                        .collect(),
                ))
                .throttle_time_ms(Some(0)),
        )
    }
}

impl<State, E> Service<State, Body> for ApiVersionsService<E>
where
    State: Clone + Send + Sync + 'static,
    E: error::Error + From<tansu_sans_io::Error> + Send + Sync + 'static,
{
    type Response = Body;
    type Error = E;

    async fn serve(&self, ctx: Context<State>, req: Body) -> Result<Self::Response, Self::Error> {
        let req = ApiVersionsRequest::try_from(req)?;
        self.serve(ctx, req).await.map(Into::into)
    }
}

impl<State, E> Service<State, Frame> for ApiVersionsService<E>
where
    State: Clone + Send + Sync + 'static,
    E: error::Error + From<tansu_sans_io::Error> + Send + Sync + 'static,
{
    type Response = Frame;
    type Error = E;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let correlation_id = req.correlation_id()?;
        self.serve(ctx, req.body).await.map(|body| Frame {
            size: 0,
            header: Header::Response { correlation_id },
            body,
        })
    }
}

/// A layer that transforms Frames into Requests
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameRequestLayer<Q> {
    request: PhantomData<Q>,
}

impl<Q> FrameRequestLayer<Q> {
    pub fn new() -> Self {
        Self {
            request: PhantomData,
        }
    }
}

impl<S, Q> Layer<S> for FrameRequestLayer<Q> {
    type Service = FrameRequestService<S, Q>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            request: PhantomData,
        }
    }
}

/// Transform a Frame into a Request
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameRequestService<S, Q> {
    inner: S,
    request: PhantomData<Q>,
}

impl<S, Q, State> Service<State, Frame> for FrameRequestService<S, Q>
where
    S: Service<State, Q>,
    S::Response: Response,
    S::Error: From<tansu_sans_io::Error>,
    Q: Request + TryFrom<Body>,
    <Q as TryFrom<Body>>::Error: Into<S::Error>,
    State: Send + Sync + 'static,
{
    type Response = Frame;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        debug!(?req);
        let correlation_id = req.correlation_id()?;

        let req = Q::try_from(req.body).map_err(Into::into)?;

        self.inner.serve(ctx, req).await.map(|response| Frame {
            size: 0,
            header: Header::Response { correlation_id },
            body: response.into(),
        })
    }
}

impl<S, Q, State> Matcher<State, Frame> for FrameRequestService<S, Q>
where
    S: Clone + Send + Sync + 'static,
    Q: Request,
    State: Clone + Debug,
{
    fn matches(&self, ext: Option<&mut Extensions>, ctx: &Context<State>, req: &Frame) -> bool {
        debug!(?ext, ?ctx, ?req);
        req.api_key().is_ok_and(|api_key| api_key == Q::KEY)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesLayer;

impl<S> Layer<S> for BytesLayer {
    type Service = BytesService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesService<S> {
    inner: S,
}

impl<S, State> Service<State, Bytes> for BytesService<S>
where
    S: Service<State, Bytes, Response = Bytes>,
    State: Clone + Send + Sync + 'static,
{
    type Response = Bytes;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Bytes) -> Result<Self::Response, Self::Error> {
        debug!(?req);
        self.inner
            .serve(ctx, req)
            .await
            .inspect(|response| debug!(?response))
    }
}

/// A layer that transforms Bytes into Frames
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesFrameLayer;

impl<S> Layer<S> for BytesFrameLayer {
    type Service = BytesFrameService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A service transforming Bytes into Frames
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesFrameService<S> {
    inner: S,
}

impl<S, State> Service<State, Bytes> for BytesFrameService<S>
where
    S: Service<State, Frame, Response = Frame>,
    State: Clone + Send + Sync + 'static,
    S::Error: From<tansu_sans_io::Error> + Debug,
{
    type Response = Bytes;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Bytes) -> Result<Self::Response, Self::Error> {
        let req = Frame::request_from_bytes(req).inspect(|req| debug!(?req))?;
        let api_key = req.api_key()?;
        let api_version = req.api_version()?;
        let correlation_id = req.correlation_id()?;

        let span = span!(
            Level::DEBUG,
            "frame",
            api_name = req.api_name(),
            api_version,
            correlation_id
        );

        async move {
            let attributes = vec![
                KeyValue::new("api_key", api_key as i64),
                KeyValue::new("api_version", api_version as i64),
            ];

            self.inner
                .serve(ctx, req)
                .await
                .inspect(|response| debug!(?response))
                .and_then(|Frame { body, .. }| {
                    Frame::response(
                        Header::Response { correlation_id },
                        body,
                        api_key,
                        api_version,
                    )
                    .map_err(Into::into)
                })
                .inspect(|response| {
                    debug!(?response);
                    API_REQUESTS.add(1, &attributes);
                })
                .inspect_err(|err| {
                    error!(api_key, api_version, ?err);
                    API_ERRORS.add(1, &attributes);
                })
        }
        .instrument(span)
        .await
    }
}

/// A layer that transforms Frames into Bytes
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBytesLayer;

impl<S> Layer<S> for FrameBytesLayer {
    type Service = FrameBytesService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A service that transforms Frames into Bytes
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBytesService<S> {
    inner: S,
}

impl<S, State> Service<State, Frame> for FrameBytesService<S>
where
    S: Service<State, Bytes, Response = Bytes>,
    S::Error: From<tansu_sans_io::Error>,
    State: Send + Sync + 'static,
{
    type Response = Frame;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        debug!(?req);

        let api_key = req.api_key()?;
        let api_version = req.api_version()?;

        let req = Frame::request(req.header, req.body)?;

        self.inner
            .serve(ctx, req)
            .await
            .and_then(|response| {
                Frame::response_from_bytes(response, api_key, api_version).map_err(Into::into)
            })
            .inspect(|response| debug!(?response))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBodyLayer;

impl<S> Layer<S> for FrameBodyLayer {
    type Service = FrameBodyService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBodyService<S> {
    inner: S,
}

impl<S, State> Service<State, Frame> for FrameBodyService<S>
where
    S: Service<State, Body, Response = Body>,
    S::Error: From<tansu_sans_io::Error>,
    State: Send + Sync + 'static,
{
    type Response = Frame;

    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let correlation_id = req.correlation_id()?;

        self.inner.serve(ctx, req.body).await.map(|body| Frame {
            size: 0,
            header: Header::Response { correlation_id },
            body,
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BodyRequestLayer<Q> {
    request: PhantomData<Q>,
}

impl<Q> BodyRequestLayer<Q> {
    pub fn new() -> Self {
        Self {
            request: PhantomData,
        }
    }
}

impl<S, Q> Layer<S> for BodyRequestLayer<Q> {
    type Service = BodyRequestService<S, Q>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            request: PhantomData,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BodyRequestService<S, Q> {
    inner: S,
    request: PhantomData<Q>,
}

impl<S, Q> ApiKey for BodyRequestService<S, Q>
where
    Q: Request,
{
    const KEY: i16 = Q::KEY;
}

impl<S, State, Q> Service<State, Body> for BodyRequestService<S, Q>
where
    S: Service<State, Q>,
    Q: Request,
    S::Error: From<<Q as TryFrom<Body>>::Error> + From<<S as Service<State, Q>>::Error>,
    Body: From<<S as Service<State, Q>>::Response>,
    State: Send + Sync + 'static,
{
    type Response = Body;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Body) -> Result<Self::Response, Self::Error> {
        let req = Q::try_from(req)?;
        self.inner.serve(ctx, req).await.map(Body::from)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChannelFrameService<S> {
    inner: S,
}

type FrameReceiver = mpsc::Receiver<(Frame, oneshot::Sender<Frame>)>;

impl<S, State> Service<State, FrameReceiver> for ChannelFrameService<S>
where
    S: Service<State, Frame, Response = Frame, Error = Error>,
    State: Clone + Send + Sync + 'static,
{
    type Response = ();
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        mut req: FrameReceiver,
    ) -> Result<Self::Response, Self::Error> {
        while let Some((frame, tx)) = req.recv().await {
            self.inner
                .serve(ctx.clone(), frame)
                .await
                .and_then(|response| {
                    tx.send(response)
                        .map_err(|unsent| Error::UnableToSend(Box::new(unsent)))
                })?
        }

        Ok(())
    }
}

type FrameSender = mpsc::Sender<(Frame, oneshot::Sender<Frame>)>;

#[derive(Clone, Debug)]
pub struct FrameChannelService {
    tx: FrameSender,
}

impl<State> Service<State, Frame> for FrameChannelService
where
    State: Send + Sync + 'static,
{
    type Response = Frame;

    type Error = Error;

    async fn serve(&self, _ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx
            .send((req, resp_tx))
            .await
            .map_err(|send_error| Error::UnableToSend(Box::new(send_error.0.0)))?;

        resp_rx.await.map_err(Error::OneshotRecv)
    }
}

#[allow(dead_code)]
async fn pqr() {
    let (_tx, mut _rx) = mpsc::channel::<(Frame, oneshot::Sender<Frame>)>(100);
}

/// A layer that transforms Requests into Frames
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestFrameLayer;

impl<S> Layer<S> for RequestFrameLayer {
    type Service = RequestFrameService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A service that transforms Requests into Frames
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestFrameService<S> {
    inner: S,
}

impl<S, State, Q> Service<State, Q> for RequestFrameService<S>
where
    Q: Request,
    S: Service<State, Frame, Response = Frame>,
    S::Error: From<<<Q as Request>::Response as TryFrom<Body>>::Error>,
    State: Send + Sync + 'static,
{
    type Response = Q::Response;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        debug!(?req);

        let api_key = Q::KEY;
        let api_version = RootMessageMeta::messages()
            .requests()
            .get(&api_key)
            .map(|message_meta| message_meta.version.valid().end)
            .unwrap_or_default();
        let correlation_id = 0;
        let client_id = Some(env!("CARGO_CRATE_NAME").into());

        let req = Frame {
            size: 0,
            header: Header::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
            },
            body: req.into(),
        };

        self.inner
            .serve(ctx, req)
            .await
            .and_then(|response| Q::Response::try_from(response.body).map_err(Into::into))
            .inspect(|response| debug!(?response))
    }
}

impl<S, State, Q, E> From<RequestService<S, Q>> for BoxService<State, Body, Body, E>
where
    S: Service<State, Q, Error = E>,
    Q: Request,
    <S as Service<State, Q>>::Response: Response,
    E: From<<Q as TryFrom<Body>>::Error> + From<<S as Service<State, Q>>::Error>,
    Body: From<<S as Service<State, Q>>::Response>,
    State: Send + Sync + 'static,
{
    fn from(value: RequestService<S, Q>) -> Self {
        BodyRequestLayer::<Q>::new().into_layer(value).boxed()
    }
}

impl<S, State, Q, E> From<RequestService<S, Q>> for BoxService<State, Frame, Frame, E>
where
    S: Service<State, Q, Error = E>,
    Q: Request,
    <S as Service<State, Q>>::Response: Response,
    E: From<tansu_sans_io::Error>
        + From<<Q as TryFrom<Body>>::Error>
        + From<<S as Service<State, Q>>::Error>,
    Body: From<<S as Service<State, Q>>::Response>,
    State: Send + Sync + 'static,
{
    fn from(value: RequestService<S, Q>) -> Self {
        (FrameBodyLayer, BodyRequestLayer::<Q>::new())
            .into_layer(value)
            .boxed()
    }
}

impl<S, State, Q, E> From<BodyRequestService<S, Q>> for BoxService<State, Frame, Frame, E>
where
    S: Service<State, Q, Error = E>,
    Q: Request,
    E: From<tansu_sans_io::Error>
        + From<<Q as TryFrom<Body>>::Error>
        + From<<S as Service<State, Q>>::Error>,
    Body: From<<S as Service<State, Q>>::Response>,
    State: Send + Sync + 'static,
{
    fn from(value: BodyRequestService<S, Q>) -> Self {
        FrameBodyLayer.into_layer(value).boxed()
    }
}

#[derive(Clone, Copy, Debug, Hash)]
pub struct FrameService<F> {
    response: F,
}

impl<State, E, F> Service<State, Frame> for FrameService<F>
where
    F: Fn(Context<State>, Frame) -> Result<Frame, E> + Clone + Send + Sync + 'static,
    E: Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    type Response = Frame;
    type Error = E;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        (self.response)(ctx, req)
    }
}

impl<F> FrameService<F> {
    pub fn new<State, E>(response: F) -> Self
    where
        F: Fn(Context<State>, Frame) -> Result<Frame, E> + Clone,
        E: Send + Sync + 'static,
    {
        Self { response }
    }
}

#[derive(Clone, Copy, Hash)]
pub struct ResponseService<F> {
    response: F,
}

impl<F> Debug for ResponseService<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseService").finish()
    }
}

impl<State, Q, E, F> Service<State, Q> for ResponseService<F>
where
    F: Fn(Context<State>, Q) -> Result<Q::Response, E> + Clone + Send + Sync + 'static,
    Q: Request,
    E: Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    type Response = Q::Response;
    type Error = E;

    async fn serve(&self, ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        (self.response)(ctx, req)
    }
}

impl<F> ResponseService<F> {
    pub fn new<State, Q, E>(response: F) -> Self
    where
        F: Fn(Context<State>, Q) -> Result<Q::Response, E> + Clone,
        Q: Request,
        E: Send + Sync + 'static,
    {
        Self { response }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, sync::Arc, thread};

    use rama::layer::{HijackLayer, MapResponseLayer};
    use tansu_sans_io::{
        ApiKey, MetadataRequest, MetadataResponse, metadata_response::MetadataResponseBroker,
    };
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

    #[tokio::test]
    async fn simple_layers() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let cluster_id = "abc";

        type State = ();

        let service = (
            RequestFrameLayer,
            FrameBytesLayer,
            BytesLayer,
            BytesFrameLayer,
        )
            .into_layer(
                FrameRouteService::builder()
                    .with_service(RequestLayer::<MetadataRequest>::new().into_layer(
                        ResponseService::new(|_ctx: Context<State>, _req: MetadataRequest| {
                            Ok::<_, Error>(
                                MetadataResponse::default()
                                    .brokers(Some([].into()))
                                    .topics(Some([].into()))
                                    .cluster_id(Some(cluster_id.into()))
                                    .controller_id(Some(111))
                                    .throttle_time_ms(Some(0))
                                    .cluster_authorized_operations(Some(-1)),
                            )
                        }),
                    ))
                    .and_then(|builder| builder.build())?,
            );

        let ctx = Context::default();

        let request = MetadataRequest::default()
            .topics(Some([].into()))
            .allow_auto_topic_creation(Some(false))
            .include_cluster_authorized_operations(Some(false))
            .include_topic_authorized_operations(Some(false));

        let response = service.serve(ctx, request).await?;
        assert_eq!(Some(cluster_id.into()), response.cluster_id);

        Ok(())
    }

    #[tokio::test]
    async fn simple_routes() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let cluster_id = "abc";

        type State = ();

        let service = (
            RequestFrameLayer,
            FrameBytesLayer,
            BytesLayer,
            BytesFrameLayer,
        )
            .into_layer(
                FrameRouteService::builder()
                    .with_service(RequestLayer::<MetadataRequest>::new().into_layer(
                        ResponseService::new(|_ctx: Context<State>, _req: MetadataRequest| {
                            Ok::<_, Error>(
                                MetadataResponse::default()
                                    .brokers(Some([].into()))
                                    .topics(Some([].into()))
                                    .cluster_id(Some(cluster_id.into()))
                                    .controller_id(Some(111))
                                    .throttle_time_ms(Some(0))
                                    .cluster_authorized_operations(Some(-1)),
                            )
                        }),
                    ))
                    .and_then(|builder| builder.build())?,
            );

        let ctx = Context::default();

        {
            let client_software_name = "abcba";
            let client_software_version = "12321";

            let request = ApiVersionsRequest::default()
                .client_software_name(Some(client_software_name.into()))
                .client_software_version(Some(client_software_version.into()));

            let response = service.serve(ctx.clone(), request).await?;

            assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

            let api_versions = response
                .api_keys
                .unwrap_or_default()
                .into_iter()
                .map(|api_version| api_version.api_key)
                .collect::<Vec<_>>();

            assert_eq!(2, api_versions.len());
            assert!(api_versions.contains(&ApiVersionsRequest::KEY));
            assert!(api_versions.contains(&MetadataRequest::KEY));
        }

        let request = MetadataRequest::default()
            .topics(Some([].into()))
            .allow_auto_topic_creation(Some(false))
            .include_cluster_authorized_operations(Some(false))
            .include_topic_authorized_operations(Some(false));

        let response = service.serve(ctx, request).await?;
        assert_eq!(Some(cluster_id.into()), response.cluster_id);

        Ok(())
    }

    #[tokio::test]
    async fn route_request_map_response() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let cluster_id = "abc";

        let node_id = 12321;
        let host = "defgfed";
        let port = 32123;

        type State = ();

        let rl = (
            RequestLayer::<MetadataRequest>::new(),
            MapResponseLayer::new(move |response: MetadataResponse| {
                response.brokers(Some(vec![
                    MetadataResponseBroker::default()
                        .node_id(node_id)
                        .host(host.into())
                        .port(port)
                        .rack(None),
                ]))
            }),
        )
            .into_layer(ResponseService::new(
                |_ctx: Context<State>, _req: MetadataRequest| {
                    Ok::<_, Error>(
                        MetadataResponse::default()
                            .brokers(Some([].into()))
                            .topics(Some([].into()))
                            .cluster_id(Some(cluster_id.into()))
                            .controller_id(Some(111))
                            .throttle_time_ms(Some(0))
                            .cluster_authorized_operations(Some(-1)),
                    )
                },
            ));

        let service = (
            RequestFrameLayer,
            FrameBytesLayer,
            BytesLayer,
            BytesFrameLayer,
        )
            .into_layer(
                FrameRouteService::<(), Error>::builder()
                    .with_service(rl)
                    .and_then(|builder| builder.build())?,
            );

        let ctx = Context::default();

        {
            let client_software_name = "abcba";
            let client_software_version = "12321";

            let request = ApiVersionsRequest::default()
                .client_software_name(Some(client_software_name.into()))
                .client_software_version(Some(client_software_version.into()));

            let response = service.serve(ctx.clone(), request).await?;

            assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

            let api_versions = response
                .api_keys
                .unwrap_or_default()
                .into_iter()
                .map(|api_version| api_version.api_key)
                .collect::<Vec<_>>();

            assert_eq!(2, api_versions.len());
            assert!(api_versions.contains(&ApiVersionsRequest::KEY));
            assert!(api_versions.contains(&MetadataRequest::KEY));
        }

        let request = MetadataRequest::default()
            .topics(Some([].into()))
            .allow_auto_topic_creation(Some(false))
            .include_cluster_authorized_operations(Some(false))
            .include_topic_authorized_operations(Some(false));

        let response = service.serve(ctx, request).await?;
        assert_eq!(Some(cluster_id.into()), response.cluster_id);
        assert_eq!(Some(111), response.controller_id);

        let brokers = response.brokers.unwrap_or_default();
        assert_eq!(1, brokers.len());

        assert_eq!(node_id, brokers[0].node_id);
        assert_eq!(host, brokers[0].host);
        assert_eq!(port, brokers[0].port);

        Ok(())
    }

    #[tokio::test]
    async fn api_key_hijack() -> Result<(), Error> {
        let _guard = init_tracing()?;

        const CLUSTER_ID: &str = "abc";
        const NODE_ID: i32 = 111;
        const HOST: &str = "localhost";
        const PORT: i32 = 9092;

        let service = (
            FrameRequestLayer::<MetadataRequest>::new(),
            MapResponseLayer::new(move |response: MetadataResponse| {
                response.brokers(Some(vec![
                    MetadataResponseBroker::default()
                        .node_id(NODE_ID)
                        .host(HOST.into())
                        .port(PORT)
                        .rack(None),
                ]))
            }),
        )
            .into_layer(ResponseService::new(|_, _req: MetadataRequest| {
                Ok::<_, Error>(
                    MetadataResponse::default()
                        .brokers(Some([].into()))
                        .topics(Some([].into()))
                        .cluster_id(Some(CLUSTER_ID.into()))
                        .controller_id(Some(NODE_ID))
                        .throttle_time_ms(Some(0))
                        .cluster_authorized_operations(Some(-1)),
                )
            }));

        let hijack = HijackLayer::new(FrameApiKeyMatcher(MetadataRequest::KEY), service.clone())
            .into_layer(FrameService::new(|_, req: Frame| {
                debug!(?req);
                Err(Error::Message("unmapped".into()))
            }));

        let frame = hijack
            .serve(
                Context::default(),
                Frame {
                    header: Header::Request {
                        api_key: MetadataRequest::KEY,
                        api_version: 12,
                        correlation_id: 0,
                        client_id: Some("tansu".into()),
                    },
                    body: MetadataRequest::default().into(),
                    size: 0,
                },
            )
            .await?;

        let response: MetadataResponse = frame.body.try_into()?;
        assert_eq!(Some(111), response.controller_id);

        let brokers = response.brokers.unwrap_or_default();
        assert_eq!(1, brokers.len());
        assert_eq!(111, brokers[0].node_id);

        Ok(())
    }

    async fn server(cancellation: CancellationToken, listener: TcpListener) -> Result<(), Error> {
        let server = (
            TcpListenerLayer::new(cancellation),
            TcpContextLayer::default(),
            TcpBytesLayer::<()>::default(),
            BytesFrameLayer,
        )
            .into_layer(FrameService::new(|_, req: Frame| {
                debug!(?req);

                req.correlation_id()
                    .map(|correlation_id| Frame {
                        size: 0,
                        header: Header::Response { correlation_id },
                        body: MetadataResponse::default()
                            .brokers(Some([].into()))
                            .topics(Some([].into()))
                            .cluster_id(Some("abc".into()))
                            .controller_id(Some(111))
                            .throttle_time_ms(Some(0))
                            .cluster_authorized_operations(Some(-1))
                            .into(),
                    })
                    .map_err(Error::from)
            }));

        server.serve(Context::default(), listener).await
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

        let stream = TcpStream::connect(local_addr).await?;

        let client = FrameBytesLayer.into_layer(BytesTcpService);

        let frame = client
            .serve(
                Context::with_state(stream),
                Frame {
                    header: Header::Request {
                        api_key: MetadataRequest::KEY,
                        api_version: 12,
                        correlation_id: 0,
                        client_id: Some(env!("CARGO_PKG_NAME").into()),
                    },
                    body: MetadataRequest::default()
                        .topics(Some([].into()))
                        .allow_auto_topic_creation(Some(false))
                        .include_cluster_authorized_operations(Some(false))
                        .include_topic_authorized_operations(Some(false))
                        .into(),
                    size: 0,
                },
            )
            .await?;

        let response = MetadataResponse::try_from(frame.body)?;
        assert_eq!(Some("abc"), response.cluster_id.as_deref());
        assert_eq!(Some(111), response.controller_id);

        cancellation.cancel();

        let joined = join.join_all().await;
        debug!(?joined);

        Ok(())
    }
}
