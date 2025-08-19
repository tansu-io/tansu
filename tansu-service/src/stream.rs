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

use std::{fmt, io, sync::LazyLock, time::SystemTime};

use bytes::Bytes;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use rama::{Context, Layer, Service};
use tansu_sans_io::{Body, Frame, Header, Request};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
};
use tracing::{Instrument as _, Level, debug, error, span};

use crate::{METER, frame_length};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    Io(io::Error),
    Message(String),
    Protocol(#[from] tansu_sans_io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

trait ServiceState: Send + Sync + 'static {}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestService<S> {
    inner: S,
}

impl<State, S, Q> Service<State, Q> for RequestService<S>
where
    Q: Request,
    S: Service<State, Q>,
    State: ServiceState,
{
    type Response = S::Response;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        self.inner.serve(ctx, req).await
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestLayer;

impl<S> Layer<S> for RequestLayer {
    type Service = RequestService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpContextService<S> {
    inner: S,
    state: TcpContextState,
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpContextState {
    cluster_id: Option<String>,
    maximum_frame_size: Option<usize>,
}

impl<State, S> Service<State, TcpStream> for TcpContextService<S>
where
    S: Service<TcpContextState, TcpStream>,
    State: ServiceState,
{
    type Response = S::Response;
    type Error = S::Error;

    async fn serve(
        &self,
        _ctx: Context<State>,
        req: TcpStream,
    ) -> Result<Self::Response, Self::Error> {
        let ctx = Context::with_state(self.state.clone());
        self.inner.serve(ctx, req).await
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpContextLayer {
    state: TcpContextState,
}

impl TcpContextLayer {
    pub fn new(state: TcpContextState) -> Self {
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
pub struct TcpService<S> {
    inner: S,
}

impl<S> Service<TcpContextState, TcpStream> for TcpService<S>
where
    S: Service<(), Bytes, Response = Bytes>,
    S::Error: From<io::Error> + fmt::Debug,
{
    type Response = ();

    type Error = S::Error;

    async fn serve(
        &self,
        ctx: Context<TcpContextState>,
        mut req: TcpStream,
    ) -> Result<Self::Response, Self::Error> {
        let peer = req.peer_addr()?;

        let span = span!(
            Level::DEBUG,
            "tcp",
            %peer,
        );

        let mut size = [0u8; 4];

        let state = ctx.state();

        async move {
            loop {
                _ = req.read_exact(&mut size).await?;
                debug!(frame_length = frame_length(size));

                let mut request: Vec<u8> = vec![0u8; frame_length(size)];
                request[0..size.len()].copy_from_slice(&size[..]);
                _ = req.read_exact(&mut request[4..]).await?;

                let attributes = [KeyValue::new(
                    "cluster_id",
                    state.cluster_id.clone().unwrap_or_default(),
                )];
                REQUEST_SIZE.record(request.len() as u64, &attributes);

                let ctx = Context::default();
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

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpLayer;

impl<S> Layer<S> for TcpLayer {
    type Service = TcpService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
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

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FramingService<S> {
    inner: S,
}

impl<S> FramingService<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, State> Service<State, Bytes> for FramingService<S>
where
    S: Service<State, Frame>,
    S::Response: Into<Body>,
    S::Error: From<tansu_sans_io::Error>,
    State: ServiceState,
{
    type Response = Bytes;
    type Error = S::Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        request: Bytes,
    ) -> Result<Self::Response, Self::Error> {
        let request = Frame::request_from_bytes(&request[..])?;

        let api_key = request.api_key()?;
        let api_version = request.api_version()?;
        let correlation_id = request.correlation_id()?;

        let span = span!(
            Level::DEBUG,
            "frame",
            api_name = request.api_name(),
            api_version,
            correlation_id
        );

        debug!(?request);

        async move {
            let body = self.inner.serve(ctx, request).await.map(Into::into)?;

            let attributes = vec![
                KeyValue::new("api_key", api_key as i64),
                KeyValue::new("api_version", api_version as i64),
            ];

            Frame::response(
                Header::Response { correlation_id },
                body,
                api_key,
                api_version,
            )
            .inspect(|response| {
                debug!(?response);
                API_REQUESTS.add(1, &attributes);
            })
            .inspect_err(|err| {
                error!(api_key, api_version, ?err);
                API_ERRORS.add(1, &attributes);
            })
            .map_err(Into::into)
        }
        .instrument(span)
        .await
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FramingLayer;

impl<S> Layer<S> for FramingLayer {
    type Service = FramingService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

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
