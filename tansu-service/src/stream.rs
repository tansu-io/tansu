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

use std::{error, fmt::Debug, io, marker::PhantomData, time::SystemTime};

use bytes::Bytes;
use opentelemetry::KeyValue;
use rama::{Context, Layer, Service};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument as _, Level, debug, error, span};

use crate::{Error, REQUEST_DURATION, REQUEST_SIZE, RESPONSE_SIZE, frame_length};

/// A [`Layer`] that listens for TCP connections
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

/// A [`Service`] that listens for TCP connections
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

/// A [context state][`Context#method.state`] state used by [`TcpContextLayer`] and [`TcpContextService`]
#[non_exhaustive]
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

/// A [`Layer`] that injects the [`TcpContext`] into the service [`Context`] state
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

/// A [`Service`] that requires the [`TcpContext`] as the service [`Context`] state
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

/// A [`Service`] writing [`Bytes`] into a [`TcpStream`], responding with a length delimited frame of [`Bytes`]
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

/// A [`Layer`] receiving [`Bytes`] from a [`TcpStream`]
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

/// A [`Service`] receiving [`Bytes`] from a [`TcpStream`], calling an inner [`Service`] and sending [`Bytes`] into the [`TcpStream`]
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

/// A [`Layer`] that handles and responds with [`Bytes`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesLayer;

impl<S> Layer<S> for BytesLayer {
    type Service = BytesService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A [`Service`] that handles and responds with [`Bytes`]
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
