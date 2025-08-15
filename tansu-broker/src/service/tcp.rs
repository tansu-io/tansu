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

use std::{fmt::Debug, marker::PhantomData, net::SocketAddr, sync::LazyLock, time::SystemTime};

use bytes::Bytes;
use opentelemetry::{KeyValue, metrics::Histogram};
use rama::{Context, Service};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
};
use tracing::{Instrument as _, Level, debug, error, span};

use crate::{Error, METER, Result};

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ServiceBuilder<C, S> {
    inner: S,
    maximum_frame_size: Option<usize>,
    cluster_id: C,
}

impl<C, S> ServiceBuilder<C, S> {
    pub fn maximum_frame_size(self, maximum_frame_size: Option<usize>) -> Self {
        Self {
            maximum_frame_size,
            ..self
        }
    }

    pub fn cluster_id(self, cluster_id: impl Into<String>) -> ServiceBuilder<String, S> {
        ServiceBuilder {
            inner: self.inner,
            maximum_frame_size: self.maximum_frame_size,
            cluster_id: cluster_id.into(),
        }
    }

    pub fn inner<I, State>(self, inner: I) -> ServiceBuilder<C, I>
    where
        I: Service<State, Bytes, Response = Bytes>,
        State: Clone + Send + Sync + 'static,
    {
        ServiceBuilder {
            inner,
            maximum_frame_size: self.maximum_frame_size,
            cluster_id: self.cluster_id,
        }
    }
}

impl<S> ServiceBuilder<String, S>
where
    S: Service<(), Bytes, Response = Bytes>,
{
    pub fn build(self) -> Result<TcpService<S>> {
        Ok(TcpService {
            inner: self.inner,
            maximum_frame_size: self.maximum_frame_size,
            cluster_id: self.cluster_id,
        })
    }
}

impl<S> From<ServiceBuilder<String, S>> for TcpService<S>
where
    S: Service<(), Bytes, Response = Bytes>,
{
    fn from(builder: ServiceBuilder<String, S>) -> Self {
        Self {
            inner: builder.inner,
            maximum_frame_size: builder.maximum_frame_size,
            cluster_id: builder.cluster_id,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpService<S> {
    inner: S,
    maximum_frame_size: Option<usize>,
    cluster_id: String,
}

impl<S> TcpService<S> {
    pub fn new(inner: S, maximum_frame_size: Option<usize>, cluster_id: String) -> Self {
        Self {
            inner,
            maximum_frame_size,
            cluster_id,
        }
    }
}

impl<S> TcpService<S> {
    pub fn builder() -> ServiceBuilder<PhantomData<String>, PhantomData<S>> {
        ServiceBuilder::default()
    }
}

#[derive(Debug)]
pub struct TcpRequest {
    stream: TcpStream,
    peer: SocketAddr,
}

impl From<(TcpStream, SocketAddr)> for TcpRequest {
    fn from(value: (TcpStream, SocketAddr)) -> Self {
        Self {
            stream: value.0,
            peer: value.1,
        }
    }
}

impl<S> Service<(), TcpRequest> for TcpService<S>
where
    S: Service<(), Bytes, Response = Bytes>,
    S::Error: Into<Error> + Debug,
{
    type Response = ();
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<()>,
        mut req: TcpRequest,
    ) -> Result<Self::Response, Self::Error> {
        let span = span!(
            Level::DEBUG,
            "tcp",
            addr = %req.peer,
        );

        let mut size = [0u8; 4];

        async move {
            loop {
                _ = req.stream.read_exact(&mut size).await?;
                debug!(frame_length = frame_length(size));

                let mut request: Vec<u8> = vec![0u8; frame_length(size)];
                request[0..size.len()].copy_from_slice(&size[..]);
                _ = req.stream.read_exact(&mut request[4..]).await?;

                let attributes = [KeyValue::new("cluster_id", self.cluster_id.clone())];
                REQUEST_SIZE.record(request.len() as u64, &attributes);

                let ctx = ctx.clone();
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
                    })
                    .map_err(Into::into)?;

                req.stream.write_all(&response).await?
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

fn frame_length(encoded: [u8; 4]) -> usize {
    i32::from_be_bytes(encoded) as usize + encoded.len()
}
