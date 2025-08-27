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
    fmt, io,
    net::SocketAddr,
    sync::{Arc, LazyLock},
    time::SystemTime,
};

use bytes::Bytes;
use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    metrics::{Counter, Histogram, Meter},
};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use tansu_sans_io::{Body, Frame};
use tokio::{
    io::AsyncReadExt as _,
    net::{TcpStream, lookup_host},
    sync::oneshot,
};
use tracing::debug;
use url::Url;

mod api;
mod channel;
mod frame;
mod stream;

pub use api::{ApiVersionsService, FrameRouteService};

pub use channel::{
    ChannelFrameLayer, ChannelFrameService, FrameChannelService, FrameReceiver, FrameSender,
};

pub use frame::{
    BodyRequestLayer, BytesFrameLayer, FrameApiKeyMatcher, FrameBodyLayer, FrameBytesLayer,
    FrameBytesService, FrameRequestLayer, FrameService, RequestApiKeyMatcher, RequestFrameLayer,
    RequestLayer, ResponseService,
};

pub use stream::{BytesLayer, BytesTcpService, TcpBytesLayer, TcpContextLayer, TcpListenerLayer};

#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    DuplicateRoute(i16),
    FrameTooBig(usize),
    Io(Arc<io::Error>),
    Message(String),
    OneshotRecv(oneshot::error::RecvError),
    Protocol(#[from] tansu_sans_io::Error),
    UnableToSend(Box<Frame>),
    UnknownHost(Url),
    UnknownServiceBody(Box<Body>),
    UnknownServiceFrame(Box<Frame>),
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

fn frame_length(encoded: [u8; 4]) -> usize {
    i32::from_be_bytes(encoded) as usize + encoded.len()
}

pub async fn read_frame(stream: &mut TcpStream) -> Result<Bytes, Error> {
    let mut size = [0u8; 4];
    _ = stream.read_exact(&mut size).await?;

    let mut buffer: Vec<u8> = vec![0u8; frame_length(size)];
    buffer[0..size.len()].copy_from_slice(&size[..]);
    _ = stream.read_exact(&mut buffer[4..]).await?;
    Ok(Bytes::from(buffer))
}

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

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

pub(crate) static DNS_LOOKUP_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("dns_lookup_duration")
        .with_unit("ms")
        .with_description("DNS lookup latencies")
        .build()
});

pub(crate) static REQUEST_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tansu_request_size")
        .with_unit("By")
        .with_description("The API request size in bytes")
        .build()
});

pub(crate) static RESPONSE_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tansu_response_size")
        .with_unit("By")
        .with_description("The API response size in bytes")
        .build()
});

pub(crate) static REQUEST_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tansu_request_duration")
        .with_unit("ms")
        .with_description("The API request latencies in milliseconds")
        .build()
});

pub(crate) static API_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_api_requests")
        .with_description("The number of API requests made")
        .build()
});

pub(crate) static API_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_api_errors")
        .with_description("The number of API errors")
        .build()
});
