// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use bytes::Bytes;
use rama::{Context, context::Extensions, error::OpaqueError, matcher::Matcher};
use std::{
    error,
    fmt::{self, Debug},
    io::{self, ErrorKind},
    result,
    sync::{Arc, PoisonError},
};
use tansu_kafka_sans_io::{
    Body, ErrorCode, Frame, Header,
    metadata_request::MetadataRequestTopic,
    metadata_response::{MetadataResponseBroker, MetadataResponseTopic},
    produce_request::TopicProduceData,
    produce_response::{NodeEndpoint, TopicProduceResponse},
};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinError, JoinSet},
};
use tracing::{Instrument, Level, debug, error, info, span};
use tracing_subscriber::filter::ParseError;
use url::Url;

mod batch;
mod service;

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Error, Debug)]
pub enum Error {
    Boxed(#[from] Box<dyn error::Error + Send + Sync>),
    Io(Arc<io::Error>),
    Join(#[from] JoinError),
    Message(String),
    Opaque(#[from] OpaqueError),
    ParseFilter(#[from] ParseError),
    Poison,
    Protocol(#[from] tansu_kafka_sans_io::Error),
    UnexpectedBody(Box<Body>),
    UnexpectedType(Box<Frame>),
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

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub struct Proxy {
    listener: Url,
    origin: Url,
}

impl Proxy {
    pub fn new(listener: Url, origin: Url) -> Self {
        Self { listener, origin }
    }

    pub async fn listen(&self) -> Result<()> {
        debug!(%self.listener);

        let listener = TcpListener::bind(format!(
            "{}:{}",
            self.listener.host_str().unwrap(),
            self.listener.port().unwrap()
        ))
        .await?;

        loop {
            let (stream, addr) = listener.accept().await?;

            let mut connection = Connection::open(&self.origin, stream).await?;

            _ = tokio::spawn(async move {
                let span = span!(Level::DEBUG, "peer", addr = %addr);

                async move {
                    match connection.stream_handler().await {
                        Err(ref error @ Error::Io(ref io))
                            if io.kind() == ErrorKind::UnexpectedEof =>
                        {
                            info!(?error);
                        }

                        Err(error) => {
                            error!(?error);
                        }

                        Ok(_) => {}
                    }
                }
                .instrument(span)
                .await
            });
        }
    }

    pub async fn main(listener_url: Url, origin_url: Url) -> Result<ErrorCode> {
        let mut set = JoinSet::new();

        {
            let proxy = Proxy::new(listener_url, origin_url);
            _ = set.spawn(async move { proxy.listen().await.unwrap() });
        }

        loop {
            if set.join_next().await.is_none() {
                break;
            }
        }

        Ok(ErrorCode::None)
    }
}

struct Connection {
    proxy: TcpStream,
    origin: TcpStream,
}

impl Connection {
    async fn open(origin: &Url, proxy: TcpStream) -> Result<Self> {
        debug!(%origin, ?proxy);

        TcpStream::connect(format!(
            "{}:{}",
            origin.host_str().unwrap(),
            origin.port().unwrap()
        ))
        .await
        .map(|origin| Self { proxy, origin })
        .map_err(Into::into)
    }

    fn frame_length(encoded: [u8; 4]) -> usize {
        i32::from_be_bytes(encoded) as usize + encoded.len()
    }

    async fn stream_handler(&mut self) -> Result<()> {
        let mut size = [0u8; 4];

        loop {
            _ = self.proxy.read_exact(&mut size).await?;

            let mut request_buffer: Vec<u8> = vec![0u8; Self::frame_length(size)];
            request_buffer[0..size.len()].copy_from_slice(&size[..]);
            _ = self.proxy.read_exact(&mut request_buffer[4..]).await?;

            debug!(?request_buffer);

            let Ok(
                request @ Frame {
                    header:
                        Header::Request {
                            api_key,
                            api_version,
                            ..
                        },
                    ..
                },
            ) = Frame::request_from_bytes(&request_buffer)
            else {
                continue;
            };

            debug!(?request);
            debug!(body = ?request.body);

            self.origin.write_all(&request_buffer).await?;

            _ = self.origin.read_exact(&mut size).await?;

            let mut response_buffer: Vec<u8> = vec![0u8; Self::frame_length(size)];
            response_buffer[0..size.len()].copy_from_slice(&size[..]);
            _ = self
                .origin
                .read_exact(&mut response_buffer[size.len()..])
                .await?;

            let response = Frame::response_from_bytes(&response_buffer, api_key, api_version)?;
            debug!(?response);
            debug!(body = ?response.body);

            debug!(?response_buffer);

            self.proxy.write_all(&response_buffer).await?;
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiKey(i16);

impl AsRef<i16> for ApiKey {
    fn as_ref(&self) -> &i16 {
        &self.0
    }
}

type ApiVersion = i16;
type CorrelationId = i32;
type ClientId = Option<String>;

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct ApiRequest {
    api_key: ApiKey,
    api_version: ApiVersion,
    correlation_id: CorrelationId,
    client_id: ClientId,
    body: Body,
}

impl<State> Matcher<State, ApiRequest> for ApiKey {
    fn matches(
        &self,
        ext: Option<&mut Extensions>,
        ctx: &Context<State>,
        req: &ApiRequest,
    ) -> bool {
        let _ = (ext, ctx);
        self.0 == req.api_key.0
    }
}

impl TryFrom<ApiRequest> for Bytes {
    type Error = Error;

    fn try_from(api_request: ApiRequest) -> Result<Self, Self::Error> {
        Frame::request(
            Header::Request {
                api_key: api_request.api_key.0,
                api_version: api_request.api_version,
                correlation_id: api_request.correlation_id,
                client_id: api_request.client_id,
            },
            api_request.body,
        )
        .map(Bytes::from)
        .map_err(Into::into)
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, PartialOrd)]
struct ApiResponse {
    api_key: ApiKey,
    api_version: ApiVersion,
    correlation_id: CorrelationId,
    body: Body,
}

impl TryFrom<ApiResponse> for Bytes {
    type Error = Error;

    fn try_from(api_response: ApiResponse) -> Result<Self, Self::Error> {
        Frame::response(
            Header::Response {
                correlation_id: api_response.correlation_id,
            },
            api_response.body,
            api_response.api_key.0,
            api_response.api_version,
        )
        .map(Bytes::from)
        .map_err(Into::into)
    }
}

fn frame_length(encoded: [u8; 4]) -> usize {
    i32::from_be_bytes(encoded) as usize + encoded.len()
}

pub async fn read_frame(tcp_stream: &mut TcpStream) -> Result<Bytes, Error> {
    let mut size = [0u8; 4];
    tcp_stream.read_exact(&mut size).await?;

    let mut buffer: Vec<u8> = vec![0u8; frame_length(size)];
    buffer[0..size.len()].copy_from_slice(&size[..]);
    _ = tcp_stream.read_exact(&mut buffer[4..]).await?;
    Ok(Bytes::from(buffer))
}

pub fn read_api_request(buffer: Bytes) -> Result<ApiRequest, Error> {
    match Frame::request_from_bytes(&buffer[..])? {
        Frame {
            header:
                Header::Request {
                    api_key,
                    api_version,
                    correlation_id,
                    client_id,
                },
            body,
            ..
        } => Ok(ApiRequest {
            api_key: ApiKey(api_key),
            api_version,
            correlation_id,
            client_id,
            body,
        }),

        frame => Err(Error::UnexpectedType(Box::new(frame))),
    }
}

fn read_api_response(
    buffer: Bytes,
    api_key: ApiKey,
    api_version: ApiVersion,
) -> Result<ApiResponse, Error> {
    match Frame::response_from_bytes(&buffer[..], api_key.0, api_version)? {
        Frame {
            header: Header::Response { correlation_id },
            body,
            ..
        } => Ok(ApiResponse {
            api_key,
            api_version,
            correlation_id,
            body,
        }),

        frame => Err(Error::UnexpectedType(Box::new(frame))),
    }
}

#[allow(dead_code)]
struct MetadataRequest {
    topics: Option<Vec<MetadataRequestTopic>>,
    allow_auto_topic_creation: Option<bool>,
    include_cluster_authorized_operations: Option<bool>,
    include_topic_authorized_operations: Option<bool>,
}

impl TryFrom<Body> for MetadataRequest {
    type Error = Error;

    fn try_from(body: Body) -> Result<Self, Self::Error> {
        if let Body::MetadataRequest {
            topics,
            allow_auto_topic_creation,
            include_cluster_authorized_operations,
            include_topic_authorized_operations,
        } = body
        {
            Ok(MetadataRequest {
                topics,
                allow_auto_topic_creation,
                include_cluster_authorized_operations,
                include_topic_authorized_operations,
            })
        } else {
            Err(Error::UnexpectedBody(Box::new(body)))
        }
    }
}

pub struct MetadataResponse {
    throttle_time_ms: Option<i32>,
    brokers: Option<Vec<MetadataResponseBroker>>,
    cluster_id: Option<String>,
    controller_id: Option<i32>,
    topics: Option<Vec<MetadataResponseTopic>>,
    cluster_authorized_operations: Option<i32>,
}

impl From<MetadataResponse> for Body {
    fn from(response: MetadataResponse) -> Self {
        Body::MetadataResponse {
            throttle_time_ms: response.throttle_time_ms,
            brokers: response.brokers,
            cluster_id: response.cluster_id,
            controller_id: response.controller_id,
            topics: response.topics,
            cluster_authorized_operations: response.cluster_authorized_operations,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProduceRequest {
    transactional_id: Option<String>,
    acks: i16,
    timeout_ms: i32,
    topic_data: Option<Vec<TopicProduceData>>,
}

impl TryFrom<Body> for ProduceRequest {
    type Error = Error;

    fn try_from(body: Body) -> Result<Self, Self::Error> {
        if let Body::ProduceRequest {
            transactional_id,
            acks,
            timeout_ms,
            topic_data,
        } = body
        {
            Ok(ProduceRequest {
                transactional_id,
                acks,
                timeout_ms,
                topic_data,
            })
        } else {
            Err(Error::UnexpectedBody(Box::new(body)))
        }
    }
}

impl From<ProduceRequest> for Body {
    fn from(request: ProduceRequest) -> Self {
        Body::ProduceRequest {
            transactional_id: request.transactional_id,
            acks: request.acks,
            timeout_ms: request.timeout_ms,
            topic_data: request.topic_data,
        }
    }
}

impl TryFrom<Frame> for ProduceRequest {
    type Error = Error;

    fn try_from(frame: Frame) -> Result<Self, Self::Error> {
        if let Frame {
            body:
                Body::ProduceRequest {
                    transactional_id,
                    acks,
                    timeout_ms,
                    topic_data,
                },
            ..
        } = frame
        {
            Ok(ProduceRequest {
                transactional_id,
                acks,
                timeout_ms,
                topic_data,
            })
        } else {
            Err(Error::UnexpectedType(Box::new(frame)))
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProduceResponse {
    responses: Option<Vec<TopicProduceResponse>>,
    throttle_time_ms: Option<i32>,
    node_endpoints: Option<Vec<NodeEndpoint>>,
}

impl From<ProduceResponse> for Body {
    fn from(response: ProduceResponse) -> Self {
        Body::ProduceResponse {
            responses: response.responses,
            throttle_time_ms: response.throttle_time_ms,
            node_endpoints: response.node_endpoints,
        }
    }
}

impl TryFrom<Frame> for ProduceResponse {
    type Error = Error;

    fn try_from(frame: Frame) -> Result<Self, Self::Error> {
        if let Frame {
            body:
                Body::ProduceResponse {
                    responses,
                    throttle_time_ms,
                    node_endpoints,
                },
            ..
        } = frame
        {
            Ok(ProduceResponse {
                responses,
                throttle_time_ms,
                node_endpoints,
            })
        } else {
            Err(Error::UnexpectedType(Box::new(frame)))
        }
    }
}
