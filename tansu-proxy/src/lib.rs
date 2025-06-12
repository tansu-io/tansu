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

use rama::{
    Layer,
    error::OpaqueError,
    layer::{HijackLayer, MapResponseLayer},
    tcp::server::TcpListener,
};
use std::{
    error,
    fmt::{self, Debug},
    io::{self},
    result,
    sync::{Arc, PoisonError},
    time::Duration,
};
use tansu_kafka_sans_io::{Body, ErrorCode, Frame, metadata_response::MetadataResponseBroker};
use thiserror::Error;
use tokio::task::{JoinError, JoinSet};
use tracing::debug;
use tracing_subscriber::filter::ParseError;
use url::Url;

use crate::{
    api::{
        ApiKey, ApiRequest, ApiResponse,
        metadata::{MetadataIntoApiLayer, MetadataLayer, MetadataResponse},
        produce::{ProduceIntoApiLayer, ProduceLayer},
    },
    batch::BatchProduceLayer,
    service::{ApiClient, ApiRequestLayer, ByteLayer, TcpStreamLayer},
};

mod api;
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
    UnexpectedApiRequest(Box<ApiRequest>),
    UnexpectedApiResponse(Box<ApiResponse>),
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

fn host_port(url: &Url) -> String {
    format!("{}:{}", url.host_str().unwrap(), url.port().unwrap())
}

impl Proxy {
    const PRODUCE_API_KEY: ApiKey = ApiKey(0);
    const METADATA_API_KEY: ApiKey = ApiKey(3);

    const NODE_ID: i32 = 111;

    pub fn new(listener: Url, origin: Url) -> Self {
        Self { listener, origin }
    }

    pub async fn listen(&self) -> Result<()> {
        debug!(%self.listener);

        let listener = TcpListener::bind(host_port(&self.listener)).await?;

        let origin = host_port(&self.origin).parse().map(ApiClient::new)?;

        let host = String::from(self.listener.host_str().unwrap_or("localhost"));
        let port = i32::from(self.listener.port().unwrap_or(9092));

        let meta = HijackLayer::new(
            Self::METADATA_API_KEY,
            (
                MetadataLayer,
                MapResponseLayer::new(move |response: MetadataResponse| MetadataResponse {
                    brokers: Some(vec![MetadataResponseBroker {
                        node_id: Self::NODE_ID,
                        host,
                        port,
                        rack: None,
                    }]),
                    ..response
                }),
                MetadataIntoApiLayer,
            )
                .into_layer(origin.clone()),
        );

        let produce = HijackLayer::new(
            Self::PRODUCE_API_KEY,
            (
                ProduceLayer,
                BatchProduceLayer::new(Duration::from_millis(5_000)),
                ProduceIntoApiLayer,
            )
                .into_layer(origin.clone()),
        );

        let stack = (TcpStreamLayer, ByteLayer, ApiRequestLayer, meta, produce).into_layer(origin);

        listener.serve(stack).await;

        Ok(())
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
