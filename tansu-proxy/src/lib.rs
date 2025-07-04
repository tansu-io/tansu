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

use opentelemetry::{InstrumentationScope, global, metrics::Meter};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use rama::{
    Layer,
    error::{BoxError, OpaqueError},
    layer::{HijackLayer, MapResponseLayer},
    tcp::server::TcpListener,
};
use std::{fmt::Debug, net::SocketAddr, ops::Deref, result, sync::LazyLock};
use tansu_otel::meter_provider;
use tansu_sans_io::{ErrorCode, metadata_response::MetadataResponseBroker};
use tansu_service::{
    api::{
        ApiKey, ApiKeyVersionLayer,
        describe_config::{ResourceConfig, ResourceConfigValueMatcher, TopicConfigLayer},
        metadata::{MetadataIntoApiLayer, MetadataLayer, MetadataResponse},
        produce::{self, ProduceIntoApiLayer, ProduceLayer},
    },
    service::{ApiClient, ApiRequestLayer, ByteLayer, TcpStreamLayer},
};
use tokio::{net::lookup_host, task::JoinSet};
use tracing::debug;
use url::Url;

use crate::batch::BatchProduceLayer;

mod batch;

pub type Result<T, E = BoxError> = result::Result<T, E>;

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

#[derive(Clone, Debug)]
pub struct Proxy {
    listener: Url,
    origin: Url,
}

async fn host_port(url: &Url) -> Result<SocketAddr> {
    if let Some(host) = url.host_str()
        && let Some(port) = url.port()
    {
        let mut addresses = lookup_host(format!("{host}:{port}"))
            .await?
            .filter(|socket_addr| matches!(socket_addr, SocketAddr::V4(_)));

        if let Some(socket_addr) = addresses.next().inspect(|socket_addr| debug!(?socket_addr)) {
            return Ok(socket_addr);
        }
    }

    Err(OpaqueError::from_display(format!("unknown host: {url}")).into_boxed())
}

impl Proxy {
    const METADATA_API_KEY: ApiKey = ApiKey(3);

    const NODE_ID: i32 = 111;

    pub fn new(listener: Url, origin: Url) -> Self {
        Self { listener, origin }
    }

    pub async fn listen(&self) -> Result<()> {
        debug!(%self.listener);

        let configuration = ResourceConfig::default();

        let listener = TcpListener::bind(host_port(&self.listener).await?).await?;

        let origin = host_port(&self.origin)
            .await
            .map(Into::into)
            .map(ApiClient::new)?;

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
            produce::API_KEY_VERSION.deref().0,
            (
                ApiKeyVersionLayer,
                ProduceLayer,
                TopicConfigLayer::new(configuration.clone(), origin.clone()),
                HijackLayer::new(
                    ResourceConfigValueMatcher::new(configuration.clone(), "tansu.batch", "true"),
                    (
                        BatchProduceLayer::new(configuration.clone()),
                        ProduceIntoApiLayer,
                    )
                        .into_layer(origin.clone()),
                ),
                ProduceIntoApiLayer,
            )
                .into_layer(origin.clone()),
        );

        let stack = (TcpStreamLayer, ByteLayer, ApiRequestLayer, meta, produce).into_layer(origin);

        listener.serve(stack).await;

        Ok(())
    }

    pub async fn main(
        listener_url: Url,
        origin_url: Url,
        otlp_endpoint_url: Option<Url>,
    ) -> Result<ErrorCode> {
        let mut set = JoinSet::new();

        let meter_provider = otlp_endpoint_url.map_or(Ok(None), |otlp_endpoint_url| {
            meter_provider(otlp_endpoint_url, env!("CARGO_PKG_NAME")).map(Some)
        })?;

        {
            let proxy = Proxy::new(listener_url, origin_url);
            _ = set.spawn(async move { proxy.listen().await.unwrap() });
        }

        loop {
            if set.join_next().await.is_none() {
                break;
            }
        }

        if let Some(meter_provider) = meter_provider {
            meter_provider
                .force_flush()
                .inspect(|force_flush| debug!(?force_flush))?;

            meter_provider
                .shutdown()
                .inspect(|shutdown| debug!(?shutdown))?;
        }

        Ok(ErrorCode::None)
    }
}
