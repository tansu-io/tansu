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

pub mod group;

use crate::{
    CancelKind, Error, Result,
    coordinator::group::{Coordinator, administrator::Controller},
    otel,
    service::services,
};
use rama::{Context, Service};
use std::{
    io::ErrorKind,
    marker::PhantomData,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    str::FromStr,
    time::{Duration, SystemTime},
};
use tansu_sans_io::{ErrorCode, RootMessageMeta};
use tansu_schema::{Registry, lake::House};
use tansu_storage::{BrokerRegistrationRequest, Storage, StorageContainer};
use tokio::{
    net::TcpListener,
    signal::unix::{SignalKind, signal},
    task::JoinSet,
    time::{self, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Level, debug, error, span};
use url::Url;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct Broker<G, S> {
    node_id: i32,
    cluster_id: String,
    incarnation_id: Uuid,
    listener: Url,
    advertised_listener: Url,
    storage: S,
    groups: G,

    #[allow(dead_code)]
    otlp_endpoint_url: Option<Url>,

    cancellation: CancellationToken,
}

impl<G, S> Broker<G, S>
where
    G: Coordinator,
    S: Storage + Clone + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: i32,
        cluster_id: &str,
        listener: Url,
        advertised_listener: Url,
        storage: S,
        groups: G,
        incarnation_id: Uuid,
    ) -> Self {
        Self {
            node_id,
            cluster_id: cluster_id.to_owned(),
            incarnation_id,
            listener,
            advertised_listener,
            storage,
            groups,
            otlp_endpoint_url: None,

            cancellation: CancellationToken::new(),
        }
    }

    pub fn builder() -> PhantomBuilder {
        Builder::default()
    }

    pub async fn main(mut self) -> Result<ErrorCode> {
        {
            let root_meta = RootMessageMeta::messages();
            debug!(
                messages = root_meta
                    .requests()
                    .values()
                    .map(|meta| meta.name)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        let mut set = JoinSet::new();

        let mut interrupt_signal = signal(SignalKind::interrupt()).unwrap();
        debug!(?interrupt_signal);

        let mut terminate_signal = signal(SignalKind::terminate()).unwrap();
        debug!(?terminate_signal);

        let token = self.cancellation.clone();

        _ = set.spawn(async move {
            self.serve().await.inspect_err(|err| error!(?err)).unwrap();
        });

        let kind = tokio::select! {
            v = set.join_next() => {
                debug!(?v);
                None
            }

            interrupt = interrupt_signal.recv() => {
                debug!(?interrupt);
                Some(CancelKind::Interrupt)
            }

            terminate = terminate_signal.recv() => {
                debug!(?terminate);
                Some(CancelKind::Terminate)
            }
        };

        if let Some(kind) = kind {
            token.cancel();

            let cleanup = async {
                while !set.is_empty() {
                    debug!(len = set.len());

                    _ = set.join_next().await;
                }
            };

            let patience = sleep(Duration::from(kind));

            tokio::select! {
                v = cleanup => {
                    debug!(?v)
                }

                _ = patience => {
                    debug!(aborting = set.len());
                    set.abort_all();

                    while !set.is_empty() {
                        _ = set.join_next().await;
                    }
                }
            }
        }

        Ok(ErrorCode::None)
    }

    pub async fn serve(&mut self) -> Result<()> {
        self.register().await?;
        self.listen().await
    }

    pub async fn register(&mut self) -> Result<()> {
        self.storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: self.node_id,
                cluster_id: self.cluster_id.clone(),
                incarnation_id: self.incarnation_id,
                rack: None,
            })
            .await
            .map_err(Into::into)
    }

    pub async fn listen(&self) -> Result<()> {
        debug!(%self.listener, %self.advertised_listener);

        let listener = TcpListener::bind(self.listener.host().map_or_else(
            || {
                SocketAddr::from((
                    IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                    self.listener.port().unwrap_or(9092),
                ))
            },
            |host| {
                let port = self.listener.port().unwrap_or(9092);
                debug!(?host, port);

                match host {
                    url::Host::Domain(domain) => SocketAddr::from_str(&format!("{domain}:{port}"))
                        .unwrap_or(SocketAddr::from((IpAddr::V6(Ipv6Addr::UNSPECIFIED), port))),
                    url::Host::Ipv4(ipv4_addr) => SocketAddr::from((IpAddr::V4(ipv4_addr), port)),
                    url::Host::Ipv6(ipv6_addr) => SocketAddr::from((IpAddr::V6(ipv6_addr), port)),
                }
            },
        ))
        .await
        .inspect(|listener| debug!(listener = ?listener.local_addr().ok()))
        .inspect_err(|err| error!(?err, %self.advertised_listener))?;

        let mut interval = time::interval(Duration::from_millis(600_000));

        let mut set = JoinSet::new();

        let service = services(
            self.cluster_id.as_str(),
            self.groups.clone(),
            self.storage.clone(),
        )?;

        loop {
            tokio::select! {
                Ok((stream, _addr)) = listener.accept() => {
                    stream.set_nodelay(true)?;

                    let service = service.clone();

                    let handle = set.spawn(async move {
                            match service.serve(Context::default(), stream).await {
                                Err(Error::Io(ref io))
                                    if io.kind() == ErrorKind::UnexpectedEof
                                        || io.kind() == ErrorKind::BrokenPipe
                                        || io.kind() == ErrorKind::ConnectionReset => {}

                                Err(error) => {
                                    error!(?error);
                                },

                                Ok(response) => {
                                    debug!(?response)
                                }
                        }
                    });

                    debug!(?handle);

                    continue;
                }

                _ = interval.tick() => {
                    let storage = self.storage.clone();


                    let handle = set.spawn(async move {
                        let span = span!(Level::DEBUG, "maintenance");

                        async move {
                            _ = storage.maintain(SystemTime::now()).await.inspect(|maintain|debug!(?maintain)).inspect_err(|err|debug!(?err)).ok();

                        }.instrument(span).await

                    });

                    debug!(?handle);
                }

                v = set.join_next(), if !set.is_empty() => {
                    debug!(?v);
                }

                message = self.cancellation.cancelled() => {
                    debug!(?message);
                    break;
                }
            }
        }

        while !set.is_empty() {
            debug!(len = set.len());

            _ = set.join_next().await;
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct Builder<N, C, I, A, S, L> {
    node_id: N,
    cluster_id: C,
    incarnation_id: I,
    advertised_listener: A,
    storage: S,
    listener: L,
    otlp_endpoint_url: Option<Url>,
    schema_registry: Option<Registry>,
    lake_house: Option<House>,

    cancellation: CancellationToken,
}

type PhantomBuilder = Builder<
    PhantomData<i32>,
    PhantomData<String>,
    PhantomData<Uuid>,
    PhantomData<Url>,
    PhantomData<Url>,
    PhantomData<Url>,
>;

impl<N, C, I, A, S, L> Builder<N, C, I, A, S, L> {
    pub fn node_id(self, node_id: i32) -> Builder<i32, C, I, A, S, L> {
        Builder {
            node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,

            cancellation: self.cancellation,
        }
    }

    pub fn cluster_id(self, cluster_id: impl Into<String>) -> Builder<N, String, I, A, S, L> {
        Builder {
            node_id: self.node_id,
            cluster_id: cluster_id.into(),
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,

            cancellation: self.cancellation,
        }
    }

    pub fn incarnation_id(self, incarnation_id: impl Into<Uuid>) -> Builder<N, C, Uuid, A, S, L> {
        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: incarnation_id.into(),
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,

            cancellation: self.cancellation,
        }
    }

    pub fn advertised_listener(
        self,
        advertised_listener: impl Into<Url>,
    ) -> Builder<N, C, I, Url, S, L> {
        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: advertised_listener.into(),
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,

            cancellation: self.cancellation,
        }
    }

    pub fn storage(self, storage: Url) -> Builder<N, C, I, A, Url, L> {
        debug!(%storage);

        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,

            cancellation: self.cancellation,
        }
    }

    pub fn listener(self, listener: Url) -> Builder<N, C, I, A, S, Url> {
        debug!(%listener);

        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,

            cancellation: self.cancellation,
        }
    }

    pub fn schema_registry(self, schema_registry: Option<Registry>) -> Self {
        Self {
            schema_registry,
            ..self
        }
    }

    pub fn lake_house(self, lake_house: Option<House>) -> Self {
        _ = lake_house
            .as_ref()
            .inspect(|lake_house| debug!(?lake_house));

        Self { lake_house, ..self }
    }

    pub fn otlp_endpoint_url(self, otlp_endpoint_url: Option<Url>) -> Self {
        Self {
            otlp_endpoint_url,
            ..self
        }
    }
}

impl Builder<i32, String, Uuid, Url, Url, Url> {
    pub async fn build(self) -> Result<Broker<Controller<StorageContainer>, StorageContainer>> {
        if let Some(otlp_endpoint_url) = self
            .otlp_endpoint_url
            .clone()
            .inspect(|otlp_endpoint_url| debug!(%otlp_endpoint_url))
        {
            otel::metric_exporter(otlp_endpoint_url)?;
        }

        let storage = StorageContainer::builder()
            .cluster_id(self.cluster_id.clone())
            .node_id(self.node_id)
            .advertised_listener(self.advertised_listener.clone())
            .schema_registry(self.schema_registry.clone())
            .lake_house(self.lake_house.clone())
            .storage(self.storage.clone())
            .cancellation(self.cancellation.clone())
            .build()
            .await?;

        let groups = Controller::with_storage(storage.clone())?;

        Ok(Broker {
            node_id: self.node_id,
            cluster_id: self.cluster_id.clone(),
            incarnation_id: self.incarnation_id,
            listener: self.listener,
            advertised_listener: self.advertised_listener,
            storage,
            groups,
            otlp_endpoint_url: self.otlp_endpoint_url,
            cancellation: self.cancellation,
        })
    }
}
