// Copyright ⓒ 2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::{collections::BTreeMap, fmt::Debug, sync::Arc};

use crate::{
    Error,
    api::{ApiKey, ApiRequest, ApiResponse, read_api_request, read_api_response, read_frame},
};
use bytes::Bytes;
use rama::{
    Context, Layer, Service,
    net::{
        address::{Authority, Host},
        client::{ConnectorService, EstablishedClientConnection},
        stream::Stream,
    },
    service::BoxService,
    tcp::{TcpStream, client::default_tcp_connect},
};
use tokio::io::AsyncWriteExt;
use tracing::{Instrument, Level, debug, span};

#[allow(dead_code)]
async fn client() -> Result<(), Error> {
    let context = Context::default();

    let (mut stream, _address) =
        default_tcp_connect(&context, Authority::new(Host::LOCALHOST_IPV4, 9092)).await?;

    let (_reader, _writer) = stream.split();

    Ok(())
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpStreamService<S> {
    inner: S,
}

impl<S, State> Service<State, TcpStream> for TcpStreamService<S>
where
    S: Service<State, Bytes, Response = Bytes>,
    S::Error: From<Error> + Send + Debug + 'static,
    State: Clone + Send + Sync + 'static,
{
    type Response = ();
    type Error = S::Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        mut req: TcpStream,
    ) -> Result<Self::Response, Self::Error> {
        let peer = req.peer_addr().expect("peer");

        let span = span!(Level::DEBUG, "peer", addr = %peer);
        async move {
            loop {
                let buffer = read_frame(&mut req).await?;
                let buffer = self.inner.serve(ctx.clone(), buffer).await?;
                req.write_all(&buffer[..]).await.expect("write_all");
            }
        }
        .instrument(span)
        .await
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpStreamLayer;

impl<S> Layer<S> for TcpStreamLayer {
    type Service = TcpStreamService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TcpStreamService { inner }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ByteService<S> {
    inner: S,
}

impl<S, State> Service<State, Bytes> for ByteService<S>
where
    S: Service<State, ApiRequest, Response = ApiResponse>,
    S::Error: From<Error> + Send + Debug + 'static,
    State: Clone + Send + Sync + 'static,
{
    type Response = Bytes;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Bytes) -> Result<Self::Response, Self::Error> {
        let request = read_api_request(req).inspect(|api_request| debug!(?api_request))?;
        let response = self
            .inner
            .serve(ctx.clone(), request)
            .await
            .inspect(|api_response| debug!(?api_response))?;

        Bytes::try_from(response).map_err(Into::into)
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ByteLayer;

impl<S> Layer<S> for ByteLayer {
    type Service = ByteService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ByteService { inner }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiRequestService<S> {
    inner: S,
}

impl<S, State> Service<State, ApiRequest> for ApiRequestService<S>
where
    S: Service<State, ApiRequest, Response = ApiResponse>,
    S::Error: From<Error> + Send + Debug + 'static,
    State: Send + Sync + 'static,
{
    type Response = ApiResponse;
    type Error = S::Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        req: ApiRequest,
    ) -> Result<Self::Response, Self::Error> {
        self.inner.serve(ctx, req).await
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiRequestLayer;

impl<S> Layer<S> for ApiRequestLayer {
    type Service = ApiRequestService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ApiRequestService { inner }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct ApiRouteService<State, Request, Response, Error> {
    routes: Arc<BTreeMap<ApiKey, BoxService<State, Request, Response, Error>>>,
    otherwise: Arc<BoxService<State, Request, Response, Error>>,
}

impl<State> Service<State, ApiRequest> for ApiRouteService<State, ApiRequest, ApiResponse, Error>
where
    State: Send + Sync + 'static,
{
    type Response = ApiResponse;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        req: ApiRequest,
    ) -> Result<Self::Response, Self::Error> {
        if let Some(service) = self.routes.get(&req.api_key) {
            service.serve(ctx, req).await
        } else {
            self.otherwise.serve(ctx, req).await
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct ApiRouteLayer<State, Request, Response, Error> {
    routes: Arc<BTreeMap<ApiKey, BoxService<State, Request, Response, Error>>>,
    otherwise: Arc<BoxService<State, Request, Response, Error>>,
}

#[allow(dead_code)]
impl<State, Request, Response, Error> ApiRouteLayer<State, Request, Response, Error> {
    fn new(
        routes: impl Into<BTreeMap<ApiKey, BoxService<State, Request, Response, Error>>>,
        otherwise: impl Into<BoxService<State, Request, Response, Error>>,
    ) -> Self {
        Self {
            routes: Arc::new(routes.into()),
            otherwise: Arc::new(otherwise.into()),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiClient {
    authority: Authority,
}

impl ApiClient {
    pub fn new(authority: Authority) -> Self {
        Self { authority }
    }
}

impl<State> Service<State, ApiRequest> for ApiClient
where
    State: Clone + Send + Sync + 'static,
{
    type Response = ApiResponse;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        req: ApiRequest,
    ) -> Result<Self::Response, Self::Error> {
        let (mut stream, address) = default_tcp_connect(&ctx, self.authority.clone()).await?;

        debug!(
            ?address,
            api_key = req.api_key.0,
            api_version = req.api_version,
            correlation_id = req.correlation_id,
            body = ?req.body
        );

        let api_key = req.api_key;
        let api_version = req.api_version;
        let buffer = Bytes::try_from(req)?;
        stream.write_all(&buffer[..]).await?;
        let buffer = read_frame(&mut stream).await?;
        read_api_response(buffer, api_key, api_version)
            .inspect(|api_response| debug!(?api_response))
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TansuClientService<S> {
    inner: S,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TansuConnector<S> {
    inner: S,
}

impl<S, State> Service<State, ApiRequest> for TansuConnector<S>
where
    S: ConnectorService<State, ApiRequest, Connection: Stream + Unpin, Error: Into<Error>>,
    State: Clone + Send + Sync + 'static,
{
    type Response = EstablishedClientConnection<TansuClientService<ApiResponse>, State, ApiRequest>;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        req: ApiRequest,
    ) -> Result<Self::Response, Self::Error> {
        let EstablishedClientConnection { ctx, req, conn } =
            self.inner.connect(ctx, req).await.map_err(Into::into)?;

        let _ = (ctx, req, conn);

        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, sync::Arc, thread};

    use crate::api::produce::{ProduceRequest, ProduceResponse};

    use super::*;
    use rama::service::service_fn;
    use tansu_kafka_sans_io::{
        Body, ErrorCode, Frame, Header, MESSAGE_META,
        produce_request::{PartitionProduceData, TopicProduceData},
        produce_response::{LeaderIdAndEpoch, PartitionProduceResponse, TopicProduceResponse},
        record::deflated,
    };
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    const PRODUCE_REQUEST: &str = "ProduceRequest";

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
    async fn simple_proxy() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let (api_key, api_version) = MESSAGE_META
            .iter()
            .find(|(name, _)| *name == PRODUCE_REQUEST)
            .map(|(_, meta)| (meta.api_key, meta.version.valid.end))
            .unwrap();

        let correlation_id = 87678;
        let client_id = "abc";
        let partition_index = 43234;
        let topic_name = "pqr";

        let frame = Frame::request(
            Header::Request {
                api_key,
                api_version,
                correlation_id,
                client_id: Some(client_id.to_owned()),
            },
            Body::ProduceRequest {
                transactional_id: None,
                acks: 0,
                timeout_ms: 5_000,
                topic_data: Some(
                    [TopicProduceData {
                        name: topic_name.into(),
                        partition_data: Some(
                            [PartitionProduceData {
                                index: partition_index,
                                records: Some(deflated::Frame { batches: [].into() }),
                            }]
                            .into(),
                        ),
                    }]
                    .into(),
                ),
            },
        )
        .map(Bytes::from)
        .inspect(|frame| debug!(?frame))?;

        let service =
            (ByteLayer, ApiRequestLayer).into_layer(service_fn(async |req: ApiRequest| {
                ProduceRequest::try_from(req).map(|produce_request| ApiResponse {
                    api_key: produce_request.api_key,
                    api_version: produce_request.api_version,
                    correlation_id: produce_request.correlation_id,
                    body: Body::ProduceResponse {
                        responses: produce_request.topic_data.map(|topic_data| {
                            topic_data
                                .iter()
                                .map(|topic_produce_data| TopicProduceResponse {
                                    name: topic_produce_data.name.clone(),
                                    partition_responses: topic_produce_data
                                        .partition_data
                                        .as_ref()
                                        .map(|partition_produce_data| {
                                            partition_produce_data
                                                .iter()
                                                .map(|partition_produce| PartitionProduceResponse {
                                                    index: partition_produce.index,
                                                    error_code: ErrorCode::None.into(),
                                                    base_offset: 65456,
                                                    log_append_time_ms: Some(0),
                                                    log_start_offset: Some(0),
                                                    record_errors: Some([].into()),
                                                    error_message: Some("none".into()),
                                                    current_leader: Some(LeaderIdAndEpoch {
                                                        leader_id: 12321,
                                                        leader_epoch: 23432,
                                                    }),
                                                })
                                                .collect()
                                        }),
                                })
                                .collect()
                        }),
                        throttle_time_ms: Some(54345),
                        node_endpoints: Some([].into()),
                    },
                })
            }));

        let produce_response = service
            .serve(Context::default(), frame)
            .await
            .and_then(|response| read_api_response(response, ApiKey(api_key), api_version))
            .and_then(ProduceResponse::try_from)?;

        assert!(produce_response.responses.is_some());

        let topic_produce_responses = produce_response.responses.unwrap_or_default();
        assert_eq!(1, topic_produce_responses.len());
        assert_eq!(topic_name, topic_produce_responses[0].name);

        Ok(())
    }

    #[tokio::test]
    async fn simple_router() -> Result<(), Error> {
        let _guard = init_tracing()?;

        Ok(())
    }
}
