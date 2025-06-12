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

use rama::{Context, Layer, Service};
use std::fmt::Debug;
use tansu_kafka_sans_io::{
    Body,
    produce_request::TopicProduceData,
    produce_response::{NodeEndpoint, TopicProduceResponse},
};

use crate::{
    Error,
    api::{ApiKey, ApiRequest, ApiResponse, ApiVersion, ClientId, CorrelationId},
};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProduceRequest {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,
    pub client_id: ClientId,

    pub transactional_id: Option<String>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topic_data: Option<Vec<TopicProduceData>>,
}

impl TryFrom<ApiRequest> for ProduceRequest {
    type Error = Error;

    fn try_from(api_request: ApiRequest) -> Result<Self, Self::Error> {
        if let ApiRequest {
            api_key,
            api_version,
            correlation_id,
            client_id,
            body:
                Body::ProduceRequest {
                    transactional_id,
                    acks,
                    timeout_ms,
                    topic_data,
                },
        } = api_request
        {
            Ok(ProduceRequest {
                api_key,
                api_version,
                correlation_id,
                client_id,

                transactional_id,
                acks,
                timeout_ms,
                topic_data,
            })
        } else {
            Err(Error::UnexpectedApiRequest(Box::new(api_request)))
        }
    }
}

impl From<ProduceRequest> for ApiRequest {
    fn from(produce: ProduceRequest) -> Self {
        Self {
            api_key: produce.api_key,
            api_version: produce.api_version,
            correlation_id: produce.correlation_id,
            client_id: produce.client_id,
            body: Body::ProduceRequest {
                transactional_id: produce.transactional_id,
                acks: produce.acks,
                timeout_ms: produce.timeout_ms,
                topic_data: produce.topic_data,
            },
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProduceResponse {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,

    pub responses: Option<Vec<TopicProduceResponse>>,
    pub throttle_time_ms: Option<i32>,
    pub node_endpoints: Option<Vec<NodeEndpoint>>,
}

impl From<ProduceResponse> for ApiResponse {
    fn from(produce: ProduceResponse) -> Self {
        Self {
            api_key: produce.api_key,
            api_version: produce.api_version,
            correlation_id: produce.correlation_id,

            body: Body::ProduceResponse {
                responses: produce.responses,
                throttle_time_ms: produce.throttle_time_ms,
                node_endpoints: produce.node_endpoints,
            },
        }
    }
}

impl TryFrom<ApiResponse> for ProduceResponse {
    type Error = Error;

    fn try_from(api_response: ApiResponse) -> Result<Self, Self::Error> {
        if let ApiResponse {
            api_key,
            api_version,
            correlation_id,
            body:
                Body::ProduceResponse {
                    responses,
                    throttle_time_ms,
                    node_endpoints,
                },
        } = api_response
        {
            Ok(Self {
                api_key,
                api_version,
                correlation_id,
                responses,
                throttle_time_ms,
                node_endpoints,
            })
        } else {
            Err(Error::UnexpectedApiResponse(Box::new(api_response)))
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProduceService<S> {
    inner: S,
}

impl<S, State> Service<State, ApiRequest> for ProduceService<S>
where
    S: Service<State, ProduceRequest, Response = ProduceResponse>,
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
        let produce_request = ProduceRequest::try_from(req)?;

        self.inner
            .serve(ctx, produce_request)
            .await
            .map(ProduceResponse::into)
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProduceLayer;

impl<S> Layer<S> for ProduceLayer {
    type Service = ProduceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ProduceService { inner }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProduceIntoApiService<S> {
    inner: S,
}

impl<S, State> Service<State, ProduceRequest> for ProduceIntoApiService<S>
where
    S: Service<State, ApiRequest, Response = ApiResponse>,
    S::Error: From<Error> + Send + Debug + 'static,
    State: Send + Sync + 'static,
{
    type Response = ProduceResponse;
    type Error = S::Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        req: ProduceRequest,
    ) -> Result<Self::Response, Self::Error> {
        self.inner
            .serve(ctx, ApiRequest::from(req))
            .await
            .and_then(|response| TryInto::try_into(response).map_err(Into::into))
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProduceIntoApiLayer;

impl<S> Layer<S> for ProduceIntoApiLayer {
    type Service = ProduceIntoApiService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ProduceIntoApiService { inner }
    }
}
