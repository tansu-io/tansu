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

use rama::{Context, Layer, Service, error::BoxError};
use std::{fmt::Debug, sync::LazyLock};
use tansu_sans_io::{
    Body, MESSAGE_META,
    produce_request::TopicProduceData,
    produce_response::{NodeEndpoint, TopicProduceResponse},
    record::deflated::Batch,
};

use crate::{
    Result,
    api::{
        ApiKey, ApiRequest, ApiResponse, ApiVersion, ClientId, CorrelationId,
        UnexpectedApiBodyError,
    },
};

pub static API_KEY_VERSION: LazyLock<(ApiKey, ApiVersion)> = LazyLock::new(|| {
    MESSAGE_META
        .iter()
        .find(|(name, _)| *name == "ProduceRequest")
        .map_or((ApiKey(-1), 0), |(_, meta)| {
            (ApiKey(meta.api_key), meta.version.valid.end)
        })
});

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
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

impl Default for ProduceRequest {
    fn default() -> Self {
        Self {
            api_key: API_KEY_VERSION.0,
            api_version: API_KEY_VERSION.1,
            correlation_id: 0,
            client_id: Some("tansu".into()),
            transactional_id: None,
            acks: 0,
            timeout_ms: 5_000,
            topic_data: None,
        }
    }
}

impl ProduceRequest {
    pub fn topic_names(&self) -> Vec<String> {
        let mut topics = self.topic_data.as_ref().map_or(vec![], |topics| {
            topics
                .iter()
                .map(|topic| topic.name.clone())
                .collect::<Vec<_>>()
        });

        topics.sort();
        topics.dedup();
        topics
    }

    fn size_of(&self, unit: fn(&Batch) -> usize) -> usize {
        self.topic_data.as_ref().map_or(0, |topics| {
            topics
                .iter()
                .map(|topic| {
                    topic.partition_data.as_ref().map_or(0, |partitions| {
                        partitions
                            .iter()
                            .map(|partition| {
                                partition
                                    .records
                                    .as_ref()
                                    .map_or(0, |frame| frame.batches.iter().map(unit).sum())
                            })
                            .sum()
                    })
                })
                .sum()
        })
    }

    pub fn number_of_records(&self) -> usize {
        self.size_of(|batch| batch.record_count as usize)
    }

    pub fn number_of_bytes(&self) -> usize {
        self.size_of(|batch| batch.record_data.len())
    }
}

impl TryFrom<ApiRequest> for ProduceRequest {
    type Error = UnexpectedApiBodyError;

    fn try_from(api_request: ApiRequest) -> Result<Self, Self::Error> {
        if let ApiRequest {
            api_key,
            api_version,
            correlation_id,
            client_id,
            body:
                Body::ProduceRequest(tansu_sans_io::ProduceRequest {
                    transactional_id,
                    acks,
                    timeout_ms,
                    topic_data,
                    ..
                }),
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
            Err(UnexpectedApiBodyError::Request(Box::new(api_request)))
        }
    }
}

impl TryFrom<&ApiRequest> for ProduceRequest {
    type Error = UnexpectedApiBodyError;

    fn try_from(api_request: &ApiRequest) -> Result<Self, Self::Error> {
        Self::try_from(api_request.to_owned())
    }
}

impl From<ApiRequest> for Option<ProduceRequest> {
    fn from(api_request: ApiRequest) -> Self {
        ProduceRequest::try_from(api_request).ok()
    }
}

impl From<ProduceRequest> for ApiRequest {
    fn from(produce: ProduceRequest) -> Self {
        Self {
            api_key: produce.api_key,
            api_version: produce.api_version,
            correlation_id: produce.correlation_id,
            client_id: produce.client_id,
            body: Body::ProduceRequest(
                tansu_sans_io::ProduceRequest::default()
                    .transactional_id(produce.transactional_id)
                    .acks(produce.acks)
                    .timeout_ms(produce.timeout_ms)
                    .topic_data(produce.topic_data),
            ),
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

            body: Body::ProduceResponse(
                tansu_sans_io::ProduceResponse::default()
                    .responses(produce.responses)
                    .throttle_time_ms(produce.throttle_time_ms)
                    .node_endpoints(produce.node_endpoints),
            ),
        }
    }
}

impl TryFrom<ApiResponse> for ProduceResponse {
    type Error = UnexpectedApiBodyError;

    fn try_from(api_response: ApiResponse) -> Result<Self, Self::Error> {
        if let ApiResponse {
            api_key,
            api_version,
            correlation_id,
            body:
                Body::ProduceResponse(tansu_sans_io::ProduceResponse {
                    responses,
                    throttle_time_ms,
                    node_endpoints,
                    ..
                }),
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
            Err(UnexpectedApiBodyError::Response(Box::new(api_response)))
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
    S::Error: Into<BoxError> + Send + Debug + 'static,
    State: Send + Sync + 'static,
{
    type Response = ApiResponse;

    type Error = BoxError;

    async fn serve(
        &self,
        ctx: Context<State>,
        req: ApiRequest,
    ) -> Result<Self::Response, Self::Error> {
        let produce_request = ProduceRequest::try_from(req)?;

        self.inner
            .serve(ctx, produce_request)
            .await
            .map_err(Into::into)
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
    S::Error: Into<BoxError> + Send + Debug + 'static,
    State: Send + Sync + 'static,
{
    type Response = ProduceResponse;
    type Error = BoxError;

    async fn serve(
        &self,
        ctx: Context<State>,
        req: ProduceRequest,
    ) -> Result<Self::Response, Self::Error> {
        self.inner
            .serve(ctx, ApiRequest::from(req))
            .await
            .map_err(Into::into)
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
