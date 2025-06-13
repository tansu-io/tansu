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
    metadata_request::MetadataRequestTopic,
    metadata_response::{MetadataResponseBroker, MetadataResponseTopic},
};

use crate::{
    Error,
    api::{ApiKey, ApiRequest, ApiResponse, ApiVersion, ClientId, CorrelationId},
};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MetadataRequest {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,
    pub client_id: ClientId,

    pub topics: Option<Vec<MetadataRequestTopic>>,
    pub allow_auto_topic_creation: Option<bool>,
    pub include_cluster_authorized_operations: Option<bool>,
    pub include_topic_authorized_operations: Option<bool>,
}

impl TryFrom<ApiRequest> for MetadataRequest {
    type Error = Error;

    fn try_from(api_request: ApiRequest) -> Result<Self, Self::Error> {
        if let ApiRequest {
            api_key,
            api_version,
            correlation_id,
            client_id,
            body:
                Body::MetadataRequest {
                    topics,
                    allow_auto_topic_creation,
                    include_cluster_authorized_operations,
                    include_topic_authorized_operations,
                },
        } = api_request
        {
            Ok(Self {
                api_key,
                api_version,
                correlation_id,
                client_id,
                topics,
                allow_auto_topic_creation,
                include_cluster_authorized_operations,
                include_topic_authorized_operations,
            })
        } else {
            Err(Error::UnexpectedApiRequest(Box::new(api_request)))
        }
    }
}

impl From<MetadataRequest> for ApiRequest {
    fn from(metadata: MetadataRequest) -> Self {
        Self {
            api_key: metadata.api_key,
            api_version: metadata.api_version,
            correlation_id: metadata.correlation_id,
            client_id: metadata.client_id,
            body: Body::MetadataRequest {
                topics: metadata.topics,
                allow_auto_topic_creation: metadata.allow_auto_topic_creation,
                include_cluster_authorized_operations: metadata
                    .include_cluster_authorized_operations,
                include_topic_authorized_operations: metadata.include_topic_authorized_operations,
            },
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MetadataResponse {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,

    pub throttle_time_ms: Option<i32>,
    pub brokers: Option<Vec<MetadataResponseBroker>>,
    pub cluster_id: Option<String>,
    pub controller_id: Option<i32>,
    pub topics: Option<Vec<MetadataResponseTopic>>,
    pub cluster_authorized_operations: Option<i32>,
}

impl From<MetadataResponse> for ApiResponse {
    fn from(metadata: MetadataResponse) -> Self {
        Self {
            api_key: metadata.api_key,
            api_version: metadata.api_version,
            correlation_id: metadata.correlation_id,
            body: Body::MetadataResponse {
                throttle_time_ms: metadata.throttle_time_ms,
                brokers: metadata.brokers,
                cluster_id: metadata.cluster_id,
                controller_id: metadata.controller_id,
                topics: metadata.topics,
                cluster_authorized_operations: metadata.cluster_authorized_operations,
            },
        }
    }
}

impl TryFrom<ApiResponse> for MetadataResponse {
    type Error = Error;

    fn try_from(api_response: ApiResponse) -> Result<Self, Self::Error> {
        if let ApiResponse {
            api_key,
            api_version,
            correlation_id,
            body:
                Body::MetadataResponse {
                    throttle_time_ms,
                    brokers,
                    cluster_id,
                    controller_id,
                    topics,
                    cluster_authorized_operations,
                },
        } = api_response
        {
            Ok(Self {
                api_key,
                api_version,
                correlation_id,
                throttle_time_ms,
                brokers,
                cluster_id,
                controller_id,
                topics,
                cluster_authorized_operations,
            })
        } else {
            Err(Error::UnexpectedApiResponse(Box::new(api_response)))
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MetadataService<S> {
    inner: S,
}

impl<S, State> Service<State, ApiRequest> for MetadataService<S>
where
    S: Service<State, MetadataRequest, Response = MetadataResponse>,
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
        let metadata_request = MetadataRequest::try_from(req)?;

        self.inner
            .serve(ctx, metadata_request)
            .await
            .map(MetadataResponse::into)
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MetadataLayer;

impl<S> Layer<S> for MetadataLayer {
    type Service = MetadataService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetadataService { inner }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MetadataIntoApiService<S> {
    inner: S,
}

impl<S, State> Service<State, MetadataRequest> for MetadataIntoApiService<S>
where
    S: Service<State, ApiRequest, Response = ApiResponse>,
    S::Error: From<Error> + Send + Debug + 'static,
    State: Send + Sync + 'static,
{
    type Response = MetadataResponse;
    type Error = S::Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        req: MetadataRequest,
    ) -> Result<Self::Response, Self::Error> {
        self.inner
            .serve(ctx, ApiRequest::from(req))
            .await
            .and_then(|response| TryInto::try_into(response).map_err(Into::into))
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MetadataIntoApiLayer;

impl<S> Layer<S> for MetadataIntoApiLayer {
    type Service = MetadataIntoApiService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetadataIntoApiService { inner }
    }
}
