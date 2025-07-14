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

use std::sync::LazyLock;

use tansu_sans_io::{Body, MESSAGE_META, api_versions_response};

use crate::api::{
    ApiKey, ApiRequest, ApiResponse, ApiVersion, ClientId, CorrelationId, UnexpectedApiBodyError,
};

pub static API_KEY_VERSION: LazyLock<(ApiKey, ApiVersion)> = LazyLock::new(|| {
    MESSAGE_META
        .iter()
        .find(|(name, _)| *name == "ApiVersionsRequest")
        .map_or((ApiKey(-1), 0), |(_, meta)| {
            (ApiKey(meta.api_key), meta.version.valid.end)
        })
});

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiVersionRequest {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,
    pub client_id: ClientId,

    pub client_software_name: Option<String>,
    pub client_software_version: Option<String>,
}

impl TryFrom<ApiRequest> for ApiVersionRequest {
    type Error = UnexpectedApiBodyError;

    fn try_from(api_request: ApiRequest) -> Result<Self, Self::Error> {
        if let ApiRequest {
            api_key,
            api_version,
            correlation_id,
            client_id,
            body:
                Body::ApiVersionsRequest(tansu_sans_io::ApiVersionsRequest {
                    client_software_name,
                    client_software_version,
                    ..
                }),
        } = api_request
        {
            Ok(ApiVersionRequest {
                api_key,
                api_version,
                correlation_id,
                client_id,

                client_software_name,
                client_software_version,
            })
        } else {
            Err(UnexpectedApiBodyError::Request(Box::new(api_request)))
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiVersionResponse {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,

    pub error_code: i16,
    pub api_keys: Option<Vec<api_versions_response::ApiVersion>>,
    pub throttle_time_ms: Option<i32>,
    pub supported_features: Option<Vec<api_versions_response::SupportedFeatureKey>>,
    pub finalized_features_epoch: Option<i64>,
    pub finalized_features: Option<Vec<api_versions_response::FinalizedFeatureKey>>,
    pub zk_migration_ready: Option<bool>,
}

impl TryFrom<ApiResponse> for ApiVersionResponse {
    type Error = UnexpectedApiBodyError;

    fn try_from(api_response: ApiResponse) -> Result<Self, Self::Error> {
        if let ApiResponse {
            api_key,
            api_version,
            correlation_id,
            body:
                Body::ApiVersionsResponse(tansu_sans_io::ApiVersionsResponse {
                    error_code,
                    api_keys,
                    throttle_time_ms,
                    supported_features,
                    finalized_features_epoch,
                    finalized_features,
                    zk_migration_ready,
                    ..
                }),
        } = api_response
        {
            Ok(ApiVersionResponse {
                api_key,
                api_version,
                correlation_id,
                error_code,
                api_keys,
                throttle_time_ms,
                supported_features,
                finalized_features_epoch,
                finalized_features,
                zk_migration_ready,
            })
        } else {
            Err(UnexpectedApiBodyError::Response(Box::new(api_response)))
        }
    }
}
