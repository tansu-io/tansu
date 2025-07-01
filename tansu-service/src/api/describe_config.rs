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

use std::{
    collections::BTreeMap,
    error,
    fmt::{self, Debug, Display},
    ops::Deref,
    sync::{Arc, LazyLock, Mutex},
};

use rama::{Context, Layer, Service, context::Extensions, error::BoxError, matcher::Matcher};
use tansu_sans_io::{
    Body, ConfigResource, ConfigType, ErrorCode, MESSAGE_META,
    describe_configs_request::DescribeConfigsResource,
    describe_configs_response::DescribeConfigsResult,
};
use tracing::{debug, error};

use crate::{
    Result,
    api::{
        ApiKey, ApiRequest, ApiResponse, ApiVersion, ClientId, CorrelationId,
        UnexpectedApiBodyError,
        produce::{ProduceRequest, ProduceResponse},
    },
};

pub static API_KEY_VERSION: LazyLock<(ApiKey, ApiVersion)> = LazyLock::new(|| {
    MESSAGE_META
        .iter()
        .find(|(name, _)| *name == "DescribeConfigsRequest")
        .map_or((ApiKey(-1), 0), |(_, meta)| {
            (ApiKey(meta.api_key), meta.version.valid.end)
        })
});

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DescribeConfigRequest {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,
    pub client_id: ClientId,

    pub resources: Option<Vec<DescribeConfigsResource>>,
    pub include_synonyms: Option<bool>,
    pub include_documentation: Option<bool>,
}

impl Default for DescribeConfigRequest {
    fn default() -> Self {
        Self {
            api_key: API_KEY_VERSION.deref().0,
            api_version: API_KEY_VERSION.deref().1,
            correlation_id: 0,
            client_id: Some("tansu-proxy".into()),
            resources: Some([].into()),
            include_synonyms: Some(false),
            include_documentation: Some(false),
        }
    }
}

impl DescribeConfigRequest {
    pub fn with_topics(self, topics: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let mut resources = self.resources.unwrap_or_default();
        resources.extend(topics.into_iter().map(|topic| DescribeConfigsResource {
            resource_type: i8::from(ConfigResource::Topic),
            resource_name: topic.into(),
            configuration_keys: Some([].into()),
        }));

        Self {
            resources: Some(resources),
            ..self
        }
    }

    pub fn with_topic(self, resource_name: &str) -> Self {
        let mut resources = self.resources.unwrap_or_default();
        resources.push(DescribeConfigsResource {
            resource_type: i8::from(ConfigResource::Topic),
            resource_name: resource_name.into(),
            configuration_keys: Some([].into()),
        });

        Self {
            resources: Some(resources),
            ..self
        }
    }
}

impl From<DescribeConfigRequest> for ApiRequest {
    fn from(describe: DescribeConfigRequest) -> Self {
        Self {
            api_key: describe.api_key,
            api_version: describe.api_version,
            correlation_id: describe.correlation_id,
            client_id: describe.client_id,
            body: Body::DescribeConfigsRequest {
                resources: describe.resources,
                include_synonyms: describe.include_synonyms,
                include_documentation: describe.include_documentation,
            },
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DescribeConfigResponse {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,

    pub throttle_time_ms: i32,
    pub results: Option<Vec<DescribeConfigsResult>>,
}

impl TryFrom<ApiResponse> for DescribeConfigResponse {
    type Error = UnexpectedApiBodyError;

    fn try_from(api_response: ApiResponse) -> Result<Self, Self::Error> {
        if let ApiResponse {
            api_key,
            api_version,
            correlation_id,
            body:
                Body::DescribeConfigsResponse {
                    throttle_time_ms,
                    results,
                },
        } = api_response
        {
            Ok(Self {
                api_key,
                api_version,
                correlation_id,
                throttle_time_ms,
                results,
            })
        } else {
            Err(UnexpectedApiBodyError::Response(Box::new(api_response)))
        }
    }
}

impl From<(String, ConfigType)> for ResourceConfigValue {
    fn from((value, config_type): (String, ConfigType)) -> Self {
        match config_type {
            ConfigType::Unknown => Self::Unknown(value),
            ConfigType::Boolean => value.parse().ok().map_or(
                ResourceConfigValue::Unknown(value),
                ResourceConfigValue::Boolean,
            ),
            ConfigType::String => ResourceConfigValue::String(value),
            ConfigType::Int => value.parse().ok().map_or(
                ResourceConfigValue::Unknown(value),
                ResourceConfigValue::Int,
            ),
            ConfigType::Short => value.parse().ok().map_or(
                ResourceConfigValue::Unknown(value),
                ResourceConfigValue::Short,
            ),
            ConfigType::Long => value.parse().ok().map_or(
                ResourceConfigValue::Unknown(value),
                ResourceConfigValue::Long,
            ),
            ConfigType::Double => value.parse().ok().map_or(
                ResourceConfigValue::Unknown(value),
                ResourceConfigValue::Double,
            ),
            ConfigType::List => ResourceConfigValue::String(value),
            ConfigType::Class => ResourceConfigValue::String(value),
            ConfigType::Password => ResourceConfigValue::String(value),
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum ResourceConfigValue {
    Unknown(String),
    Boolean(bool),
    String(String),
    Int(i32),
    Short(i16),
    Long(i64),
    Double(f64),
    List(Vec<String>),
    Class(String),
    Password(String),
}

impl From<bool> for ResourceConfigValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<i32> for ResourceConfigValue {
    fn from(value: i32) -> Self {
        Self::Int(value)
    }
}

impl From<&str> for ResourceConfigValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

#[derive(Clone, Debug)]
pub struct ResourceConfigValueMatcher {
    resource_config: ResourceConfig,
    key: String,
    value: ResourceConfigValue,
}

impl ResourceConfigValueMatcher {
    pub fn new(
        resource_config: ResourceConfig,
        key: impl Into<String>,
        value: impl Into<ResourceConfigValue>,
    ) -> Self {
        Self {
            resource_config,
            key: key.into(),
            value: value.into(),
        }
    }
}

impl<State> Matcher<State, ProduceRequest> for ResourceConfigValueMatcher {
    fn matches(
        &self,
        ext: Option<&mut Extensions>,
        ctx: &Context<State>,
        req: &ProduceRequest,
    ) -> bool {
        let _ = (ext, ctx);
        debug!(?req);

        req.topic_names()
            .into_iter()
            .inspect(|topic_name| debug!(%topic_name))
            .any(|topic_name| {
                self.resource_config
                    .get(&topic_name, &self.key)
                    .inspect(|value| debug!(%topic_name, key = %self.key, ?value))
                    .is_some_and(|value| value == self.value)
            })
    }
}

impl ResourceConfigValue {
    #[allow(dead_code)]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(value) => Some(*value),
            Self::String(value) => value.parse().ok(),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Short(value) if !value.is_negative() => Some(*value as u64),
            Self::Int(value) if !value.is_negative() => Some(*value as u64),
            Self::Long(value) if !value.is_negative() => Some(*value as u64),
            Self::String(value) => value.parse().ok(),
            _ => None,
        }
    }

    pub fn as_usize(&self) -> Option<usize> {
        match self {
            Self::Short(value) if !value.is_negative() => Some(*value as usize),
            Self::Int(value) if !value.is_negative() => Some(*value as usize),
            Self::Long(value) if !value.is_negative() => Some(*value as usize),
            Self::String(value) => value.parse().ok(),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn as_i16(&self) -> Option<i16> {
        match self {
            Self::Short(value) => Some(*value),
            Self::String(value) => value.parse().ok(),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Self::Int(value) => Some(*value),
            Self::String(value) => value.parse().ok(),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Long(value) => Some(*value),
            Self::String(value) => value.parse().ok(),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, PartialOrd)]
struct LockError {
    resource_name: String,
    key: Option<String>,
    value: Option<ResourceConfigValue>,
}

impl Display for LockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "resource: {}, key: {:?}", self.resource_name, self.key)
    }
}

impl error::Error for LockError {}

#[derive(Clone, Debug, Default)]
pub struct ResourceConfig {
    configuration: Arc<Mutex<BTreeMap<String, BTreeMap<String, ResourceConfigValue>>>>,
}

impl ResourceConfig {
    pub fn has_resource(&self, resource_name: &str) -> bool {
        self.configuration
            .lock()
            .map(|guard| guard.contains_key(resource_name))
            .ok()
            .unwrap_or_default()
    }

    pub fn get(&self, resource_name: &str, key: &str) -> Option<ResourceConfigValue> {
        self.configuration
            .lock()
            .map(|guard| {
                guard
                    .get(resource_name)
                    .and_then(|resource_configuration| resource_configuration.get(key).cloned())
            })
            .inspect_err(|err| error!(resource_name, key, ?err))
            .ok()
            .flatten()
    }

    pub fn put(
        &self,
        resource_name: impl Into<String>,
        key: impl Into<String>,
        value: impl Into<ResourceConfigValue>,
    ) -> Result<()> {
        let resource_name = resource_name.into();
        let key = key.into();
        let value = value.into();

        self.configuration
            .lock()
            .map(|mut guard| {
                guard
                    .entry(resource_name.clone())
                    .or_default()
                    .entry(key.clone())
                    .or_insert(value.clone());
            })
            .inspect_err(|err| error!(resource_name, key, ?value, ?err))
            .map_err(|_| {
                LockError {
                    resource_name,
                    key: Some(key),
                    value: Some(value),
                }
                .into()
            })
    }
}

#[derive(Clone, Debug, Default)]
pub struct TopicConfigService<I, O> {
    resource_config: ResourceConfig,
    inner: I,
    outer: O,
}

impl<I, O> TopicConfigService<I, O> {
    fn add_topic_configuration(&self, response: DescribeConfigResponse) -> Result<()> {
        debug!(?response);
        for result in response.results.unwrap_or_default() {
            debug!(?result);

            if result.error_code != i16::from(ErrorCode::None)
                || result.resource_type != i8::from(ConfigResource::Topic)
                || result.configs.is_none()
            {
                continue;
            }

            for resource_result in result.configs.as_deref().unwrap_or_default() {
                debug!(?resource_result);

                if let Some(config_value) = resource_result
                    .value
                    .as_deref()
                    .and_then(|value| {
                        resource_result
                            .config_type
                            .map(ConfigType::from)
                            .map(|config_type| (value.to_string(), config_type))
                            .map(ResourceConfigValue::from)
                    })
                    .inspect(|config_value| debug!(?config_value))
                {
                    self.resource_config.put(
                        result.resource_name.clone(),
                        resource_result.name.clone(),
                        config_value,
                    )?;
                }
            }
        }

        Ok(())
    }
}

impl<I, O, State> Service<State, ProduceRequest> for TopicConfigService<I, O>
where
    I: Service<State, ProduceRequest, Response = ProduceResponse>,
    I::Error: Into<BoxError> + Send + Debug + 'static,
    O: Service<State, ApiRequest, Response = ApiResponse>,
    O::Error: Into<BoxError> + Send + Debug + 'static,
    State: Clone + Send + Sync + 'static,
{
    type Response = ProduceResponse;
    type Error = BoxError;

    async fn serve(
        &self,
        ctx: Context<State>,
        req: ProduceRequest,
    ) -> Result<Self::Response, Self::Error> {
        debug!(?req);

        if !req
            .topic_names()
            .iter()
            .all(|topic| self.resource_config.has_resource(topic))
        {
            let config_response = self
                .outer
                .serve(
                    ctx.clone(),
                    ApiRequest::from(
                        DescribeConfigRequest::default().with_topics(req.topic_names()),
                    ),
                )
                .await
                .inspect_err(|err| debug!(?err))
                .map_err(Into::into)
                .and_then(|response| DescribeConfigResponse::try_from(response).map_err(Into::into))
                .inspect(|response| debug!(?response))?;

            self.add_topic_configuration(config_response)?;
        }

        self.inner.serve(ctx, req).await.map_err(Into::into)
    }
}

#[derive(Clone, Debug, Default)]
pub struct TopicConfigLayer<O> {
    resource_config: ResourceConfig,
    outer: O,
}

impl<O> TopicConfigLayer<O> {
    pub fn new(resource_config: ResourceConfig, outer: O) -> Self {
        Self {
            resource_config,
            outer,
        }
    }
}

impl<I, O> Layer<I> for TopicConfigLayer<O>
where
    O: Clone,
{
    type Service = TopicConfigService<I, O>;

    fn layer(&self, inner: I) -> Self::Service {
        TopicConfigService {
            resource_config: self.resource_config.clone(),
            inner,
            outer: self.outer.clone(),
        }
    }
}
