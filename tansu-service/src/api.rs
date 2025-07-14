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

use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{self, Debug, Display},
    sync::{Arc, LazyLock, Mutex},
    time::SystemTime,
};

use bytes::Bytes;
use opentelemetry::{KeyValue, metrics::Histogram};
use rama::{Context, Layer, Service, context::Extensions, error::BoxError, matcher::Matcher};
use tansu_sans_io::{Body, Frame, Header};
use tracing::debug;

use crate::{METER, Result};

pub mod api_version;
pub mod describe_config;
pub mod metadata;
pub mod produce;

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiKey(pub i16);

impl AsRef<i16> for ApiKey {
    fn as_ref(&self) -> &i16 {
        &self.0
    }
}

pub type ApiVersion = i16;
pub type CorrelationId = i32;
pub type ClientId = Option<String>;

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct ApiRequest {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,
    pub client_id: ClientId,
    pub body: Body,
}

impl TryFrom<ApiRequest> for Bytes {
    type Error = BoxError;

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
        .map_err(Into::into)
    }
}

impl<State> Matcher<State, ApiRequest> for ApiKey
where
    State: Debug,
{
    fn matches(
        &self,
        ext: Option<&mut Extensions>,
        ctx: &Context<State>,
        req: &ApiRequest,
    ) -> bool {
        let _ = (ext, ctx);
        debug!(api_key = self.0, ?req);
        self.0 == req.api_key.0
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct ApiResponse {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,
    pub body: Body,
}

impl TryFrom<ApiResponse> for Bytes {
    type Error = BoxError;

    fn try_from(api_response: ApiResponse) -> Result<Self, Self::Error> {
        Frame::response(
            Header::Response {
                correlation_id: api_response.correlation_id,
            },
            api_response.body,
            api_response.api_key.0,
            api_response.api_version,
        )
        .map_err(Into::into)
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
struct FrameTypeError {
    frame: Box<Frame>,
}

impl Display for FrameTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unexpected frame type")
    }
}
impl Error for FrameTypeError {}

pub fn read_api_request(buffer: Bytes) -> Result<ApiRequest> {
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

        frame => Err(FrameTypeError {
            frame: Box::new(frame),
        }
        .into()),
    }
}

static API_RESPONSE_FROM_BYTES_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("api_response_from_bytes_duration")
        .with_unit("ms")
        .with_description("Latency of converting bytes into an API response in milliseconds")
        .build()
});

pub fn read_api_response(
    buffer: Bytes,
    api_key: ApiKey,
    api_version: ApiVersion,
) -> Result<ApiResponse> {
    let attributes = [KeyValue::new("api_key", api_key.0.to_string())];
    let start = SystemTime::now();

    match Frame::response_from_bytes(&buffer[..], api_key.0, api_version).inspect(|_| {
        API_RESPONSE_FROM_BYTES_DURATION.record(
            start
                .elapsed()
                .map_or(0, |duration| duration.as_millis() as u64),
            &attributes,
        )
    })? {
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

        frame => Err(FrameTypeError {
            frame: Box::new(frame),
        }
        .into()),
    }
}

type KeyVersionService<S> = Arc<Mutex<BTreeMap<(ApiKey, ApiVersion), Arc<S>>>>;

#[derive(Clone, Debug, Default)]
pub struct ApiKeyVersionService<S> {
    inner: KeyVersionService<S>,
    model: S,
}

impl<S, State> Service<State, ApiRequest> for ApiKeyVersionService<S>
where
    S: Service<State, ApiRequest, Response = ApiResponse> + Clone,
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
        debug!(?req);

        let service = self
            .inner
            .lock()
            .map(|mut guard| {
                let key = (req.api_key, req.api_version);

                guard
                    .entry(key)
                    .or_insert(Arc::new(self.model.clone()))
                    .to_owned()
            })
            .expect("poison");

        service
            .serve(ctx, req)
            .await
            .inspect(|response| debug!(?response))
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiKeyVersionLayer;

impl<S> Layer<S> for ApiKeyVersionLayer {
    type Service = ApiKeyVersionService<S>;

    fn layer(&self, model: S) -> Self::Service {
        ApiKeyVersionService {
            inner: Arc::new(Mutex::new(BTreeMap::new())),
            model,
        }
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum UnexpectedApiBodyError {
    Request(Box<ApiRequest>),
    Response(Box<ApiResponse>),
}

impl Display for UnexpectedApiBodyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unexpected response")
    }
}

impl Error for UnexpectedApiBodyError {}
