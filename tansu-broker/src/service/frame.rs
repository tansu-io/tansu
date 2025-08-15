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

use bytes::Bytes;
use opentelemetry::{KeyValue, metrics::Counter};
use rama::{Context, Service};
use tansu_sans_io::{Body, Frame, Header};
use tracing::{Instrument as _, Level, debug, error, span};

use crate::{Error, METER};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FramingService<S> {
    inner: S,
}

impl<S> FramingService<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, State> Service<State, Bytes> for FramingService<S>
where
    S: Service<State, Frame, Response = Body>,
    S::Error: Into<Error>,
    State: Clone + Send + Sync + 'static,
{
    type Response = Bytes;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        request: Bytes,
    ) -> Result<Self::Response, Self::Error> {
        let request = Frame::request_from_bytes(&request[..])?;

        let api_key = request.api_key()?;
        let api_version = request.api_version()?;
        let correlation_id = request.correlation_id()?;

        let span = span!(
            Level::DEBUG,
            "frame",
            api_name = request.api_name(),
            api_version,
            correlation_id
        );

        debug!(?request);

        async move {
            let body = self.inner.serve(ctx, request).await.map_err(Into::into)?;

            let attributes = vec![
                KeyValue::new("api_key", api_key as i64),
                KeyValue::new("api_version", api_version as i64),
            ];

            Frame::response(
                Header::Response { correlation_id },
                body,
                api_key,
                api_version,
            )
            .inspect(|response| {
                debug!(?response);
                API_REQUESTS.add(1, &attributes);
            })
            .inspect_err(|err| {
                error!(api_key, api_version, ?err);
                API_ERRORS.add(1, &attributes);
            })
            .map_err(Into::into)
        }
        .instrument(span)
        .await
    }
}

static API_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_api_requests")
        .with_description("The number of API requests made")
        .build()
});

static API_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_api_errors")
        .with_description("The number of API errors")
        .build()
});
