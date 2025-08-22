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

use rama::{Context, Service};
use tansu_sans_io::{
    ApiKey, ErrorCode, GetTelemetrySubscriptionsRequest, GetTelemetrySubscriptionsResponse,
};
use uuid::Uuid;

use crate::{Error, Result, Storage};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GetTelemetrySubscriptionsService;

impl ApiKey for GetTelemetrySubscriptionsService {
    const KEY: i16 = GetTelemetrySubscriptionsRequest::KEY;
}

impl<G> Service<G, GetTelemetrySubscriptionsRequest> for GetTelemetrySubscriptionsService
where
    G: Storage,
{
    type Response = GetTelemetrySubscriptionsResponse;
    type Error = Error;

    async fn serve(
        &self,
        _ctx: Context<G>,
        _req: GetTelemetrySubscriptionsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let client_instance_id = *Uuid::new_v4().as_bytes();

        Ok(GetTelemetrySubscriptionsResponse::default()
            .throttle_time_ms(0)
            .error_code(ErrorCode::None.into())
            .client_instance_id(client_instance_id)
            .subscription_id(0)
            .accepted_compression_types(Some([0].into()))
            .push_interval_ms(5_000)
            .telemetry_max_bytes(1_024)
            .delta_temporality(false)
            .requested_metrics(Some([].into())))
    }
}
