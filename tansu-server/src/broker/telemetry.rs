// Copyright ⓒ 2024 Peter Morgan <peter.james.morgan@gmail.com>
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

use tansu_kafka_sans_io::{Body, ErrorCode};
use uuid::Uuid;

pub(crate) struct GetTelemetrySubscriptionsRequest;

impl GetTelemetrySubscriptionsRequest {
    pub(crate) fn response(&self, client_instance_id: [u8; 16]) -> Body {
        let _ = client_instance_id;

        let client_instance_id = *Uuid::new_v4().as_bytes();

        Body::GetTelemetrySubscriptionsResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None.into(),
            client_instance_id,
            subscription_id: 0,
            accepted_compression_types: Some([0].into()),
            push_interval_ms: 5_000,
            telemetry_max_bytes: 1_024,
            delta_temporality: false,
            requested_metrics: Some([].into()),
        }
    }
}
