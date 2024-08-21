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

use tansu_kafka_sans_io::{find_coordinator_response::Coordinator, Body, ErrorCode};
use url::Url;

pub(crate) struct FindCoordinatorRequest;

impl FindCoordinatorRequest {
    pub(crate) fn response(
        &self,
        key: Option<&str>,
        key_type: Option<i8>,
        coordinator_keys: Option<&[String]>,
        node_id: i32,
        listener: &Url,
    ) -> Body {
        let _ = key;
        let _ = key_type;

        Body::FindCoordinatorResponse {
            throttle_time_ms: Some(0),
            error_code: None,
            error_message: None,
            node_id: None,
            host: None,
            port: None,
            coordinators: coordinator_keys.map(|keys| {
                keys.iter()
                    .map(|key| Coordinator {
                        key: key.to_string(),
                        node_id,
                        host: listener.host_str().unwrap_or("localhost").into(),
                        port: i32::from(listener.port().unwrap_or(9092)),
                        error_code: ErrorCode::None.into(),
                        error_message: None,
                    })
                    .collect()
            }),
        }
    }
}
