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

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FindCoordinatorRequest;

impl FindCoordinatorRequest {
    pub fn response(
        &self,
        key: Option<&str>,
        key_type: Option<i8>,
        coordinator_keys: Option<&[String]>,
        node_id: i32,
        listener: &Url,
    ) -> Body {
        let _ = key;
        let _ = key_type;

        let host = listener.host_str().unwrap_or("localhost");
        let port = i32::from(listener.port().unwrap_or(9092));

        Body::FindCoordinatorResponse {
            throttle_time_ms: Some(0),
            error_code: Some(ErrorCode::None.into()),
            error_message: Some("NONE".into()),
            node_id: Some(node_id),
            host: Some(host.into()),
            port: Some(port),
            coordinators: coordinator_keys.map(|keys| {
                keys.iter()
                    .map(|key| Coordinator {
                        key: key.to_string(),
                        node_id,
                        host: host.into(),
                        port,
                        error_code: ErrorCode::None.into(),
                        error_message: None,
                    })
                    .collect()
            }),
        }
    }
}
