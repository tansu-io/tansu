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

use tansu_sans_io::{
    Body, ErrorCode,
    find_coordinator_response::{Coordinator, FindCoordinatorResponse},
};
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

        FindCoordinatorResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(Some(ErrorCode::None.into()))
            .error_message(Some("NONE".into()))
            .node_id(Some(node_id))
            .host(Some(host.into()))
            .port(Some(port))
            .coordinators(coordinator_keys.map(|keys| {
                keys.iter()
                    .map(|key| {
                        Coordinator::default()
                            .key(key.to_string())
                            .node_id(node_id)
                            .host(host.into())
                            .port(port)
                            .error_code(ErrorCode::None.into())
                            .error_message(None)
                    })
                    .collect()
            }))
            .into()
    }
}
