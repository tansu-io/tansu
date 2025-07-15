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

use tansu_sans_io::{
    ApiVersionsResponse, Body, ErrorCode, RootMessageMeta, api_versions_response::ApiVersion,
};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiVersionsRequest;

const TELEMETRY: [i16; 3] = [71, 72, 74];
const SASL: [i16; 1] = [17];

static UNSUPPORTED: LazyLock<Vec<i16>> = LazyLock::new(|| {
    let mut unsupported = vec![];
    unsupported.extend_from_slice(&TELEMETRY);
    unsupported.extend_from_slice(&SASL);
    unsupported
});

impl ApiVersionsRequest {
    pub fn response(
        &self,
        client_software_name: Option<&str>,
        client_software_version: Option<&str>,
    ) -> Body {
        let _ = client_software_name;
        let _ = client_software_version;

        ApiVersionsResponse::default()
            .finalized_features(None)
            .finalized_features_epoch(None)
            .supported_features(None)
            .zk_migration_ready(None)
            .error_code(ErrorCode::None.into())
            .api_keys(Some(
                RootMessageMeta::messages()
                    .requests()
                    .iter()
                    .filter(|(api_key, _)| !UNSUPPORTED.contains(api_key))
                    .map(|(_, meta)| {
                        ApiVersion::default()
                            .api_key(meta.api_key)
                            .min_version(meta.version.valid.start)
                            .max_version(meta.version.valid.end)
                    })
                    .collect(),
            ))
            .throttle_time_ms(Some(0))
            .into()
    }
}
