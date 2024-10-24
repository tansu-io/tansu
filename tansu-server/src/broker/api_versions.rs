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

use std::sync::LazyLock;

use tansu_kafka_sans_io::{api_versions_response::ApiVersion, Body, ErrorCode, RootMessageMeta};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiVersionsRequest;

const TELEMETRY: [i16; 3] = [71, 72, 74];
const SASL: [i16; 1] = [17];
const DESCRIBE_TOPIC_PARTITIONS: [i16; 1] = [75];

static UNSUPPORTED: LazyLock<Vec<i16>> = LazyLock::new(|| {
    let mut unsupported = vec![];
    unsupported.extend_from_slice(&TELEMETRY);
    unsupported.extend_from_slice(&SASL);
    unsupported.extend_from_slice(&DESCRIBE_TOPIC_PARTITIONS);
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

        Body::ApiVersionsResponse {
            finalized_features: None,
            finalized_features_epoch: None,
            supported_features: None,
            zk_migration_ready: None,
            error_code: ErrorCode::None.into(),
            api_keys: Some(
                RootMessageMeta::messages()
                    .requests()
                    .iter()
                    .filter(|(api_key, _)| !UNSUPPORTED.contains(api_key))
                    .map(|(_, meta)| ApiVersion {
                        api_key: meta.api_key,
                        min_version: meta.version.valid.start,
                        max_version: meta.version.valid.end,
                    })
                    .collect(),
            ),
            throttle_time_ms: Some(0),
        }
    }
}
