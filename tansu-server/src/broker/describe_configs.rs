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

use tansu_kafka_sans_io::{
    describe_configs_request::DescribeConfigsResource,
    describe_configs_response::DescribeConfigsResult, Body, ErrorCode,
};

pub(crate) struct DescribeConfigsRequest;

impl DescribeConfigsRequest {
    pub(crate) fn response(
        &self,
        resources: Option<&[DescribeConfigsResource]>,
        include_synonyms: Option<bool>,
        include_documentation: Option<bool>,
    ) -> Body {
        let _ = include_synonyms;
        let _ = include_documentation;

        Body::DescribeConfigsResponse {
            throttle_time_ms: 0,
            results: resources.map(|resources| {
                resources
                    .iter()
                    .map(|resource| match resource {
                        DescribeConfigsResource {
                            resource_type,
                            resource_name,
                            configuration_keys: None,
                        } if *resource_type == 2 => DescribeConfigsResult {
                            error_code: ErrorCode::None.into(),
                            error_message: None,
                            resource_type: *resource_type,
                            resource_name: resource_name.clone(),
                            configs: Some([].into()),
                        },

                        _ => todo!(),
                    })
                    .collect()
            }),
        }
    }
}
