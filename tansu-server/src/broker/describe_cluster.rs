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
    broker_registration_request::Listener, describe_cluster_response::DescribeClusterBroker, Body,
    ErrorCode,
};

use crate::State;

pub(crate) struct DescribeClusterRequest;

impl DescribeClusterRequest {
    pub(crate) fn response(
        &self,
        include_cluster_authorized_operations: bool,
        endpoint_type: Option<i8>,
        state: &State,
    ) -> Body {
        let _ = include_cluster_authorized_operations;
        let _ = endpoint_type;

        Body::DescribeClusterResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None.into(),
            error_message: None,
            endpoint_type: Some(1),
            cluster_id: state.cluster_id.clone().unwrap_or(String::from("")),
            controller_id: -1,
            brokers: Some(
                state
                    .brokers
                    .iter()
                    .map(|(broker_id, broker)| {
                        let listener = broker
                            .listeners
                            .as_ref()
                            .unwrap_or(&vec![])
                            .iter()
                            .find(|&listener| listener.name == "broker")
                            .cloned()
                            .unwrap_or(Listener {
                                name: "broker".into(),
                                host: "localhost".into(),
                                port: 9092,
                                security_protocol: 0,
                            });

                        DescribeClusterBroker {
                            broker_id: *broker_id,
                            host: listener.host,
                            port: listener.port as i32,
                            rack: broker.rack.clone(),
                        }
                    })
                    .collect(),
            ),
            cluster_authorized_operations: -2_147_483_648,
        }
    }
}
