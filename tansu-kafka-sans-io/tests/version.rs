// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use bytes::Bytes;
use common::init_tracing;
use pretty_assertions::assert_eq;
use tansu_kafka_sans_io::{
    Body, ErrorCode, Frame, Header, Result,
    join_group_response::JoinGroupResponseMember,
    offset_fetch_response::{
        OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
        OffsetFetchResponseTopic, OffsetFetchResponseTopics,
    },
};
use tracing::debug;

pub mod common;

#[test]
fn join_group_response() -> Result<()> {
    let _guard = init_tracing()?;

    let throttle_time_ms = Some(0);
    let error_code = ErrorCode::None.into();
    let generation_id = 1;
    let protocol_type = Some("consumer".into());
    let protocol_name = Some("range".into());
    let leader = "rdkafka-499e5770-375e-4990-bf84-a39634e3bfe4".to_owned();
    let skip_assignment = Some(true);
    let member_id = leader.clone();
    let metadata =
        Bytes::from_static(b"\0\x03\0\0\0\x01\0\tbenchmark\0\0\0\0\0\0\0\0\xff\xff\xff\xff\0\0");
    let members = Some(
        [JoinGroupResponseMember {
            member_id: member_id.clone(),
            group_instance_id: Some("group_instance_id".into()),
            metadata: metadata.clone(),
        }]
        .into(),
    );

    let correlation_id = 4;

    let header = Header::Response { correlation_id };

    let body = Body::JoinGroupResponse {
        throttle_time_ms,
        error_code,
        generation_id,
        protocol_type,
        protocol_name: protocol_name.clone(),
        leader: leader.clone(),
        skip_assignment,
        member_id: member_id.clone(),
        members: members.clone(),
    };

    let api_key = 11;
    let api_version = 5;

    let encoded = Frame::response(header, body, api_key, api_version)?;

    debug!(?encoded);

    match Frame::response_from_bytes(&encoded, api_key, api_version) {
        Ok(Frame {
            header: Header::Response { .. },
            body:
                Body::JoinGroupResponse {
                    throttle_time_ms: encoded_throttle_time_ms,
                    error_code: encoded_error_code,
                    generation_id: encoded_generation_id,
                    protocol_type: encoded_protocol_type,
                    protocol_name: encoded_protocol_name,
                    leader: encoded_leader,
                    skip_assignment: encoded_skip_assignment,
                    member_id: encoded_member_id,
                    members: encoded_members,
                },
            ..
        }) => {
            assert_eq!(throttle_time_ms, encoded_throttle_time_ms);
            assert_eq!(error_code, encoded_error_code);
            assert_eq!(generation_id, encoded_generation_id);
            assert_eq!(None, encoded_protocol_type);
            assert_eq!(protocol_name, encoded_protocol_name);
            assert_eq!(leader, encoded_leader);
            assert_eq!(None, encoded_skip_assignment);
            assert_eq!(member_id, encoded_member_id);
            assert_eq!(members, encoded_members);
        }
        otherwise => panic!("{otherwise:?}"),
    }

    Ok(())
}

#[test]
fn offset_fetch_response() -> Result<()> {
    let _guard = init_tracing()?;

    let correlation_id = 8;
    let header = Header::Response { correlation_id };

    let encoded_throttle_time_ms = Some(0);
    let encoded_error_code = Some(ErrorCode::None.into());
    let encoded_groups = Some(
        [OffsetFetchResponseGroup {
            group_id: "example_consumer_group_id".into(),
            topics: Some(
                [OffsetFetchResponseTopics {
                    name: "topic-name-a".into(),
                    partitions: Some(
                        [OffsetFetchResponsePartitions {
                            partition_index: 32123,
                            committed_offset: 4343,
                            committed_leader_epoch: 87678,
                            metadata: Some("".into()),
                            error_code: ErrorCode::None.into(),
                        }]
                        .into(),
                    ),
                }]
                .into(),
            ),
            error_code: ErrorCode::None.into(),
        }]
        .into(),
    );
    let encoded_topics = Some(
        [OffsetFetchResponseTopic {
            name: "benchmark".into(),
            partitions: Some(
                [1, 0, 6, 5, 4, 3, 2]
                    .into_iter()
                    .map(|partition_index| OffsetFetchResponsePartition {
                        partition_index,
                        committed_offset: -1,
                        committed_leader_epoch: Some(-1),
                        metadata: Some("".into()),
                        error_code: ErrorCode::None.into(),
                    })
                    .collect(),
            ),
        }]
        .into(),
    );

    let body = Body::OffsetFetchResponse {
        throttle_time_ms: encoded_throttle_time_ms,
        error_code: encoded_error_code,
        groups: encoded_groups.clone(),
        topics: encoded_topics.clone(),
    };

    let api_key = 9;
    let api_version = 7;

    let encoded = Frame::response(header, body, api_key, api_version)?;

    match Frame::response_from_bytes(&encoded, api_key, api_version) {
        Ok(Frame {
            header: Header::Response { .. },
            body:
                Body::OffsetFetchResponse {
                    throttle_time_ms,
                    topics,
                    error_code,
                    groups,
                },
            ..
        }) => {
            assert_eq!(throttle_time_ms, encoded_throttle_time_ms);
            assert_eq!(topics, encoded_topics);
            assert_eq!(error_code, encoded_error_code);
            assert_eq!(groups, None);
        }

        otherwise => panic!("{otherwise:?}"),
    }

    debug!(?encoded);

    Ok(())
}
