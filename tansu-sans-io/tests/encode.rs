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

use bytes::Bytes;
use common::init_tracing;
use pretty_assertions::assert_eq;
use tansu_sans_io::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, CreateTopicsResponse, DeleteTopicsRequest,
    DescribeClusterRequest, DescribeConfigsRequest, DescribeConfigsResponse, DescribeGroupsRequest,
    DescribeGroupsResponse, DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse,
    FetchRequest, FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse, Frame, Header,
    HeartbeatRequest, InitProducerIdRequest, JoinGroupRequest, JoinGroupResponse,
    ListGroupsRequest, ListOffsetsResponse, ListPartitionReassignmentsRequest, MetadataRequest,
    MetadataResponse, OffsetFetchRequest, OffsetForLeaderEpochRequest, ProduceRequest,
    ProduceResponse, Result,
    join_group_request::JoinGroupRequestProtocol,
    join_group_response::JoinGroupResponseMember,
    record::{
        Record,
        inflated::{self, Batch},
    },
};

pub mod common;

#[test]
fn api_versions_request_v3_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 18,
        api_version: 3,
        correlation_id: 3,
        client_id: Some("console-producer".into()),
    };

    let body = ApiVersionsRequest::default()
        .client_software_name(Some("apache-kafka-java".into()))
        .client_software_version(Some("3.6.1".into()))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 52, 0, 18, 0, 3, 0, 0, 0, 3, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
            114, 111, 100, 117, 99, 101, 114, 0, 18, 97, 112, 97, 99, 104, 101, 45, 107, 97, 102,
            107, 97, 45, 106, 97, 118, 97, 6, 51, 46, 54, 46, 49, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn api_versions_response_v1_000() -> Result<()> {
    use tansu_sans_io::api_versions_response::ApiVersion;

    let _guard = init_tracing()?;

    let header = Header::Response { correlation_id: 0 };

    let body = ApiVersionsResponse::default()
        .finalized_features(None)
        .finalized_features_epoch(None)
        .supported_features(None)
        .zk_migration_ready(None)
        .error_code(0)
        .api_keys(Some(
            [
                ApiVersion::default()
                    .api_key(0)
                    .min_version(0)
                    .max_version(5),
                ApiVersion::default()
                    .api_key(1)
                    .min_version(0)
                    .max_version(6),
                ApiVersion::default()
                    .api_key(2)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(3)
                    .min_version(0)
                    .max_version(5),
                ApiVersion::default()
                    .api_key(4)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(5)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(6)
                    .min_version(0)
                    .max_version(4),
                ApiVersion::default()
                    .api_key(7)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(8)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(9)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(10)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(11)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(12)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(13)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(14)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(15)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(16)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(17)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(18)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(19)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(20)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(21)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(22)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(23)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(24)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(25)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(26)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(27)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(28)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(29)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(30)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(31)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(32)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(33)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(34)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(35)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(36)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(37)
                    .min_version(0)
                    .max_version(0),
            ]
            .into(),
        ))
        .throttle_time_ms(Some(0))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 242, 0, 0, 0, 0, 0, 0, 0, 0, 0, 38, 0, 0, 0, 0, 0, 5, 0, 1, 0, 0, 0, 6, 0, 2,
            0, 0, 0, 2, 0, 3, 0, 0, 0, 5, 0, 4, 0, 0, 0, 1, 0, 5, 0, 0, 0, 0, 0, 6, 0, 0, 0, 4, 0,
            7, 0, 0, 0, 1, 0, 8, 0, 0, 0, 3, 0, 9, 0, 0, 0, 3, 0, 10, 0, 0, 0, 1, 0, 11, 0, 0, 0,
            2, 0, 12, 0, 0, 0, 1, 0, 13, 0, 0, 0, 1, 0, 14, 0, 0, 0, 1, 0, 15, 0, 0, 0, 1, 0, 16,
            0, 0, 0, 1, 0, 17, 0, 0, 0, 1, 0, 18, 0, 0, 0, 1, 0, 19, 0, 0, 0, 2, 0, 20, 0, 0, 0, 1,
            0, 21, 0, 0, 0, 0, 0, 22, 0, 0, 0, 0, 0, 23, 0, 0, 0, 0, 0, 24, 0, 0, 0, 0, 0, 25, 0,
            0, 0, 0, 0, 26, 0, 0, 0, 0, 0, 27, 0, 0, 0, 0, 0, 28, 0, 0, 0, 0, 0, 29, 0, 0, 0, 0, 0,
            30, 0, 0, 0, 0, 0, 31, 0, 0, 0, 0, 0, 32, 0, 0, 0, 0, 0, 33, 0, 0, 0, 0, 0, 34, 0, 0,
            0, 0, 0, 35, 0, 0, 0, 0, 0, 36, 0, 0, 0, 0, 0, 37, 0, 0, 0, 0, 0, 0, 0, 0,
        ],
        Frame::response(header, body, ApiVersionsResponse::KEY, 1)?,
    );

    Ok(())
}

#[test]
fn api_versions_response_v3_000() -> Result<()> {
    use tansu_sans_io::api_versions_response::{
        ApiVersion, FinalizedFeatureKey, SupportedFeatureKey,
    };

    let _guard = init_tracing()?;

    let header = Header::Response { correlation_id: 0 };
    let body = ApiVersionsResponse::default()
        .finalized_features(Some(vec![
            FinalizedFeatureKey::default()
                .name("metadata.version".into())
                .min_version_level(14)
                .max_version_level(14),
        ]))
        .finalized_features_epoch(Some(76i64))
        .supported_features(Some(vec![
            SupportedFeatureKey::default()
                .name("metadata.version".into())
                .min_version(1)
                .max_version(14),
        ]))
        .zk_migration_ready(None)
        .error_code(0)
        .api_keys(Some(
            [
                ApiVersion::default()
                    .api_key(0)
                    .min_version(0)
                    .max_version(9),
                ApiVersion::default()
                    .api_key(1)
                    .min_version(0)
                    .max_version(15),
                ApiVersion::default()
                    .api_key(2)
                    .min_version(0)
                    .max_version(8),
                ApiVersion::default()
                    .api_key(3)
                    .min_version(0)
                    .max_version(12),
                ApiVersion::default()
                    .api_key(8)
                    .min_version(0)
                    .max_version(8),
                ApiVersion::default()
                    .api_key(9)
                    .min_version(0)
                    .max_version(8),
                ApiVersion::default()
                    .api_key(10)
                    .min_version(0)
                    .max_version(4),
                ApiVersion::default()
                    .api_key(11)
                    .min_version(0)
                    .max_version(9),
                ApiVersion::default()
                    .api_key(12)
                    .min_version(0)
                    .max_version(4),
                ApiVersion::default()
                    .api_key(13)
                    .min_version(0)
                    .max_version(5),
                ApiVersion::default()
                    .api_key(14)
                    .min_version(0)
                    .max_version(5),
                ApiVersion::default()
                    .api_key(15)
                    .min_version(0)
                    .max_version(5),
                ApiVersion::default()
                    .api_key(16)
                    .min_version(0)
                    .max_version(4),
                ApiVersion::default()
                    .api_key(17)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(18)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(19)
                    .min_version(0)
                    .max_version(7),
                ApiVersion::default()
                    .api_key(20)
                    .min_version(0)
                    .max_version(6),
                ApiVersion::default()
                    .api_key(21)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(22)
                    .min_version(0)
                    .max_version(4),
                ApiVersion::default()
                    .api_key(23)
                    .min_version(0)
                    .max_version(4),
                ApiVersion::default()
                    .api_key(24)
                    .min_version(0)
                    .max_version(4),
                ApiVersion::default()
                    .api_key(25)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(26)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(27)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(28)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(29)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(30)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(31)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(32)
                    .min_version(0)
                    .max_version(4),
                ApiVersion::default()
                    .api_key(33)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(34)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(35)
                    .min_version(0)
                    .max_version(4),
                ApiVersion::default()
                    .api_key(36)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(37)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(38)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(39)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(40)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(41)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(42)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(43)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(44)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(45)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(46)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(47)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(48)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(49)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(50)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(51)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(55)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(57)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(60)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(61)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(64)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(65)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(66)
                    .min_version(0)
                    .max_version(0),
            ]
            .into(),
        ))
        .throttle_time_ms(Some(0))
        .into();

    assert_eq!(
        vec![
            0, 0, 1, 201, 0, 0, 0, 0, 0, 0, 56, 0, 0, 0, 0, 0, 9, 0, 0, 1, 0, 0, 0, 15, 0, 0, 2, 0,
            0, 0, 8, 0, 0, 3, 0, 0, 0, 12, 0, 0, 8, 0, 0, 0, 8, 0, 0, 9, 0, 0, 0, 8, 0, 0, 10, 0,
            0, 0, 4, 0, 0, 11, 0, 0, 0, 9, 0, 0, 12, 0, 0, 0, 4, 0, 0, 13, 0, 0, 0, 5, 0, 0, 14, 0,
            0, 0, 5, 0, 0, 15, 0, 0, 0, 5, 0, 0, 16, 0, 0, 0, 4, 0, 0, 17, 0, 0, 0, 1, 0, 0, 18, 0,
            0, 0, 3, 0, 0, 19, 0, 0, 0, 7, 0, 0, 20, 0, 0, 0, 6, 0, 0, 21, 0, 0, 0, 2, 0, 0, 22, 0,
            0, 0, 4, 0, 0, 23, 0, 0, 0, 4, 0, 0, 24, 0, 0, 0, 4, 0, 0, 25, 0, 0, 0, 3, 0, 0, 26, 0,
            0, 0, 3, 0, 0, 27, 0, 0, 0, 1, 0, 0, 28, 0, 0, 0, 3, 0, 0, 29, 0, 0, 0, 3, 0, 0, 30, 0,
            0, 0, 3, 0, 0, 31, 0, 0, 0, 3, 0, 0, 32, 0, 0, 0, 4, 0, 0, 33, 0, 0, 0, 2, 0, 0, 34, 0,
            0, 0, 2, 0, 0, 35, 0, 0, 0, 4, 0, 0, 36, 0, 0, 0, 2, 0, 0, 37, 0, 0, 0, 3, 0, 0, 38, 0,
            0, 0, 3, 0, 0, 39, 0, 0, 0, 2, 0, 0, 40, 0, 0, 0, 2, 0, 0, 41, 0, 0, 0, 3, 0, 0, 42, 0,
            0, 0, 2, 0, 0, 43, 0, 0, 0, 2, 0, 0, 44, 0, 0, 0, 1, 0, 0, 45, 0, 0, 0, 0, 0, 0, 46, 0,
            0, 0, 0, 0, 0, 47, 0, 0, 0, 0, 0, 0, 48, 0, 0, 0, 1, 0, 0, 49, 0, 0, 0, 1, 0, 0, 50, 0,
            0, 0, 0, 0, 0, 51, 0, 0, 0, 0, 0, 0, 55, 0, 0, 0, 1, 0, 0, 57, 0, 0, 0, 1, 0, 0, 60, 0,
            0, 0, 0, 0, 0, 61, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 65, 0, 0, 0, 0, 0, 0, 66, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 23, 2, 17, 109, 101, 116, 97, 100, 97, 116, 97, 46, 118,
            101, 114, 115, 105, 111, 110, 0, 1, 0, 14, 0, 1, 8, 0, 0, 0, 0, 0, 0, 0, 76, 2, 23, 2,
            17, 109, 101, 116, 97, 100, 97, 116, 97, 46, 118, 101, 114, 115, 105, 111, 110, 0, 14,
            0, 14, 0,
        ],
        Frame::response(header, body, ApiVersionsResponse::KEY, 3)?,
    );

    Ok(())
}

#[test]
fn create_topics_request_v7_000() -> Result<()> {
    use tansu_sans_io::{
        ApiKey as _, CreateTopicsRequest, Frame, Header,
        create_topics_request::{CreatableTopic, CreatableTopicConfig},
    };

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: CreateTopicsRequest::KEY,
        api_version: 7,
        correlation_id: 298,
        client_id: Some("adminclient-1".into()),
    };

    let body = CreateTopicsRequest::default()
        .topics(Some(
            [CreatableTopic::default()
                .name("balances".into())
                .num_partitions(-1)
                .replication_factor(-1)
                .assignments(Some([].into()))
                .configs(Some(
                    [CreatableTopicConfig::default()
                        .name("cleanup.policy".into())
                        .value(Some("compact".into()))]
                    .into(),
                ))]
            .into(),
        ))
        .timeout_ms(30_000)
        .validate_only(Some(false))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 73, 0, 19, 0, 7, 0, 0, 1, 42, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105,
            101, 110, 116, 45, 49, 0, 2, 9, 98, 97, 108, 97, 110, 99, 101, 115, 255, 255, 255, 255,
            255, 255, 1, 2, 15, 99, 108, 101, 97, 110, 117, 112, 46, 112, 111, 108, 105, 99, 121,
            8, 99, 111, 109, 112, 97, 99, 116, 0, 0, 0, 0, 117, 48, 0, 0,
        ],
        Frame::request(header, body)?
    );

    Ok(())
}

#[test]
fn create_topics_response_v7_000() -> Result<()> {
    use tansu_sans_io::create_topics_response::{CreatableTopicConfigs, CreatableTopicResult};

    let _guard = init_tracing()?;

    let api_key = 19;
    let api_version = 7;

    let header = Header::Response {
        correlation_id: 298,
    };

    let body = CreateTopicsResponse::default()
        .throttle_time_ms(Some(0))
        .topics(Some(
            [CreatableTopicResult::default()
                .name("balances".into())
                .topic_id(Some([
                    222, 159, 182, 217, 102, 152, 68, 189, 174, 152, 214, 59, 29, 216, 240, 198,
                ]))
                .error_code(0)
                .error_message(None)
                .topic_config_error_code(None)
                .num_partitions(Some(1))
                .replication_factor(Some(1))
                .configs(Some(
                    [
                        CreatableTopicConfigs::default()
                            .name("cleanup.policy".into())
                            .value(Some("compact".into()))
                            .read_only(false)
                            .config_source(1)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("compression.type".into())
                            .value(Some("producer".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("delete.retention.ms".into())
                            .value(Some("86400000".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("file.delete.delay.ms".into())
                            .value(Some("60000".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("flush.messages".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("flush.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("follower.replication.throttled.replicas".into())
                            .value(Some("".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("index.interval.bytes".into())
                            .value(Some("4096".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("leader.replication.throttled.replicas".into())
                            .value(Some("".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("local.retention.bytes".into())
                            .value(Some("-2".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("local.retention.ms".into())
                            .value(Some("-2".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("max.compaction.lag.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("max.message.bytes".into())
                            .value(Some("1048588".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("message.downconversion.enable".into())
                            .value(Some("true".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("message.format.version".into())
                            .value(Some("3.0-IV1".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("message.timestamp.after.max.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("message.timestamp.before.max.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("message.timestamp.difference.max.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("message.timestamp.type".into())
                            .value(Some("CreateTime".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("min.cleanable.dirty.ratio".into())
                            .value(Some("0.5".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("min.compaction.lag.ms".into())
                            .value(Some("0".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("min.insync.replicas".into())
                            .value(Some("1".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("preallocate".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("remote.storage.enable".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("retention.bytes".into())
                            .value(Some("-1".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("retention.ms".into())
                            .value(Some("604800000".into()))
                            .read_only(false)
                            .config_source(4)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("segment.bytes".into())
                            .value(Some("1073741824".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("segment.index.bytes".into())
                            .value(Some("10485760".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("segment.jitter.ms".into())
                            .value(Some("0".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("segment.ms".into())
                            .value(Some("604800000".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("unclean.leader.election.enable".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                    ]
                    .into(),
                ))]
            .into(),
        ))
        .into();

    let encoded = vec![
        0, 0, 4, 92, 0, 0, 1, 42, 0, 0, 0, 0, 0, 2, 9, 98, 97, 108, 97, 110, 99, 101, 115, 222,
        159, 182, 217, 102, 152, 68, 189, 174, 152, 214, 59, 29, 216, 240, 198, 0, 0, 0, 0, 0, 0,
        1, 0, 1, 32, 15, 99, 108, 101, 97, 110, 117, 112, 46, 112, 111, 108, 105, 99, 121, 8, 99,
        111, 109, 112, 97, 99, 116, 0, 1, 0, 0, 17, 99, 111, 109, 112, 114, 101, 115, 115, 105,
        111, 110, 46, 116, 121, 112, 101, 9, 112, 114, 111, 100, 117, 99, 101, 114, 0, 5, 0, 0, 20,
        100, 101, 108, 101, 116, 101, 46, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 109,
        115, 9, 56, 54, 52, 48, 48, 48, 48, 48, 0, 5, 0, 0, 21, 102, 105, 108, 101, 46, 100, 101,
        108, 101, 116, 101, 46, 100, 101, 108, 97, 121, 46, 109, 115, 6, 54, 48, 48, 48, 48, 0, 5,
        0, 0, 15, 102, 108, 117, 115, 104, 46, 109, 101, 115, 115, 97, 103, 101, 115, 20, 57, 50,
        50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 0, 5, 0, 0, 9, 102,
        108, 117, 115, 104, 46, 109, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52,
        55, 55, 53, 56, 48, 55, 0, 5, 0, 0, 40, 102, 111, 108, 108, 111, 119, 101, 114, 46, 114,
        101, 112, 108, 105, 99, 97, 116, 105, 111, 110, 46, 116, 104, 114, 111, 116, 116, 108, 101,
        100, 46, 114, 101, 112, 108, 105, 99, 97, 115, 1, 0, 5, 0, 0, 21, 105, 110, 100, 101, 120,
        46, 105, 110, 116, 101, 114, 118, 97, 108, 46, 98, 121, 116, 101, 115, 5, 52, 48, 57, 54,
        0, 5, 0, 0, 38, 108, 101, 97, 100, 101, 114, 46, 114, 101, 112, 108, 105, 99, 97, 116, 105,
        111, 110, 46, 116, 104, 114, 111, 116, 116, 108, 101, 100, 46, 114, 101, 112, 108, 105, 99,
        97, 115, 1, 0, 5, 0, 0, 22, 108, 111, 99, 97, 108, 46, 114, 101, 116, 101, 110, 116, 105,
        111, 110, 46, 98, 121, 116, 101, 115, 3, 45, 50, 0, 5, 0, 0, 19, 108, 111, 99, 97, 108, 46,
        114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 109, 115, 3, 45, 50, 0, 5, 0, 0, 22, 109,
        97, 120, 46, 99, 111, 109, 112, 97, 99, 116, 105, 111, 110, 46, 108, 97, 103, 46, 109, 115,
        20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 0, 5, 0, 0,
        18, 109, 97, 120, 46, 109, 101, 115, 115, 97, 103, 101, 46, 98, 121, 116, 101, 115, 8, 49,
        48, 52, 56, 53, 56, 56, 0, 5, 0, 0, 30, 109, 101, 115, 115, 97, 103, 101, 46, 100, 111,
        119, 110, 99, 111, 110, 118, 101, 114, 115, 105, 111, 110, 46, 101, 110, 97, 98, 108, 101,
        5, 116, 114, 117, 101, 0, 5, 0, 0, 23, 109, 101, 115, 115, 97, 103, 101, 46, 102, 111, 114,
        109, 97, 116, 46, 118, 101, 114, 115, 105, 111, 110, 8, 51, 46, 48, 45, 73, 86, 49, 0, 5,
        0, 0, 31, 109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109, 101, 115, 116, 97, 109, 112,
        46, 97, 102, 116, 101, 114, 46, 109, 97, 120, 46, 109, 115, 20, 57, 50, 50, 51, 51, 55, 50,
        48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 0, 5, 0, 0, 32, 109, 101, 115, 115, 97,
        103, 101, 46, 116, 105, 109, 101, 115, 116, 97, 109, 112, 46, 98, 101, 102, 111, 114, 101,
        46, 109, 97, 120, 46, 109, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55,
        55, 53, 56, 48, 55, 0, 5, 0, 0, 36, 109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109,
        101, 115, 116, 97, 109, 112, 46, 100, 105, 102, 102, 101, 114, 101, 110, 99, 101, 46, 109,
        97, 120, 46, 109, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53,
        56, 48, 55, 0, 5, 0, 0, 23, 109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109, 101, 115,
        116, 97, 109, 112, 46, 116, 121, 112, 101, 11, 67, 114, 101, 97, 116, 101, 84, 105, 109,
        101, 0, 5, 0, 0, 26, 109, 105, 110, 46, 99, 108, 101, 97, 110, 97, 98, 108, 101, 46, 100,
        105, 114, 116, 121, 46, 114, 97, 116, 105, 111, 4, 48, 46, 53, 0, 5, 0, 0, 22, 109, 105,
        110, 46, 99, 111, 109, 112, 97, 99, 116, 105, 111, 110, 46, 108, 97, 103, 46, 109, 115, 2,
        48, 0, 5, 0, 0, 20, 109, 105, 110, 46, 105, 110, 115, 121, 110, 99, 46, 114, 101, 112, 108,
        105, 99, 97, 115, 2, 49, 0, 5, 0, 0, 12, 112, 114, 101, 97, 108, 108, 111, 99, 97, 116,
        101, 6, 102, 97, 108, 115, 101, 0, 5, 0, 0, 22, 114, 101, 109, 111, 116, 101, 46, 115, 116,
        111, 114, 97, 103, 101, 46, 101, 110, 97, 98, 108, 101, 6, 102, 97, 108, 115, 101, 0, 5, 0,
        0, 16, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 98, 121, 116, 101, 115, 3, 45, 49,
        0, 5, 0, 0, 13, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 109, 115, 10, 54, 48, 52,
        56, 48, 48, 48, 48, 48, 0, 4, 0, 0, 14, 115, 101, 103, 109, 101, 110, 116, 46, 98, 121,
        116, 101, 115, 11, 49, 48, 55, 51, 55, 52, 49, 56, 50, 52, 0, 5, 0, 0, 20, 115, 101, 103,
        109, 101, 110, 116, 46, 105, 110, 100, 101, 120, 46, 98, 121, 116, 101, 115, 9, 49, 48, 52,
        56, 53, 55, 54, 48, 0, 5, 0, 0, 18, 115, 101, 103, 109, 101, 110, 116, 46, 106, 105, 116,
        116, 101, 114, 46, 109, 115, 2, 48, 0, 5, 0, 0, 11, 115, 101, 103, 109, 101, 110, 116, 46,
        109, 115, 10, 54, 48, 52, 56, 48, 48, 48, 48, 48, 0, 5, 0, 0, 31, 117, 110, 99, 108, 101,
        97, 110, 46, 108, 101, 97, 100, 101, 114, 46, 101, 108, 101, 99, 116, 105, 111, 110, 46,
        101, 110, 97, 98, 108, 101, 6, 102, 97, 108, 115, 101, 0, 5, 0, 0, 0, 0,
    ];

    assert_eq!(
        encoded,
        Frame::response(header, body, api_key, api_version)?
    );

    Ok(())
}

#[test]
fn delete_topics_request_v6_000() -> Result<()> {
    use tansu_sans_io::delete_topics_request::DeleteTopicState;

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 20,
        api_version: 6,
        correlation_id: 4,
        client_id: Some("adminclient-1".into()),
    };

    let body = DeleteTopicsRequest::default()
        .topics(Some(
            [DeleteTopicState::default()
                .name(Some("test".into()))
                .topic_id([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])]
            .into(),
        ))
        .topic_names(None)
        .timeout_ms(30000)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 52, 0, 20, 0, 6, 0, 0, 0, 4, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
            110, 116, 45, 49, 0, 2, 5, 116, 101, 115, 116, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 117, 48, 0,
        ],
        Frame::request(header, body)?,
    );
    Ok(())
}

#[test]
fn describe_cluster_request_v1_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 60,
        api_version: 1,
        correlation_id: 7,
        client_id: Some("adminclient-1".into()),
    };

    let body = DescribeClusterRequest::default()
        .include_cluster_authorized_operations(false)
        .endpoint_type(Some(1))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 27, 0, 60, 0, 1, 0, 0, 0, 7, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
            110, 116, 45, 49, 0, 0, 1, 0,
        ],
        Frame::request(header, body)?,
    );
    Ok(())
}

#[test]
fn describe_configs_request_v4_000() -> Result<()> {
    use tansu_sans_io::describe_configs_request::DescribeConfigsResource;

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 32,
        api_version: 4,
        correlation_id: 5,
        client_id: Some("adminclient-1".into()),
    };

    let body = DescribeConfigsRequest::default()
        .resources(Some(
            [DescribeConfigsResource::default()
                .resource_type(2)
                .resource_name("test".into())
                .configuration_keys(None)]
            .into(),
        ))
        .include_synonyms(Some(false))
        .include_documentation(Some(false))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 36, 0, 32, 0, 4, 0, 0, 0, 5, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
            110, 116, 45, 49, 0, 2, 2, 5, 116, 101, 115, 116, 0, 0, 0, 0, 0,
        ],
        Frame::request(header, body)?,
    );
    Ok(())
}

#[test]
fn describe_configs_request_v4_001() -> Result<()> {
    use tansu_sans_io::describe_configs_request::DescribeConfigsResource;

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 32,
        api_version: 4,
        correlation_id: 3,
        client_id: Some("adminclient-1".into()),
    };

    let body = DescribeConfigsRequest::default()
        .resources(Some(
            [DescribeConfigsResource::default()
                .resource_type(2)
                .resource_name("_schemas".into())
                .configuration_keys(None)]
            .into(),
        ))
        .include_synonyms(Some(true))
        .include_documentation(Some(false))
        .into();

    let v = vec![
        0, 0, 0, 40, 0, 32, 0, 4, 0, 0, 0, 3, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
        110, 116, 45, 49, 0, 2, 2, 9, 95, 115, 99, 104, 101, 109, 97, 115, 0, 0, 1, 0, 0,
    ];

    assert_eq!(v, Frame::request(header, body)?);

    Ok(())
}

#[test]
fn describe_configs_request_v4_002() -> Result<()> {
    use tansu_sans_io::describe_configs_request::DescribeConfigsResource;

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 32,
        api_version: 4,
        correlation_id: 6,
        client_id: Some("adminclient-1".into()),
    };

    let body = DescribeConfigsRequest::default()
        .resources(Some(
            [DescribeConfigsResource::default()
                .resource_type(2)
                .resource_name("test".into())
                .configuration_keys(None)]
            .into(),
        ))
        .include_synonyms(Some(false))
        .include_documentation(Some(false))
        .into();

    let v = vec![
        0, 0, 0, 36, 0, 32, 0, 4, 0, 0, 0, 6, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
        110, 116, 45, 49, 0, 2, 2, 5, 116, 101, 115, 116, 0, 0, 0, 0, 0,
    ];

    assert_eq!(v, Frame::request(header, body)?);

    Ok(())
}

#[test]
fn describe_configs_response_v4_001() -> Result<()> {
    use tansu_sans_io::describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsResult, DescribeConfigsSynonym,
    };

    let _guard = init_tracing()?;

    let expected = vec![
        0, 0, 9, 26, 0, 0, 0, 3, 0, 0, 0, 0, 0, 2, 0, 0, 1, 2, 9, 95, 115, 99, 104, 101, 109, 97,
        115, 35, 17, 99, 111, 109, 112, 114, 101, 115, 115, 105, 111, 110, 46, 116, 121, 112, 101,
        9, 112, 114, 111, 100, 117, 99, 101, 114, 0, 5, 0, 2, 17, 99, 111, 109, 112, 114, 101, 115,
        115, 105, 111, 110, 46, 116, 121, 112, 101, 9, 112, 114, 111, 100, 117, 99, 101, 114, 5, 0,
        2, 0, 0, 38, 108, 101, 97, 100, 101, 114, 46, 114, 101, 112, 108, 105, 99, 97, 116, 105,
        111, 110, 46, 116, 104, 114, 111, 116, 116, 108, 101, 100, 46, 114, 101, 112, 108, 105, 99,
        97, 115, 1, 0, 5, 0, 1, 7, 0, 0, 22, 114, 101, 109, 111, 116, 101, 46, 115, 116, 111, 114,
        97, 103, 101, 46, 101, 110, 97, 98, 108, 101, 6, 102, 97, 108, 115, 101, 0, 5, 0, 1, 1, 0,
        0, 30, 109, 101, 115, 115, 97, 103, 101, 46, 100, 111, 119, 110, 99, 111, 110, 118, 101,
        114, 115, 105, 111, 110, 46, 101, 110, 97, 98, 108, 101, 5, 116, 114, 117, 101, 0, 5, 0, 2,
        34, 108, 111, 103, 46, 109, 101, 115, 115, 97, 103, 101, 46, 100, 111, 119, 110, 99, 111,
        110, 118, 101, 114, 115, 105, 111, 110, 46, 101, 110, 97, 98, 108, 101, 5, 116, 114, 117,
        101, 5, 0, 1, 0, 0, 20, 109, 105, 110, 46, 105, 110, 115, 121, 110, 99, 46, 114, 101, 112,
        108, 105, 99, 97, 115, 2, 49, 0, 5, 0, 2, 20, 109, 105, 110, 46, 105, 110, 115, 121, 110,
        99, 46, 114, 101, 112, 108, 105, 99, 97, 115, 2, 49, 5, 0, 3, 0, 0, 18, 115, 101, 103, 109,
        101, 110, 116, 46, 106, 105, 116, 116, 101, 114, 46, 109, 115, 2, 48, 0, 5, 0, 1, 5, 0, 0,
        19, 108, 111, 99, 97, 108, 46, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 109, 115,
        3, 45, 50, 0, 5, 0, 2, 23, 108, 111, 103, 46, 108, 111, 99, 97, 108, 46, 114, 101, 116,
        101, 110, 116, 105, 111, 110, 46, 109, 115, 3, 45, 50, 5, 0, 5, 0, 0, 15, 99, 108, 101, 97,
        110, 117, 112, 46, 112, 111, 108, 105, 99, 121, 7, 100, 101, 108, 101, 116, 101, 0, 5, 0,
        2, 19, 108, 111, 103, 46, 99, 108, 101, 97, 110, 117, 112, 46, 112, 111, 108, 105, 99, 121,
        7, 100, 101, 108, 101, 116, 101, 5, 0, 7, 0, 0, 9, 102, 108, 117, 115, 104, 46, 109, 115,
        20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 0, 5, 0, 1,
        5, 0, 0, 40, 102, 111, 108, 108, 111, 119, 101, 114, 46, 114, 101, 112, 108, 105, 99, 97,
        116, 105, 111, 110, 46, 116, 104, 114, 111, 116, 116, 108, 101, 100, 46, 114, 101, 112,
        108, 105, 99, 97, 115, 1, 0, 5, 0, 1, 7, 0, 0, 22, 99, 111, 109, 112, 114, 101, 115, 115,
        105, 111, 110, 46, 108, 122, 52, 46, 108, 101, 118, 101, 108, 2, 57, 0, 5, 0, 2, 22, 99,
        111, 109, 112, 114, 101, 115, 115, 105, 111, 110, 46, 108, 122, 52, 46, 108, 101, 118, 101,
        108, 2, 57, 5, 0, 3, 0, 0, 14, 115, 101, 103, 109, 101, 110, 116, 46, 98, 121, 116, 101,
        115, 11, 49, 48, 55, 51, 55, 52, 49, 56, 50, 52, 0, 4, 0, 3, 18, 108, 111, 103, 46, 115,
        101, 103, 109, 101, 110, 116, 46, 98, 121, 116, 101, 115, 11, 49, 48, 55, 51, 55, 52, 49,
        56, 50, 52, 4, 0, 18, 108, 111, 103, 46, 115, 101, 103, 109, 101, 110, 116, 46, 98, 121,
        116, 101, 115, 11, 49, 48, 55, 51, 55, 52, 49, 56, 50, 52, 5, 0, 3, 0, 0, 13, 114, 101,
        116, 101, 110, 116, 105, 111, 110, 46, 109, 115, 10, 54, 48, 52, 56, 48, 48, 48, 48, 48, 0,
        5, 0, 1, 5, 0, 0, 23, 99, 111, 109, 112, 114, 101, 115, 115, 105, 111, 110, 46, 103, 122,
        105, 112, 46, 108, 101, 118, 101, 108, 3, 45, 49, 0, 5, 0, 2, 23, 99, 111, 109, 112, 114,
        101, 115, 115, 105, 111, 110, 46, 103, 122, 105, 112, 46, 108, 101, 118, 101, 108, 3, 45,
        49, 5, 0, 3, 0, 0, 15, 102, 108, 117, 115, 104, 46, 109, 101, 115, 115, 97, 103, 101, 115,
        2, 49, 0, 1, 0, 3, 15, 102, 108, 117, 115, 104, 46, 109, 101, 115, 115, 97, 103, 101, 115,
        2, 49, 1, 0, 28, 108, 111, 103, 46, 102, 108, 117, 115, 104, 46, 105, 110, 116, 101, 114,
        118, 97, 108, 46, 109, 101, 115, 115, 97, 103, 101, 115, 20, 57, 50, 50, 51, 51, 55, 50,
        48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 5, 0, 5, 0, 0, 23, 99, 111, 109, 112, 114,
        101, 115, 115, 105, 111, 110, 46, 122, 115, 116, 100, 46, 108, 101, 118, 101, 108, 2, 51,
        0, 5, 0, 2, 23, 99, 111, 109, 112, 114, 101, 115, 115, 105, 111, 110, 46, 122, 115, 116,
        100, 46, 108, 101, 118, 101, 108, 2, 51, 5, 0, 3, 0, 0, 23, 109, 101, 115, 115, 97, 103,
        101, 46, 102, 111, 114, 109, 97, 116, 46, 118, 101, 114, 115, 105, 111, 110, 8, 51, 46, 48,
        45, 73, 86, 49, 0, 5, 0, 2, 27, 108, 111, 103, 46, 109, 101, 115, 115, 97, 103, 101, 46,
        102, 111, 114, 109, 97, 116, 46, 118, 101, 114, 115, 105, 111, 110, 8, 51, 46, 48, 45, 73,
        86, 49, 5, 0, 2, 0, 0, 22, 109, 97, 120, 46, 99, 111, 109, 112, 97, 99, 116, 105, 111, 110,
        46, 108, 97, 103, 46, 109, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55,
        55, 53, 56, 48, 55, 0, 5, 0, 2, 34, 108, 111, 103, 46, 99, 108, 101, 97, 110, 101, 114, 46,
        109, 97, 120, 46, 99, 111, 109, 112, 97, 99, 116, 105, 111, 110, 46, 108, 97, 103, 46, 109,
        115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 5, 0,
        5, 0, 0, 21, 102, 105, 108, 101, 46, 100, 101, 108, 101, 116, 101, 46, 100, 101, 108, 97,
        121, 46, 109, 115, 6, 54, 48, 48, 48, 48, 0, 5, 0, 2, 28, 108, 111, 103, 46, 115, 101, 103,
        109, 101, 110, 116, 46, 100, 101, 108, 101, 116, 101, 46, 100, 101, 108, 97, 121, 46, 109,
        115, 6, 54, 48, 48, 48, 48, 5, 0, 5, 0, 0, 18, 109, 97, 120, 46, 109, 101, 115, 115, 97,
        103, 101, 46, 98, 121, 116, 101, 115, 6, 54, 52, 48, 48, 48, 0, 1, 0, 3, 18, 109, 97, 120,
        46, 109, 101, 115, 115, 97, 103, 101, 46, 98, 121, 116, 101, 115, 6, 54, 52, 48, 48, 48, 1,
        0, 18, 109, 101, 115, 115, 97, 103, 101, 46, 109, 97, 120, 46, 98, 121, 116, 101, 115, 8,
        49, 48, 52, 56, 53, 56, 56, 5, 0, 3, 0, 0, 22, 109, 105, 110, 46, 99, 111, 109, 112, 97,
        99, 116, 105, 111, 110, 46, 108, 97, 103, 46, 109, 115, 2, 48, 0, 5, 0, 2, 34, 108, 111,
        103, 46, 99, 108, 101, 97, 110, 101, 114, 46, 109, 105, 110, 46, 99, 111, 109, 112, 97, 99,
        116, 105, 111, 110, 46, 108, 97, 103, 46, 109, 115, 2, 48, 5, 0, 5, 0, 0, 23, 109, 101,
        115, 115, 97, 103, 101, 46, 116, 105, 109, 101, 115, 116, 97, 109, 112, 46, 116, 121, 112,
        101, 11, 67, 114, 101, 97, 116, 101, 84, 105, 109, 101, 0, 5, 0, 2, 27, 108, 111, 103, 46,
        109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109, 101, 115, 116, 97, 109, 112, 46, 116,
        121, 112, 101, 11, 67, 114, 101, 97, 116, 101, 84, 105, 109, 101, 5, 0, 2, 0, 0, 22, 108,
        111, 99, 97, 108, 46, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 98, 121, 116, 101,
        115, 3, 45, 50, 0, 5, 0, 2, 26, 108, 111, 103, 46, 108, 111, 99, 97, 108, 46, 114, 101,
        116, 101, 110, 116, 105, 111, 110, 46, 98, 121, 116, 101, 115, 3, 45, 50, 5, 0, 5, 0, 0,
        12, 112, 114, 101, 97, 108, 108, 111, 99, 97, 116, 101, 6, 102, 97, 108, 115, 101, 0, 5, 0,
        2, 16, 108, 111, 103, 46, 112, 114, 101, 97, 108, 108, 111, 99, 97, 116, 101, 6, 102, 97,
        108, 115, 101, 5, 0, 1, 0, 0, 26, 109, 105, 110, 46, 99, 108, 101, 97, 110, 97, 98, 108,
        101, 46, 100, 105, 114, 116, 121, 46, 114, 97, 116, 105, 111, 4, 48, 46, 53, 0, 5, 0, 2,
        32, 108, 111, 103, 46, 99, 108, 101, 97, 110, 101, 114, 46, 109, 105, 110, 46, 99, 108,
        101, 97, 110, 97, 98, 108, 101, 46, 114, 97, 116, 105, 111, 4, 48, 46, 53, 5, 0, 6, 0, 0,
        21, 105, 110, 100, 101, 120, 46, 105, 110, 116, 101, 114, 118, 97, 108, 46, 98, 121, 116,
        101, 115, 5, 52, 48, 57, 54, 0, 5, 0, 2, 25, 108, 111, 103, 46, 105, 110, 100, 101, 120,
        46, 105, 110, 116, 101, 114, 118, 97, 108, 46, 98, 121, 116, 101, 115, 5, 52, 48, 57, 54,
        5, 0, 3, 0, 0, 31, 117, 110, 99, 108, 101, 97, 110, 46, 108, 101, 97, 100, 101, 114, 46,
        101, 108, 101, 99, 116, 105, 111, 110, 46, 101, 110, 97, 98, 108, 101, 6, 102, 97, 108,
        115, 101, 0, 5, 0, 2, 31, 117, 110, 99, 108, 101, 97, 110, 46, 108, 101, 97, 100, 101, 114,
        46, 101, 108, 101, 99, 116, 105, 111, 110, 46, 101, 110, 97, 98, 108, 101, 6, 102, 97, 108,
        115, 101, 5, 0, 1, 0, 0, 16, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 98, 121, 116,
        101, 115, 3, 45, 49, 0, 5, 0, 2, 20, 108, 111, 103, 46, 114, 101, 116, 101, 110, 116, 105,
        111, 110, 46, 98, 121, 116, 101, 115, 3, 45, 49, 5, 0, 5, 0, 0, 20, 100, 101, 108, 101,
        116, 101, 46, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 109, 115, 9, 56, 54, 52, 48,
        48, 48, 48, 48, 0, 5, 0, 2, 32, 108, 111, 103, 46, 99, 108, 101, 97, 110, 101, 114, 46,
        100, 101, 108, 101, 116, 101, 46, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 109,
        115, 9, 56, 54, 52, 48, 48, 48, 48, 48, 5, 0, 5, 0, 0, 31, 109, 101, 115, 115, 97, 103,
        101, 46, 116, 105, 109, 101, 115, 116, 97, 109, 112, 46, 97, 102, 116, 101, 114, 46, 109,
        97, 120, 46, 109, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53,
        56, 48, 55, 0, 5, 0, 2, 35, 108, 111, 103, 46, 109, 101, 115, 115, 97, 103, 101, 46, 116,
        105, 109, 101, 115, 116, 97, 109, 112, 46, 97, 102, 116, 101, 114, 46, 109, 97, 120, 46,
        109, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55,
        5, 0, 5, 0, 0, 32, 109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109, 101, 115, 116, 97,
        109, 112, 46, 98, 101, 102, 111, 114, 101, 46, 109, 97, 120, 46, 109, 115, 20, 57, 50, 50,
        51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 0, 5, 0, 2, 36, 108, 111,
        103, 46, 109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109, 101, 115, 116, 97, 109, 112,
        46, 98, 101, 102, 111, 114, 101, 46, 109, 97, 120, 46, 109, 115, 20, 57, 50, 50, 51, 51,
        55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 5, 0, 5, 0, 0, 11, 115, 101, 103,
        109, 101, 110, 116, 46, 109, 115, 10, 54, 48, 52, 56, 48, 48, 48, 48, 48, 0, 5, 0, 1, 5, 0,
        0, 36, 109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109, 101, 115, 116, 97, 109, 112,
        46, 100, 105, 102, 102, 101, 114, 101, 110, 99, 101, 46, 109, 97, 120, 46, 109, 115, 20,
        57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 0, 5, 0, 2, 40,
        108, 111, 103, 46, 109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109, 101, 115, 116, 97,
        109, 112, 46, 100, 105, 102, 102, 101, 114, 101, 110, 99, 101, 46, 109, 97, 120, 46, 109,
        115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 5, 0,
        5, 0, 0, 20, 115, 101, 103, 109, 101, 110, 116, 46, 105, 110, 100, 101, 120, 46, 98, 121,
        116, 101, 115, 9, 49, 48, 52, 56, 53, 55, 54, 48, 0, 5, 0, 2, 25, 108, 111, 103, 46, 105,
        110, 100, 101, 120, 46, 115, 105, 122, 101, 46, 109, 97, 120, 46, 98, 121, 116, 101, 115,
        9, 49, 48, 52, 56, 53, 55, 54, 48, 5, 0, 3, 0, 0, 0, 0,
    ];

    let api_key = 32;
    let api_version = 4;

    let header = Header::Response { correlation_id: 3 };
    let body = DescribeConfigsResponse::default()
        .throttle_time_ms(0)
        .results(Some(
            [DescribeConfigsResult::default()
                .error_code(0)
                .error_message(Some("".into()))
                .resource_type(2)
                .resource_name("_schemas".into())
                .configs(Some(
                    [
                        DescribeConfigsResourceResult::default()
                            .name("compression.type".into())
                            .value(Some("producer".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("compression.type".into())
                                    .value(Some("producer".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(2))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("leader.replication.throttled.replicas".into())
                            .value(Some("".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(7))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("remote.storage.enable".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.downconversion.enable".into())
                            .value(Some("true".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.message.downconversion.enable".into())
                                    .value(Some("true".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("min.insync.replicas".into())
                            .value(Some("1".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("min.insync.replicas".into())
                                    .value(Some("1".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("segment.jitter.ms".into())
                            .value(Some("0".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("local.retention.ms".into())
                            .value(Some("-2".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.local.retention.ms".into())
                                    .value(Some("-2".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("cleanup.policy".into())
                            .value(Some("delete".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.cleanup.policy".into())
                                    .value(Some("delete".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(7))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("flush.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("follower.replication.throttled.replicas".into())
                            .value(Some("".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(7))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("compression.lz4.level".into())
                            .value(Some("9".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("compression.lz4.level".into())
                                    .value(Some("9".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("segment.bytes".into())
                            .value(Some("1073741824".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(4))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [
                                    DescribeConfigsSynonym::default()
                                        .name("log.segment.bytes".into())
                                        .value(Some("1073741824".into()))
                                        .source(4),
                                    DescribeConfigsSynonym::default()
                                        .name("log.segment.bytes".into())
                                        .value(Some("1073741824".into()))
                                        .source(5),
                                ]
                                .into(),
                            ))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("retention.ms".into())
                            .value(Some("604800000".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("compression.gzip.level".into())
                            .value(Some("-1".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("compression.gzip.level".into())
                                    .value(Some("-1".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("flush.messages".into())
                            .value(Some("1".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(1))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [
                                    DescribeConfigsSynonym::default()
                                        .name("flush.messages".into())
                                        .value(Some("1".into()))
                                        .source(1),
                                    DescribeConfigsSynonym::default()
                                        .name("log.flush.interval.messages".into())
                                        .value(Some("9223372036854775807".into()))
                                        .source(5),
                                ]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("compression.zstd.level".into())
                            .value(Some("3".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("compression.zstd.level".into())
                                    .value(Some("3".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.format.version".into())
                            .value(Some("3.0-IV1".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.message.format.version".into())
                                    .value(Some("3.0-IV1".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(2))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("max.compaction.lag.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.cleaner.max.compaction.lag.ms".into())
                                    .value(Some("9223372036854775807".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("file.delete.delay.ms".into())
                            .value(Some("60000".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.segment.delete.delay.ms".into())
                                    .value(Some("60000".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("max.message.bytes".into())
                            .value(Some("64000".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(1))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [
                                    DescribeConfigsSynonym::default()
                                        .name("max.message.bytes".into())
                                        .value(Some("64000".into()))
                                        .source(1),
                                    DescribeConfigsSynonym::default()
                                        .name("message.max.bytes".into())
                                        .value(Some("1048588".into()))
                                        .source(5),
                                ]
                                .into(),
                            ))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("min.compaction.lag.ms".into())
                            .value(Some("0".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.cleaner.min.compaction.lag.ms".into())
                                    .value(Some("0".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.timestamp.type".into())
                            .value(Some("CreateTime".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.message.timestamp.type".into())
                                    .value(Some("CreateTime".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(2))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("local.retention.bytes".into())
                            .value(Some("-2".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.local.retention.bytes".into())
                                    .value(Some("-2".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("preallocate".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.preallocate".into())
                                    .value(Some("false".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("min.cleanable.dirty.ratio".into())
                            .value(Some("0.5".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.cleaner.min.cleanable.ratio".into())
                                    .value(Some("0.5".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(6))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("index.interval.bytes".into())
                            .value(Some("4096".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.index.interval.bytes".into())
                                    .value(Some("4096".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("unclean.leader.election.enable".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("unclean.leader.election.enable".into())
                                    .value(Some("false".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("retention.bytes".into())
                            .value(Some("-1".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.retention.bytes".into())
                                    .value(Some("-1".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("delete.retention.ms".into())
                            .value(Some("86400000".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.cleaner.delete.retention.ms".into())
                                    .value(Some("86400000".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.timestamp.after.max.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.message.timestamp.after.max.ms".into())
                                    .value(Some("9223372036854775807".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.timestamp.before.max.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.message.timestamp.before.max.ms".into())
                                    .value(Some("9223372036854775807".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("segment.ms".into())
                            .value(Some("604800000".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.timestamp.difference.max.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.message.timestamp.difference.max.ms".into())
                                    .value(Some("9223372036854775807".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("segment.index.bytes".into())
                            .value(Some("10485760".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.index.size.max.bytes".into())
                                    .value(Some("10485760".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(3))
                            .documentation(None),
                    ]
                    .into(),
                ))]
            .into(),
        ))
        .into();

    assert_eq!(
        expected,
        Frame::response(header, body, api_key, api_version)?
    );

    Ok(())
}

#[test]
fn describe_configs_response_v4_002() -> Result<()> {
    use tansu_sans_io::describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsResult,
    };

    let _guard = init_tracing()?;

    let expected = vec![
        0, 0, 5, 79, 0, 0, 0, 6, 0, 0, 0, 0, 0, 2, 0, 0, 1, 2, 5, 116, 101, 115, 116, 37, 17, 99,
        111, 109, 112, 114, 101, 115, 115, 105, 111, 110, 46, 116, 121, 112, 101, 9, 112, 114, 111,
        100, 117, 99, 101, 114, 0, 5, 0, 1, 2, 0, 0, 29, 114, 101, 109, 111, 116, 101, 46, 108,
        111, 103, 46, 100, 101, 108, 101, 116, 101, 46, 111, 110, 46, 100, 105, 115, 97, 98, 108,
        101, 6, 102, 97, 108, 115, 101, 0, 5, 0, 1, 1, 0, 0, 38, 108, 101, 97, 100, 101, 114, 46,
        114, 101, 112, 108, 105, 99, 97, 116, 105, 111, 110, 46, 116, 104, 114, 111, 116, 116, 108,
        101, 100, 46, 114, 101, 112, 108, 105, 99, 97, 115, 1, 0, 5, 0, 1, 7, 0, 0, 22, 114, 101,
        109, 111, 116, 101, 46, 115, 116, 111, 114, 97, 103, 101, 46, 101, 110, 97, 98, 108, 101,
        6, 102, 97, 108, 115, 101, 0, 5, 0, 1, 1, 0, 0, 30, 109, 101, 115, 115, 97, 103, 101, 46,
        100, 111, 119, 110, 99, 111, 110, 118, 101, 114, 115, 105, 111, 110, 46, 101, 110, 97, 98,
        108, 101, 5, 116, 114, 117, 101, 0, 5, 0, 1, 1, 0, 0, 20, 109, 105, 110, 46, 105, 110, 115,
        121, 110, 99, 46, 114, 101, 112, 108, 105, 99, 97, 115, 2, 49, 0, 5, 0, 1, 3, 0, 0, 18,
        115, 101, 103, 109, 101, 110, 116, 46, 106, 105, 116, 116, 101, 114, 46, 109, 115, 2, 48,
        0, 5, 0, 1, 5, 0, 0, 24, 114, 101, 109, 111, 116, 101, 46, 108, 111, 103, 46, 99, 111, 112,
        121, 46, 100, 105, 115, 97, 98, 108, 101, 6, 102, 97, 108, 115, 101, 0, 5, 0, 1, 1, 0, 0,
        19, 108, 111, 99, 97, 108, 46, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 109, 115,
        3, 45, 50, 0, 5, 0, 1, 5, 0, 0, 15, 99, 108, 101, 97, 110, 117, 112, 46, 112, 111, 108,
        105, 99, 121, 8, 99, 111, 109, 112, 97, 99, 116, 0, 1, 0, 1, 7, 0, 0, 9, 102, 108, 117,
        115, 104, 46, 109, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53,
        56, 48, 55, 0, 5, 0, 1, 5, 0, 0, 40, 102, 111, 108, 108, 111, 119, 101, 114, 46, 114, 101,
        112, 108, 105, 99, 97, 116, 105, 111, 110, 46, 116, 104, 114, 111, 116, 116, 108, 101, 100,
        46, 114, 101, 112, 108, 105, 99, 97, 115, 1, 0, 5, 0, 1, 7, 0, 0, 22, 99, 111, 109, 112,
        114, 101, 115, 115, 105, 111, 110, 46, 108, 122, 52, 46, 108, 101, 118, 101, 108, 2, 57, 0,
        5, 0, 1, 3, 0, 0, 14, 115, 101, 103, 109, 101, 110, 116, 46, 98, 121, 116, 101, 115, 11,
        49, 48, 55, 51, 55, 52, 49, 56, 50, 52, 0, 4, 0, 1, 3, 0, 0, 13, 114, 101, 116, 101, 110,
        116, 105, 111, 110, 46, 109, 115, 10, 54, 48, 52, 56, 48, 48, 48, 48, 48, 0, 5, 0, 1, 5, 0,
        0, 23, 99, 111, 109, 112, 114, 101, 115, 115, 105, 111, 110, 46, 103, 122, 105, 112, 46,
        108, 101, 118, 101, 108, 3, 45, 49, 0, 5, 0, 1, 3, 0, 0, 15, 102, 108, 117, 115, 104, 46,
        109, 101, 115, 115, 97, 103, 101, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53,
        52, 55, 55, 53, 56, 48, 55, 0, 5, 0, 1, 5, 0, 0, 23, 99, 111, 109, 112, 114, 101, 115, 115,
        105, 111, 110, 46, 122, 115, 116, 100, 46, 108, 101, 118, 101, 108, 2, 51, 0, 5, 0, 1, 3,
        0, 0, 23, 109, 101, 115, 115, 97, 103, 101, 46, 102, 111, 114, 109, 97, 116, 46, 118, 101,
        114, 115, 105, 111, 110, 8, 51, 46, 48, 45, 73, 86, 49, 0, 5, 0, 1, 2, 0, 0, 22, 109, 97,
        120, 46, 99, 111, 109, 112, 97, 99, 116, 105, 111, 110, 46, 108, 97, 103, 46, 109, 115, 20,
        57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 0, 5, 0, 1, 5,
        0, 0, 21, 102, 105, 108, 101, 46, 100, 101, 108, 101, 116, 101, 46, 100, 101, 108, 97, 121,
        46, 109, 115, 6, 54, 48, 48, 48, 48, 0, 5, 0, 1, 5, 0, 0, 18, 109, 97, 120, 46, 109, 101,
        115, 115, 97, 103, 101, 46, 98, 121, 116, 101, 115, 8, 49, 48, 52, 56, 53, 56, 56, 0, 5, 0,
        1, 3, 0, 0, 22, 109, 105, 110, 46, 99, 111, 109, 112, 97, 99, 116, 105, 111, 110, 46, 108,
        97, 103, 46, 109, 115, 2, 48, 0, 5, 0, 1, 5, 0, 0, 23, 109, 101, 115, 115, 97, 103, 101,
        46, 116, 105, 109, 101, 115, 116, 97, 109, 112, 46, 116, 121, 112, 101, 11, 67, 114, 101,
        97, 116, 101, 84, 105, 109, 101, 0, 5, 0, 1, 2, 0, 0, 22, 108, 111, 99, 97, 108, 46, 114,
        101, 116, 101, 110, 116, 105, 111, 110, 46, 98, 121, 116, 101, 115, 3, 45, 50, 0, 5, 0, 1,
        5, 0, 0, 12, 112, 114, 101, 97, 108, 108, 111, 99, 97, 116, 101, 6, 102, 97, 108, 115, 101,
        0, 5, 0, 1, 1, 0, 0, 26, 109, 105, 110, 46, 99, 108, 101, 97, 110, 97, 98, 108, 101, 46,
        100, 105, 114, 116, 121, 46, 114, 97, 116, 105, 111, 4, 48, 46, 53, 0, 5, 0, 1, 6, 0, 0,
        21, 105, 110, 100, 101, 120, 46, 105, 110, 116, 101, 114, 118, 97, 108, 46, 98, 121, 116,
        101, 115, 5, 52, 48, 57, 54, 0, 5, 0, 1, 3, 0, 0, 31, 117, 110, 99, 108, 101, 97, 110, 46,
        108, 101, 97, 100, 101, 114, 46, 101, 108, 101, 99, 116, 105, 111, 110, 46, 101, 110, 97,
        98, 108, 101, 6, 102, 97, 108, 115, 101, 0, 5, 0, 1, 1, 0, 0, 16, 114, 101, 116, 101, 110,
        116, 105, 111, 110, 46, 98, 121, 116, 101, 115, 3, 45, 49, 0, 5, 0, 1, 5, 0, 0, 20, 100,
        101, 108, 101, 116, 101, 46, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 109, 115, 9,
        56, 54, 52, 48, 48, 48, 48, 48, 0, 5, 0, 1, 5, 0, 0, 31, 109, 101, 115, 115, 97, 103, 101,
        46, 116, 105, 109, 101, 115, 116, 97, 109, 112, 46, 97, 102, 116, 101, 114, 46, 109, 97,
        120, 46, 109, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56,
        48, 55, 0, 5, 0, 1, 5, 0, 0, 32, 109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109, 101,
        115, 116, 97, 109, 112, 46, 98, 101, 102, 111, 114, 101, 46, 109, 97, 120, 46, 109, 115,
        20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 0, 5, 0, 1,
        5, 0, 0, 11, 115, 101, 103, 109, 101, 110, 116, 46, 109, 115, 10, 54, 48, 52, 56, 48, 48,
        48, 48, 48, 0, 5, 0, 1, 5, 0, 0, 36, 109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109,
        101, 115, 116, 97, 109, 112, 46, 100, 105, 102, 102, 101, 114, 101, 110, 99, 101, 46, 109,
        97, 120, 46, 109, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53,
        56, 48, 55, 0, 5, 0, 1, 5, 0, 0, 20, 115, 101, 103, 109, 101, 110, 116, 46, 105, 110, 100,
        101, 120, 46, 98, 121, 116, 101, 115, 9, 49, 48, 52, 56, 53, 55, 54, 48, 0, 5, 0, 1, 3, 0,
        0, 0, 0,
    ];

    let api_key = 32;
    let api_version = 4;

    let header = Header::Response { correlation_id: 6 };
    let body = DescribeConfigsResponse::default()
        .throttle_time_ms(0)
        .results(Some(
            [DescribeConfigsResult::default()
                .error_code(0)
                .error_message(Some("".into()))
                .resource_type(2)
                .resource_name("test".into())
                .configs(Some(
                    [
                        DescribeConfigsResourceResult::default()
                            .name("compression.type".into())
                            .value(Some("producer".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(2))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("remote.log.delete.on.disable".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("leader.replication.throttled.replicas".into())
                            .value(Some("".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(7))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("remote.storage.enable".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.downconversion.enable".into())
                            .value(Some("true".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("min.insync.replicas".into())
                            .value(Some("1".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("segment.jitter.ms".into())
                            .value(Some("0".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("remote.log.copy.disable".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("local.retention.ms".into())
                            .value(Some("-2".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("cleanup.policy".into())
                            .value(Some("compact".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(1))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(7))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("flush.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("follower.replication.throttled.replicas".into())
                            .value(Some("".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(7))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("compression.lz4.level".into())
                            .value(Some("9".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("segment.bytes".into())
                            .value(Some("1073741824".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(4))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("retention.ms".into())
                            .value(Some("604800000".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("compression.gzip.level".into())
                            .value(Some("-1".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("flush.messages".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("compression.zstd.level".into())
                            .value(Some("3".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.format.version".into())
                            .value(Some("3.0-IV1".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(2))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("max.compaction.lag.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("file.delete.delay.ms".into())
                            .value(Some("60000".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("max.message.bytes".into())
                            .value(Some("1048588".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("min.compaction.lag.ms".into())
                            .value(Some("0".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.timestamp.type".into())
                            .value(Some("CreateTime".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(2))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("local.retention.bytes".into())
                            .value(Some("-2".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("preallocate".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("min.cleanable.dirty.ratio".into())
                            .value(Some("0.5".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(6))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("index.interval.bytes".into())
                            .value(Some("4096".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("unclean.leader.election.enable".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("retention.bytes".into())
                            .value(Some("-1".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("delete.retention.ms".into())
                            .value(Some("86400000".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.timestamp.after.max.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.timestamp.before.max.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("segment.ms".into())
                            .value(Some("604800000".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.timestamp.difference.max.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("segment.index.bytes".into())
                            .value(Some("10485760".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(3))
                            .documentation(None),
                    ]
                    .into(),
                ))]
            .into(),
        ))
        .into();

    assert_eq!(
        expected,
        Frame::response(header, body, api_key, api_version)?
    );

    Ok(())
}

#[test]
fn describe_configs_response_v4_003() -> Result<()> {
    use tansu_sans_io::describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsResult,
    };

    let _guard = init_tracing()?;

    let expected = vec![
        0, 0, 0, 61, 0, 0, 0, 7, 0, 0, 0, 0, 0, 2, 0, 0, 10, 78, 111, 32, 101, 114, 114, 111, 114,
        46, 2, 5, 116, 101, 115, 116, 2, 15, 99, 108, 101, 97, 110, 117, 112, 46, 112, 111, 108,
        105, 99, 121, 8, 99, 111, 109, 112, 97, 99, 116, 0, 5, 0, 1, 2, 1, 0, 0, 0,
    ];

    let api_key = 32;
    let api_version = 4;

    let header = Header::Response { correlation_id: 7 };
    let body = DescribeConfigsResponse::default()
        .throttle_time_ms(0)
        .results(Some(
            [DescribeConfigsResult::default()
                .error_code(0)
                .error_message(Some("No error.".into()))
                .resource_type(2)
                .resource_name("test".into())
                .configs(Some(
                    [DescribeConfigsResourceResult::default()
                        .name("cleanup.policy".into())
                        .value(Some("compact".into()))
                        .read_only(false)
                        .is_default(None)
                        .config_source(Some(5))
                        .is_sensitive(false)
                        .synonyms(Some([].into()))
                        .config_type(Some(2))
                        .documentation(Some("".into()))]
                    .into(),
                ))]
            .into(),
        ))
        .into();

    assert_eq!(
        expected,
        Frame::response(header, body, api_key, api_version)?
    );

    Ok(())
}

#[test]
fn describe_groups_request_v1_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 15,
        api_version: 1,
        correlation_id: 0,
        client_id: None,
    };

    let body = DescribeGroupsRequest::default()
        .groups(Some(["abcabc".into()].into()))
        .include_authorized_operations(None)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 22, 0, 15, 0, 1, 0, 0, 0, 0, 255, 255, 0, 0, 0, 1, 0, 6, 97, 98, 99, 97, 98,
            99,
        ],
        Frame::request(header, body)?,
    );
    Ok(())
}

#[test]
fn describe_groups_response_v1_000() -> Result<()> {
    use tansu_sans_io::describe_groups_response::DescribedGroup;

    let _guard = init_tracing()?;
    let header = Header::Response { correlation_id: 0 };
    let body = DescribeGroupsResponse::default()
        .throttle_time_ms(Some(0))
        .groups(Some(
            [DescribedGroup::default()
                .error_code(16)
                .group_id("abcabc".into())
                .group_state("".into())
                .protocol_type("".into())
                .protocol_data("".into())
                .members(Some([].into()))
                .authorized_operations(None)]
            .into(),
        ))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 16, 0, 6, 97, 98, 99, 97, 98, 99,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ],
        Frame::response(header, body, DescribeGroupsResponse::KEY, 1)?,
    );
    Ok(())
}

#[test]
fn fetch_request_v6_000() -> Result<()> {
    use tansu_sans_io::fetch_request::{FetchPartition, FetchTopic};

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 1,
        api_version: 6,
        correlation_id: 0,
        client_id: None,
    };

    let body = FetchRequest::default()
        .cluster_id(None)
        .replica_state(None)
        .replica_id(Some(-1))
        .max_wait_ms(5000)
        .min_bytes(1024)
        .max_bytes(Some(4096))
        .isolation_level(Some(1))
        .session_id(None)
        .session_epoch(None)
        .topics(Some(
            [FetchTopic::default()
                .topic(Some("abcabcabcab".into()))
                .topic_id(None)
                .partitions(Some(
                    [FetchPartition::default()
                        .partition(0)
                        .current_leader_epoch(None)
                        .fetch_offset(0)
                        .last_fetched_epoch(None)
                        .log_start_offset(Some(0))
                        .partition_max_bytes(4096)
                        .replica_directory_id(None)]
                    .into(),
                ))]
            .into(),
        ))
        .forgotten_topics_data(None)
        .rack_id(None)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 72, 0, 1, 0, 6, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 0, 0, 19, 136, 0, 0,
            4, 0, 0, 0, 16, 0, 1, 0, 0, 0, 1, 0, 11, 97, 98, 99, 97, 98, 99, 97, 98, 99, 97, 98, 0,
            0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn fetch_request_v16_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 1,
        api_version: 16,
        correlation_id: 12,
        client_id: Some("console-consumer".into()),
    };

    let body = FetchRequest::default()
        .cluster_id(None)
        .replica_id(None)
        .replica_state(None)
        .max_wait_ms(500)
        .min_bytes(1)
        .max_bytes(Some(52428800))
        .isolation_level(Some(0))
        .session_id(Some(0))
        .session_epoch(Some(-1))
        .topics(Some([].into()))
        .forgotten_topics_data(Some([].into()))
        .rack_id(Some("".into()))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 52, 0, 1, 0, 16, 0, 0, 0, 12, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99,
            111, 110, 115, 117, 109, 101, 114, 0, 0, 0, 1, 244, 0, 0, 0, 1, 3, 32, 0, 0, 0, 0, 0,
            0, 0, 255, 255, 255, 255, 1, 1, 1, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn fetch_response_v12_000() -> Result<()> {
    use tansu_sans_io::fetch_response::{FetchableTopicResponse, PartitionData};

    let _guard = init_tracing()?;

    let api_key = 1;
    let api_version = 12;

    let header = Header::Response { correlation_id: 8 };
    let body = FetchResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(Some(0))
        .session_id(Some(888121338))
        .responses(Some(
            [FetchableTopicResponse::default()
                .topic(Some("test".into()))
                .topic_id(None)
                .partitions(Some(
                    [
                        PartitionData::default()
                            .partition_index(1)
                            .error_code(3)
                            .high_watermark(-1)
                            .last_stable_offset(Some(-1))
                            .log_start_offset(Some(-1))
                            .diverging_epoch(None)
                            .current_leader(None)
                            .snapshot_id(None)
                            .aborted_transactions(Some([].into()))
                            .preferred_read_replica(Some(-1))
                            .records(None),
                        PartitionData::default()
                            .partition_index(0)
                            .error_code(3)
                            .high_watermark(-1)
                            .last_stable_offset(Some(-1))
                            .log_start_offset(Some(-1))
                            .diverging_epoch(None)
                            .current_leader(None)
                            .snapshot_id(None)
                            .aborted_transactions(Some([].into()))
                            .preferred_read_replica(Some(-1))
                            .records(None),
                        PartitionData::default()
                            .partition_index(2)
                            .error_code(3)
                            .high_watermark(-1)
                            .last_stable_offset(Some(-1))
                            .log_start_offset(Some(-1))
                            .diverging_epoch(None)
                            .current_leader(None)
                            .snapshot_id(None)
                            .aborted_transactions(Some([].into()))
                            .preferred_read_replica(Some(-1))
                            .records(None),
                    ]
                    .into(),
                ))]
            .into(),
        ))
        .node_endpoints(None)
        .into();

    let expected = vec![
        0, 0, 0, 135, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 52, 239, 167, 250, 2, 5, 116, 101, 115, 116,
        4, 0, 0, 0, 1, 0, 3, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0,
        0, 3, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 1, 255, 255, 255, 255, 1, 0, 0, 0, 0, 2, 0, 3, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 1, 255, 255, 255, 255, 1, 0, 0, 0,
    ];

    assert_eq!(
        expected,
        Frame::response(header, body, api_key, api_version)?
    );
    Ok(())
}

#[test]
fn fetch_response_v12_001() -> Result<()> {
    use tansu_sans_io::fetch_response::{FetchableTopicResponse, PartitionData};

    let _guard = init_tracing()?;

    let api_key = 1;
    let api_version = 12;

    let header = Header::Response { correlation_id: 8 };
    let body = FetchResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(Some(0))
        .session_id(Some(2086460889))
        .responses(Some(
            [FetchableTopicResponse::default()
                .topic(Some("test".into()))
                .topic_id(None)
                .partitions(Some(
                    [
                        PartitionData::default()
                            .partition_index(1)
                            .error_code(0)
                            .high_watermark(0)
                            .last_stable_offset(Some(0))
                            .log_start_offset(Some(0))
                            .diverging_epoch(None)
                            .current_leader(None)
                            .snapshot_id(None)
                            .aborted_transactions(None)
                            .preferred_read_replica(Some(-1))
                            .records(None),
                        PartitionData::default()
                            .partition_index(0)
                            .error_code(0)
                            .high_watermark(2)
                            .last_stable_offset(Some(2))
                            .log_start_offset(Some(0))
                            .diverging_epoch(None)
                            .current_leader(None)
                            .snapshot_id(None)
                            .aborted_transactions(None)
                            .preferred_read_replica(Some(-1))
                            .records(Some(
                                inflated::Frame {
                                    batches: [
                                        Batch {
                                            base_offset: 0,
                                            batch_length: 62,
                                            partition_leader_epoch: 0,
                                            magic: 2,
                                            crc: 2915995653,
                                            attributes: 0,
                                            last_offset_delta: 0,
                                            base_timestamp: 1707058170165,
                                            max_timestamp: 1707058170165,
                                            producer_id: 1,
                                            producer_epoch: 0,
                                            base_sequence: 1,
                                            records: [Record {
                                                length: 12,
                                                attributes: 0,
                                                timestamp_delta: 0,
                                                offset_delta: 0,
                                                key: Some(Bytes::from_static(&[97, 98, 99])),
                                                value: Some(Bytes::from_static(&[112, 113, 114])),
                                                headers: [].into(),
                                            }]
                                            .into(),
                                        },
                                        Batch {
                                            base_offset: 1,
                                            batch_length: 62,
                                            partition_leader_epoch: 0,
                                            magic: 2,
                                            crc: 2915995653,
                                            attributes: 0,
                                            last_offset_delta: 0,
                                            base_timestamp: 1707058170165,
                                            max_timestamp: 1707058170165,
                                            producer_id: 1,
                                            producer_epoch: 0,
                                            base_sequence: 1,
                                            records: [Record {
                                                length: 12,
                                                attributes: 0,
                                                timestamp_delta: 0,
                                                offset_delta: 0,
                                                key: Some(Bytes::from_static(&[97, 98, 99])),
                                                value: Some(Bytes::from_static(&[112, 113, 114])),
                                                headers: [].into(),
                                            }]
                                            .into(),
                                        },
                                    ]
                                    .into(),
                                }
                                .try_into()?,
                            )),
                        PartitionData::default()
                            .partition_index(2)
                            .error_code(0)
                            .high_watermark(0)
                            .last_stable_offset(Some(0))
                            .log_start_offset(Some(0))
                            .diverging_epoch(None)
                            .current_leader(None)
                            .snapshot_id(None)
                            .aborted_transactions(None)
                            .preferred_read_replica(Some(-1))
                            .records(None),
                    ]
                    .into(),
                ))]
            .into(),
        ))
        .node_endpoints(None)
        .into();

    let expected = vec![
        0, 0, 1, 28, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 124, 92, 221, 217, 2, 5, 116, 101, 115, 116,
        4, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0,
        0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 149, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 62, 0, 0, 0, 0, 2, 173, 206, 144, 5, 0, 0, 0, 0, 0, 0, 0, 0, 1, 141, 116, 152, 137, 53,
        0, 0, 1, 141, 116, 152, 137, 53, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 24,
        0, 0, 0, 6, 97, 98, 99, 6, 112, 113, 114, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 62, 0, 0, 0,
        0, 2, 173, 206, 144, 5, 0, 0, 0, 0, 0, 0, 0, 0, 1, 141, 116, 152, 137, 53, 0, 0, 1, 141,
        116, 152, 137, 53, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 24, 0, 0, 0, 6,
        97, 98, 99, 6, 112, 113, 114, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 1, 0, 0, 0,
    ];

    assert_eq!(
        expected,
        Frame::response(header, body, api_key, api_version)?
    );
    Ok(())
}

#[test]
fn fetch_response_v16_001() -> Result<()> {
    use tansu_sans_io::fetch_response::{
        EpochEndOffset, FetchableTopicResponse, LeaderIdAndEpoch, PartitionData, SnapshotId,
    };

    let _guard = init_tracing()?;

    let api_key = 1;
    let api_version = 16;

    let header = Header::Response { correlation_id: 8 };
    let body = FetchResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(Some(0))
        .session_id(Some(0))
        .responses(Some(
            [FetchableTopicResponse::default()
                .topic(None)
                .topic_id(Some([
                    28, 205, 172, 195, 142, 19, 71, 71, 182, 128, 13, 18, 65, 142, 210, 222,
                ]))
                .partitions(Some(
                    [PartitionData::default()
                        .partition_index(0)
                        .error_code(0)
                        .high_watermark(0)
                        .last_stable_offset(Some(1))
                        .log_start_offset(Some(-1))
                        .diverging_epoch(Some(EpochEndOffset::default().epoch(-1).end_offset(-1)))
                        .current_leader(Some(
                            LeaderIdAndEpoch::default().leader_id(0).leader_epoch(0),
                        ))
                        .snapshot_id(Some(SnapshotId::default().end_offset(-1).epoch(-1)))
                        .aborted_transactions(Some([].into()))
                        .preferred_read_replica(Some(0))
                        .records(Some(
                            inflated::Frame {
                                batches: [Batch {
                                    base_offset: 0,
                                    batch_length: 61,
                                    partition_leader_epoch: -1,
                                    magic: 2,
                                    crc: 2576291984,
                                    attributes: 0,
                                    last_offset_delta: 0,
                                    base_timestamp: 1721989616694,
                                    max_timestamp: 1721989616694,
                                    producer_id: 1,
                                    producer_epoch: 0,
                                    base_sequence: 0,
                                    records: [Record {
                                        length: 11,
                                        attributes: 0,
                                        timestamp_delta: 0,
                                        offset_delta: 0,
                                        key: None,
                                        value: Some(Bytes::from_static(&[112, 111, 105, 117, 121])),
                                        headers: [].into(),
                                    }]
                                    .into(),
                                }]
                                .into(),
                            }
                            .try_into()?,
                        ))]
                    .into(),
                ))]
            .into(),
        ))
        .node_endpoints(Some([].into()))
        .into();

    let expected = vec![
        0, 0, 0, 189, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 28, 205, 172, 195, 142, 19,
        71, 71, 182, 128, 13, 18, 65, 142, 210, 222, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 1, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 74, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 61, 255, 255, 255, 255, 2, 153, 143, 24, 144, 0, 0, 0, 0, 0, 0, 0,
        0, 1, 144, 238, 148, 84, 54, 0, 0, 1, 144, 238, 148, 84, 54, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 1, 22, 0, 0, 0, 1, 10, 112, 111, 105, 117, 121, 0, 3, 0, 13, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 1, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
        13, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 1, 0, 1, 1,
    ];

    assert_eq!(
        expected,
        Frame::response(header, body, api_key, api_version)?
    );
    Ok(())
}

#[test]
fn fetch_response_v16_002() -> Result<()> {
    use tansu_sans_io::fetch_response::{
        EpochEndOffset, FetchableTopicResponse, LeaderIdAndEpoch, PartitionData, SnapshotId,
    };

    let _guard = init_tracing()?;

    let api_key = 1;
    let api_version = 16;

    let header = Header::Response { correlation_id: 8 };
    let body = FetchResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(Some(0))
        .session_id(Some(0))
        .responses(Some(
            [FetchableTopicResponse::default()
                .topic(None)
                .topic_id(Some([
                    28, 205, 172, 195, 142, 19, 71, 71, 182, 128, 13, 18, 65, 142, 210, 222,
                ]))
                .partitions(Some(
                    [PartitionData::default()
                        .partition_index(0)
                        .error_code(0)
                        .high_watermark(0)
                        .last_stable_offset(Some(1))
                        .log_start_offset(Some(-1))
                        .diverging_epoch(Some(EpochEndOffset::default().epoch(-1).end_offset(-1)))
                        .current_leader(Some(
                            LeaderIdAndEpoch::default().leader_id(0).leader_epoch(0),
                        ))
                        .snapshot_id(Some(SnapshotId::default().end_offset(-1).epoch(-1)))
                        .aborted_transactions(Some([].into()))
                        .preferred_read_replica(Some(0))
                        .records(Some(
                            inflated::Frame {
                                batches: [Batch {
                                    base_offset: 0,
                                    batch_length: 61,
                                    partition_leader_epoch: -1,
                                    magic: 2,
                                    crc: 2576291984,
                                    attributes: 0,
                                    last_offset_delta: 0,
                                    base_timestamp: 1721989616694,
                                    max_timestamp: 1721989616694,
                                    producer_id: 1,
                                    producer_epoch: 0,
                                    base_sequence: 0,
                                    records: [Record {
                                        length: 11,
                                        attributes: 0,
                                        timestamp_delta: 0,
                                        offset_delta: 0,
                                        key: None,
                                        value: Some(Bytes::from_static(&[112, 111, 105, 117, 121])),
                                        headers: [].into(),
                                    }]
                                    .into(),
                                }]
                                .into(),
                            }
                            .try_into()?,
                        ))]
                    .into(),
                ))]
            .into(),
        ))
        .node_endpoints(None)
        .into();

    let expected = vec![
        0, 0, 0, 186, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 28, 205, 172, 195, 142, 19,
        71, 71, 182, 128, 13, 18, 65, 142, 210, 222, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 1, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 74, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 61, 255, 255, 255, 255, 2, 153, 143, 24, 144, 0, 0, 0, 0, 0, 0, 0,
        0, 1, 144, 238, 148, 84, 54, 0, 0, 1, 144, 238, 148, 84, 54, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 1, 22, 0, 0, 0, 1, 10, 112, 111, 105, 117, 121, 0, 3, 0, 13, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 1, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
        13, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0,
    ];

    assert_eq!(
        expected,
        Frame::response(header, body, api_key, api_version)?
    );
    Ok(())
}

#[test]
fn find_coordinator_request_v1_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 10,
        api_version: 1,
        correlation_id: 0,
        client_id: None,
    };

    let body = FindCoordinatorRequest::default()
        .key(Some("abcdef".into()))
        .key_type(Some(0))
        .coordinator_keys(None)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 19, 0, 10, 0, 1, 0, 0, 0, 0, 255, 255, 0, 6, 97, 98, 99, 100, 101, 102, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn find_coordinator_response_v1_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Response { correlation_id: 0 };
    let body = FindCoordinatorResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(Some(0))
        .error_message(None)
        .node_id(Some(1002))
        .host(Some("ip-10-2-91-66.eu-west-1.compute.internal".into()))
        .port(Some(9092))
        .coordinators(None)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 62, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 0, 0, 3, 234, 0, 40, 105, 112, 45,
            49, 48, 45, 50, 45, 57, 49, 45, 54, 54, 46, 101, 117, 45, 119, 101, 115, 116, 45, 49,
            46, 99, 111, 109, 112, 117, 116, 101, 46, 105, 110, 116, 101, 114, 110, 97, 108, 0, 0,
            35, 132,
        ],
        Frame::response(header, body, FindCoordinatorResponse::KEY, 1)?,
    );

    Ok(())
}

#[test]
fn heartbeat_request_v4_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 12,
        api_version: 4,
        correlation_id: 10288,
        client_id: Some("console-consumer".into()),
    };

    let body = HeartbeatRequest::default()
        .group_id("test-consumer-group".into())
        .generation_id(0)
        .member_id("1000".into())
        .group_instance_id(None)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 58, 0, 12, 0, 4, 0, 0, 40, 48, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45,
            99, 111, 110, 115, 117, 109, 101, 114, 0, 20, 116, 101, 115, 116, 45, 99, 111, 110,
            115, 117, 109, 101, 114, 45, 103, 114, 111, 117, 112, 0, 0, 0, 0, 5, 49, 48, 48, 48, 0,
            0,
        ],
        Frame::request(header, body)?,
    );
    Ok(())
}

#[test]
fn init_producer_id_request_v4_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 22,
        api_version: 4,
        correlation_id: 2,
        client_id: Some("console-producer".into()),
    };

    let body = InitProducerIdRequest::default()
        .transactional_id(None)
        .transaction_timeout_ms(2147483647)
        .producer_id(Some(-1))
        .producer_epoch(Some(-1))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 43, 0, 22, 0, 4, 0, 0, 0, 2, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
            114, 111, 100, 117, 99, 101, 114, 0, 0, 127, 255, 255, 255, 255, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn join_group_request_v5_000() -> Result<()> {
    let _guard = init_tracing()?;

    let range_metadata =
        Bytes::from_static(b"\0\x03\0\0\0\x01\0\tbenchmark\0\0\0\0\0\0\0\0\xff\xff\xff\xff\0\0");
    let roundrobin_metadata =
        Bytes::from_static(b"\0\x03\0\0\0\x01\0\tbenchmark\0\0\0\0\0\0\0\0\xff\xff\xff\xff\0\0");

    let header = Header::Request {
        api_key: 11,
        api_version: 5,
        correlation_id: 3,
        client_id: Some("rdkafka".into()),
    };

    let body = JoinGroupRequest::default()
        .group_id("example_consumer_group_id".into())
        .session_timeout_ms(6000)
        .rebalance_timeout_ms(Some(300000))
        .member_id("".into())
        .group_instance_id(None)
        .protocol_type("consumer".into())
        .protocols(Some(
            [
                JoinGroupRequestProtocol::default()
                    .name("range".into())
                    .metadata(range_metadata),
                JoinGroupRequestProtocol::default()
                    .name("roundrobin".into())
                    .metadata(roundrobin_metadata),
            ]
            .into(),
        ))
        .reason(None)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 159, 0, 11, 0, 5, 0, 0, 0, 3, 0, 7, 114, 100, 107, 97, 102, 107, 97, 0, 25,
            101, 120, 97, 109, 112, 108, 101, 95, 99, 111, 110, 115, 117, 109, 101, 114, 95, 103,
            114, 111, 117, 112, 95, 105, 100, 0, 0, 23, 112, 0, 4, 147, 224, 0, 0, 255, 255, 0, 8,
            99, 111, 110, 115, 117, 109, 101, 114, 0, 0, 0, 2, 0, 5, 114, 97, 110, 103, 101, 0, 0,
            0, 31, 0, 3, 0, 0, 0, 1, 0, 9, 98, 101, 110, 99, 104, 109, 97, 114, 107, 0, 0, 0, 0, 0,
            0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 10, 114, 111, 117, 110, 100, 114, 111, 98, 105,
            110, 0, 0, 0, 31, 0, 3, 0, 0, 0, 1, 0, 9, 98, 101, 110, 99, 104, 109, 97, 114, 107, 0,
            0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 0, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn join_group_response_v5_000() -> Result<()> {
    let _guard = init_tracing()?;

    let api_key = 11;
    let api_version = 5;

    let metadata =
        Bytes::from_static(b"\0\x03\0\0\0\x01\0\tbenchmark\0\0\0\0\0\0\0\0\xff\xff\xff\xff\0\0");

    let header = Header::Response { correlation_id: 4 };

    let body = JoinGroupResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(0)
        .generation_id(1)
        .protocol_type(None)
        .protocol_name(Some("range".into()))
        .leader("rdkafka-499e5770-375e-4990-bf84-a39634e3bfe4".into())
        .skip_assignment(None)
        .member_id("rdkafka-499e5770-375e-4990-bf84-a39634e3bfe4".into())
        .members(Some(
            [JoinGroupResponseMember::default()
                .member_id("rdkafka-499e5770-375e-4990-bf84-a39634e3bfe4".into())
                .group_instance_id(None)
                .metadata(metadata)]
            .into(),
        ))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 200, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 5, 114, 97, 110, 103, 101,
            0, 44, 114, 100, 107, 97, 102, 107, 97, 45, 52, 57, 57, 101, 53, 55, 55, 48, 45, 51,
            55, 53, 101, 45, 52, 57, 57, 48, 45, 98, 102, 56, 52, 45, 97, 51, 57, 54, 51, 52, 101,
            51, 98, 102, 101, 52, 0, 44, 114, 100, 107, 97, 102, 107, 97, 45, 52, 57, 57, 101, 53,
            55, 55, 48, 45, 51, 55, 53, 101, 45, 52, 57, 57, 48, 45, 98, 102, 56, 52, 45, 97, 51,
            57, 54, 51, 52, 101, 51, 98, 102, 101, 52, 0, 0, 0, 1, 0, 44, 114, 100, 107, 97, 102,
            107, 97, 45, 52, 57, 57, 101, 53, 55, 55, 48, 45, 51, 55, 53, 101, 45, 52, 57, 57, 48,
            45, 98, 102, 56, 52, 45, 97, 51, 57, 54, 51, 52, 101, 51, 98, 102, 101, 52, 255, 255,
            0, 0, 0, 31, 0, 3, 0, 0, 0, 1, 0, 9, 98, 101, 110, 99, 104, 109, 97, 114, 107, 0, 0, 0,
            0, 0, 0, 0, 0, 255, 255, 255, 255, 0, 0,
        ],
        Frame::response(header, body, api_key, api_version)?,
    );

    Ok(())
}

#[test]
fn list_groups_request_v4_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 16,
        api_version: 4,
        correlation_id: 84,
        client_id: Some("adminclient-1".into()),
    };

    let body = ListGroupsRequest::default()
        .states_filter(Some([].into()))
        .types_filter(Some([].into()))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 26, 0, 16, 0, 4, 0, 0, 0, 84, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105,
            101, 110, 116, 45, 49, 0, 1, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn list_offsets_response_v0_000() -> Result<()> {
    use tansu_sans_io::list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
    };

    let _guard = init_tracing()?;

    let header = Header::Response { correlation_id: 0 };

    let body = ListOffsetsResponse::default()
        .throttle_time_ms(None)
        .topics(Some(
            [ListOffsetsTopicResponse::default()
                .name("abcabcabcab".into())
                .partitions(Some(
                    [ListOffsetsPartitionResponse::default()
                        .partition_index(1)
                        .error_code(0)
                        .old_style_offsets(Some([1189284, 1174012, 1164900, 0].into()))
                        .timestamp(None)
                        .offset(None)
                        .leader_epoch(None)]
                    .into(),
                ))]
            .into(),
        ))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 67, 0, 0, 0, 0, 0, 0, 0, 1, 0, 11, 97, 98, 99, 97, 98, 99, 97, 98, 99, 97, 98,
            0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 18, 37, 164, 0, 0, 0, 0, 0,
            17, 233, 252, 0, 0, 0, 0, 0, 17, 198, 100, 0, 0, 0, 0, 0, 0, 0, 0,
        ],
        Frame::response(header, body, ListOffsetsResponse::KEY, 0)?,
    );

    Ok(())
}

#[test]
fn list_partition_reassignments_request_v0_000() -> Result<()> {
    use tansu_sans_io::list_partition_reassignments_request::ListPartitionReassignmentsTopics;

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 46,
        api_version: 0,
        correlation_id: 7,
        client_id: Some("adminclient-1".into()),
    };

    let body = ListPartitionReassignmentsRequest::default()
        .timeout_ms(30_000)
        .topics(Some(
            [ListPartitionReassignmentsTopics::default()
                .name("test".into())
                .partition_indexes(Some([1, 0, 2].into()))]
            .into(),
        ))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 49, 0, 46, 0, 0, 0, 0, 0, 7, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
            110, 116, 45, 49, 0, 0, 0, 117, 48, 2, 5, 116, 101, 115, 116, 4, 0, 0, 0, 1, 0, 0, 0,
            0, 0, 0, 0, 2, 0, 0,
        ],
        Frame::request(header, body)?,
    );
    Ok(())
}

#[test]
fn metadata_request_v12_000() -> Result<()> {
    use tansu_sans_io::metadata_request::MetadataRequestTopic;

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 3,
        api_version: 12,
        correlation_id: 5,
        client_id: Some("console-producer".into()),
    };

    let body = MetadataRequest::default()
        .topics(Some(
            [MetadataRequestTopic::default()
                .topic_id(Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]))
                .name(Some("test".into()))]
            .into(),
        ))
        .allow_auto_topic_creation(Some(true))
        .include_cluster_authorized_operations(None)
        .include_topic_authorized_operations(Some(false))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 53, 0, 3, 0, 12, 0, 0, 0, 5, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
            114, 111, 100, 117, 99, 101, 114, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            5, 116, 101, 115, 116, 0, 1, 0, 0,
        ],
        Frame::request(header, body)?,
    );
    Ok(())
}

#[test]
fn metadata_request_v7_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 3,
        api_version: 7,
        correlation_id: 0,
        client_id: Some("sarama".into()),
    };

    let body = MetadataRequest::default()
        .topics(None)
        .allow_auto_topic_creation(Some(false))
        .include_cluster_authorized_operations(None)
        .include_topic_authorized_operations(None)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 21, 0, 3, 0, 7, 0, 0, 0, 0, 0, 6, 115, 97, 114, 97, 109, 97, 255, 255, 255,
            255, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn metadata_response_v7_0000() -> Result<()> {
    use tansu_sans_io::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };

    let _guard = init_tracing()?;

    let header = Header::Response { correlation_id: 0 };
    let body = MetadataResponse::default()
        .throttle_time_ms(Some(0))
        .brokers(Some(
            [MetadataResponseBroker::default()
                .node_id(1)
                .host("localhost".into())
                .port(9092)
                .rack(None)]
            .into(),
        ))
        .cluster_id(Some("5L6g3nShT-eMCtK--X86sw".into()))
        .controller_id(Some(1))
        .topics(Some(
            [MetadataResponseTopic::default()
                .error_code(0)
                .name(Some("test".into()))
                .topic_id(None)
                .is_internal(Some(false))
                .partitions(Some(
                    [
                        MetadataResponsePartition::default()
                            .error_code(0)
                            .partition_index(1)
                            .leader_id(1)
                            .leader_epoch(Some(0))
                            .replica_nodes(Some([1].into()))
                            .isr_nodes(Some([1].into()))
                            .offline_replicas(Some([].into())),
                        MetadataResponsePartition::default()
                            .error_code(0)
                            .partition_index(2)
                            .leader_id(1)
                            .leader_epoch(Some(0))
                            .replica_nodes(Some([1].into()))
                            .isr_nodes(Some([1].into()))
                            .offline_replicas(Some([].into())),
                        MetadataResponsePartition::default()
                            .error_code(0)
                            .partition_index(0)
                            .leader_id(1)
                            .leader_epoch(Some(0))
                            .replica_nodes(Some([1].into()))
                            .isr_nodes(Some([1].into()))
                            .offline_replicas(Some([].into())),
                    ]
                    .into(),
                ))
                .topic_authorized_operations(None)]
            .into(),
        ))
        .cluster_authorized_operations(None)
        .into();

    let api_key = 3;
    let api_version = 7;

    assert_eq!(
        vec![
            0, 0, 0, 180, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 9, 108, 111, 99, 97,
            108, 104, 111, 115, 116, 0, 0, 35, 132, 255, 255, 0, 22, 53, 76, 54, 103, 51, 110, 83,
            104, 84, 45, 101, 77, 67, 116, 75, 45, 45, 88, 56, 54, 115, 119, 0, 0, 0, 1, 0, 0, 0,
            1, 0, 0, 0, 4, 116, 101, 115, 116, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0,
            0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0,
            0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0,
            0, 0, 0,
        ],
        Frame::response(header, body, MetadataResponse::KEY, api_version)?,
    );

    Ok(())
}

#[test]
fn metadata_response_v7_0001() -> Result<()> {
    use tansu_sans_io::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };

    let _guard = init_tracing()?;

    let header = Header::Response { correlation_id: 0 };

    // note that topic_id is not part of a v7 response
    let body = MetadataResponse::default()
        .throttle_time_ms(Some(0))
        .brokers(Some(
            [MetadataResponseBroker::default()
                .node_id(1)
                .host("localhost".into())
                .port(9092)
                .rack(None)]
            .into(),
        ))
        .cluster_id(Some("5L6g3nShT-eMCtK--X86sw".into()))
        .controller_id(Some(1))
        .topics(Some(
            [MetadataResponseTopic::default()
                .error_code(0)
                .name(Some("test".into()))
                .topic_id(Some([
                    118, 154, 146, 249, 19, 231, 73, 33, 136, 41, 108, 64, 151, 75, 30, 65,
                ]))
                .is_internal(Some(false))
                .partitions(Some(
                    [
                        MetadataResponsePartition::default()
                            .error_code(0)
                            .partition_index(1)
                            .leader_id(1)
                            .leader_epoch(Some(0))
                            .replica_nodes(Some([1].into()))
                            .isr_nodes(Some([1].into()))
                            .offline_replicas(Some([].into())),
                        MetadataResponsePartition::default()
                            .error_code(0)
                            .partition_index(2)
                            .leader_id(1)
                            .leader_epoch(Some(0))
                            .replica_nodes(Some([1].into()))
                            .isr_nodes(Some([1].into()))
                            .offline_replicas(Some([].into())),
                        MetadataResponsePartition::default()
                            .error_code(0)
                            .partition_index(0)
                            .leader_id(1)
                            .leader_epoch(Some(0))
                            .replica_nodes(Some([1].into()))
                            .isr_nodes(Some([1].into()))
                            .offline_replicas(Some([].into())),
                    ]
                    .into(),
                ))
                .topic_authorized_operations(None)]
            .into(),
        ))
        .cluster_authorized_operations(None)
        .into();

    let api_key = 3;
    let api_version = 7;

    assert_eq!(
        vec![
            0, 0, 0, 180, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 9, 108, 111, 99, 97,
            108, 104, 111, 115, 116, 0, 0, 35, 132, 255, 255, 0, 22, 53, 76, 54, 103, 51, 110, 83,
            104, 84, 45, 101, 77, 67, 116, 75, 45, 45, 88, 56, 54, 115, 119, 0, 0, 0, 1, 0, 0, 0,
            1, 0, 0, 0, 4, 116, 101, 115, 116, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0,
            0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0,
            0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0,
            0, 0, 0,
        ],
        Frame::response(header, body, api_key, api_version)?,
    );

    Ok(())
}

#[test]
fn metadata_request_v12_001() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 3,
        api_version: 12,
        correlation_id: 1,
        client_id: Some("console-producer".into()),
    };

    let body = MetadataRequest::default()
        .topics(Some([].into()))
        .allow_auto_topic_creation(Some(true))
        .include_cluster_authorized_operations(None)
        .include_topic_authorized_operations(Some(false))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 31, 0, 3, 0, 12, 0, 0, 0, 1, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
            114, 111, 100, 117, 99, 101, 114, 0, 1, 1, 0, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn metadata_request_v12_002() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 3,
        api_version: 12,
        correlation_id: 1,
        client_id: Some("console-producer".into()),
    };

    let body = MetadataRequest::default()
        .topics(Some([].into()))
        .allow_auto_topic_creation(Some(true))
        .include_cluster_authorized_operations(Some(false))
        .include_topic_authorized_operations(Some(false))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 31, 0, 3, 0, 12, 0, 0, 0, 1, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
            114, 111, 100, 117, 99, 101, 114, 0, 1, 1, 0, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn metadata_response_v12_0000() -> Result<()> {
    use tansu_sans_io::metadata_response::{MetadataResponseBroker, MetadataResponseTopic};

    let _guard = init_tracing()?;

    let header = Header::Response { correlation_id: 5 };

    let body = MetadataResponse::default()
        .throttle_time_ms(Some(0))
        .brokers(Some(vec![
            MetadataResponseBroker::default()
                .node_id(0)
                .host("kafka-server".into())
                .port(9092)
                .rack(None),
        ]))
        .cluster_id(Some("RvQwrYegSUCkIPkaiAZQlQ".into()))
        .controller_id(Some(0))
        .topics(Some(vec![
            MetadataResponseTopic::default()
                .error_code(3)
                .name(Some("test".into()))
                .topic_id(Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]))
                .is_internal(Some(false))
                .partitions(Some(vec![]))
                .topic_authorized_operations(Some(-2147483648)),
        ]))
        .cluster_authorized_operations(None)
        .into();

    let api_key = 3;
    let api_version = 12;

    assert_eq!(
        vec![
            0, 0, 0, 92, 0, 0, 0, 5, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 13, 107, 97, 102, 107, 97, 45,
            115, 101, 114, 118, 101, 114, 0, 0, 35, 132, 0, 0, 23, 82, 118, 81, 119, 114, 89, 101,
            103, 83, 85, 67, 107, 73, 80, 107, 97, 105, 65, 90, 81, 108, 81, 0, 0, 0, 0, 2, 0, 3,
            5, 116, 101, 115, 116, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 128, 0, 0,
            0, 0, 0
        ],
        Frame::response(header, body, api_key, api_version)?,
    );

    Ok(())
}

#[test]
fn metadata_response_v12_0001() -> Result<()> {
    use tansu_sans_io::metadata_response::{MetadataResponseBroker, MetadataResponseTopic};

    let _guard = init_tracing()?;

    let header = Header::Response { correlation_id: 5 };

    let body = MetadataResponse::default()
        .throttle_time_ms(Some(0))
        .brokers(Some(vec![
            MetadataResponseBroker::default()
                .node_id(0)
                .host("kafka-server".into())
                .port(9092)
                .rack(None),
        ]))
        .cluster_id(Some("RvQwrYegSUCkIPkaiAZQlQ".into()))
        .controller_id(Some(0))
        .topics(Some(vec![
            MetadataResponseTopic::default()
                .error_code(3)
                .name(Some("test".into()))
                .topic_id(Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]))
                .is_internal(Some(false))
                .partitions(Some(vec![]))
                .topic_authorized_operations(Some(-2147483648)),
        ]))
        .cluster_authorized_operations(Some(-1))
        .into();

    let api_key = 3;
    let api_version = 12;

    assert_eq!(
        vec![
            0, 0, 0, 92, 0, 0, 0, 5, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 13, 107, 97, 102, 107, 97, 45,
            115, 101, 114, 118, 101, 114, 0, 0, 35, 132, 0, 0, 23, 82, 118, 81, 119, 114, 89, 101,
            103, 83, 85, 67, 107, 73, 80, 107, 97, 105, 65, 90, 81, 108, 81, 0, 0, 0, 0, 2, 0, 3,
            5, 116, 101, 115, 116, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 128, 0, 0,
            0, 0, 0
        ],
        Frame::response(header, body, api_key, api_version)?,
    );

    Ok(())
}

#[test]
fn offset_fetch_request_v3_000() -> Result<()> {
    use tansu_sans_io::offset_fetch_request::OffsetFetchRequestTopic;

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 9,
        api_version: 3,
        correlation_id: 0,
        client_id: None,
    };

    let body = OffsetFetchRequest::default()
        .group_id(Some("abc".into()))
        .topics(Some(
            [
                OffsetFetchRequestTopic::default()
                    .name("test2".into())
                    .partition_indexes(Some([3, 4, 5].into())),
                OffsetFetchRequestTopic::default()
                    .name("test1".into())
                    .partition_indexes(Some([0, 1, 2].into())),
            ]
            .into(),
        ))
        .groups(None)
        .require_stable(None)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 65, 0, 9, 0, 3, 0, 0, 0, 0, 255, 255, 0, 3, 97, 98, 99, 0, 0, 0, 2, 0, 5, 116,
            101, 115, 116, 50, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 5, 116, 101, 115,
            116, 49, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn offset_for_leader_request_v0_000() -> Result<()> {
    use tansu_sans_io::offset_for_leader_epoch_request::OffsetForLeaderTopic;

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 23,
        api_version: 0,
        correlation_id: 0,
        client_id: None,
    };

    let body = OffsetForLeaderEpochRequest::default()
        .replica_id(None)
        .topics(Some(
            [OffsetForLeaderTopic::default()
                .topic("abcabcabcab".into())
                .partitions(Some([].into()))]
            .into(),
        ))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 31, 0, 23, 0, 0, 0, 0, 0, 0, 255, 255, 0, 0, 0, 1, 0, 11, 97, 98, 99, 97, 98,
            99, 97, 98, 99, 97, 98, 0, 0, 0, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn produce_request_v9_000() -> Result<()> {
    use tansu_sans_io::produce_request::{PartitionProduceData, TopicProduceData};

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 0,
        api_version: 9,
        correlation_id: 6,
        client_id: Some("console-producer".into()),
    };

    let body = ProduceRequest::default()
        .transactional_id(None)
        .acks(-1)
        .timeout_ms(1500)
        .topic_data(Some(
            [TopicProduceData::default()
                .name("test".into())
                .partition_data(Some(
                    [PartitionProduceData::default().index(0).records(Some(
                        inflated::Frame {
                            batches: [Batch {
                                base_offset: 0,
                                batch_length: 59,
                                partition_leader_epoch: -1,
                                magic: 2,
                                crc: 1126819645,
                                attributes: 0,
                                last_offset_delta: 0,
                                base_timestamp: 1707058170165,
                                max_timestamp: 1707058170165,
                                producer_id: 1,
                                producer_epoch: 0,
                                base_sequence: 1,
                                records: [Record {
                                    length: 9,
                                    attributes: 0,
                                    timestamp_delta: 0,
                                    offset_delta: 0,
                                    key: None,
                                    value: Some(Bytes::from_static(&[100, 101, 102])),
                                    headers: [].into(),
                                }]
                                .into(),
                            }]
                            .into(),
                        }
                        .try_into()?,
                    ))]
                    .into(),
                ))]
            .into(),
        ))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 120, 0, 0, 0, 9, 0, 0, 0, 6, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
            114, 111, 100, 117, 99, 101, 114, 0, 0, 255, 255, 0, 0, 5, 220, 2, 5, 116, 101, 115,
            116, 2, 0, 0, 0, 0, 72, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 59, 255, 255, 255, 255, 2, 67,
            41, 231, 61, 0, 0, 0, 0, 0, 0, 0, 0, 1, 141, 116, 152, 137, 53, 0, 0, 1, 141, 116, 152,
            137, 53, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 18, 0, 0, 0, 1, 6, 100,
            101, 102, 0, 0, 0, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn produce_response_v9_000() -> Result<()> {
    use tansu_sans_io::produce_response::{PartitionProduceResponse, TopicProduceResponse};

    let _guard = init_tracing()?;

    let header = Header::Response { correlation_id: 6 };
    let body = ProduceResponse::default()
        .responses(Some(
            [TopicProduceResponse::default()
                .name("test".into())
                .partition_responses(Some(
                    [PartitionProduceResponse::default()
                        .index(0)
                        .error_code(0)
                        .base_offset(2)
                        .log_append_time_ms(Some(-1))
                        .log_start_offset(Some(0))
                        .record_errors(Some([].into()))
                        .error_message(None)
                        .current_leader(None)]
                    .into(),
                ))]
            .into(),
        ))
        .node_endpoints(None)
        .throttle_time_ms(Some(0))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 51, 0, 0, 0, 6, 0, 2, 5, 116, 101, 115, 116, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 2, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
            0, 0, 0, 0, 0,
        ],
        Frame::response(header, body, 0, 9)?
    );

    Ok(())
}

#[test]
fn describe_topic_partitions_request_v0_000() -> Result<()> {
    use tansu_sans_io::describe_topic_partitions_request::TopicRequest;

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 75,
        api_version: 0,
        correlation_id: 5,
        client_id: Some("adminclient-1".into()),
    };

    let body = DescribeTopicPartitionsRequest::default()
        .topics(Some([TopicRequest::default().name("test".into())].into()))
        .response_partition_limit(2000)
        .cursor(None)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 37, 0, 75, 0, 0, 0, 0, 0, 5, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
            110, 116, 45, 49, 0, 2, 5, 116, 101, 115, 116, 0, 0, 0, 7, 208, 255, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn describe_topic_partitions_response_v0_000() -> Result<()> {
    use tansu_sans_io::describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    };

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 126, 0, 0, 0, 5, 0, 0, 0, 0, 0, 2, 0, 0, 5, 116, 101, 115, 116, 113, 142, 248, 9,
        90, 152, 68, 142, 161, 218, 25, 210, 166, 234, 204, 62, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
        0, 0, 0, 0, 2, 0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0,
        0, 0, 2, 0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0,
        2, 0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 13, 248, 0, 255, 0,
    ];

    let api_key = 75;
    let api_version = 0;

    let header = Header::Response { correlation_id: 5 };

    let body = DescribeTopicPartitionsResponse::default()
        .throttle_time_ms(0)
        .topics(Some(
            [DescribeTopicPartitionsResponseTopic::default()
                .error_code(0)
                .name(Some("test".into()))
                .topic_id([
                    113, 142, 248, 9, 90, 152, 68, 142, 161, 218, 25, 210, 166, 234, 204, 62,
                ])
                .is_internal(false)
                .partitions(Some(
                    [
                        DescribeTopicPartitionsResponsePartition::default()
                            .error_code(0)
                            .partition_index(0)
                            .leader_id(1)
                            .leader_epoch(0)
                            .replica_nodes(Some([1].into()))
                            .isr_nodes(Some([1].into()))
                            .eligible_leader_replicas(Some([].into()))
                            .last_known_elr(Some([].into()))
                            .offline_replicas(Some([].into())),
                        DescribeTopicPartitionsResponsePartition::default()
                            .error_code(0)
                            .partition_index(1)
                            .leader_id(1)
                            .leader_epoch(0)
                            .replica_nodes(Some([1].into()))
                            .isr_nodes(Some([1].into()))
                            .eligible_leader_replicas(Some([].into()))
                            .last_known_elr(Some([].into()))
                            .offline_replicas(Some([].into())),
                        DescribeTopicPartitionsResponsePartition::default()
                            .error_code(0)
                            .partition_index(2)
                            .leader_id(1)
                            .leader_epoch(0)
                            .replica_nodes(Some([1].into()))
                            .isr_nodes(Some([1].into()))
                            .eligible_leader_replicas(Some([].into()))
                            .last_known_elr(Some([].into()))
                            .offline_replicas(Some([].into())),
                    ]
                    .into(),
                ))
                .topic_authorized_operations(3576)]
            .into(),
        ))
        .next_cursor(None)
        .into();

    assert_eq!(v, Frame::response(header, body, api_key, api_version)?);

    Ok(())
}
