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

use bytes::Bytes;
use serde::Deserialize;
use std::{fs::File, io::Cursor, sync::Arc, thread};
use tansu_kafka_sans_io::{
    de::Decoder,
    describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsResult, DescribeConfigsSynonym,
    },
    describe_groups_response::DescribedGroup,
    fetch_response::{
        EpochEndOffset, FetchableTopicResponse, LeaderIdAndEpoch, PartitionData, SnapshotId,
    },
    join_group_response::JoinGroupResponseMember,
    list_transactions_response::TransactionState,
    metadata_request::MetadataRequestTopic,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    offset_fetch_response::{OffsetFetchResponsePartition, OffsetFetchResponseTopic},
    record::{self, deflated, inflated, Record},
    Body, Error, ErrorCode, Frame, Header, Result,
};
use tracing::{debug, subscriber::DefaultGuard};
use tracing_subscriber::fmt::format::FmtSpan;

#[cfg(miri)]
fn init_tracing() -> Result<()> {
    Ok(())
}

#[cfg(not(miri))]
fn init_tracing() -> Result<DefaultGuard> {
    Ok(tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_span_events(FmtSpan::ACTIVE)
            .with_writer(
                thread::current()
                    .name()
                    .ok_or(Error::Message(String::from("unnamed thread")))
                    .and_then(|name| {
                        File::create(format!(
                            "../logs/{}/decode-{name}.log",
                            env!("CARGO_PKG_NAME")
                        ))
                        .map_err(Into::into)
                    })
                    .map(Arc::new)?,
            )
            .finish(),
    ))
}

#[test]
fn api_versions_request_v3_000() -> Result<()> {
    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 52, 0, 18, 0, 3, 0, 0, 0, 3, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
        114, 111, 100, 117, 99, 101, 114, 0, 18, 97, 112, 97, 99, 104, 101, 45, 107, 97, 102, 107,
        97, 45, 106, 97, 118, 97, 6, 51, 46, 54, 46, 49, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 52,
            header: Header::Request {
                api_key: 18,
                api_version: 3,
                correlation_id: 3,
                client_id: Some("console-producer".into()),
            },
            body: Body::ApiVersionsRequest {
                client_software_name: Some("apache-kafka-java".into()),
                client_software_version: Some("3.6.1".into()),
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn api_versions_response_v1_000() -> Result<()> {
    use tansu_kafka_sans_io::api_versions_response::ApiVersion;

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 242, 0, 0, 0, 0, 0, 0, 0, 0, 0, 38, 0, 0, 0, 0, 0, 5, 0, 1, 0, 0, 0, 6, 0, 2, 0,
        0, 0, 2, 0, 3, 0, 0, 0, 5, 0, 4, 0, 0, 0, 1, 0, 5, 0, 0, 0, 0, 0, 6, 0, 0, 0, 4, 0, 7, 0,
        0, 0, 1, 0, 8, 0, 0, 0, 3, 0, 9, 0, 0, 0, 3, 0, 10, 0, 0, 0, 1, 0, 11, 0, 0, 0, 2, 0, 12,
        0, 0, 0, 1, 0, 13, 0, 0, 0, 1, 0, 14, 0, 0, 0, 1, 0, 15, 0, 0, 0, 1, 0, 16, 0, 0, 0, 1, 0,
        17, 0, 0, 0, 1, 0, 18, 0, 0, 0, 1, 0, 19, 0, 0, 0, 2, 0, 20, 0, 0, 0, 1, 0, 21, 0, 0, 0, 0,
        0, 22, 0, 0, 0, 0, 0, 23, 0, 0, 0, 0, 0, 24, 0, 0, 0, 0, 0, 25, 0, 0, 0, 0, 0, 26, 0, 0, 0,
        0, 0, 27, 0, 0, 0, 0, 0, 28, 0, 0, 0, 0, 0, 29, 0, 0, 0, 0, 0, 30, 0, 0, 0, 0, 0, 31, 0, 0,
        0, 0, 0, 32, 0, 0, 0, 0, 0, 33, 0, 0, 0, 0, 0, 34, 0, 0, 0, 0, 0, 35, 0, 0, 0, 0, 0, 36, 0,
        0, 0, 0, 0, 37, 0, 0, 0, 0, 0, 0, 0, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::response(&mut c, 18, 1);

    assert_eq!(
        Frame {
            size: 242,
            header: Header::Response { correlation_id: 0 },
            body: Body::ApiVersionsResponse {
                finalized_features: None,
                finalized_features_epoch: None,
                supported_features: None,
                zk_migration_ready: None,
                error_code: 0,
                api_keys: Some(
                    [
                        ApiVersion {
                            api_key: 0,
                            min_version: 0,
                            max_version: 5
                        },
                        ApiVersion {
                            api_key: 1,
                            min_version: 0,
                            max_version: 6
                        },
                        ApiVersion {
                            api_key: 2,
                            min_version: 0,
                            max_version: 2
                        },
                        ApiVersion {
                            api_key: 3,
                            min_version: 0,
                            max_version: 5
                        },
                        ApiVersion {
                            api_key: 4,
                            min_version: 0,
                            max_version: 1
                        },
                        ApiVersion {
                            api_key: 5,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 6,
                            min_version: 0,
                            max_version: 4
                        },
                        ApiVersion {
                            api_key: 7,
                            min_version: 0,
                            max_version: 1
                        },
                        ApiVersion {
                            api_key: 8,
                            min_version: 0,
                            max_version: 3
                        },
                        ApiVersion {
                            api_key: 9,
                            min_version: 0,
                            max_version: 3
                        },
                        ApiVersion {
                            api_key: 10,
                            min_version: 0,
                            max_version: 1
                        },
                        ApiVersion {
                            api_key: 11,
                            min_version: 0,
                            max_version: 2
                        },
                        ApiVersion {
                            api_key: 12,
                            min_version: 0,
                            max_version: 1
                        },
                        ApiVersion {
                            api_key: 13,
                            min_version: 0,
                            max_version: 1
                        },
                        ApiVersion {
                            api_key: 14,
                            min_version: 0,
                            max_version: 1
                        },
                        ApiVersion {
                            api_key: 15,
                            min_version: 0,
                            max_version: 1
                        },
                        ApiVersion {
                            api_key: 16,
                            min_version: 0,
                            max_version: 1
                        },
                        ApiVersion {
                            api_key: 17,
                            min_version: 0,
                            max_version: 1
                        },
                        ApiVersion {
                            api_key: 18,
                            min_version: 0,
                            max_version: 1
                        },
                        ApiVersion {
                            api_key: 19,
                            min_version: 0,
                            max_version: 2
                        },
                        ApiVersion {
                            api_key: 20,
                            min_version: 0,
                            max_version: 1
                        },
                        ApiVersion {
                            api_key: 21,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 22,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 23,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 24,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 25,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 26,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 27,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 28,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 29,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 30,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 31,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 32,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 33,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 34,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 35,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 36,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 37,
                            min_version: 0,
                            max_version: 0
                        }
                    ]
                    .into()
                ),
                throttle_time_ms: Some(0),
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn api_versions_response_v3_000() -> Result<()> {
    use tansu_kafka_sans_io::api_versions_response::{
        ApiVersion, FinalizedFeatureKey, SupportedFeatureKey,
    };

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 1, 201, 0, 0, 0, 0, 0, 0, 56, 0, 0, 0, 0, 0, 9, 0, 0, 1, 0, 0, 0, 15, 0, 0, 2, 0, 0,
        0, 8, 0, 0, 3, 0, 0, 0, 12, 0, 0, 8, 0, 0, 0, 8, 0, 0, 9, 0, 0, 0, 8, 0, 0, 10, 0, 0, 0, 4,
        0, 0, 11, 0, 0, 0, 9, 0, 0, 12, 0, 0, 0, 4, 0, 0, 13, 0, 0, 0, 5, 0, 0, 14, 0, 0, 0, 5, 0,
        0, 15, 0, 0, 0, 5, 0, 0, 16, 0, 0, 0, 4, 0, 0, 17, 0, 0, 0, 1, 0, 0, 18, 0, 0, 0, 3, 0, 0,
        19, 0, 0, 0, 7, 0, 0, 20, 0, 0, 0, 6, 0, 0, 21, 0, 0, 0, 2, 0, 0, 22, 0, 0, 0, 4, 0, 0, 23,
        0, 0, 0, 4, 0, 0, 24, 0, 0, 0, 4, 0, 0, 25, 0, 0, 0, 3, 0, 0, 26, 0, 0, 0, 3, 0, 0, 27, 0,
        0, 0, 1, 0, 0, 28, 0, 0, 0, 3, 0, 0, 29, 0, 0, 0, 3, 0, 0, 30, 0, 0, 0, 3, 0, 0, 31, 0, 0,
        0, 3, 0, 0, 32, 0, 0, 0, 4, 0, 0, 33, 0, 0, 0, 2, 0, 0, 34, 0, 0, 0, 2, 0, 0, 35, 0, 0, 0,
        4, 0, 0, 36, 0, 0, 0, 2, 0, 0, 37, 0, 0, 0, 3, 0, 0, 38, 0, 0, 0, 3, 0, 0, 39, 0, 0, 0, 2,
        0, 0, 40, 0, 0, 0, 2, 0, 0, 41, 0, 0, 0, 3, 0, 0, 42, 0, 0, 0, 2, 0, 0, 43, 0, 0, 0, 2, 0,
        0, 44, 0, 0, 0, 1, 0, 0, 45, 0, 0, 0, 0, 0, 0, 46, 0, 0, 0, 0, 0, 0, 47, 0, 0, 0, 0, 0, 0,
        48, 0, 0, 0, 1, 0, 0, 49, 0, 0, 0, 1, 0, 0, 50, 0, 0, 0, 0, 0, 0, 51, 0, 0, 0, 0, 0, 0, 55,
        0, 0, 0, 1, 0, 0, 57, 0, 0, 0, 1, 0, 0, 60, 0, 0, 0, 0, 0, 0, 61, 0, 0, 0, 0, 0, 0, 64, 0,
        0, 0, 0, 0, 0, 65, 0, 0, 0, 0, 0, 0, 66, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 23, 2, 17, 109,
        101, 116, 97, 100, 97, 116, 97, 46, 118, 101, 114, 115, 105, 111, 110, 0, 1, 0, 14, 0, 1,
        8, 0, 0, 0, 0, 0, 0, 0, 76, 2, 23, 2, 17, 109, 101, 116, 97, 100, 97, 116, 97, 46, 118,
        101, 114, 115, 105, 111, 110, 0, 14, 0, 14, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::response(&mut c, 18, 3);

    assert_eq!(
        Frame {
            size: 457,
            header: Header::Response { correlation_id: 0 },
            body: Body::ApiVersionsResponse {
                finalized_features: Some(vec![FinalizedFeatureKey {
                    name: "metadata.version".into(),
                    min_version_level: 14,
                    max_version_level: 14,
                }]),
                finalized_features_epoch: Some(76i64),
                supported_features: Some(vec![SupportedFeatureKey {
                    name: "metadata.version".into(),
                    min_version: 1,
                    max_version: 14,
                }]),
                zk_migration_ready: None,
                error_code: 0,
                api_keys: Some(
                    [
                        ApiVersion {
                            api_key: 0,
                            min_version: 0,
                            max_version: 9
                        },
                        ApiVersion {
                            api_key: 1,
                            min_version: 0,
                            max_version: 15
                        },
                        ApiVersion {
                            api_key: 2,
                            min_version: 0,
                            max_version: 8
                        },
                        ApiVersion {
                            api_key: 3,
                            min_version: 0,
                            max_version: 12
                        },
                        ApiVersion {
                            api_key: 8,
                            min_version: 0,
                            max_version: 8
                        },
                        ApiVersion {
                            api_key: 9,
                            min_version: 0,
                            max_version: 8
                        },
                        ApiVersion {
                            api_key: 10,
                            min_version: 0,
                            max_version: 4
                        },
                        ApiVersion {
                            api_key: 11,
                            min_version: 0,
                            max_version: 9
                        },
                        ApiVersion {
                            api_key: 12,
                            min_version: 0,
                            max_version: 4
                        },
                        ApiVersion {
                            api_key: 13,
                            min_version: 0,
                            max_version: 5
                        },
                        ApiVersion {
                            api_key: 14,
                            min_version: 0,
                            max_version: 5
                        },
                        ApiVersion {
                            api_key: 15,
                            min_version: 0,
                            max_version: 5
                        },
                        ApiVersion {
                            api_key: 16,
                            min_version: 0,
                            max_version: 4
                        },
                        ApiVersion {
                            api_key: 17,
                            min_version: 0,
                            max_version: 1
                        },
                        ApiVersion {
                            api_key: 18,
                            min_version: 0,
                            max_version: 3
                        },
                        ApiVersion {
                            api_key: 19,
                            min_version: 0,
                            max_version: 7
                        },
                        ApiVersion {
                            api_key: 20,
                            min_version: 0,
                            max_version: 6
                        },
                        ApiVersion {
                            api_key: 21,
                            min_version: 0,
                            max_version: 2
                        },
                        ApiVersion {
                            api_key: 22,
                            min_version: 0,
                            max_version: 4
                        },
                        ApiVersion {
                            api_key: 23,
                            min_version: 0,
                            max_version: 4
                        },
                        ApiVersion {
                            api_key: 24,
                            min_version: 0,
                            max_version: 4
                        },
                        ApiVersion {
                            api_key: 25,
                            min_version: 0,
                            max_version: 3
                        },
                        ApiVersion {
                            api_key: 26,
                            min_version: 0,
                            max_version: 3
                        },
                        ApiVersion {
                            api_key: 27,
                            min_version: 0,
                            max_version: 1
                        },
                        ApiVersion {
                            api_key: 28,
                            min_version: 0,
                            max_version: 3
                        },
                        ApiVersion {
                            api_key: 29,
                            min_version: 0,
                            max_version: 3
                        },
                        ApiVersion {
                            api_key: 30,
                            min_version: 0,
                            max_version: 3
                        },
                        ApiVersion {
                            api_key: 31,
                            min_version: 0,
                            max_version: 3
                        },
                        ApiVersion {
                            api_key: 32,
                            min_version: 0,
                            max_version: 4
                        },
                        ApiVersion {
                            api_key: 33,
                            min_version: 0,
                            max_version: 2
                        },
                        ApiVersion {
                            api_key: 34,
                            min_version: 0,
                            max_version: 2
                        },
                        ApiVersion {
                            api_key: 35,
                            min_version: 0,
                            max_version: 4
                        },
                        ApiVersion {
                            api_key: 36,
                            min_version: 0,
                            max_version: 2
                        },
                        ApiVersion {
                            api_key: 37,
                            min_version: 0,
                            max_version: 3
                        },
                        ApiVersion {
                            api_key: 38,
                            min_version: 0,
                            max_version: 3
                        },
                        ApiVersion {
                            api_key: 39,
                            min_version: 0,
                            max_version: 2
                        },
                        ApiVersion {
                            api_key: 40,
                            min_version: 0,
                            max_version: 2
                        },
                        ApiVersion {
                            api_key: 41,
                            min_version: 0,
                            max_version: 3
                        },
                        ApiVersion {
                            api_key: 42,
                            min_version: 0,
                            max_version: 2
                        },
                        ApiVersion {
                            api_key: 43,
                            min_version: 0,
                            max_version: 2
                        },
                        ApiVersion {
                            api_key: 44,
                            min_version: 0,
                            max_version: 1
                        },
                        ApiVersion {
                            api_key: 45,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 46,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 47,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 48,
                            min_version: 0,
                            max_version: 1
                        },
                        ApiVersion {
                            api_key: 49,
                            min_version: 0,
                            max_version: 1
                        },
                        ApiVersion {
                            api_key: 50,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 51,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 55,
                            min_version: 0,
                            max_version: 1
                        },
                        ApiVersion {
                            api_key: 57,
                            min_version: 0,
                            max_version: 1
                        },
                        ApiVersion {
                            api_key: 60,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 61,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 64,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 65,
                            min_version: 0,
                            max_version: 0
                        },
                        ApiVersion {
                            api_key: 66,
                            min_version: 0,
                            max_version: 0
                        }
                    ]
                    .into()
                ),
                throttle_time_ms: Some(0),
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn create_topics_request_v7_000() -> Result<()> {
    use tansu_kafka_sans_io::create_topics_request::{CreatableTopic, CreateableTopicConfig};

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 73, 0, 19, 0, 7, 0, 0, 1, 42, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
        110, 116, 45, 49, 0, 2, 9, 98, 97, 108, 97, 110, 99, 101, 115, 255, 255, 255, 255, 255,
        255, 1, 2, 15, 99, 108, 101, 97, 110, 117, 112, 46, 112, 111, 108, 105, 99, 121, 8, 99,
        111, 109, 112, 97, 99, 116, 0, 0, 0, 0, 117, 48, 0, 0,
    ];

    let timeout_ms = 30_000;
    let validate_only = Some(false);

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 73,
            header: Header::Request {
                api_key: 19,
                api_version: 7,
                correlation_id: 298,
                client_id: Some("adminclient-1".into()),
            },
            body: Body::CreateTopicsRequest {
                topics: Some(
                    [CreatableTopic {
                        name: "balances".into(),
                        num_partitions: -1,
                        replication_factor: -1,
                        assignments: Some([].into()),
                        configs: Some(
                            [CreateableTopicConfig {
                                name: "cleanup.policy".into(),
                                value: Some("compact".into()),
                            }]
                            .into()
                        ),
                    }]
                    .into()
                ),
                timeout_ms,
                validate_only,
            },
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn create_topics_response_v7_000() -> Result<()> {
    use tansu_kafka_sans_io::create_topics_response::{
        CreatableTopicConfigs, CreatableTopicResult,
    };
    let _guard = init_tracing()?;

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

    let api_key = 19;
    let api_version = 7;

    let frame = Frame {
        size: 1116,
        header: Header::Response {
            correlation_id: 298,
        },
        body: Body::CreateTopicsResponse {
            throttle_time_ms: Some(0),
            topics: Some(
                [CreatableTopicResult {
                    name: "balances".into(),
                    topic_id: Some([
                        222, 159, 182, 217, 102, 152, 68, 189, 174, 152, 214, 59, 29, 216, 240, 198,
                    ]),
                    error_code: 0,
                    error_message: None,
                    topic_config_error_code: None,
                    num_partitions: Some(1),
                    replication_factor: Some(1),
                    configs: Some(
                        [
                            CreatableTopicConfigs {
                                name: "cleanup.policy".into(),
                                value: Some("compact".into()),
                                read_only: false,
                                config_source: 1,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "compression.type".into(),
                                value: Some("producer".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "delete.retention.ms".into(),
                                value: Some("86400000".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "file.delete.delay.ms".into(),
                                value: Some("60000".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "flush.messages".into(),
                                value: Some("9223372036854775807".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "flush.ms".into(),
                                value: Some("9223372036854775807".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "follower.replication.throttled.replicas".into(),
                                value: Some("".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "index.interval.bytes".into(),
                                value: Some("4096".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "leader.replication.throttled.replicas".into(),
                                value: Some("".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "local.retention.bytes".into(),
                                value: Some("-2".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "local.retention.ms".into(),
                                value: Some("-2".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "max.compaction.lag.ms".into(),
                                value: Some("9223372036854775807".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "max.message.bytes".into(),
                                value: Some("1048588".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "message.downconversion.enable".into(),
                                value: Some("true".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "message.format.version".into(),
                                value: Some("3.0-IV1".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "message.timestamp.after.max.ms".into(),
                                value: Some("9223372036854775807".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "message.timestamp.before.max.ms".into(),
                                value: Some("9223372036854775807".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "message.timestamp.difference.max.ms".into(),
                                value: Some("9223372036854775807".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "message.timestamp.type".into(),
                                value: Some("CreateTime".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "min.cleanable.dirty.ratio".into(),
                                value: Some("0.5".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "min.compaction.lag.ms".into(),
                                value: Some("0".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "min.insync.replicas".into(),
                                value: Some("1".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "preallocate".into(),
                                value: Some("false".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "remote.storage.enable".into(),
                                value: Some("false".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "retention.bytes".into(),
                                value: Some("-1".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "retention.ms".into(),
                                value: Some("604800000".into()),
                                read_only: false,
                                config_source: 4,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "segment.bytes".into(),
                                value: Some("1073741824".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "segment.index.bytes".into(),
                                value: Some("10485760".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "segment.jitter.ms".into(),
                                value: Some("0".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "segment.ms".into(),
                                value: Some("604800000".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                            CreatableTopicConfigs {
                                name: "unclean.leader.election.enable".into(),
                                value: Some("false".into()),
                                read_only: false,
                                config_source: 5,
                                is_sensitive: false,
                            },
                        ]
                        .into(),
                    ),
                }]
                .into(),
            ),
        },
    };

    assert_eq!(
        frame,
        Frame::response_from_bytes(&encoded, api_key, api_version)
            .inspect(|frame| debug!(?frame))?
    );

    Ok(())
}

#[test]
fn delete_topics_request_v6_000() -> Result<()> {
    use tansu_kafka_sans_io::delete_topics_request::DeleteTopicState;

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 52, 0, 20, 0, 6, 0, 0, 0, 4, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
        110, 116, 45, 49, 0, 2, 5, 116, 101, 115, 116, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 117, 48, 0,
    ];

    assert_eq!(
        Frame {
            size: 52,
            header: Header::Request {
                api_key: 20,
                api_version: 6,
                correlation_id: 4,
                client_id: Some("adminclient-1".into())
            },
            body: Body::DeleteTopicsRequest {
                topics: Some(
                    [DeleteTopicState {
                        name: Some("test".into()),
                        topic_id: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                    }]
                    .into()
                ),
                topic_names: None,
                timeout_ms: 30000
            }
        },
        Frame::request_from_bytes(&v)?
    );

    Ok(())
}

#[test]
fn describe_cluster_request_v1_000() -> Result<()> {
    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 27, 0, 60, 0, 1, 0, 0, 0, 7, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
        110, 116, 45, 49, 0, 0, 1, 0,
    ];

    assert_eq!(
        Frame {
            size: 27,
            header: Header::Request {
                api_key: 60,
                api_version: 1,
                correlation_id: 7,
                client_id: Some("adminclient-1".into())
            },
            body: Body::DescribeClusterRequest {
                include_cluster_authorized_operations: false,
                endpoint_type: Some(1)
            }
        },
        Frame::request_from_bytes(&v)?
    );

    Ok(())
}

#[test]
fn describe_configs_request_v4_000() -> Result<()> {
    use tansu_kafka_sans_io::describe_configs_request::DescribeConfigsResource;

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 36, 0, 32, 0, 4, 0, 0, 0, 5, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
        110, 116, 45, 49, 0, 2, 2, 5, 116, 101, 115, 116, 0, 0, 0, 0, 0,
    ];

    assert_eq!(
        Frame {
            size: 36,
            header: Header::Request {
                api_key: 32,
                api_version: 4,
                correlation_id: 5,
                client_id: Some("adminclient-1".into())
            },
            body: Body::DescribeConfigsRequest {
                resources: Some(
                    [DescribeConfigsResource {
                        resource_type: 2,
                        resource_name: "test".into(),
                        configuration_keys: None
                    }]
                    .into()
                ),
                include_synonyms: Some(false),
                include_documentation: Some(false)
            }
        },
        Frame::request_from_bytes(&v)?
    );

    Ok(())
}

#[test]
fn describe_configs_request_v4_001() -> Result<()> {
    use tansu_kafka_sans_io::describe_configs_request::DescribeConfigsResource;

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 40, 0, 32, 0, 4, 0, 0, 0, 3, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
        110, 116, 45, 49, 0, 2, 2, 9, 95, 115, 99, 104, 101, 109, 97, 115, 0, 0, 1, 0, 0,
    ];

    assert_eq!(
        Frame {
            size: 40,
            header: Header::Request {
                api_key: 32,
                api_version: 4,
                correlation_id: 3,
                client_id: Some("adminclient-1".into())
            },
            body: Body::DescribeConfigsRequest {
                resources: Some(
                    [DescribeConfigsResource {
                        resource_type: 2,
                        resource_name: "_schemas".into(),
                        configuration_keys: None
                    }]
                    .into()
                ),
                include_synonyms: Some(true),
                include_documentation: Some(false)
            }
        },
        Frame::request_from_bytes(&v)?
    );

    Ok(())
}

#[test]
fn describe_configs_response_v4_001() -> Result<()> {
    let _guard = init_tracing()?;

    let v = vec![
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

    assert_eq!(
        Frame {
            size: 2330,
            header: Header::Response { correlation_id: 3 },
            body: Body::DescribeConfigsResponse {
                throttle_time_ms: 0,
                results: Some(
                    [DescribeConfigsResult {
                        error_code: 0,
                        error_message: Some("".into()),
                        resource_type: 2,
                        resource_name: "_schemas".into(),
                        configs: Some(
                            [
                                DescribeConfigsResourceResult {
                                    name: "compression.type".into(),
                                    value: Some("producer".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "compression.type".into(),
                                            value: Some("producer".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(2),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "leader.replication.throttled.replicas".into(),
                                    value: Some("".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some([].into()),
                                    config_type: Some(7),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "remote.storage.enable".into(),
                                    value: Some("false".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some([].into()),
                                    config_type: Some(1),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "message.downconversion.enable".into(),
                                    value: Some("true".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "log.message.downconversion.enable".into(),
                                            value: Some("true".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(1),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "min.insync.replicas".into(),
                                    value: Some("1".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "min.insync.replicas".into(),
                                            value: Some("1".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(3),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "segment.jitter.ms".into(),
                                    value: Some("0".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some([].into()),
                                    config_type: Some(5),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "local.retention.ms".into(),
                                    value: Some("-2".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "log.local.retention.ms".into(),
                                            value: Some("-2".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(5),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "cleanup.policy".into(),
                                    value: Some("delete".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "log.cleanup.policy".into(),
                                            value: Some("delete".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(7),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "flush.ms".into(),
                                    value: Some("9223372036854775807".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some([].into()),
                                    config_type: Some(5),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "follower.replication.throttled.replicas".into(),
                                    value: Some("".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some([].into()),
                                    config_type: Some(7),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "compression.lz4.level".into(),
                                    value: Some("9".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "compression.lz4.level".into(),
                                            value: Some("9".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(3),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "segment.bytes".into(),
                                    value: Some("1073741824".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(4),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [
                                            DescribeConfigsSynonym {
                                                name: "log.segment.bytes".into(),
                                                value: Some("1073741824".into()),
                                                source: 4
                                            },
                                            DescribeConfigsSynonym {
                                                name: "log.segment.bytes".into(),
                                                value: Some("1073741824".into()),
                                                source: 5
                                            }
                                        ]
                                        .into()
                                    ),
                                    config_type: Some(3),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "retention.ms".into(),
                                    value: Some("604800000".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some([].into()),
                                    config_type: Some(5),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "compression.gzip.level".into(),
                                    value: Some("-1".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "compression.gzip.level".into(),
                                            value: Some("-1".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(3),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "flush.messages".into(),
                                    value: Some("1".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(1),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [
                                            DescribeConfigsSynonym {
                                                name: "flush.messages".into(),
                                                value: Some("1".into()),
                                                source: 1
                                            },
                                            DescribeConfigsSynonym {
                                                name: "log.flush.interval.messages".into(),
                                                value: Some("9223372036854775807".into()),
                                                source: 5
                                            }
                                        ]
                                        .into()
                                    ),
                                    config_type: Some(5),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "compression.zstd.level".into(),
                                    value: Some("3".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "compression.zstd.level".into(),
                                            value: Some("3".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(3),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "message.format.version".into(),
                                    value: Some("3.0-IV1".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "log.message.format.version".into(),
                                            value: Some("3.0-IV1".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(2),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "max.compaction.lag.ms".into(),
                                    value: Some("9223372036854775807".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "log.cleaner.max.compaction.lag.ms".into(),
                                            value: Some("9223372036854775807".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(5),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "file.delete.delay.ms".into(),
                                    value: Some("60000".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "log.segment.delete.delay.ms".into(),
                                            value: Some("60000".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(5),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "max.message.bytes".into(),
                                    value: Some("64000".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(1),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [
                                            DescribeConfigsSynonym {
                                                name: "max.message.bytes".into(),
                                                value: Some("64000".into()),
                                                source: 1
                                            },
                                            DescribeConfigsSynonym {
                                                name: "message.max.bytes".into(),
                                                value: Some("1048588".into()),
                                                source: 5
                                            }
                                        ]
                                        .into()
                                    ),
                                    config_type: Some(3),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "min.compaction.lag.ms".into(),
                                    value: Some("0".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "log.cleaner.min.compaction.lag.ms".into(),
                                            value: Some("0".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(5),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "message.timestamp.type".into(),
                                    value: Some("CreateTime".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "log.message.timestamp.type".into(),
                                            value: Some("CreateTime".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(2),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "local.retention.bytes".into(),
                                    value: Some("-2".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "log.local.retention.bytes".into(),
                                            value: Some("-2".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(5),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "preallocate".into(),
                                    value: Some("false".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "log.preallocate".into(),
                                            value: Some("false".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(1),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "min.cleanable.dirty.ratio".into(),
                                    value: Some("0.5".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "log.cleaner.min.cleanable.ratio".into(),
                                            value: Some("0.5".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(6),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "index.interval.bytes".into(),
                                    value: Some("4096".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "log.index.interval.bytes".into(),
                                            value: Some("4096".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(3),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "unclean.leader.election.enable".into(),
                                    value: Some("false".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "unclean.leader.election.enable".into(),
                                            value: Some("false".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(1),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "retention.bytes".into(),
                                    value: Some("-1".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "log.retention.bytes".into(),
                                            value: Some("-1".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(5),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "delete.retention.ms".into(),
                                    value: Some("86400000".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "log.cleaner.delete.retention.ms".into(),
                                            value: Some("86400000".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(5),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "message.timestamp.after.max.ms".into(),
                                    value: Some("9223372036854775807".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "log.message.timestamp.after.max.ms".into(),
                                            value: Some("9223372036854775807".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(5),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "message.timestamp.before.max.ms".into(),
                                    value: Some("9223372036854775807".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "log.message.timestamp.before.max.ms".into(),
                                            value: Some("9223372036854775807".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(5),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "segment.ms".into(),
                                    value: Some("604800000".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some([].into()),
                                    config_type: Some(5),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "message.timestamp.difference.max.ms".into(),
                                    value: Some("9223372036854775807".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "log.message.timestamp.difference.max.ms".into(),
                                            value: Some("9223372036854775807".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(5),
                                    documentation: None
                                },
                                DescribeConfigsResourceResult {
                                    name: "segment.index.bytes".into(),
                                    value: Some("10485760".into()),
                                    read_only: false,
                                    is_default: None,
                                    config_source: Some(5),
                                    is_sensitive: false,
                                    synonyms: Some(
                                        [DescribeConfigsSynonym {
                                            name: "log.index.size.max.bytes".into(),
                                            value: Some("10485760".into()),
                                            source: 5
                                        }]
                                        .into()
                                    ),
                                    config_type: Some(3),
                                    documentation: None
                                }
                            ]
                            .into()
                        )
                    }]
                    .into()
                )
            }
        },
        Frame::response_from_bytes(&v, api_key, api_version)?
    );

    Ok(())
}

#[test]
fn describe_groups_request_v1_000() -> Result<()> {
    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 22, 0, 15, 0, 1, 0, 0, 0, 0, 255, 255, 0, 0, 0, 1, 0, 6, 97, 98, 99, 97, 98, 99,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 22,
            header: Header::Request {
                api_key: 15,
                api_version: 1,
                correlation_id: 0,
                client_id: None,
            },
            body: Body::DescribeGroupsRequest {
                groups: Some(["abcabc".into()].into()),
                include_authorized_operations: None,
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn describe_groups_response_v1_000() -> Result<()> {
    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 16, 0, 6, 97, 98, 99, 97, 98, 99, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::response(&mut c, 15, 1);

    assert_eq!(
        Frame {
            size: 32,
            header: Header::Response { correlation_id: 0 },
            body: Body::DescribeGroupsResponse {
                throttle_time_ms: Some(0),
                groups: Some(
                    [DescribedGroup {
                        error_code: 16,
                        group_id: "abcabc".into(),
                        group_state: "".into(),
                        protocol_type: "".into(),
                        protocol_data: "".into(),
                        members: Some([].into()),
                        authorized_operations: None,
                    }]
                    .into()
                ),
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn fetch_request_v6_000() -> Result<()> {
    use tansu_kafka_sans_io::fetch_request::{FetchPartition, FetchTopic};

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 72, 0, 1, 0, 6, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 0, 0, 19, 136, 0, 0, 4,
        0, 0, 0, 16, 0, 1, 0, 0, 0, 1, 0, 11, 97, 98, 99, 97, 98, 99, 97, 98, 99, 97, 98, 0, 0, 0,
        1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 72,
            header: Header::Request {
                api_key: 1,
                api_version: 6,
                correlation_id: 0,
                client_id: None,
            },
            body: Body::FetchRequest {
                cluster_id: None,
                replica_state: None,
                replica_id: Some(-1),
                max_wait_ms: 5000,
                min_bytes: 1024,
                max_bytes: Some(4096),
                isolation_level: Some(1),
                session_id: None,
                session_epoch: None,
                topics: Some(
                    [FetchTopic {
                        topic: Some("abcabcabcab".into()),
                        topic_id: None,
                        partitions: Some(
                            [FetchPartition {
                                partition: 0,
                                current_leader_epoch: None,
                                fetch_offset: 0,
                                last_fetched_epoch: None,
                                log_start_offset: Some(0),
                                partition_max_bytes: 4096,
                            }]
                            .into()
                        ),
                    }]
                    .into()
                ),
                forgotten_topics_data: None,
                rack_id: None,
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn fetch_request_v12_000() -> Result<()> {
    use tansu_kafka_sans_io::fetch_request::{FetchPartition, FetchTopic};

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 162, 0, 1, 0, 12, 0, 0, 0, 8, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99,
        111, 110, 115, 117, 109, 101, 114, 0, 255, 255, 255, 255, 0, 0, 1, 244, 0, 0, 0, 1, 3, 32,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 5, 116, 101, 115, 116, 4, 0, 0, 0, 1, 255, 255, 255,
        255, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0,
        16, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 0, 16, 0, 0, 0, 0, 0, 0, 2, 255, 255, 255, 255, 0,
        0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 16, 0,
        0, 0, 0, 1, 1, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 162,
            header: Header::Request {
                api_key: 1,
                api_version: 12,
                correlation_id: 8,
                client_id: Some("console-consumer".into())
            },
            body: Body::FetchRequest {
                cluster_id: None,
                replica_id: Some(-1),
                replica_state: None,
                max_wait_ms: 500,
                min_bytes: 1,
                max_bytes: Some(52428800),
                isolation_level: Some(0),
                session_id: Some(0),
                session_epoch: Some(0),
                topics: Some(
                    [FetchTopic {
                        topic: Some("test".into()),
                        topic_id: None,
                        partitions: Some(
                            [
                                FetchPartition {
                                    partition: 1,
                                    current_leader_epoch: Some(-1),
                                    fetch_offset: 0,
                                    last_fetched_epoch: Some(-1),
                                    log_start_offset: Some(-1),
                                    partition_max_bytes: 1048576
                                },
                                FetchPartition {
                                    partition: 0,
                                    current_leader_epoch: Some(-1),
                                    fetch_offset: 0,
                                    last_fetched_epoch: Some(-1),
                                    log_start_offset: Some(-1),
                                    partition_max_bytes: 1048576
                                },
                                FetchPartition {
                                    partition: 2,
                                    current_leader_epoch: Some(-1),
                                    fetch_offset: 0,
                                    last_fetched_epoch: Some(-1),
                                    log_start_offset: Some(-1),
                                    partition_max_bytes: 1048576
                                }
                            ]
                            .into()
                        )
                    }]
                    .into()
                ),
                forgotten_topics_data: Some([].into()),
                rack_id: Some("".into())
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn fetch_request_v15_000() -> Result<()> {
    use tansu_kafka_sans_io::fetch_request::{FetchPartition, FetchTopic};

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 2, 96, 0, 1, 0, 15, 0, 0, 0, 14, 0, 26, 99, 111, 110, 115, 117, 109, 101, 114, 45,
        115, 117, 98, 45, 48, 48, 48, 45, 87, 116, 51, 97, 112, 52, 65, 45, 49, 0, 0, 0, 1, 244, 0,
        0, 0, 1, 3, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 139, 193, 249, 209, 188, 231, 73, 214,
        186, 217, 20, 95, 74, 239, 160, 61, 17, 0, 0, 0, 7, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0,
        0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 160, 0, 0, 0, 0, 0, 0,
        6, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 0, 160, 0, 0, 0, 0, 0, 0, 8, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 160, 0, 0, 0, 0, 0, 0, 11,
        255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 0, 160, 0, 0, 0, 0, 0, 0, 10, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 160, 0, 0, 0, 0, 0, 0, 13,
        255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 0, 160, 0, 0, 0, 0, 0, 0, 12, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 160, 0, 0, 0, 0, 0, 0, 15,
        255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 0, 160, 0, 0, 0, 0, 0, 0, 14, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 160, 0, 0, 0, 0, 0, 0, 1,
        255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 0, 160, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 160, 0, 0, 0, 0, 0, 0, 3,
        255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 0, 160, 0, 0, 0, 0, 0, 0, 2, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 160, 0, 0, 0, 0, 0, 0, 5,
        255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 0, 160, 0, 0, 0, 0, 0, 0, 4, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 160, 0, 0, 0, 0, 0, 0, 9,
        255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 109, 160, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 0, 160, 0, 0, 0, 0, 1, 1, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 608,
            header: Header::Request {
                api_key: 1,
                api_version: 15,
                correlation_id: 14,
                client_id: Some("consumer-sub-000-Wt3ap4A-1".into())
            },
            body: Body::FetchRequest {
                cluster_id: None,
                replica_id: None,
                replica_state: None,
                max_wait_ms: 500,
                min_bytes: 1,
                max_bytes: Some(52428800),
                isolation_level: Some(0),
                session_id: Some(0),
                session_epoch: Some(0),
                topics: Some(
                    [FetchTopic {
                        topic: None,
                        topic_id: Some([
                            139, 193, 249, 209, 188, 231, 73, 214, 186, 217, 20, 95, 74, 239, 160,
                            61
                        ]),
                        partitions: Some(
                            [
                                FetchPartition {
                                    partition: 7,
                                    current_leader_epoch: Some(-1),
                                    fetch_offset: 0,
                                    last_fetched_epoch: Some(-1),
                                    log_start_offset: Some(-1),
                                    partition_max_bytes: 10485760
                                },
                                FetchPartition {
                                    partition: 6,
                                    current_leader_epoch: Some(-1),
                                    fetch_offset: 0,
                                    last_fetched_epoch: Some(-1),
                                    log_start_offset: Some(-1),
                                    partition_max_bytes: 10485760
                                },
                                FetchPartition {
                                    partition: 8,
                                    current_leader_epoch: Some(-1),
                                    fetch_offset: 0,
                                    last_fetched_epoch: Some(-1),
                                    log_start_offset: Some(-1),
                                    partition_max_bytes: 10485760
                                },
                                FetchPartition {
                                    partition: 11,
                                    current_leader_epoch: Some(-1),
                                    fetch_offset: 0,
                                    last_fetched_epoch: Some(-1),
                                    log_start_offset: Some(-1),
                                    partition_max_bytes: 10485760
                                },
                                FetchPartition {
                                    partition: 10,
                                    current_leader_epoch: Some(-1),
                                    fetch_offset: 0,
                                    last_fetched_epoch: Some(-1),
                                    log_start_offset: Some(-1),
                                    partition_max_bytes: 10485760
                                },
                                FetchPartition {
                                    partition: 13,
                                    current_leader_epoch: Some(-1),
                                    fetch_offset: 0,
                                    last_fetched_epoch: Some(-1),
                                    log_start_offset: Some(-1),
                                    partition_max_bytes: 10485760
                                },
                                FetchPartition {
                                    partition: 12,
                                    current_leader_epoch: Some(-1),
                                    fetch_offset: 0,
                                    last_fetched_epoch: Some(-1),
                                    log_start_offset: Some(-1),
                                    partition_max_bytes: 10485760
                                },
                                FetchPartition {
                                    partition: 15,
                                    current_leader_epoch: Some(-1),
                                    fetch_offset: 0,
                                    last_fetched_epoch: Some(-1),
                                    log_start_offset: Some(-1),
                                    partition_max_bytes: 10485760
                                },
                                FetchPartition {
                                    partition: 14,
                                    current_leader_epoch: Some(-1),
                                    fetch_offset: 0,
                                    last_fetched_epoch: Some(-1),
                                    log_start_offset: Some(-1),
                                    partition_max_bytes: 10485760
                                },
                                FetchPartition {
                                    partition: 1,
                                    current_leader_epoch: Some(-1),
                                    fetch_offset: 0,
                                    last_fetched_epoch: Some(-1),
                                    log_start_offset: Some(-1),
                                    partition_max_bytes: 10485760
                                },
                                FetchPartition {
                                    partition: 0,
                                    current_leader_epoch: Some(-1),
                                    fetch_offset: 0,
                                    last_fetched_epoch: Some(-1),
                                    log_start_offset: Some(-1),
                                    partition_max_bytes: 10485760
                                },
                                FetchPartition {
                                    partition: 3,
                                    current_leader_epoch: Some(-1),
                                    fetch_offset: 0,
                                    last_fetched_epoch: Some(-1),
                                    log_start_offset: Some(-1),
                                    partition_max_bytes: 10485760
                                },
                                FetchPartition {
                                    partition: 2,
                                    current_leader_epoch: Some(-1),
                                    fetch_offset: 0,
                                    last_fetched_epoch: Some(-1),
                                    log_start_offset: Some(-1),
                                    partition_max_bytes: 10485760
                                },
                                FetchPartition {
                                    partition: 5,
                                    current_leader_epoch: Some(-1),
                                    fetch_offset: 0,
                                    last_fetched_epoch: Some(-1),
                                    log_start_offset: Some(-1),
                                    partition_max_bytes: 10485760
                                },
                                FetchPartition {
                                    partition: 4,
                                    current_leader_epoch: Some(-1),
                                    fetch_offset: 0,
                                    last_fetched_epoch: Some(-1),
                                    log_start_offset: Some(-1),
                                    partition_max_bytes: 10485760
                                },
                                FetchPartition {
                                    partition: 9,
                                    current_leader_epoch: Some(-1),
                                    fetch_offset: 28064,
                                    last_fetched_epoch: Some(-1),
                                    log_start_offset: Some(-1),
                                    partition_max_bytes: 10485760
                                }
                            ]
                            .into()
                        )
                    }]
                    .into()
                ),
                forgotten_topics_data: Some([].into()),
                rack_id: Some("".into())
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn fetch_request_v16_000() -> Result<()> {
    let _guard = init_tracing()?;

    let encoded = vec![
        0, 0, 0, 52, 0, 1, 0, 16, 0, 0, 0, 12, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99,
        111, 110, 115, 117, 109, 101, 114, 0, 0, 0, 1, 244, 0, 0, 0, 1, 3, 32, 0, 0, 0, 0, 0, 0, 0,
        255, 255, 255, 255, 1, 1, 1, 0,
    ];

    assert_eq!(
        Frame {
            size: 52,
            header: Header::Request {
                api_key: 1,
                api_version: 16,
                correlation_id: 12,
                client_id: Some("console-consumer".into())
            },
            body: Body::FetchRequest {
                cluster_id: None,
                replica_id: None,
                replica_state: None,
                max_wait_ms: 500,
                min_bytes: 1,
                max_bytes: Some(52428800),
                isolation_level: Some(0),
                session_id: Some(0),
                session_epoch: Some(-1),
                topics: Some([].into()),
                forgotten_topics_data: Some([].into()),
                rack_id: Some("".into())
            }
        },
        Frame::request_from_bytes(&encoded)?
    );

    Ok(())
}

#[test]
fn fetch_request_v16_001() -> Result<()> {
    use tansu_kafka_sans_io::fetch_request::{FetchPartition, FetchTopic};
    let _guard = init_tracing()?;

    let encoded = vec![
        0, 0, 0, 103, 0, 1, 0, 16, 0, 0, 0, 8, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99,
        111, 110, 115, 117, 109, 101, 114, 0, 0, 0, 1, 244, 0, 0, 0, 1, 3, 32, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 2, 246, 177, 3, 16, 190, 12, 74, 195, 190, 197, 130, 25, 106, 235, 221, 30, 2,
        0, 0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 0, 16, 0, 0, 0, 0, 1, 1, 0,
    ];

    assert_eq!(
        Frame {
            size: 103,
            header: Header::Request {
                api_key: 1,
                api_version: 16,
                correlation_id: 8,
                client_id: Some("console-consumer".into())
            },
            body: Body::FetchRequest {
                cluster_id: None,
                replica_id: None,
                replica_state: None,
                max_wait_ms: 500,
                min_bytes: 1,
                max_bytes: Some(52428800),
                isolation_level: Some(0),
                session_id: Some(0),
                session_epoch: Some(0),
                topics: Some(
                    [FetchTopic {
                        topic: None,
                        topic_id: Some([
                            246, 177, 3, 16, 190, 12, 74, 195, 190, 197, 130, 25, 106, 235, 221, 30
                        ]),
                        partitions: Some(
                            [FetchPartition {
                                partition: 0,
                                current_leader_epoch: Some(-1),
                                fetch_offset: 0,
                                last_fetched_epoch: Some(-1),
                                log_start_offset: Some(-1),
                                partition_max_bytes: 1048576
                            }]
                            .into()
                        )
                    }]
                    .into()
                ),
                forgotten_topics_data: Some([].into()),
                rack_id: Some("".into())
            }
        },
        Frame::request_from_bytes(&encoded)?
    );

    Ok(())
}

#[test]
fn fetch_response_v12_000() -> Result<()> {
    use tansu_kafka_sans_io::fetch_response::{FetchableTopicResponse, PartitionData};

    let _guard = init_tracing()?;

    let api_key = 1;
    let api_version = 12;

    let v = vec![
        0, 0, 0, 135, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 52, 239, 167, 250, 2, 5, 116, 101, 115, 116,
        4, 0, 0, 0, 1, 0, 3, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0,
        0, 3, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 1, 255, 255, 255, 255, 1, 0, 0, 0, 0, 2, 0, 3, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 1, 255, 255, 255, 255, 1, 0, 0, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::response(&mut c, api_key, api_version);

    assert_eq!(
        Frame {
            size: 135,
            header: Header::Response { correlation_id: 8 },
            body: Body::FetchResponse {
                throttle_time_ms: Some(0),
                error_code: Some(0),
                session_id: Some(888121338),
                responses: Some(
                    [FetchableTopicResponse {
                        topic: Some("test".into()),
                        topic_id: None,
                        partitions: Some(
                            [
                                PartitionData {
                                    partition_index: 1,
                                    error_code: 3,
                                    high_watermark: -1,
                                    last_stable_offset: Some(-1),
                                    log_start_offset: Some(-1),
                                    diverging_epoch: None,
                                    current_leader: None,
                                    snapshot_id: None,
                                    aborted_transactions: Some([].into()),
                                    preferred_read_replica: Some(-1),
                                    records: None
                                },
                                PartitionData {
                                    partition_index: 0,
                                    error_code: 3,
                                    high_watermark: -1,
                                    last_stable_offset: Some(-1),
                                    log_start_offset: Some(-1),
                                    diverging_epoch: None,
                                    current_leader: None,
                                    snapshot_id: None,
                                    aborted_transactions: Some([].into()),
                                    preferred_read_replica: Some(-1),
                                    records: None
                                },
                                PartitionData {
                                    partition_index: 2,
                                    error_code: 3,
                                    high_watermark: -1,
                                    last_stable_offset: Some(-1),
                                    log_start_offset: Some(-1),
                                    diverging_epoch: None,
                                    current_leader: None,
                                    snapshot_id: None,
                                    aborted_transactions: Some([].into()),
                                    preferred_read_replica: Some(-1),
                                    records: None
                                }
                            ]
                            .into()
                        )
                    }]
                    .into()
                ),
                node_endpoints: None
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn fetch_response_v12_001() -> Result<()> {
    let _guard = init_tracing()?;

    let api_key = 1;
    let api_version = 12;

    let v = vec![
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

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::response(&mut c, api_key, api_version);

    assert_eq!(
        Frame {
            size: 284,
            header: Header::Response { correlation_id: 8 },
            body: Body::FetchResponse {
                throttle_time_ms: Some(0),
                error_code: Some(0),
                session_id: Some(2086460889),
                responses: Some(
                    [FetchableTopicResponse {
                        topic: Some("test".into()),
                        topic_id: None,
                        partitions: Some(
                            [
                                PartitionData {
                                    partition_index: 1,
                                    error_code: 0,
                                    high_watermark: 0,
                                    last_stable_offset: Some(0),
                                    log_start_offset: Some(0),
                                    diverging_epoch: None,
                                    current_leader: None,
                                    snapshot_id: None,
                                    aborted_transactions: None,
                                    preferred_read_replica: Some(-1),
                                    records: None
                                },
                                PartitionData {
                                    partition_index: 0,
                                    error_code: 0,
                                    high_watermark: 2,
                                    last_stable_offset: Some(2),
                                    log_start_offset: Some(0),
                                    diverging_epoch: None,
                                    current_leader: None,
                                    snapshot_id: None,
                                    aborted_transactions: None,
                                    preferred_read_replica: Some(-1),
                                    records: Some(
                                        inflated::Frame {
                                            batches: [
                                                inflated::Batch {
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
                                                        key: Some(Bytes::from_static(&[
                                                            97, 98, 99
                                                        ])),
                                                        value: Some(Bytes::from_static(&[
                                                            112, 113, 114
                                                        ])),
                                                        headers: [].into()
                                                    }]
                                                    .into()
                                                },
                                                inflated::Batch {
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
                                                        key: Some(Bytes::from_static(&[
                                                            97, 98, 99
                                                        ])),
                                                        value: Some(Bytes::from_static(&[
                                                            112, 113, 114
                                                        ])),
                                                        headers: [].into()
                                                    }]
                                                    .into()
                                                }
                                            ]
                                            .into()
                                        }
                                        .try_into()?
                                    )
                                },
                                PartitionData {
                                    partition_index: 2,
                                    error_code: 0,
                                    high_watermark: 0,
                                    last_stable_offset: Some(0),
                                    log_start_offset: Some(0),
                                    diverging_epoch: None,
                                    current_leader: None,
                                    snapshot_id: None,
                                    aborted_transactions: None,
                                    preferred_read_replica: Some(-1),
                                    records: None
                                }
                            ]
                            .into()
                        )
                    }]
                    .into()
                ),
                node_endpoints: None
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn fetch_response_v12_002() -> Result<()> {
    let _guard = init_tracing()?;

    let api_key = 1;
    let api_version = 12;

    let v = vec![
        0, 0, 1, 64, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 58, 96, 28, 234, 2, 5, 116, 101, 115, 116, 4,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 22, 0, 0, 0, 0, 0, 0, 0, 22, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 255, 255, 255, 255, 185, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 172, 0, 0, 0, 0, 2, 143,
        254, 2, 228, 0, 0, 0, 0, 0, 10, 0, 0, 1, 141, 116, 152, 137, 53, 0, 0, 1, 141, 116, 152,
        137, 53, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 11, 20, 0, 0, 0, 4, 107, 49, 4,
        118, 49, 0, 20, 0, 0, 2, 4, 107, 50, 4, 118, 50, 0, 20, 0, 0, 4, 4, 107, 49, 4, 118, 51, 0,
        20, 0, 0, 6, 4, 107, 49, 4, 118, 52, 0, 20, 0, 0, 8, 4, 107, 51, 4, 118, 53, 0, 20, 0, 0,
        10, 4, 107, 50, 4, 118, 54, 0, 20, 0, 0, 12, 4, 107, 52, 4, 118, 55, 0, 20, 0, 0, 14, 4,
        107, 53, 4, 118, 56, 0, 20, 0, 0, 16, 4, 107, 53, 4, 118, 57, 0, 22, 0, 0, 18, 4, 107, 50,
        6, 118, 49, 48, 0, 22, 0, 0, 20, 4, 107, 54, 6, 118, 49, 49, 0, 0, 0, 0, 0, 1, 0, 3, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 1, 255, 255, 255, 255, 1, 0, 0, 0, 0, 2, 0, 3, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 1, 255, 255, 255, 255, 1, 0, 0, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::response(&mut c, api_key, api_version);

    assert_eq!(
        Frame {
            size: 320,
            header: Header::Response { correlation_id: 8 },
            body: Body::FetchResponse {
                throttle_time_ms: Some(0),
                error_code: Some(0),
                session_id: Some(979377386),
                responses: Some(
                    [FetchableTopicResponse {
                        topic: Some("test".into()),
                        topic_id: None,
                        partitions: Some(
                            [
                                PartitionData {
                                    partition_index: 0,
                                    error_code: 0,
                                    high_watermark: 22,
                                    last_stable_offset: Some(22),
                                    log_start_offset: Some(0),
                                    diverging_epoch: None,
                                    current_leader: None,
                                    snapshot_id: None,
                                    aborted_transactions: None,
                                    preferred_read_replica: Some(-1),
                                    records: Some(
                                        inflated::Frame {
                                            batches: [inflated::Batch {
                                                base_offset: 0,
                                                batch_length: 172,
                                                partition_leader_epoch: 0,
                                                magic: 2,
                                                crc: 2415788772,
                                                attributes: 0,
                                                last_offset_delta: 10,
                                                base_timestamp: 1707058170165,
                                                max_timestamp: 1707058170165,
                                                producer_id: 1,
                                                producer_epoch: 0,
                                                base_sequence: 1,
                                                records: [
                                                    Record {
                                                        length: 10,
                                                        attributes: 0,
                                                        timestamp_delta: 0,
                                                        offset_delta: 0,
                                                        key: Some(Bytes::from_static(&[107, 49])),
                                                        value: Some(Bytes::from_static(&[118, 49])),
                                                        headers: [].into()
                                                    },
                                                    Record {
                                                        length: 10,
                                                        attributes: 0,
                                                        timestamp_delta: 0,
                                                        offset_delta: 1,
                                                        key: Some(Bytes::from_static(&[107, 50])),
                                                        value: Some(Bytes::from_static(&[118, 50])),
                                                        headers: [].into()
                                                    },
                                                    Record {
                                                        length: 10,
                                                        attributes: 0,
                                                        timestamp_delta: 0,
                                                        offset_delta: 2,
                                                        key: Some(Bytes::from_static(&[107, 49])),
                                                        value: Some(Bytes::from_static(&[118, 51])),
                                                        headers: [].into()
                                                    },
                                                    Record {
                                                        length: 10,
                                                        attributes: 0,
                                                        timestamp_delta: 0,
                                                        offset_delta: 3,
                                                        key: Some(Bytes::from_static(&[107, 49])),
                                                        value: Some(Bytes::from_static(&[118, 52])),
                                                        headers: [].into()
                                                    },
                                                    Record {
                                                        length: 10,
                                                        attributes: 0,
                                                        timestamp_delta: 0,
                                                        offset_delta: 4,
                                                        key: Some(Bytes::from_static(&[107, 51])),
                                                        value: Some(Bytes::from_static(&[118, 53])),
                                                        headers: [].into()
                                                    },
                                                    Record {
                                                        length: 10,
                                                        attributes: 0,
                                                        timestamp_delta: 0,
                                                        offset_delta: 5,
                                                        key: Some(Bytes::from_static(&[107, 50])),
                                                        value: Some(Bytes::from_static(&[118, 54])),
                                                        headers: [].into()
                                                    },
                                                    Record {
                                                        length: 10,
                                                        attributes: 0,
                                                        timestamp_delta: 0,
                                                        offset_delta: 6,
                                                        key: Some(Bytes::from_static(&[107, 52])),
                                                        value: Some(Bytes::from_static(&[118, 55])),
                                                        headers: [].into()
                                                    },
                                                    Record {
                                                        length: 10,
                                                        attributes: 0,
                                                        timestamp_delta: 0,
                                                        offset_delta: 7,
                                                        key: Some(Bytes::from_static(&[107, 53])),
                                                        value: Some(Bytes::from_static(&[118, 56])),
                                                        headers: [].into()
                                                    },
                                                    Record {
                                                        length: 10,
                                                        attributes: 0,
                                                        timestamp_delta: 0,
                                                        offset_delta: 8,
                                                        key: Some(Bytes::from_static(&[107, 53])),
                                                        value: Some(Bytes::from_static(&[118, 57])),
                                                        headers: [].into()
                                                    },
                                                    Record {
                                                        length: 11,
                                                        attributes: 0,
                                                        timestamp_delta: 0,
                                                        offset_delta: 9,
                                                        key: Some(Bytes::from_static(&[107, 50])),
                                                        value: Some(Bytes::from_static(&[
                                                            118, 49, 48
                                                        ])),
                                                        headers: [].into()
                                                    },
                                                    Record {
                                                        length: 11,
                                                        attributes: 0,
                                                        timestamp_delta: 0,
                                                        offset_delta: 10,
                                                        key: Some(Bytes::from_static(&[107, 54])),
                                                        value: Some(Bytes::from_static(&[
                                                            118, 49, 49
                                                        ])),
                                                        headers: [].into()
                                                    }
                                                ]
                                                .into()
                                            }]
                                            .into()
                                        }
                                        .try_into()?
                                    )
                                },
                                PartitionData {
                                    partition_index: 1,
                                    error_code: 3,
                                    high_watermark: -1,
                                    last_stable_offset: Some(-1),
                                    log_start_offset: Some(-1),
                                    diverging_epoch: None,
                                    current_leader: None,
                                    snapshot_id: None,
                                    aborted_transactions: Some([].into()),
                                    preferred_read_replica: Some(-1),
                                    records: None
                                },
                                PartitionData {
                                    partition_index: 2,
                                    error_code: 3,
                                    high_watermark: -1,
                                    last_stable_offset: Some(-1),
                                    log_start_offset: Some(-1),
                                    diverging_epoch: None,
                                    current_leader: None,
                                    snapshot_id: None,
                                    aborted_transactions: Some([].into()),
                                    preferred_read_replica: Some(-1),
                                    records: None
                                }
                            ]
                            .into()
                        )
                    }]
                    .into()
                ),
                node_endpoints: None
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn fetch_response_v16_001() -> Result<()> {
    let _guard = init_tracing()?;

    let api_key = 1;
    let api_version = 16;

    let v = vec![
        0, 0, 0, 189, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 28, 205, 172, 195, 142, 19,
        71, 71, 182, 128, 13, 18, 65, 142, 210, 222, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 1, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 74, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 61, 255, 255, 255, 255, 2, 153, 143, 24, 144, 0, 0, 0, 0, 0, 0, 0,
        0, 1, 144, 238, 148, 84, 54, 0, 0, 1, 144, 238, 148, 84, 54, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 1, 22, 0, 0, 0, 1, 10, 112, 111, 105, 117, 121, 0, 3, 0, 13, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 1, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
        13, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 1, 0, 1, 1,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::response(&mut c, api_key, api_version);

    assert_eq!(
        Frame {
            size: 189,
            header: Header::Response { correlation_id: 8 },
            body: Body::FetchResponse {
                throttle_time_ms: Some(0),
                error_code: Some(0),
                session_id: Some(0),
                responses: Some(
                    [FetchableTopicResponse {
                        topic: None,
                        topic_id: Some([
                            28, 205, 172, 195, 142, 19, 71, 71, 182, 128, 13, 18, 65, 142, 210, 222
                        ]),
                        partitions: Some(
                            [PartitionData {
                                partition_index: 0,
                                error_code: 0,
                                high_watermark: 0,
                                last_stable_offset: Some(1),
                                log_start_offset: Some(-1),
                                diverging_epoch: Some(EpochEndOffset {
                                    epoch: -1,
                                    end_offset: -1
                                }),
                                current_leader: Some(LeaderIdAndEpoch {
                                    leader_id: 0,
                                    leader_epoch: 0
                                }),
                                snapshot_id: Some(SnapshotId {
                                    end_offset: -1,
                                    epoch: -1
                                }),
                                aborted_transactions: Some([].into()),
                                preferred_read_replica: Some(0),
                                records: Some(
                                    inflated::Frame {
                                        batches: [inflated::Batch {
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
                                                value: Some(Bytes::from_static(&[
                                                    112, 111, 105, 117, 121
                                                ])),
                                                headers: [].into()
                                            }]
                                            .into()
                                        }]
                                        .into()
                                    }
                                    .try_into()?
                                )
                            }]
                            .into()
                        )
                    }]
                    .into()
                ),
                node_endpoints: Some([].into())
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn fetch_response_v16_002() -> Result<()> {
    let _guard = init_tracing()?;

    let api_key = 1;
    let api_version = 16;

    let v = vec![
        0, 0, 0, 186, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 28, 205, 172, 195, 142, 19,
        71, 71, 182, 128, 13, 18, 65, 142, 210, 222, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 1, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 74, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 61, 255, 255, 255, 255, 2, 153, 143, 24, 144, 0, 0, 0, 0, 0, 0, 0,
        0, 1, 144, 238, 148, 84, 54, 0, 0, 1, 144, 238, 148, 84, 54, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 1, 22, 0, 0, 0, 1, 10, 112, 111, 105, 117, 121, 0, 3, 0, 13, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 1, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
        13, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::response(&mut c, api_key, api_version);

    assert_eq!(
        Frame {
            size: 186,
            header: Header::Response { correlation_id: 8 },
            body: Body::FetchResponse {
                throttle_time_ms: Some(0),
                error_code: Some(0),
                session_id: Some(0),
                responses: Some(
                    [FetchableTopicResponse {
                        topic: None,
                        topic_id: Some([
                            28, 205, 172, 195, 142, 19, 71, 71, 182, 128, 13, 18, 65, 142, 210, 222
                        ]),
                        partitions: Some(
                            [PartitionData {
                                partition_index: 0,
                                error_code: 0,
                                high_watermark: 0,
                                last_stable_offset: Some(1),
                                log_start_offset: Some(-1),
                                diverging_epoch: Some(EpochEndOffset {
                                    epoch: -1,
                                    end_offset: -1
                                }),
                                current_leader: Some(LeaderIdAndEpoch {
                                    leader_id: 0,
                                    leader_epoch: 0
                                }),
                                snapshot_id: Some(SnapshotId {
                                    end_offset: -1,
                                    epoch: -1
                                }),
                                aborted_transactions: Some([].into()),
                                preferred_read_replica: Some(0),
                                records: Some(
                                    inflated::Frame {
                                        batches: [inflated::Batch {
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
                                                value: Some(Bytes::from_static(&[
                                                    112, 111, 105, 117, 121
                                                ])),
                                                headers: [].into()
                                            }]
                                            .into()
                                        }]
                                        .into()
                                    }
                                    .try_into()?
                                )
                            }]
                            .into()
                        )
                    }]
                    .into()
                ),
                node_endpoints: None
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn find_coordinator_request_v1_000() -> Result<()> {
    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 19, 0, 10, 0, 1, 0, 0, 0, 0, 255, 255, 0, 6, 97, 98, 99, 100, 101, 102, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 19,
            header: Header::Request {
                api_key: 10,
                api_version: 1,
                correlation_id: 0,
                client_id: None,
            },
            body: Body::FindCoordinatorRequest {
                key: Some("abcdef".into()),
                key_type: Some(0),
                coordinator_keys: None,
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn find_coordinator_request_v2_000() -> Result<()> {
    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 45, 0, 10, 0, 2, 0, 0, 0, 3, 0, 7, 114, 100, 107, 97, 102, 107, 97, 0, 25, 101,
        120, 97, 109, 112, 108, 101, 95, 99, 111, 110, 115, 117, 109, 101, 114, 95, 103, 114, 111,
        117, 112, 95, 105, 100, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 45,
            header: Header::Request {
                api_key: 10,
                api_version: 2,
                correlation_id: 3,
                client_id: Some("rdkafka".into())
            },
            body: Body::FindCoordinatorRequest {
                key: Some("example_consumer_group_id".into()),
                key_type: Some(0),
                coordinator_keys: None
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn find_coordinator_response_v1_000() -> Result<()> {
    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 62, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 0, 0, 3, 234, 0, 40, 105, 112, 45, 49,
        48, 45, 50, 45, 57, 49, 45, 54, 54, 46, 101, 117, 45, 119, 101, 115, 116, 45, 49, 46, 99,
        111, 109, 112, 117, 116, 101, 46, 105, 110, 116, 101, 114, 110, 97, 108, 0, 0, 35, 132,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::response(&mut c, 10, 1);

    assert_eq!(
        Frame {
            size: 62,
            header: Header::Response { correlation_id: 0 },
            body: Body::FindCoordinatorResponse {
                throttle_time_ms: Some(0),
                error_code: Some(0),
                error_message: None,
                node_id: Some(1002),
                host: Some("ip-10-2-91-66.eu-west-1.compute.internal".into()),
                port: Some(9092),
                coordinators: None,
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn find_coordinator_request_v4_000() -> Result<()> {
    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 50, 0, 10, 0, 4, 0, 0, 0, 0, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99, 111,
        110, 115, 117, 109, 101, 114, 0, 0, 2, 20, 116, 101, 115, 116, 45, 99, 111, 110, 115, 117,
        109, 101, 114, 45, 103, 114, 111, 117, 112, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 50,
            header: Header::Request {
                api_key: 10,
                api_version: 4,
                correlation_id: 0,
                client_id: Some("console-consumer".into())
            },
            body: Body::FindCoordinatorRequest {
                key: None,
                key_type: Some(0),
                coordinator_keys: Some(["test-consumer-group".into()].into())
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn heartbeat_request_v4_000() -> Result<()> {
    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 58, 0, 12, 0, 4, 0, 0, 40, 48, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99,
        111, 110, 115, 117, 109, 101, 114, 0, 20, 116, 101, 115, 116, 45, 99, 111, 110, 115, 117,
        109, 101, 114, 45, 103, 114, 111, 117, 112, 0, 0, 0, 0, 5, 49, 48, 48, 48, 0, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 58,
            header: Header::Request {
                api_key: 12,
                api_version: 4,
                correlation_id: 10288,
                client_id: Some("console-consumer".into())
            },
            body: Body::HeartbeatRequest {
                group_id: "test-consumer-group".into(),
                generation_id: 0,
                member_id: "1000".into(),
                group_instance_id: None
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn init_producer_id_request_v4_000() -> Result<()> {
    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 43, 0, 22, 0, 4, 0, 0, 0, 2, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
        114, 111, 100, 117, 99, 101, 114, 0, 0, 127, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 43,
            header: Header::Request {
                api_key: 22,
                api_version: 4,
                correlation_id: 2,
                client_id: Some("console-producer".into()),
            },
            body: Body::InitProducerIdRequest {
                transactional_id: None,
                transaction_timeout_ms: 2147483647,
                producer_id: Some(-1),
                producer_epoch: Some(-1),
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn join_group_request_v5_000() -> Result<()> {
    use tansu_kafka_sans_io::join_group_request::JoinGroupRequestProtocol;

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 159, 0, 11, 0, 5, 0, 0, 0, 3, 0, 7, 114, 100, 107, 97, 102, 107, 97, 0, 25, 101,
        120, 97, 109, 112, 108, 101, 95, 99, 111, 110, 115, 117, 109, 101, 114, 95, 103, 114, 111,
        117, 112, 95, 105, 100, 0, 0, 23, 112, 0, 4, 147, 224, 0, 0, 255, 255, 0, 8, 99, 111, 110,
        115, 117, 109, 101, 114, 0, 0, 0, 2, 0, 5, 114, 97, 110, 103, 101, 0, 0, 0, 31, 0, 3, 0, 0,
        0, 1, 0, 9, 98, 101, 110, 99, 104, 109, 97, 114, 107, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255,
        255, 255, 0, 0, 0, 10, 114, 111, 117, 110, 100, 114, 111, 98, 105, 110, 0, 0, 0, 31, 0, 3,
        0, 0, 0, 1, 0, 9, 98, 101, 110, 99, 104, 109, 97, 114, 107, 0, 0, 0, 0, 0, 0, 0, 0, 255,
        255, 255, 255, 0, 0,
    ];

    let range_metadata =
        Bytes::from_static(b"\0\x03\0\0\0\x01\0\tbenchmark\0\0\0\0\0\0\0\0\xff\xff\xff\xff\0\0");
    let roundrobin_metadata =
        Bytes::from_static(b"\0\x03\0\0\0\x01\0\tbenchmark\0\0\0\0\0\0\0\0\xff\xff\xff\xff\0\0");

    assert_eq!(
        Frame {
            size: 159,
            header: Header::Request {
                api_key: 11,
                api_version: 5,
                correlation_id: 3,
                client_id: Some("rdkafka".into())
            },
            body: Body::JoinGroupRequest {
                group_id: "example_consumer_group_id".into(),
                session_timeout_ms: 6000,
                rebalance_timeout_ms: Some(300000),
                member_id: "".into(),
                group_instance_id: None,
                protocol_type: "consumer".into(),
                protocols: Some(
                    [
                        JoinGroupRequestProtocol {
                            name: "range".into(),
                            metadata: range_metadata
                        },
                        JoinGroupRequestProtocol {
                            name: "roundrobin".into(),
                            metadata: roundrobin_metadata
                        }
                    ]
                    .into()
                ),
                reason: None
            }
        },
        Frame::request_from_bytes(&v)?
    );

    Ok(())
}

#[test]
fn join_group_response_v5_000() -> Result<()> {
    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 200, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 5, 114, 97, 110, 103, 101, 0,
        44, 114, 100, 107, 97, 102, 107, 97, 45, 52, 57, 57, 101, 53, 55, 55, 48, 45, 51, 55, 53,
        101, 45, 52, 57, 57, 48, 45, 98, 102, 56, 52, 45, 97, 51, 57, 54, 51, 52, 101, 51, 98, 102,
        101, 52, 0, 44, 114, 100, 107, 97, 102, 107, 97, 45, 52, 57, 57, 101, 53, 55, 55, 48, 45,
        51, 55, 53, 101, 45, 52, 57, 57, 48, 45, 98, 102, 56, 52, 45, 97, 51, 57, 54, 51, 52, 101,
        51, 98, 102, 101, 52, 0, 0, 0, 1, 0, 44, 114, 100, 107, 97, 102, 107, 97, 45, 52, 57, 57,
        101, 53, 55, 55, 48, 45, 51, 55, 53, 101, 45, 52, 57, 57, 48, 45, 98, 102, 56, 52, 45, 97,
        51, 57, 54, 51, 52, 101, 51, 98, 102, 101, 52, 255, 255, 0, 0, 0, 31, 0, 3, 0, 0, 0, 1, 0,
        9, 98, 101, 110, 99, 104, 109, 97, 114, 107, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 0,
        0,
    ];

    let api_key = 11;
    let api_version = 5;

    let metadata =
        Bytes::from_static(b"\0\x03\0\0\0\x01\0\tbenchmark\0\0\0\0\0\0\0\0\xff\xff\xff\xff\0\0");

    assert_eq!(
        Frame {
            size: 200,
            header: Header::Response { correlation_id: 4 },
            body: Body::JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code: 0,
                generation_id: 1,
                protocol_type: None,
                protocol_name: Some("range".into()),
                leader: "rdkafka-499e5770-375e-4990-bf84-a39634e3bfe4".into(),
                skip_assignment: None,
                member_id: "rdkafka-499e5770-375e-4990-bf84-a39634e3bfe4".into(),
                members: Some(
                    [JoinGroupResponseMember {
                        member_id: "rdkafka-499e5770-375e-4990-bf84-a39634e3bfe4".into(),
                        group_instance_id: None,
                        metadata
                    }]
                    .into()
                )
            }
        },
        Frame::response_from_bytes(&v, api_key, api_version)?
    );

    Ok(())
}

#[test]
fn join_group_request_v9_000() -> Result<()> {
    use tansu_kafka_sans_io::join_group_request::JoinGroupRequestProtocol;

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 154, 0, 11, 0, 9, 0, 0, 0, 5, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99,
        111, 110, 115, 117, 109, 101, 114, 0, 20, 116, 101, 115, 116, 45, 99, 111, 110, 115, 117,
        109, 101, 114, 45, 103, 114, 111, 117, 112, 0, 0, 175, 200, 0, 4, 147, 224, 1, 0, 9, 99,
        111, 110, 115, 117, 109, 101, 114, 3, 6, 114, 97, 110, 103, 101, 27, 0, 3, 0, 0, 0, 1, 0,
        4, 116, 101, 115, 116, 255, 255, 255, 255, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 0, 19,
        99, 111, 111, 112, 101, 114, 97, 116, 105, 118, 101, 45, 115, 116, 105, 99, 107, 121, 31,
        0, 3, 0, 0, 0, 1, 0, 4, 116, 101, 115, 116, 0, 0, 0, 4, 255, 255, 255, 255, 0, 0, 0, 0,
        255, 255, 255, 255, 255, 255, 0, 1, 0,
    ];

    assert_eq!(
        Frame {
            size: 154,
            header: Header::Request {
                api_key: 11,
                api_version: 9,
                correlation_id: 5,
                client_id: Some("console-consumer".into())
            },
            body: Body::JoinGroupRequest {
                group_id: "test-consumer-group".into(),
                session_timeout_ms: 45_000,
                rebalance_timeout_ms: Some(300_000),
                member_id: "".into(),
                group_instance_id: None,
                protocol_type: "consumer".into(),
                protocols: Some(
                    [JoinGroupRequestProtocol {
                        name: "range".into(),
                        metadata: Bytes::from_static(b"\0\x03\0\0\0\x01\0\x04test\xff\xff\xff\xff\0\0\0\0\xff\xff\xff\xff\xff\xff")
                    },
                    JoinGroupRequestProtocol {
                        name: "cooperative-sticky".into(),
                        metadata: Bytes::from_static(b"\0\x03\0\0\0\x01\0\x04test\0\0\0\x04\xff\xff\xff\xff\0\0\0\0\xff\xff\xff\xff\xff\xff")
                    }
                    ].into()),
                reason: Some("".into())
            }
        },
        Frame::request_from_bytes(&v)?
    );

    Ok(())
}

#[test]
fn leave_group_request_v5_000() -> Result<()> {
    use tansu_kafka_sans_io::leave_group_request::MemberIdentity;

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 85, 0, 13, 0, 5, 0, 0, 0, 11, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99,
        111, 110, 115, 117, 109, 101, 114, 0, 20, 116, 101, 115, 116, 45, 99, 111, 110, 115, 117,
        109, 101, 114, 45, 103, 114, 111, 117, 112, 2, 5, 49, 48, 48, 48, 0, 29, 116, 104, 101, 32,
        99, 111, 110, 115, 117, 109, 101, 114, 32, 105, 115, 32, 98, 101, 105, 110, 103, 32, 99,
        108, 111, 115, 101, 100, 0, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 85,
            header: Header::Request {
                api_key: 13,
                api_version: 5,
                correlation_id: 11,
                client_id: Some("console-consumer".into())
            },
            body: Body::LeaveGroupRequest {
                group_id: "test-consumer-group".into(),
                member_id: None,
                members: Some(
                    [MemberIdentity {
                        member_id: "1000".into(),
                        group_instance_id: None,
                        reason: Some("the consumer is being closed".into())
                    }]
                    .into()
                )
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn list_groups_request_v4_000() -> Result<()> {
    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 26, 0, 16, 0, 4, 0, 0, 0, 84, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
        110, 116, 45, 49, 0, 1, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 26,
            header: Header::Request {
                api_key: 16,
                api_version: 4,
                correlation_id: 84,
                client_id: Some("adminclient-1".into()),
            },
            body: Body::ListGroupsRequest {
                states_filter: Some([].into())
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn list_offsets_response_v0_000() -> Result<()> {
    use tansu_kafka_sans_io::list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
    };

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 67, 0, 0, 0, 0, 0, 0, 0, 1, 0, 11, 97, 98, 99, 97, 98, 99, 97, 98, 99, 97, 98, 0,
        0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 18, 37, 164, 0, 0, 0, 0, 0, 17, 233,
        252, 0, 0, 0, 0, 0, 17, 198, 100, 0, 0, 0, 0, 0, 0, 0, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::response(&mut c, 2, 0);

    assert_eq!(
        Frame {
            size: 67,
            header: Header::Response { correlation_id: 0 },
            body: Body::ListOffsetsResponse {
                throttle_time_ms: None,
                topics: Some(
                    [ListOffsetsTopicResponse {
                        name: "abcabcabcab".into(),
                        partitions: Some(
                            [ListOffsetsPartitionResponse {
                                partition_index: 1,
                                error_code: 0,
                                old_style_offsets: Some([1189284, 1174012, 1164900, 0].into()),
                                timestamp: None,
                                offset: None,
                                leader_epoch: None,
                            }]
                            .into()
                        ),
                    }]
                    .into()
                ),
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn list_partition_reassignments_request_v0_000() -> Result<()> {
    use tansu_kafka_sans_io::list_partition_reassignments_request::ListPartitionReassignmentsTopics;

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 49, 0, 46, 0, 0, 0, 0, 0, 7, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
        110, 116, 45, 49, 0, 0, 0, 117, 48, 2, 5, 116, 101, 115, 116, 4, 0, 0, 0, 1, 0, 0, 0, 0, 0,
        0, 0, 2, 0, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 49,
            header: Header::Request {
                api_key: 46,
                api_version: 0,
                correlation_id: 7,
                client_id: Some("adminclient-1".into())
            },
            body: Body::ListPartitionReassignmentsRequest {
                timeout_ms: 30_000,
                topics: Some(
                    [ListPartitionReassignmentsTopics {
                        name: "test".into(),
                        partition_indexes: Some([1, 0, 2].into())
                    }]
                    .into()
                )
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[ignore]
#[test]
fn list_transactions_request_v1_000() -> Result<()> {
    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 35, 0, 66, 0, 1, 0, 0, 0, 4, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
        110, 116, 45, 49, 0, 1, 1, 255, 255, 255, 255, 255, 255, 255, 255, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 35,
            header: Header::Request {
                api_key: 66,
                api_version: 1,
                correlation_id: 4,
                client_id: Some("adminclient-1".into())
            },
            body: Body::ListTransactionsRequest {
                state_filters: Some([].into()),
                producer_id_filters: Some([].into()),
                duration_filter: Some(-1),
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn list_transactions_response_v1_000() -> Result<()> {
    let _guard = init_tracing()?;

    let api_key = 66;
    let api_version = 1;

    let v = vec![
        0, 0, 0, 70, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 2, 32, 108, 105, 98, 114, 100, 107, 97,
        102, 107, 97, 95, 116, 114, 97, 110, 115, 97, 99, 116, 105, 111, 110, 115, 95, 101, 120,
        97, 109, 112, 108, 101, 0, 0, 0, 0, 0, 0, 0, 0, 15, 67, 111, 109, 112, 108, 101, 116, 101,
        67, 111, 109, 109, 105, 116, 0, 0,
    ];

    assert_eq!(
        Frame {
            size: 70,
            header: Header::Response { correlation_id: 4 },
            body: Body::ListTransactionsResponse {
                throttle_time_ms: 0,
                error_code: 0,
                unknown_state_filters: Some([].into()),
                transaction_states: Some(
                    [TransactionState {
                        transactional_id: "librdkafka_transactions_example".into(),
                        producer_id: 0,
                        transaction_state: "CompleteCommit".into()
                    }]
                    .into()
                )
            }
        },
        Frame::response_from_bytes(&v, api_key, api_version)?
    );

    Ok(())
}

#[test]
fn metadata_request_v1_000() -> Result<()> {
    use tansu_kafka_sans_io::metadata_request::MetadataRequestTopic;

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 30, 0, 3, 0, 1, 0, 0, 0, 1, 0, 5, 115, 97, 109, 115, 97, 0, 0, 0, 1, 0, 9, 98,
        101, 110, 99, 104, 109, 97, 114, 107,
    ];

    assert_eq!(
        Frame {
            size: 30,
            header: Header::Request {
                api_key: 3,
                api_version: 1,
                correlation_id: 1,
                client_id: Some("samsa".into())
            },
            body: Body::MetadataRequest {
                topics: Some(
                    [MetadataRequestTopic {
                        topic_id: None,
                        name: Some("benchmark".into())
                    }]
                    .into()
                ),
                allow_auto_topic_creation: None,
                include_cluster_authorized_operations: None,
                include_topic_authorized_operations: None
            }
        },
        Frame::request_from_bytes(&v)?
    );

    Ok(())
}

#[test]
fn metadata_response_v1_000() -> Result<()> {
    let _guard = init_tracing()?;

    let api_key = 3;
    let api_version = 1;

    let v = vec![
        0, 0, 0, 237, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 9, 108, 111, 99, 97, 108, 104, 111,
        115, 116, 0, 0, 35, 132, 255, 255, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 9, 98, 101, 110, 99,
        104, 109, 97, 114, 107, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0,
        1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0,
        1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 6, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0,
        1, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0,
        0, 0, 5, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 4, 0, 0, 0, 1, 0,
        0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1,
    ];

    assert_eq!(
        Frame {
            size: 237,
            header: Header::Response { correlation_id: 1 },
            body: Body::MetadataResponse {
                throttle_time_ms: None,
                brokers: Some(
                    [MetadataResponseBroker {
                        node_id: 1,
                        host: "localhost".into(),
                        port: 9092,
                        rack: None
                    }]
                    .into()
                ),
                cluster_id: None,
                controller_id: Some(1),
                topics: Some(
                    [MetadataResponseTopic {
                        error_code: 0,
                        name: Some("benchmark".into()),
                        topic_id: None,
                        is_internal: Some(false),
                        partitions: Some(
                            [
                                MetadataResponsePartition {
                                    error_code: 0,
                                    partition_index: 1,
                                    leader_id: 1,
                                    leader_epoch: None,
                                    replica_nodes: Some([1].into()),
                                    isr_nodes: Some([1].into()),
                                    offline_replicas: None
                                },
                                MetadataResponsePartition {
                                    error_code: 0,
                                    partition_index: 3,
                                    leader_id: 1,
                                    leader_epoch: None,
                                    replica_nodes: Some([1].into()),
                                    isr_nodes: Some([1].into()),
                                    offline_replicas: None
                                },
                                MetadataResponsePartition {
                                    error_code: 0,
                                    partition_index: 6,
                                    leader_id: 1,
                                    leader_epoch: None,
                                    replica_nodes: Some([1].into()),
                                    isr_nodes: Some([1].into()),
                                    offline_replicas: None
                                },
                                MetadataResponsePartition {
                                    error_code: 0,
                                    partition_index: 2,
                                    leader_id: 1,
                                    leader_epoch: None,
                                    replica_nodes: Some([1].into()),
                                    isr_nodes: Some([1].into()),
                                    offline_replicas: None
                                },
                                MetadataResponsePartition {
                                    error_code: 0,
                                    partition_index: 5,
                                    leader_id: 1,
                                    leader_epoch: None,
                                    replica_nodes: Some([1].into()),
                                    isr_nodes: Some([1].into()),
                                    offline_replicas: None
                                },
                                MetadataResponsePartition {
                                    error_code: 0,
                                    partition_index: 0,
                                    leader_id: 1,
                                    leader_epoch: None,
                                    replica_nodes: Some([1].into()),
                                    isr_nodes: Some([1].into()),
                                    offline_replicas: None
                                },
                                MetadataResponsePartition {
                                    error_code: 0,
                                    partition_index: 4,
                                    leader_id: 1,
                                    leader_epoch: None,
                                    replica_nodes: Some([1].into()),
                                    isr_nodes: Some([1].into()),
                                    offline_replicas: None
                                }
                            ]
                            .into()
                        ),
                        topic_authorized_operations: None
                    }]
                    .into()
                ),
                cluster_authorized_operations: None
            }
        },
        Frame::response_from_bytes(&v, api_key, api_version)?
    );

    Ok(())
}

#[test]
fn metadata_request_v12_000() -> Result<()> {
    use tansu_kafka_sans_io::metadata_request::MetadataRequestTopic;

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 53, 0, 3, 0, 12, 0, 0, 0, 5, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
        114, 111, 100, 117, 99, 101, 114, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5,
        116, 101, 115, 116, 0, 1, 0, 0,
    ];

    assert_eq!(
        Frame {
            size: 53,
            header: Header::Request {
                api_key: 3,
                api_version: 12,
                correlation_id: 5,
                client_id: Some("console-producer".into()),
            },
            body: Body::MetadataRequest {
                topics: Some(
                    [MetadataRequestTopic {
                        topic_id: Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
                        name: Some("test".into()),
                    }]
                    .into()
                ),
                allow_auto_topic_creation: Some(true),
                include_cluster_authorized_operations: None,
                include_topic_authorized_operations: Some(false),
            }
        },
        Frame::request_from_bytes(&v)?
    );

    Ok(())
}

#[test]
fn metadata_response_v12_000() -> Result<()> {
    use tansu_kafka_sans_io::metadata_response::{MetadataResponseBroker, MetadataResponseTopic};

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 92, 0, 0, 0, 5, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 13, 107, 97, 102, 107, 97, 45, 115,
        101, 114, 118, 101, 114, 0, 0, 35, 132, 0, 0, 23, 82, 118, 81, 119, 114, 89, 101, 103, 83,
        85, 67, 107, 73, 80, 107, 97, 105, 65, 90, 81, 108, 81, 0, 0, 0, 0, 2, 0, 3, 5, 116, 101,
        115, 116, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 128, 0, 0, 0, 0, 0,
    ];

    let api_key = 3;
    let api_version = 12;

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::response(&mut c, api_key, api_version);

    assert_eq!(
        Frame {
            size: 92,
            header: Header::Response { correlation_id: 5 },
            body: Body::MetadataResponse {
                throttle_time_ms: Some(0),
                brokers: Some(vec![MetadataResponseBroker {
                    node_id: 0,
                    host: "kafka-server".into(),
                    port: 9092,
                    rack: None,
                }]),
                cluster_id: Some("RvQwrYegSUCkIPkaiAZQlQ".into()),
                controller_id: Some(0),
                topics: Some(vec![MetadataResponseTopic {
                    error_code: 3,
                    name: Some("test".into()),
                    topic_id: Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
                    is_internal: Some(false),
                    partitions: Some(vec![]),
                    topic_authorized_operations: Some(-2147483648),
                }]),
                cluster_authorized_operations: None,
            },
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn metadata_request_v12_001() -> Result<()> {
    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 31, 0, 3, 0, 12, 0, 0, 0, 1, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
        114, 111, 100, 117, 99, 101, 114, 0, 1, 1, 0, 0,
    ];

    assert_eq!(
        Frame {
            size: 31,
            header: Header::Request {
                api_key: 3,
                api_version: 12,
                correlation_id: 1,
                client_id: Some("console-producer".into()),
            },
            body: Body::MetadataRequest {
                topics: Some([].into()),
                allow_auto_topic_creation: Some(true),
                include_cluster_authorized_operations: None,
                include_topic_authorized_operations: Some(false),
            }
        },
        Frame::request_from_bytes(&v)?
    );

    Ok(())
}

#[test]
fn metadata_request_v12_002() -> Result<()> {
    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 49, 0, 3, 0, 12, 0, 0, 0, 2, 0, 7, 114, 100, 107, 97, 102, 107, 97, 0, 2, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 98, 101, 110, 99, 104, 109, 97, 114, 107, 0, 1,
        0, 0,
    ];

    assert_eq!(
        Frame {
            size: 49,
            header: Header::Request {
                api_key: 3,
                api_version: 12,
                correlation_id: 2,
                client_id: Some("rdkafka".into())
            },
            body: Body::MetadataRequest {
                topics: Some(
                    [MetadataRequestTopic {
                        topic_id: Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
                        name: Some("benchmark".into())
                    }]
                    .into()
                ),
                allow_auto_topic_creation: Some(true),
                include_cluster_authorized_operations: None,
                include_topic_authorized_operations: Some(false)
            }
        },
        Frame::request_from_bytes(&v)?
    );

    Ok(())
}

#[test]
fn metadata_response_v12_002() -> Result<()> {
    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 1, 20, 0, 0, 0, 2, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 10, 108, 111, 99, 97, 108, 104, 111,
        115, 116, 0, 0, 35, 132, 0, 0, 23, 53, 76, 54, 103, 51, 110, 83, 104, 84, 45, 101, 77, 67,
        116, 75, 45, 45, 88, 56, 54, 115, 119, 0, 0, 0, 1, 2, 0, 0, 10, 98, 101, 110, 99, 104, 109,
        97, 114, 107, 177, 248, 14, 236, 65, 78, 72, 57, 179, 196, 215, 75, 145, 238, 120, 241, 0,
        8, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 0, 0, 0, 0,
        0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 6, 0,
        0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 0,
        0, 0, 0, 2, 0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 0, 0, 0, 0, 2,
        0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 0, 1,
        2, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 0, 1, 2, 0, 0, 0,
        1, 1, 0, 128, 0, 0, 0, 0, 0,
    ];

    let api_key = 3;
    let api_version = 12;

    assert_eq!(
        Frame {
            size: 276,
            header: Header::Response { correlation_id: 2 },
            body: Body::MetadataResponse {
                throttle_time_ms: Some(0),
                brokers: Some(
                    [MetadataResponseBroker {
                        node_id: 1,
                        host: "localhost".into(),
                        port: 9092,
                        rack: None
                    }]
                    .into()
                ),
                cluster_id: Some("5L6g3nShT-eMCtK--X86sw".into()),
                controller_id: Some(1),
                topics: Some(
                    [MetadataResponseTopic {
                        error_code: 0,
                        name: Some("benchmark".into()),
                        topic_id: Some([
                            177, 248, 14, 236, 65, 78, 72, 57, 179, 196, 215, 75, 145, 238, 120,
                            241
                        ]),
                        is_internal: Some(false),
                        partitions: Some(
                            [
                                MetadataResponsePartition {
                                    error_code: 0,
                                    partition_index: 1,
                                    leader_id: 1,
                                    leader_epoch: Some(0),
                                    replica_nodes: Some([1].into()),
                                    isr_nodes: Some([1].into()),
                                    offline_replicas: Some([].into())
                                },
                                MetadataResponsePartition {
                                    error_code: 0,
                                    partition_index: 3,
                                    leader_id: 1,
                                    leader_epoch: Some(0),
                                    replica_nodes: Some([1].into()),
                                    isr_nodes: Some([1].into()),
                                    offline_replicas: Some([].into())
                                },
                                MetadataResponsePartition {
                                    error_code: 0,
                                    partition_index: 6,
                                    leader_id: 1,
                                    leader_epoch: Some(0),
                                    replica_nodes: Some([1].into()),
                                    isr_nodes: Some([1].into()),
                                    offline_replicas: Some([].into())
                                },
                                MetadataResponsePartition {
                                    error_code: 0,
                                    partition_index: 2,
                                    leader_id: 1,
                                    leader_epoch: Some(0),
                                    replica_nodes: Some([1].into()),
                                    isr_nodes: Some([1].into()),
                                    offline_replicas: Some([].into())
                                },
                                MetadataResponsePartition {
                                    error_code: 0,
                                    partition_index: 5,
                                    leader_id: 1,
                                    leader_epoch: Some(0),
                                    replica_nodes: Some([1].into()),
                                    isr_nodes: Some([1].into()),
                                    offline_replicas: Some([].into())
                                },
                                MetadataResponsePartition {
                                    error_code: 0,
                                    partition_index: 0,
                                    leader_id: 1,
                                    leader_epoch: Some(0),
                                    replica_nodes: Some([1].into()),
                                    isr_nodes: Some([1].into()),
                                    offline_replicas: Some([].into())
                                },
                                MetadataResponsePartition {
                                    error_code: 0,
                                    partition_index: 4,
                                    leader_id: 1,
                                    leader_epoch: Some(0),
                                    replica_nodes: Some([1].into()),
                                    isr_nodes: Some([1].into()),
                                    offline_replicas: Some([].into())
                                }
                            ]
                            .into()
                        ),
                        topic_authorized_operations: Some(-2147483648)
                    }]
                    .into()
                ),
                cluster_authorized_operations: None
            }
        },
        Frame::response_from_bytes(&v, api_key, api_version)?
    );

    Ok(())
}

#[test]
fn offset_fetch_request_v3_000() -> Result<()> {
    use tansu_kafka_sans_io::offset_fetch_request::OffsetFetchRequestTopic;

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 65, 0, 9, 0, 3, 0, 0, 0, 0, 255, 255, 0, 3, 97, 98, 99, 0, 0, 0, 2, 0, 5, 116,
        101, 115, 116, 50, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 5, 116, 101, 115,
        116, 49, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 65,
            header: Header::Request {
                api_key: 9,
                api_version: 3,
                correlation_id: 0,
                client_id: None,
            },
            body: Body::OffsetFetchRequest {
                group_id: Some("abc".into()),
                topics: Some(
                    [
                        OffsetFetchRequestTopic {
                            name: "test2".into(),
                            partition_indexes: Some([3, 4, 5].into()),
                        },
                        OffsetFetchRequestTopic {
                            name: "test1".into(),
                            partition_indexes: Some([0, 1, 2].into()),
                        }
                    ]
                    .into()
                ),
                groups: None,
                require_stable: None,
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn offset_fetch_request_v7_000() -> Result<()> {
    use tansu_kafka_sans_io::offset_fetch_request::OffsetFetchRequestTopic;

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 87, 0, 9, 0, 7, 0, 0, 0, 8, 0, 7, 114, 100, 107, 97, 102, 107, 97, 0, 26, 101,
        120, 97, 109, 112, 108, 101, 95, 99, 111, 110, 115, 117, 109, 101, 114, 95, 103, 114, 111,
        117, 112, 95, 105, 100, 2, 10, 98, 101, 110, 99, 104, 109, 97, 114, 107, 8, 0, 0, 0, 0, 0,
        0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 6, 0, 1, 0,
    ];

    assert_eq!(
        Frame {
            size: 87,
            header: Header::Request {
                api_key: 9,
                api_version: 7,
                correlation_id: 8,
                client_id: Some("rdkafka".into()),
            },
            body: Body::OffsetFetchRequest {
                group_id: Some("example_consumer_group_id".into()),
                topics: Some(
                    [OffsetFetchRequestTopic {
                        name: "benchmark".into(),
                        partition_indexes: Some((0..7).collect()),
                    }]
                    .into()
                ),
                groups: None,
                require_stable: Some(true),
            }
        },
        Frame::request_from_bytes(&v)?
    );

    Ok(())
}

#[test]
fn offset_fetch_response_v7_000() -> Result<()> {
    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 165, 0, 0, 0, 8, 0, 0, 0, 0, 0, 2, 10, 98, 101, 110, 99, 104, 109, 97, 114, 107,
        8, 0, 0, 0, 1, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0,
        0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0, 0,
        6, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0, 0, 5, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0, 0, 4, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0, 0, 3, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0, 0, 2, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0, 0, 0,
    ];

    let api_key = 9;
    let api_version = 7;

    assert_eq!(
        Frame {
            size: 165,
            header: Header::Response { correlation_id: 8 },
            body: Body::OffsetFetchResponse {
                throttle_time_ms: Some(0),
                error_code: Some(ErrorCode::None.into()),
                groups: None,
                topics: Some(
                    [OffsetFetchResponseTopic {
                        name: "benchmark".into(),
                        partitions: Some(
                            [1, 0, 6, 5, 4, 3, 2]
                                .into_iter()
                                .map(|partition_index| {
                                    OffsetFetchResponsePartition {
                                        partition_index,
                                        committed_offset: -1,
                                        committed_leader_epoch: Some(-1),
                                        metadata: Some("".into()),
                                        error_code: ErrorCode::None.into(),
                                    }
                                })
                                .collect()
                        )
                    }]
                    .into()
                )
            }
        },
        Frame::response_from_bytes(&v, api_key, api_version)?
    );

    Ok(())
}

#[test]
fn offset_fetch_request_v9_000() -> Result<()> {
    use tansu_kafka_sans_io::offset_fetch_request::{
        OffsetFetchRequestGroup, OffsetFetchRequestTopics,
    };

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 76, 0, 9, 0, 9, 0, 0, 0, 7, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99, 111,
        110, 115, 117, 109, 101, 114, 0, 2, 20, 116, 101, 115, 116, 45, 99, 111, 110, 115, 117,
        109, 101, 114, 45, 103, 114, 111, 117, 112, 0, 255, 255, 255, 255, 2, 5, 116, 101, 115,
        116, 4, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 1, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 76,
            header: Header::Request {
                api_key: 9,
                api_version: 9,
                correlation_id: 7,
                client_id: Some("console-consumer".into())
            },
            body: Body::OffsetFetchRequest {
                group_id: None,
                topics: None,
                groups: Some(
                    [OffsetFetchRequestGroup {
                        group_id: "test-consumer-group".into(),
                        member_id: None,
                        member_epoch: Some(-1),
                        topics: Some(
                            [OffsetFetchRequestTopics {
                                name: "test".into(),
                                partition_indexes: Some([1, 0, 2].into())
                            }]
                            .into()
                        )
                    }]
                    .into()
                ),
                require_stable: Some(true)
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn offset_commit_request_v9_000() -> Result<()> {
    use tansu_kafka_sans_io::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 120, 0, 8, 0, 9, 0, 0, 0, 10, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99,
        111, 110, 115, 117, 109, 101, 114, 0, 20, 116, 101, 115, 116, 45, 99, 111, 110, 115, 117,
        109, 101, 114, 45, 103, 114, 111, 117, 112, 0, 0, 0, 0, 5, 49, 48, 48, 48, 0, 2, 5, 116,
        101, 115, 116, 4, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0,
        0, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 120,
            header: Header::Request {
                api_key: 8,
                api_version: 9,
                correlation_id: 10,
                client_id: Some("console-consumer".into())
            },
            body: Body::OffsetCommitRequest {
                group_id: "test-consumer-group".into(),
                generation_id_or_member_epoch: Some(0),
                member_id: Some("1000".into()),
                group_instance_id: None,
                retention_time_ms: None,
                topics: Some(
                    [OffsetCommitRequestTopic {
                        name: "test".into(),
                        partitions: Some(
                            [
                                OffsetCommitRequestPartition {
                                    partition_index: 1,
                                    committed_offset: 0,
                                    committed_leader_epoch: Some(0),
                                    commit_timestamp: None,
                                    committed_metadata: Some("".into())
                                },
                                OffsetCommitRequestPartition {
                                    partition_index: 0,
                                    committed_offset: 0,
                                    committed_leader_epoch: Some(0),
                                    commit_timestamp: None,
                                    committed_metadata: Some("".into())
                                },
                                OffsetCommitRequestPartition {
                                    partition_index: 2,
                                    committed_offset: 0,
                                    committed_leader_epoch: Some(0),
                                    commit_timestamp: None,
                                    committed_metadata: Some("".into())
                                }
                            ]
                            .into()
                        )
                    }]
                    .into()
                )
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn offset_for_leader_request_v0_000() -> Result<()> {
    use tansu_kafka_sans_io::offset_for_leader_epoch_request::OffsetForLeaderTopic;

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 31, 0, 23, 0, 0, 0, 0, 0, 0, 255, 255, 0, 0, 0, 1, 0, 11, 97, 98, 99, 97, 98, 99,
        97, 98, 99, 97, 98, 0, 0, 0, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 31,
            header: Header::Request {
                api_key: 23,
                api_version: 0,
                correlation_id: 0,
                client_id: None,
            },
            body: Body::OffsetForLeaderEpochRequest {
                replica_id: None,
                topics: Some(
                    [OffsetForLeaderTopic {
                        topic: "abcabcabcab".into(),
                        partitions: Some([].into()),
                    }]
                    .into()
                ),
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn produce_request_v3_000() -> Result<()> {
    use tansu_kafka_sans_io::produce_request::{PartitionProduceData, TopicProduceData};

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 196, 0, 0, 0, 3, 0, 0, 0, 1, 0, 5, 115, 97, 109, 115, 97, 255, 255, 0, 0, 0, 0, 3,
        232, 0, 0, 0, 1, 0, 9, 98, 101, 110, 99, 104, 109, 97, 114, 107, 0, 0, 0, 1, 0, 0, 0, 0, 0,
        0, 0, 146, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 134, 255, 255, 255, 255, 2, 194, 19, 88, 191,
        0, 0, 0, 0, 0, 4, 0, 0, 1, 145, 158, 51, 63, 130, 0, 0, 1, 145, 158, 51, 63, 130, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 5, 32, 0, 0, 0, 0, 20,
        48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 0, 32, 0, 0, 2, 0, 20, 48, 49, 50, 51, 52, 53, 54,
        55, 56, 57, 0, 32, 0, 0, 4, 0, 20, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 0, 32, 0, 0, 6,
        0, 20, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 0, 32, 0, 0, 8, 0, 20, 48, 49, 50, 51, 52,
        53, 54, 55, 56, 57, 0,
    ];

    assert_eq!(
        Frame {
            size: 196,
            header: Header::Request {
                api_key: 0,
                api_version: 3,
                correlation_id: 1,
                client_id: Some("samsa".into())
            },
            body: Body::ProduceRequest {
                transactional_id: None,
                acks: 0,
                timeout_ms: 1000,
                topic_data: Some(
                    [TopicProduceData {
                        name: "benchmark".into(),
                        partition_data: Some(
                            [PartitionProduceData {
                                index: 0,
                                records: Some(
                                    inflated::Frame {
                                        batches: [inflated::Batch {
                                            base_offset: 0,
                                            batch_length: 134,
                                            partition_leader_epoch: -1,
                                            magic: 2,
                                            crc: 3256047807,
                                            attributes: 0,
                                            last_offset_delta: 4,
                                            base_timestamp: 1724936044418,
                                            max_timestamp: 1724936044418,
                                            producer_id: -1,
                                            producer_epoch: -1,
                                            base_sequence: -1,
                                            records: [
                                                Record {
                                                    length: 16,
                                                    attributes: 0,
                                                    timestamp_delta: 0,
                                                    offset_delta: 0,
                                                    key: Some(Bytes::from_static(b"")),
                                                    value: Some(Bytes::from_static(b"0123456789")),
                                                    headers: [].into()
                                                },
                                                Record {
                                                    length: 16,
                                                    attributes: 0,
                                                    timestamp_delta: 0,
                                                    offset_delta: 1,
                                                    key: Some(Bytes::from_static(b"")),
                                                    value: Some(Bytes::from_static(b"0123456789")),
                                                    headers: [].into()
                                                },
                                                Record {
                                                    length: 16,
                                                    attributes: 0,
                                                    timestamp_delta: 0,
                                                    offset_delta: 2,
                                                    key: Some(Bytes::from_static(b"")),
                                                    value: Some(Bytes::from_static(b"0123456789")),
                                                    headers: [].into()
                                                },
                                                Record {
                                                    length: 16,
                                                    attributes: 0,
                                                    timestamp_delta: 0,
                                                    offset_delta: 3,
                                                    key: Some(Bytes::from_static(b"")),
                                                    value: Some(Bytes::from_static(b"0123456789")),
                                                    headers: [].into()
                                                },
                                                Record {
                                                    length: 16,
                                                    attributes: 0,
                                                    timestamp_delta: 0,
                                                    offset_delta: 4,
                                                    key: Some(Bytes::from_static(b"")),
                                                    value: Some(Bytes::from_static(b"0123456789")),
                                                    headers: [].into()
                                                }
                                            ]
                                            .into()
                                        }]
                                        .into()
                                    }
                                    .try_into()?
                                )
                            }]
                            .into()
                        )
                    }]
                    .into()
                )
            }
        },
        Frame::request_from_bytes(&v)?
    );

    Ok(())
}

#[test]
fn produce_request_v7_000() -> Result<()> {
    use tansu_kafka_sans_io::produce_request::{PartitionProduceData, TopicProduceData};

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 158, 0, 0, 0, 7, 0, 0, 0, 3, 0, 7, 114, 100, 107, 97, 102, 107, 97, 255, 255, 255,
        255, 0, 0, 117, 48, 0, 0, 0, 1, 0, 9, 98, 101, 110, 99, 104, 109, 97, 114, 107, 0, 0, 0, 1,
        0, 0, 0, 0, 0, 0, 0, 106, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 94, 0, 0, 0, 0, 2, 178, 166,
        246, 227, 0, 0, 0, 0, 0, 0, 0, 0, 1, 145, 158, 83, 211, 188, 0, 0, 1, 145, 158, 83, 211,
        188, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 88,
        0, 0, 0, 10, 75, 101, 121, 32, 48, 18, 77, 101, 115, 115, 97, 103, 101, 32, 48, 2, 20, 104,
        101, 97, 100, 101, 114, 95, 107, 101, 121, 24, 104, 101, 97, 100, 101, 114, 95, 118, 97,
        108, 117, 101,
    ];

    assert_eq!(
        Frame {
            size: 158,
            header: Header::Request {
                api_key: 0,
                api_version: 7,
                correlation_id: 3,
                client_id: Some("rdkafka".into())
            },
            body: Body::ProduceRequest {
                transactional_id: None,
                acks: -1,
                timeout_ms: 30000,
                topic_data: Some(
                    [TopicProduceData {
                        name: "benchmark".into(),
                        partition_data: Some(
                            [PartitionProduceData {
                                index: 0,
                                records: Some(
                                    inflated::Frame {
                                        batches: [inflated::Batch {
                                            base_offset: 0,
                                            batch_length: 94,
                                            partition_leader_epoch: 0,
                                            magic: 2,
                                            crc: 2997286627,
                                            attributes: 0,
                                            last_offset_delta: 0,
                                            base_timestamp: 1724938179516,
                                            max_timestamp: 1724938179516,
                                            producer_id: -1,
                                            producer_epoch: -1,
                                            base_sequence: -1,
                                            records: [Record {
                                                length: 44,
                                                attributes: 0,
                                                timestamp_delta: 0,
                                                offset_delta: 0,
                                                key: Some(Bytes::from_static(b"Key 0")),
                                                value: Some(Bytes::from_static(b"Message 0")),
                                                headers: [record::Header {
                                                    key: Some(Bytes::from_static(b"header_key")),
                                                    value: Some(Bytes::from_static(
                                                        b"header_value"
                                                    ))
                                                }]
                                                .into()
                                            }]
                                            .into()
                                        }]
                                        .into()
                                    }
                                    .try_into()?
                                )
                            }]
                            .into()
                        )
                    }]
                    .into()
                )
            }
        },
        Frame::request_from_bytes(&v)?
    );

    Ok(())
}

#[test]
fn produce_request_v9_000() -> Result<()> {
    use tansu_kafka_sans_io::produce_request::{PartitionProduceData, TopicProduceData};

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 120, 0, 0, 0, 9, 0, 0, 0, 6, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
        114, 111, 100, 117, 99, 101, 114, 0, 0, 255, 255, 0, 0, 5, 220, 2, 5, 116, 101, 115, 116,
        2, 0, 0, 0, 0, 72, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 59, 255, 255, 255, 255, 2, 67, 41, 231,
        61, 0, 0, 0, 0, 0, 0, 0, 0, 1, 141, 116, 152, 137, 53, 0, 0, 1, 141, 116, 152, 137, 53, 0,
        0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 18, 0, 0, 0, 1, 6, 100, 101, 102, 0, 0,
        0, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 120,
            header: Header::Request {
                api_key: 0,
                api_version: 9,
                correlation_id: 6,
                client_id: Some("console-producer".into()),
            },
            body: Body::ProduceRequest {
                transactional_id: None,
                acks: -1,
                timeout_ms: 1500,
                topic_data: Some(
                    [TopicProduceData {
                        name: "test".into(),
                        partition_data: Some(
                            [PartitionProduceData {
                                index: 0,
                                records: Some(
                                    inflated::Frame {
                                        batches: [inflated::Batch {
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
                                                headers: [].into()
                                            }]
                                            .into()
                                        }]
                                        .into(),
                                    }
                                    .try_into()?
                                ),
                            }]
                            .into()
                        )
                    }]
                    .into()
                ),
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn produce_request_v9_001() -> Result<()> {
    use tansu_kafka_sans_io::produce_request::{PartitionProduceData, TopicProduceData};

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 234, 0, 0, 0, 9, 0, 0, 0, 6, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
        114, 111, 100, 117, 99, 101, 114, 0, 0, 255, 255, 0, 0, 5, 220, 2, 5, 116, 101, 115, 116,
        2, 0, 0, 0, 0, 185, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 172, 255, 255, 255, 255, 2, 36,
        177, 198, 157, 0, 0, 0, 0, 0, 10, 0, 0, 1, 145, 39, 109, 210, 54, 0, 0, 1, 145, 39, 109,
        210, 54, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 11, 20, 0, 0, 0, 4, 107, 49, 4,
        118, 49, 0, 20, 0, 0, 2, 4, 107, 50, 4, 118, 50, 0, 20, 0, 0, 4, 4, 107, 49, 4, 118, 51, 0,
        20, 0, 0, 6, 4, 107, 49, 4, 118, 52, 0, 20, 0, 0, 8, 4, 107, 51, 4, 118, 53, 0, 20, 0, 0,
        10, 4, 107, 50, 4, 118, 54, 0, 20, 0, 0, 12, 4, 107, 52, 4, 118, 55, 0, 20, 0, 0, 14, 4,
        107, 53, 4, 118, 56, 0, 20, 0, 0, 16, 4, 107, 53, 4, 118, 57, 0, 22, 0, 0, 18, 4, 107, 50,
        6, 118, 49, 48, 0, 22, 0, 0, 20, 4, 107, 54, 6, 118, 49, 49, 0, 0, 0, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 234,
            header: Header::Request {
                api_key: 0,
                api_version: 9,
                correlation_id: 6,
                client_id: Some("console-producer".into())
            },
            body: Body::ProduceRequest {
                transactional_id: None,
                acks: -1,
                timeout_ms: 1500,
                topic_data: Some(
                    [TopicProduceData {
                        name: "test".into(),
                        partition_data: Some(
                            [PartitionProduceData {
                                index: 0,
                                records: Some(
                                    inflated::Frame {
                                        batches: [inflated::Batch {
                                            base_offset: 0,
                                            batch_length: 172,
                                            partition_leader_epoch: -1,
                                            magic: 2,
                                            crc: 615630493,
                                            attributes: 0,
                                            last_offset_delta: 10,
                                            base_timestamp: 1722943394358,
                                            max_timestamp: 1722943394358,
                                            producer_id: 1,
                                            producer_epoch: 0,
                                            base_sequence: 1,
                                            records: [
                                                Record {
                                                    length: 10,
                                                    attributes: 0,
                                                    timestamp_delta: 0,
                                                    offset_delta: 0,
                                                    key: Some(Bytes::from_static(&[107, 49])),
                                                    value: Some(Bytes::from_static(&[118, 49])),
                                                    headers: [].into()
                                                },
                                                Record {
                                                    length: 10,
                                                    attributes: 0,
                                                    timestamp_delta: 0,
                                                    offset_delta: 1,
                                                    key: Some(Bytes::from_static(&[107, 50])),
                                                    value: Some(Bytes::from_static(&[118, 50])),
                                                    headers: [].into()
                                                },
                                                Record {
                                                    length: 10,
                                                    attributes: 0,
                                                    timestamp_delta: 0,
                                                    offset_delta: 2,
                                                    key: Some(Bytes::from_static(&[107, 49])),
                                                    value: Some(Bytes::from_static(&[118, 51])),
                                                    headers: [].into()
                                                },
                                                Record {
                                                    length: 10,
                                                    attributes: 0,
                                                    timestamp_delta: 0,
                                                    offset_delta: 3,
                                                    key: Some(Bytes::from_static(&[107, 49])),
                                                    value: Some(Bytes::from_static(&[118, 52])),
                                                    headers: [].into()
                                                },
                                                Record {
                                                    length: 10,
                                                    attributes: 0,
                                                    timestamp_delta: 0,
                                                    offset_delta: 4,
                                                    key: Some(Bytes::from_static(&[107, 51])),
                                                    value: Some(Bytes::from_static(&[118, 53])),
                                                    headers: [].into()
                                                },
                                                Record {
                                                    length: 10,
                                                    attributes: 0,
                                                    timestamp_delta: 0,
                                                    offset_delta: 5,
                                                    key: Some(Bytes::from_static(&[107, 50])),
                                                    value: Some(Bytes::from_static(&[118, 54])),
                                                    headers: [].into()
                                                },
                                                Record {
                                                    length: 10,
                                                    attributes: 0,
                                                    timestamp_delta: 0,
                                                    offset_delta: 6,
                                                    key: Some(Bytes::from_static(&[107, 52])),
                                                    value: Some(Bytes::from_static(&[118, 55])),
                                                    headers: [].into()
                                                },
                                                Record {
                                                    length: 10,
                                                    attributes: 0,
                                                    timestamp_delta: 0,
                                                    offset_delta: 7,
                                                    key: Some(Bytes::from_static(&[107, 53])),
                                                    value: Some(Bytes::from_static(&[118, 56])),
                                                    headers: [].into()
                                                },
                                                Record {
                                                    length: 10,
                                                    attributes: 0,
                                                    timestamp_delta: 0,
                                                    offset_delta: 8,
                                                    key: Some(Bytes::from_static(&[107, 53])),
                                                    value: Some(Bytes::from_static(&[118, 57])),
                                                    headers: [].into()
                                                },
                                                Record {
                                                    length: 11,
                                                    attributes: 0,
                                                    timestamp_delta: 0,
                                                    offset_delta: 9,
                                                    key: Some(Bytes::from_static(&[107, 50])),
                                                    value: Some(Bytes::from_static(&[118, 49, 48])),
                                                    headers: [].into()
                                                },
                                                Record {
                                                    length: 11,
                                                    attributes: 0,
                                                    timestamp_delta: 0,
                                                    offset_delta: 10,
                                                    key: Some(Bytes::from_static(&[107, 54])),
                                                    value: Some(Bytes::from_static(&[118, 49, 49])),
                                                    headers: [].into()
                                                }
                                            ]
                                            .into()
                                        }]
                                        .into()
                                    }
                                    .try_into()?
                                )
                            }]
                            .into()
                        )
                    }]
                    .into()
                )
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn produce_request_v10_000() -> Result<()> {
    use tansu_kafka_sans_io::produce_request::{PartitionProduceData, TopicProduceData};

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 149, 0, 0, 0, 10, 0, 0, 0, 7, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
        114, 111, 100, 117, 99, 101, 114, 0, 0, 255, 255, 0, 0, 5, 220, 2, 5, 116, 101, 115, 116,
        2, 0, 0, 0, 0, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 88, 255, 255, 255, 255, 2, 61, 161,
        73, 50, 0, 0, 0, 0, 0, 0, 0, 0, 1, 144, 237, 224, 72, 1, 0, 0, 1, 144, 237, 224, 72, 1, 0,
        0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 76, 0, 0, 0, 12, 113, 119, 101, 114,
        116, 121, 10, 112, 111, 105, 117, 121, 6, 4, 104, 49, 6, 112, 113, 114, 4, 104, 50, 6, 106,
        107, 108, 4, 104, 51, 6, 117, 105, 111, 0, 0, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 149,
            header: Header::Request {
                api_key: 0,
                api_version: 10,
                correlation_id: 7,
                client_id: Some("console-producer".into())
            },
            body: Body::ProduceRequest {
                transactional_id: None,
                acks: -1,
                timeout_ms: 1500,
                topic_data: Some(
                    [TopicProduceData {
                        name: "test".into(),
                        partition_data: Some(
                            [PartitionProduceData {
                                index: 0,
                                records: Some(
                                    inflated::Frame {
                                        batches: [inflated::Batch {
                                            base_offset: 0,
                                            batch_length: 88,
                                            partition_leader_epoch: -1,
                                            magic: 2,
                                            crc: 1033980210,
                                            attributes: 0,
                                            last_offset_delta: 0,
                                            base_timestamp: 1721977817089,
                                            max_timestamp: 1721977817089,
                                            producer_id: 1,
                                            producer_epoch: 0,
                                            base_sequence: 0,
                                            records: [Record {
                                                length: 38,
                                                attributes: 0,
                                                timestamp_delta: 0,
                                                offset_delta: 0,
                                                key: Some(Bytes::from_static(&[
                                                    113, 119, 101, 114, 116, 121
                                                ])),
                                                value: Some(Bytes::from_static(&[
                                                    112, 111, 105, 117, 121
                                                ])),
                                                headers: [
                                                    record::Header {
                                                        key: Some(Bytes::from_static(&[104, 49])),
                                                        value: Some(Bytes::from_static(&[
                                                            112, 113, 114
                                                        ]))
                                                    },
                                                    record::Header {
                                                        key: Some(Bytes::from_static(&[104, 50])),
                                                        value: Some(Bytes::from_static(&[
                                                            106, 107, 108
                                                        ]))
                                                    },
                                                    record::Header {
                                                        key: Some(Bytes::from_static(&[104, 51])),
                                                        value: Some(Bytes::from_static(&[
                                                            117, 105, 111
                                                        ]))
                                                    }
                                                ]
                                                .into()
                                            }]
                                            .into()
                                        }]
                                        .into()
                                    }
                                    .try_into()?
                                )
                            }]
                            .into()
                        )
                    }]
                    .into()
                )
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
fn produce_request_v10_001() -> Result<()> {
    use tansu_kafka_sans_io::produce_request::{PartitionProduceData, TopicProduceData};

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 1, 84, 0, 0, 0, 10, 0, 0, 0, 6, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
        114, 111, 100, 117, 99, 101, 114, 0, 0, 255, 255, 0, 0, 5, 220, 2, 5, 116, 101, 115, 116,
        2, 0, 0, 0, 0, 163, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 22, 255, 255, 255, 255, 2, 185,
        194, 249, 184, 0, 0, 0, 0, 0, 4, 0, 0, 1, 144, 237, 238, 215, 134, 0, 0, 1, 144, 237, 238,
        215, 142, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 76, 0, 0, 0, 12, 113, 119,
        101, 114, 116, 121, 10, 112, 111, 105, 117, 121, 6, 4, 104, 49, 6, 112, 113, 114, 4, 104,
        50, 6, 106, 107, 108, 4, 104, 51, 6, 117, 105, 111, 76, 0, 16, 2, 12, 97, 115, 100, 102,
        103, 104, 10, 108, 107, 106, 104, 103, 6, 4, 104, 49, 6, 114, 116, 121, 4, 104, 50, 6, 100,
        102, 103, 4, 104, 51, 6, 108, 107, 106, 76, 0, 16, 4, 12, 106, 107, 108, 106, 107, 108, 10,
        105, 117, 105, 117, 105, 6, 4, 104, 49, 6, 122, 120, 99, 4, 104, 50, 6, 99, 118, 98, 4,
        104, 51, 6, 109, 110, 98, 84, 0, 16, 6, 12, 113, 119, 101, 114, 116, 121, 18, 121, 116,
        114, 114, 114, 119, 113, 101, 101, 6, 4, 104, 49, 6, 101, 114, 105, 4, 104, 50, 6, 101,
        105, 117, 4, 104, 51, 6, 112, 112, 111, 134, 1, 0, 16, 8, 18, 113, 119, 114, 114, 114, 116,
        105, 105, 112, 62, 106, 107, 108, 106, 107, 108, 106, 107, 108, 107, 106, 107, 106, 107,
        108, 107, 106, 108, 106, 108, 107, 106, 108, 106, 107, 108, 106, 108, 107, 106, 106, 6, 4,
        104, 49, 6, 105, 105, 111, 4, 104, 50, 6, 101, 114, 116, 4, 104, 51, 6, 113, 119, 101, 0,
        0, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 340,
            header: Header::Request {
                api_key: 0,
                api_version: 10,
                correlation_id: 6,
                client_id: Some("console-producer".into())
            },
            body: Body::ProduceRequest {
                transactional_id: None,
                acks: -1,
                timeout_ms: 1500,
                topic_data: Some(
                    [TopicProduceData {
                        name: "test".into(),
                        partition_data: Some(
                            [PartitionProduceData {
                                index: 0,
                                records: Some(
                                    inflated::Frame {
                                        batches: [inflated::Batch {
                                            base_offset: 0,
                                            batch_length: 278,
                                            partition_leader_epoch: -1,
                                            magic: 2,
                                            crc: 3116562872,
                                            attributes: 0,
                                            last_offset_delta: 4,
                                            base_timestamp: 1721978771334,
                                            max_timestamp: 1721978771342,
                                            producer_id: 1,
                                            producer_epoch: 0,
                                            base_sequence: 0,
                                            records: [
                                                Record {
                                                    length: 38,
                                                    attributes: 0,
                                                    timestamp_delta: 0,
                                                    offset_delta: 0,
                                                    key: Some(Bytes::from_static(&[
                                                        113, 119, 101, 114, 116, 121
                                                    ])),
                                                    value: Some(Bytes::from_static(&[
                                                        112, 111, 105, 117, 121
                                                    ])),
                                                    headers: [
                                                        record::Header {
                                                            key: Some(Bytes::from_static(&[
                                                                104, 49
                                                            ])),
                                                            value: Some(Bytes::from_static(&[
                                                                112, 113, 114
                                                            ]))
                                                        },
                                                        record::Header {
                                                            key: Some(Bytes::from_static(&[
                                                                104, 50
                                                            ])),
                                                            value: Some(Bytes::from_static(&[
                                                                106, 107, 108
                                                            ]))
                                                        },
                                                        record::Header {
                                                            key: Some(Bytes::from_static(&[
                                                                104, 51
                                                            ])),
                                                            value: Some(Bytes::from_static(&[
                                                                117, 105, 111
                                                            ]))
                                                        }
                                                    ]
                                                    .into()
                                                },
                                                Record {
                                                    length: 38,
                                                    attributes: 0,
                                                    timestamp_delta: 8,
                                                    offset_delta: 1,
                                                    key: Some(Bytes::from_static(&[
                                                        97, 115, 100, 102, 103, 104
                                                    ])),
                                                    value: Some(Bytes::from_static(&[
                                                        108, 107, 106, 104, 103
                                                    ])),
                                                    headers: [
                                                        record::Header {
                                                            key: Some(Bytes::from_static(&[
                                                                104, 49
                                                            ])),
                                                            value: Some(Bytes::from_static(&[
                                                                114, 116, 121
                                                            ]))
                                                        },
                                                        record::Header {
                                                            key: Some(Bytes::from_static(&[
                                                                104, 50
                                                            ])),
                                                            value: Some(Bytes::from_static(&[
                                                                100, 102, 103
                                                            ]))
                                                        },
                                                        record::Header {
                                                            key: Some(Bytes::from_static(&[
                                                                104, 51
                                                            ])),
                                                            value: Some(Bytes::from_static(&[
                                                                108, 107, 106
                                                            ]))
                                                        }
                                                    ]
                                                    .into()
                                                },
                                                Record {
                                                    length: 38,
                                                    attributes: 0,
                                                    timestamp_delta: 8,
                                                    offset_delta: 2,
                                                    key: Some(Bytes::from_static(&[
                                                        106, 107, 108, 106, 107, 108
                                                    ])),
                                                    value: Some(Bytes::from_static(&[
                                                        105, 117, 105, 117, 105
                                                    ])),
                                                    headers: [
                                                        record::Header {
                                                            key: Some(Bytes::from_static(&[
                                                                104, 49
                                                            ])),
                                                            value: Some(Bytes::from_static(&[
                                                                122, 120, 99
                                                            ]))
                                                        },
                                                        record::Header {
                                                            key: Some(Bytes::from_static(&[
                                                                104, 50
                                                            ])),
                                                            value: Some(Bytes::from_static(&[
                                                                99, 118, 98
                                                            ]))
                                                        },
                                                        record::Header {
                                                            key: Some(Bytes::from_static(&[
                                                                104, 51
                                                            ])),
                                                            value: Some(Bytes::from_static(&[
                                                                109, 110, 98
                                                            ]))
                                                        }
                                                    ]
                                                    .into()
                                                },
                                                Record {
                                                    length: 42,
                                                    attributes: 0,
                                                    timestamp_delta: 8,
                                                    offset_delta: 3,
                                                    key: Some(Bytes::from_static(&[
                                                        113, 119, 101, 114, 116, 121
                                                    ])),
                                                    value: Some(Bytes::from_static(&[
                                                        121, 116, 114, 114, 114, 119, 113, 101, 101
                                                    ])),
                                                    headers: [
                                                        record::Header {
                                                            key: Some(Bytes::from_static(&[
                                                                104, 49
                                                            ])),
                                                            value: Some(Bytes::from_static(&[
                                                                101, 114, 105
                                                            ]))
                                                        },
                                                        record::Header {
                                                            key: Some(Bytes::from_static(&[
                                                                104, 50
                                                            ])),
                                                            value: Some(Bytes::from_static(&[
                                                                101, 105, 117
                                                            ]))
                                                        },
                                                        record::Header {
                                                            key: Some(Bytes::from_static(&[
                                                                104, 51
                                                            ])),
                                                            value: Some(Bytes::from_static(&[
                                                                112, 112, 111
                                                            ]))
                                                        }
                                                    ]
                                                    .into()
                                                },
                                                Record {
                                                    length: 67,
                                                    attributes: 0,
                                                    timestamp_delta: 8,
                                                    offset_delta: 4,
                                                    key: Some(Bytes::from_static(&[
                                                        113, 119, 114, 114, 114, 116, 105, 105, 112
                                                    ])),
                                                    value: Some(Bytes::from_static(&[
                                                        106, 107, 108, 106, 107, 108, 106, 107,
                                                        108, 107, 106, 107, 106, 107, 108, 107,
                                                        106, 108, 106, 108, 107, 106, 108, 106,
                                                        107, 108, 106, 108, 107, 106, 106
                                                    ])),
                                                    headers: [
                                                        record::Header {
                                                            key: Some(Bytes::from_static(&[
                                                                104, 49
                                                            ])),
                                                            value: Some(Bytes::from_static(&[
                                                                105, 105, 111
                                                            ]))
                                                        },
                                                        record::Header {
                                                            key: Some(Bytes::from_static(&[
                                                                104, 50
                                                            ])),
                                                            value: Some(Bytes::from_static(&[
                                                                101, 114, 116
                                                            ]))
                                                        },
                                                        record::Header {
                                                            key: Some(Bytes::from_static(&[
                                                                104, 51
                                                            ])),
                                                            value: Some(Bytes::from_static(&[
                                                                113, 119, 101
                                                            ]))
                                                        }
                                                    ]
                                                    .into()
                                                }
                                            ]
                                            .into()
                                        }]
                                        .into()
                                    }
                                    .try_into()?
                                )
                            }]
                            .into()
                        )
                    }]
                    .into()
                )
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

const LOREM: Bytes = Bytes::from_static(
    b"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do \
eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad \
minim veniam, quis nostrud exercitation ullamco laboris nisi ut \
aliquip ex ea commodo consequat. Duis aute irure dolor in \
reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla \
pariatur. Excepteur sint occaecat cupidatat non proident, sunt in \
culpa qui officia deserunt mollit anim id est laborum.",
);

#[test]
fn produce_request_v10_002() -> Result<()> {
    use tansu_kafka_sans_io::produce_request::{PartitionProduceData, TopicProduceData};

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 2, 59, 0, 0, 0, 10, 0, 0, 0, 5, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
        114, 111, 100, 117, 99, 101, 114, 0, 0, 255, 255, 0, 0, 5, 220, 2, 12, 99, 111, 109, 112,
        114, 101, 115, 115, 105, 111, 110, 2, 0, 0, 0, 2, 131, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
        246, 255, 255, 255, 255, 2, 8, 43, 67, 83, 0, 2, 0, 0, 0, 0, 0, 0, 1, 146, 108, 124, 205,
        230, 0, 0, 1, 146, 108, 124, 205, 230, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        1, 130, 83, 78, 65, 80, 80, 89, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 1, 177, 198, 3, 240, 109,
        136, 7, 0, 0, 0, 1, 250, 6, 76, 111, 114, 101, 109, 32, 105, 112, 115, 117, 109, 32, 100,
        111, 108, 111, 114, 32, 115, 105, 116, 32, 97, 109, 101, 116, 44, 32, 99, 111, 110, 115,
        101, 99, 116, 101, 116, 117, 114, 32, 97, 100, 105, 112, 105, 115, 99, 105, 110, 103, 32,
        101, 108, 105, 116, 44, 32, 115, 101, 100, 32, 100, 111, 32, 101, 105, 117, 115, 109, 111,
        100, 32, 116, 101, 109, 112, 111, 114, 32, 105, 110, 99, 105, 100, 105, 100, 117, 110, 116,
        32, 117, 116, 32, 108, 97, 98, 111, 114, 101, 32, 101, 116, 9, 91, 112, 101, 32, 109, 97,
        103, 110, 97, 32, 97, 108, 105, 113, 117, 97, 46, 32, 85, 116, 32, 101, 110, 105, 109, 32,
        97, 100, 32, 109, 105, 1, 9, 168, 118, 101, 110, 105, 97, 109, 44, 32, 113, 117, 105, 115,
        32, 110, 111, 115, 116, 114, 117, 100, 32, 101, 120, 101, 114, 99, 105, 116, 97, 116, 105,
        111, 110, 32, 117, 108, 108, 97, 109, 99, 111, 32, 108, 1, 90, 1, 37, 20, 105, 115, 105,
        32, 117, 116, 9, 83, 60, 105, 112, 32, 101, 120, 32, 101, 97, 32, 99, 111, 109, 109, 111,
        100, 111, 9, 193, 24, 113, 117, 97, 116, 46, 32, 68, 1, 83, 36, 97, 117, 116, 101, 32, 105,
        114, 117, 114, 101, 9, 145, 124, 32, 105, 110, 32, 114, 101, 112, 114, 101, 104, 101, 110,
        100, 101, 114, 105, 116, 32, 105, 110, 32, 118, 111, 108, 117, 112, 116, 97, 116, 101, 32,
        118, 1, 234, 36, 32, 101, 115, 115, 101, 32, 99, 105, 108, 108, 49, 34, 240, 79, 101, 32,
        101, 117, 32, 102, 117, 103, 105, 97, 116, 32, 110, 117, 108, 108, 97, 32, 112, 97, 114,
        105, 97, 116, 117, 114, 46, 32, 69, 120, 99, 101, 112, 116, 101, 117, 114, 32, 115, 105,
        110, 116, 32, 111, 99, 99, 97, 101, 99, 97, 116, 32, 99, 117, 112, 105, 100, 97, 116, 97,
        116, 32, 110, 111, 110, 32, 112, 114, 111, 105, 100, 101, 110, 116, 44, 32, 115, 117, 110,
        116, 1, 117, 88, 99, 117, 108, 112, 97, 32, 113, 117, 105, 32, 111, 102, 102, 105, 99, 105,
        97, 32, 100, 101, 115, 101, 114, 1, 30, 28, 109, 111, 108, 108, 105, 116, 32, 97, 33, 33,
        60, 105, 100, 32, 101, 115, 116, 32, 108, 97, 98, 111, 114, 117, 109, 46, 0, 0, 0, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    let record_data = Bytes::from_static(b"\x82SNAPPY\0\0\0\0\x01\0\0\0\x01\0\0\x01\xb1\xc6\x03\xf0m\x88\x07\0\0\0\x01\xfa\x06\
Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et\
\t[pe magna aliqua. Ut enim ad mi\x01\t\xa8veniam, quis nostrud exercitation ullamco l\
\x01Z\x01%\x14isi ut\tS<ip ex ea commodo\t\xc1\x18quat. D\x01S$aute irure\t\x91| in reprehenderit in voluptate \
v\x01\xea$ esse cill1\"\xf0Oe eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, \
sunt\x01uXculpa qui officia deser\x01\x1e\x1cmollit a!!<id est laborum.\0");

    let deflated_batch = deflated::Batch {
        base_offset: 0,
        batch_length: 502,
        partition_leader_epoch: -1,
        magic: 2,
        crc: 137053011,
        attributes: 2,
        last_offset_delta: 0,
        base_timestamp: 1728396971494,
        max_timestamp: 1728396971494,
        producer_id: 1,
        producer_epoch: 0,
        base_sequence: 0,
        record_count: 1,
        record_data,
    };

    assert_eq!(
        Frame {
            size: 571,
            header: Header::Request {
                api_key: 0,
                api_version: 10,
                correlation_id: 5,
                client_id: Some("console-producer".into())
            },
            body: Body::ProduceRequest {
                transactional_id: None,
                acks: -1,
                timeout_ms: 1500,
                topic_data: Some(
                    [TopicProduceData {
                        name: "compression".into(),
                        partition_data: Some(
                            [PartitionProduceData {
                                index: 2,
                                records: Some(deflated::Frame {
                                    batches: [deflated_batch.clone()].into()
                                })
                            }]
                            .into()
                        )
                    }]
                    .into()
                )
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    assert_eq!(
        inflated::Batch {
            base_offset: 0,
            batch_length: 502,
            partition_leader_epoch: -1,
            magic: 2,
            crc: 137053011,
            attributes: 2,
            last_offset_delta: 0,
            base_timestamp: 1728396971494,
            max_timestamp: 1728396971494,
            producer_id: 1,
            producer_epoch: 0,
            base_sequence: 0,
            records: [Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(LOREM),
                headers: [].into()
            }]
            .into()
        },
        inflated::Batch::try_from(deflated_batch)?
    );

    Ok(())
}

#[test]
fn produce_request_v10_003() -> Result<()> {
    use tansu_kafka_sans_io::produce_request::{PartitionProduceData, TopicProduceData};

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 2, 22, 0, 0, 0, 10, 0, 0, 0, 3, 0, 7, 114, 100, 107, 97, 102, 107, 97, 0, 0, 255,
        255, 0, 0, 117, 48, 2, 12, 99, 111, 109, 112, 114, 101, 115, 115, 105, 111, 110, 2, 0, 0,
        0, 0, 231, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 218, 0, 0, 0, 0, 2, 39, 56, 135, 223, 0, 2,
        0, 0, 0, 0, 0, 0, 1, 146, 108, 150, 162, 246, 0, 0, 1, 146, 108, 150, 162, 246, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 198, 3, 240, 111,
        136, 7, 0, 0, 0, 1, 250, 6, 76, 111, 114, 101, 109, 32, 105, 112, 115, 117, 109, 32, 100,
        111, 108, 111, 114, 32, 115, 105, 116, 32, 97, 109, 101, 116, 44, 32, 99, 111, 110, 115,
        101, 99, 116, 101, 116, 117, 114, 32, 97, 100, 105, 112, 105, 115, 99, 105, 110, 103, 32,
        101, 108, 105, 116, 44, 32, 115, 101, 100, 32, 100, 111, 32, 101, 105, 117, 115, 109, 111,
        100, 32, 116, 101, 109, 112, 111, 114, 32, 105, 110, 99, 105, 100, 105, 100, 117, 110, 116,
        32, 117, 116, 32, 108, 97, 98, 111, 114, 101, 32, 101, 116, 32, 100, 1, 91, 112, 101, 32,
        109, 97, 103, 110, 97, 32, 97, 108, 105, 113, 117, 97, 46, 32, 85, 116, 32, 101, 110, 105,
        109, 32, 97, 100, 32, 109, 105, 1, 9, 160, 118, 101, 110, 105, 97, 109, 44, 32, 113, 117,
        105, 115, 32, 110, 111, 115, 116, 114, 117, 100, 32, 101, 120, 101, 114, 99, 105, 116, 97,
        116, 105, 111, 110, 32, 117, 108, 108, 97, 109, 99, 111, 9, 90, 1, 37, 8, 105, 115, 105, 1,
        106, 5, 83, 60, 105, 112, 32, 101, 120, 32, 101, 97, 32, 99, 111, 109, 109, 111, 100, 111,
        9, 193, 24, 113, 117, 97, 116, 46, 32, 68, 1, 83, 36, 97, 117, 116, 101, 32, 105, 114, 117,
        114, 101, 13, 236, 60, 105, 110, 32, 114, 101, 112, 114, 101, 104, 101, 110, 100, 101, 114,
        105, 116, 1, 17, 40, 118, 111, 108, 117, 112, 116, 97, 116, 101, 32, 118, 1, 234, 36, 32,
        101, 115, 115, 101, 32, 99, 105, 108, 108, 49, 34, 232, 101, 32, 101, 117, 32, 102, 117,
        103, 105, 97, 116, 32, 110, 117, 108, 108, 97, 32, 112, 97, 114, 105, 97, 116, 117, 114,
        46, 32, 69, 120, 99, 101, 112, 116, 101, 117, 114, 32, 115, 105, 110, 116, 32, 111, 99, 99,
        97, 101, 99, 97, 116, 32, 99, 117, 112, 105, 100, 97, 116, 1, 50, 60, 111, 110, 32, 112,
        114, 111, 105, 100, 101, 110, 116, 44, 32, 115, 117, 110, 5, 117, 88, 99, 117, 108, 112,
        97, 32, 113, 117, 105, 32, 111, 102, 102, 105, 99, 105, 97, 32, 100, 101, 115, 101, 114, 1,
        30, 12, 109, 111, 108, 108, 33, 147, 33, 33, 60, 105, 100, 32, 101, 115, 116, 32, 108, 97,
        98, 111, 114, 117, 109, 46, 0, 0, 0, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    let record_data = Bytes::from_static(
        b"\xc6\x03\xf0o\x88\x07\0\0\0\x01\xfa\x06\
Lorem ipsum dolor sit amet, consectetur adipiscing elit, \
sed do eiusmod tempor incididunt ut labore et d\x01[pe magna aliqua. \
Ut enim ad mi\x01\t\xa0veniam, quis nostrud exercitation \
ullamco\tZ\x01%\x08isi\x01j\x05S<ip ex ea commodo\t\xc1\x18quat. \
D\x01S$aute irure\r\xec<in reprehenderit\x01\x11(voluptate \
v\x01\xea$ esse cill1\"\xe8e eu fugiat nulla pariatur. \
Excepteur sint occaecat cupidat\x012<on proident, \
sun\x05uXculpa qui officia deser\x01\x1e\x0cmoll!\x93!!<id est laborum.\0",
    );

    let deflated_batch = deflated::Batch {
        base_offset: 0,
        batch_length: 474,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 658016223,
        attributes: 2,
        last_offset_delta: 0,
        base_timestamp: 1728398664438,
        max_timestamp: 1728398664438,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        record_count: 1,
        record_data,
    };

    assert_eq!(
        Frame {
            size: 534,
            header: Header::Request {
                api_key: 0,
                api_version: 10,
                correlation_id: 3,
                client_id: Some("rdkafka".into())
            },
            body: Body::ProduceRequest {
                transactional_id: None,
                acks: -1,
                timeout_ms: 30000,
                topic_data: Some(
                    [TopicProduceData {
                        name: "compression".into(),
                        partition_data: Some(
                            [PartitionProduceData {
                                index: 0,
                                records: Some(deflated::Frame {
                                    batches: [deflated_batch.clone()].into()
                                })
                            }]
                            .into()
                        )
                    }]
                    .into()
                )
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    assert_eq!(
        inflated::Batch {
            base_offset: 0,
            batch_length: 474,
            partition_leader_epoch: 0,
            magic: 2,
            crc: 658016223,
            attributes: 2,
            last_offset_delta: 0,
            base_timestamp: 1728398664438,
            max_timestamp: 1728398664438,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: [Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(LOREM),
                headers: [].into()
            }]
            .into()
        },
        inflated::Batch::try_from(deflated_batch)?
    );

    Ok(())
}

#[test]
fn produce_response_v9_000() -> Result<()> {
    use tansu_kafka_sans_io::produce_response::{PartitionProduceResponse, TopicProduceResponse};

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 51, 0, 0, 0, 6, 0, 2, 5, 116, 101, 115, 116, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 2, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0,
        0, 0, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::response(&mut c, 0, 9);

    assert_eq!(
        Frame {
            size: 51,
            header: Header::Response { correlation_id: 6 },
            body: Body::ProduceResponse {
                node_endpoints: None,
                responses: Some(
                    [TopicProduceResponse {
                        name: "test".into(),
                        partition_responses: Some(
                            [PartitionProduceResponse {
                                index: 0,
                                error_code: 0,
                                base_offset: 2,
                                log_append_time_ms: Some(-1),
                                log_start_offset: Some(0),
                                record_errors: Some([].into()),
                                error_message: None,
                                current_leader: None,
                            }]
                            .into()
                        ),
                    }]
                    .into()
                ),
                throttle_time_ms: Some(0),
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}

#[test]
pub fn sync_group_request_v5_000() -> Result<()> {
    use tansu_kafka_sans_io::sync_group_request::SyncGroupRequestAssignment;

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 113, 0, 14, 0, 5, 0, 0, 0, 6, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99,
        111, 110, 115, 117, 109, 101, 114, 0, 20, 116, 101, 115, 116, 45, 99, 111, 110, 115, 117,
        109, 101, 114, 45, 103, 114, 111, 117, 112, 0, 0, 0, 0, 5, 49, 48, 48, 48, 0, 9, 99, 111,
        110, 115, 117, 109, 101, 114, 6, 114, 97, 110, 103, 101, 2, 5, 49, 48, 48, 48, 33, 0, 3, 0,
        0, 0, 1, 0, 4, 116, 101, 115, 116, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 255,
        255, 255, 255, 0, 0,
    ];

    let mut c = Cursor::new(v);
    let mut deserializer = Decoder::request(&mut c);

    assert_eq!(
        Frame {
            size: 113,
            header: Header::Request {
                api_key: 14,
                api_version: 5,
                correlation_id: 6,
                client_id: Some("console-consumer".into())
            },
            body: Body::SyncGroupRequest {
                group_id: "test-consumer-group".into(),
                generation_id: 0,
                member_id: "1000".into(),
                group_instance_id: None,
                protocol_type: Some("consumer".into()),
                protocol_name: Some("range".into()),
                assignments: Some(
                    [SyncGroupRequestAssignment {
                        member_id: "1000".into(),
                        assignment: Bytes::from_static(b"\0\x03\0\0\0\x01\0\x04test\0\0\0\x03\0\0\0\0\0\0\0\x01\0\0\0\x02\xff\xff\xff\xff"),
                    }
                    ].into()
                )
            }
        },
        Frame::deserialize(&mut deserializer)?
    );

    Ok(())
}
