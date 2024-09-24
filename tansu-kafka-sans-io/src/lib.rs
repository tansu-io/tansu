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

#![cfg_attr(feature = "nightly-features", feature(error_generic_member_access))]
pub mod de;
pub mod primitive;
pub mod record;
pub mod ser;

pub use de::Decoder;
use flate2::read::GzDecoder;
use primitive::tagged::TagBuffer;
use record::deflated::Frame as RecordBatch;
pub use ser::Encoder;
use serde::{Deserialize, Serialize};
#[cfg(feature = "nightly-features")]
use std::backtrace::Backtrace;
use std::{
    collections::HashMap,
    env::VarError,
    fmt::{self, Display, Formatter},
    io::{self, BufRead, Cursor, Read, Write},
    num, str, string,
    sync::OnceLock,
    time::{Duration, SystemTime, SystemTimeError},
};
use tansu_kafka_model::{MessageKind, MessageMeta};
use tracing::{debug, warn};

#[derive(Debug)]
pub struct RootMessageMeta {
    pub(crate) requests: HashMap<i16, &'static MessageMeta>,
    pub(crate) responses: HashMap<i16, &'static MessageMeta>,
}

impl RootMessageMeta {
    fn new() -> Self {
        let (requests, responses) = MESSAGE_META.iter().fold(
            (HashMap::new(), HashMap::new()),
            |(mut requests, mut responses), (_, meta)| {
                match meta.message_kind {
                    MessageKind::Request => {
                        _ = requests.insert(meta.api_key, *meta);
                    }

                    MessageKind::Response => {
                        _ = responses.insert(meta.api_key, *meta);
                    }
                }

                (requests, responses)
            },
        );

        Self {
            requests,
            responses,
        }
    }

    pub fn messages() -> &'static RootMessageMeta {
        static MAPPING: OnceLock<RootMessageMeta> = OnceLock::new();
        MAPPING.get_or_init(RootMessageMeta::new)
    }

    #[must_use]
    pub fn requests(&self) -> &HashMap<i16, &'static MessageMeta> {
        &self.requests
    }

    #[must_use]
    pub fn responses(&self) -> &HashMap<i16, &'static MessageMeta> {
        &self.responses
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    ApiError(ErrorCode),
    EnvVar(VarError),
    FromUtf8(string::FromUtf8Error),
    InvalidAckValue(i16),
    InvalidIsolationLevel(i8),
    InvalidScramMechanism(i8),
    Io(io::Error),
    Message(String),
    NoSuchField(&'static str),
    NoSuchMessage(&'static str),
    NoSuchRequest(i16),
    StringWithoutApiVersion,
    StringWithoutLength,
    SystemTime(SystemTimeError),
    TansuKafkaModel(tansu_kafka_model::Error),
    TryFromInt {
        #[from]
        source: num::TryFromIntError,
        #[cfg(feature = "nightly-features")]
        backtrace: Backtrace,
    },
    UnexpectedTaggedHeader(HeaderMezzanine),
    UnknownApiErrorCode(i16),
    UnknownCompressionType(i16),
    Utf8(str::Utf8Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Error::Message(e) => f.write_str(e),
            e => write!(f, "{e:?}"),
        }
    }
}

impl serde::ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl serde::de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<str::Utf8Error> for Error {
    fn from(value: str::Utf8Error) -> Self {
        Self::Utf8(value)
    }
}

impl From<string::FromUtf8Error> for Error {
    fn from(value: string::FromUtf8Error) -> Self {
        Self::FromUtf8(value)
    }
}

impl From<tansu_kafka_model::Error> for Error {
    fn from(value: tansu_kafka_model::Error) -> Self {
        Self::TansuKafkaModel(value)
    }
}

impl From<VarError> for Error {
    fn from(value: VarError) -> Self {
        Self::EnvVar(value)
    }
}

impl From<SystemTimeError> for Error {
    fn from(value: SystemTimeError) -> Self {
        Self::SystemTime(value)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
pub struct Frame {
    pub size: i32,
    pub header: Header,
    pub body: Body,
}

impl Frame {
    pub fn request(header: Header, body: Body) -> Result<Vec<u8>> {
        let mut c = Cursor::new(vec![]);

        let mut serializer = Encoder::request(&mut c);

        let frame = Frame {
            size: 0,
            header,
            body,
        };

        frame.serialize(&mut serializer)?;
        let size = i32::try_from(c.position()).map(|position| position - 4)?;

        c.set_position(0);
        let buf = size.to_be_bytes();
        c.write_all(&buf)?;

        Ok(c.into_inner())
    }

    pub fn request_from_bytes(bytes: &[u8]) -> Result<Frame> {
        let mut c = Cursor::new(bytes);
        let mut deserializer = Decoder::request(&mut c);
        Frame::deserialize(&mut deserializer)
    }

    pub fn response(header: Header, body: Body, api_key: i16, api_version: i16) -> Result<Vec<u8>> {
        let mut c = Cursor::new(vec![]);
        let mut serializer = Encoder::response(&mut c, api_key, api_version);

        let frame = Frame {
            size: 0,
            header,
            body,
        };

        frame.serialize(&mut serializer)?;
        let size = i32::try_from(c.position())
            .map(|position| position - 4)
            .inspect_err(|err| {
                let position = c.position();
                warn!(?err, ?position, ?frame);
            })?;

        c.set_position(0);
        let buf = size.to_be_bytes();
        c.write_all(&buf)?;

        Ok(c.into_inner())
    }

    pub fn response_from_bytes(bytes: &[u8], api_key: i16, api_version: i16) -> Result<Frame> {
        let mut c = Cursor::new(bytes);
        let mut deserializer = Decoder::response(&mut c, api_key, api_version);
        Frame::deserialize(&mut deserializer)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "HeaderMezzanine")]
#[serde(into = "HeaderMezzanine")]
pub enum Header {
    Request {
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<String>,
    },
    Response {
        correlation_id: i32,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
pub enum HeaderMezzanine {
    Request {
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<String>,
        tag_buffer: Option<TagBuffer>,
    },
    Response {
        correlation_id: i32,
        tag_buffer: Option<TagBuffer>,
    },
}

impl TryFrom<HeaderMezzanine> for Header {
    type Error = Error;

    fn try_from(value: HeaderMezzanine) -> Result<Self, Self::Error> {
        debug!("value: {value:?}");

        match value {
            HeaderMezzanine::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
                tag_buffer: None,
            } => Ok(Self::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
            }),

            HeaderMezzanine::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
                tag_buffer,
            } if tag_buffer
                .as_ref()
                .is_some_and(|tagged| tagged == &TagBuffer(vec![])) =>
            {
                Ok(Self::Request {
                    api_key,
                    api_version,
                    correlation_id,
                    client_id,
                })
            }

            HeaderMezzanine::Response {
                correlation_id,
                tag_buffer: None,
            } => Ok(Self::Response { correlation_id }),

            HeaderMezzanine::Response {
                correlation_id,
                tag_buffer,
            } if tag_buffer
                .as_ref()
                .is_some_and(|tagged| tagged == &TagBuffer(vec![])) =>
            {
                Ok(Self::Response { correlation_id })
            }

            HeaderMezzanine::Request {
                tag_buffer: Some(..),
                ..
            }
            | HeaderMezzanine::Response {
                tag_buffer: Some(..),
                ..
            } => Err(Error::UnexpectedTaggedHeader(value)),
        }
    }
}

impl From<Header> for HeaderMezzanine {
    fn from(value: Header) -> Self {
        debug!("value: {value:?}");

        match value {
            Header::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
            } => HeaderMezzanine::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
                tag_buffer: Some(TagBuffer([].into())),
            },

            Header::Response { correlation_id } => HeaderMezzanine::Response {
                correlation_id,
                tag_buffer: Some(TagBuffer([].into())),
            },
        }
    }
}

impl TryFrom<i16> for ErrorCode {
    type Error = Error;

    #[allow(clippy::too_many_lines)]
    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            -1 => Ok(Self::UnknownServerError),
            0 => Ok(Self::None),
            1 => Ok(Self::OffsetOutOfRange),
            2 => Ok(Self::CorruptMessage),
            3 => Ok(Self::UnknownTopicOrPartition),
            4 => Ok(Self::InvalidFetchSize),
            5 => Ok(Self::LeaderNotAvailable),
            6 => Ok(Self::NotLeaderOrFollower),
            7 => Ok(Self::RequestTimedOut),
            8 => Ok(Self::BrokerNotAvailable),
            9 => Ok(Self::ReplicaNotAvailable),
            10 => Ok(Self::MessageTooLarge),
            11 => Ok(Self::StaleControllerEpoch),
            12 => Ok(Self::OffsetMetadataTooLarge),
            13 => Ok(Self::NetworkException),
            14 => Ok(Self::CoordinatorLoadInProgress),
            15 => Ok(Self::CoordinatorNotAvailable),
            16 => Ok(Self::NotCoordinator),
            17 => Ok(Self::InvalidTopicException),
            18 => Ok(Self::RecordListTooLarge),
            19 => Ok(Self::NotEnoughReplicas),
            20 => Ok(Self::NotEnoughReplicasAfterAppend),
            21 => Ok(Self::InvalidRequiredAcks),
            22 => Ok(Self::IllegalGeneration),
            23 => Ok(Self::InconsistentGroupProtocol),
            24 => Ok(Self::InvalidGroupId),
            25 => Ok(Self::UnknownMemberId),
            26 => Ok(Self::InvalidSessionTimeout),
            27 => Ok(Self::RebalanceInProgress),
            28 => Ok(Self::InvalidCommitOffsetSize),
            29 => Ok(Self::TopicAuthorizationFailed),
            30 => Ok(Self::GroupAuthorizationFailed),
            31 => Ok(Self::ClusterAuthorizationFailed),
            32 => Ok(Self::InvalidTimestamp),
            33 => Ok(Self::UnsupportedSaslMechanism),
            34 => Ok(Self::IllegalSaslState),
            35 => Ok(Self::UnsupportedVersion),
            36 => Ok(Self::TopicAlreadyExists),
            37 => Ok(Self::InvalidPartitions),
            38 => Ok(Self::InvalidReplicationFactor),
            39 => Ok(Self::InvalidReplicaAssignment),
            40 => Ok(Self::InvalidConfig),
            41 => Ok(Self::NotController),
            42 => Ok(Self::InvalidRequest),
            43 => Ok(Self::UnsupportedForMessageFormat),
            44 => Ok(Self::PolicyViolation),
            45 => Ok(Self::OutOfOrderSequenceNumber),
            46 => Ok(Self::DuplicateSequenceNumber),
            47 => Ok(Self::InvalidProducerEpoch),
            48 => Ok(Self::InvalidTxnState),
            49 => Ok(Self::InvalidProducerIdMapping),
            50 => Ok(Self::InvalidTransactionTimeout),
            51 => Ok(Self::ConcurrentTransactions),
            52 => Ok(Self::TransactionCoordinatorFenced),
            53 => Ok(Self::TransactionalIdAuthorizationFailed),
            54 => Ok(Self::SecurityDisabled),
            55 => Ok(Self::OperationNotAttempted),
            56 => Ok(Self::KafkaStorageError),
            57 => Ok(Self::LogDirNotFound),
            58 => Ok(Self::SaslAuthenticationFailed),
            59 => Ok(Self::UnknownProducerId),
            60 => Ok(Self::ReassignmentInProgress),
            61 => Ok(Self::DelegationTokenAuthDisabled),
            62 => Ok(Self::DelegationTokenNotFound),
            63 => Ok(Self::DelegationTokenOwnerMismatch),
            64 => Ok(Self::DelegationTokenRequestNotAllowed),
            65 => Ok(Self::DelegationTokenAuthorizationFailed),
            66 => Ok(Self::DelegationTokenExpired),
            67 => Ok(Self::InvalidPrincipalType),
            68 => Ok(Self::NonEmptyGroup),
            69 => Ok(Self::GroupIdNotFound),
            70 => Ok(Self::FetchSessionIdNotFound),
            71 => Ok(Self::InvalidFetchSessionEpoch),
            72 => Ok(Self::ListenerNotFound),
            73 => Ok(Self::TopicDeletionDisabled),
            74 => Ok(Self::FencedLeaderEpoch),
            75 => Ok(Self::UnknownLeaderEpoch),
            76 => Ok(Self::UnsupportedCompressionType),
            77 => Ok(Self::StaleBrokerEpoch),
            78 => Ok(Self::OffsetNotAvailable),
            79 => Ok(Self::MemberIdRequired),
            80 => Ok(Self::PreferredLeaderNotAvailable),
            81 => Ok(Self::GroupMaxSizeReached),
            82 => Ok(Self::FencedInstanceId),
            83 => Ok(Self::EligibleLeadersNotAvailable),
            84 => Ok(Self::ElectionNotNeeded),
            85 => Ok(Self::NoReassignmentInProgress),
            86 => Ok(Self::GroupSubscribedToTopic),
            87 => Ok(Self::InvalidRecord),
            88 => Ok(Self::UnstableOffsetCommit),
            89 => Ok(Self::ThrottlingQuotaExceeded),
            90 => Ok(Self::ProducerFenced),
            91 => Ok(Self::ResourceNotFound),
            92 => Ok(Self::DuplicateResource),
            93 => Ok(Self::UnacceptableCredential),
            94 => Ok(Self::InconsistentVoterSet),
            95 => Ok(Self::InvalidUpdateVersion),
            96 => Ok(Self::FeatureUpdateFailed),
            97 => Ok(Self::PrincipalDeserializationFailure),
            98 => Ok(Self::SnapshotNotFound),
            99 => Ok(Self::PositionOutOfRange),
            100 => Ok(Self::UnknownTopicId),
            101 => Ok(Self::DuplicateBrokerRegistration),
            102 => Ok(Self::BrokerIdNotRegistered),
            103 => Ok(Self::InconsistentTopicId),
            104 => Ok(Self::InconsistentClusterId),
            105 => Ok(Self::TransactionalIdNotFound),
            106 => Ok(Self::FetchSessionTopicIdError),
            107 => Ok(Self::IneligibleReplica),
            108 => Ok(Self::NewLeaderElected),
            109 => Ok(Self::OffsetMovedToTieredStorage),
            110 => Ok(Self::FencedMemberEpoch),
            111 => Ok(Self::UnreleasedInstanceId),
            112 => Ok(Self::UnsupportedAssignor),
            113 => Ok(Self::StaleMemberEpoch),
            114 => Ok(Self::MismatchedEndpointType),
            115 => Ok(Self::UnsupportedEndpointType),
            116 => Ok(Self::UnknownControllerId),
            117 => Ok(Self::UnknownSubscriptionId),
            118 => Ok(Self::TelemetryTooLarge),
            119 => Ok(Self::InvalidRegistration),
            otherwise => Err(Error::UnknownApiErrorCode(otherwise)),
        }
    }
}

impl From<ErrorCode> for i16 {
    #[allow(clippy::too_many_lines)]
    fn from(value: ErrorCode) -> Self {
        match value {
            ErrorCode::UnknownServerError => -1,
            ErrorCode::None => 0,
            ErrorCode::OffsetOutOfRange => 1,
            ErrorCode::CorruptMessage => 2,
            ErrorCode::UnknownTopicOrPartition => 3,
            ErrorCode::InvalidFetchSize => 4,
            ErrorCode::LeaderNotAvailable => 5,
            ErrorCode::NotLeaderOrFollower => 6,
            ErrorCode::RequestTimedOut => 7,
            ErrorCode::BrokerNotAvailable => 8,
            ErrorCode::ReplicaNotAvailable => 9,
            ErrorCode::MessageTooLarge => 10,
            ErrorCode::StaleControllerEpoch => 11,
            ErrorCode::OffsetMetadataTooLarge => 12,
            ErrorCode::NetworkException => 13,
            ErrorCode::CoordinatorLoadInProgress => 14,
            ErrorCode::CoordinatorNotAvailable => 15,
            ErrorCode::NotCoordinator => 16,
            ErrorCode::InvalidTopicException => 17,
            ErrorCode::RecordListTooLarge => 18,
            ErrorCode::NotEnoughReplicas => 19,
            ErrorCode::NotEnoughReplicasAfterAppend => 20,
            ErrorCode::InvalidRequiredAcks => 21,
            ErrorCode::IllegalGeneration => 22,
            ErrorCode::InconsistentGroupProtocol => 23,
            ErrorCode::InvalidGroupId => 24,
            ErrorCode::UnknownMemberId => 25,
            ErrorCode::InvalidSessionTimeout => 26,
            ErrorCode::RebalanceInProgress => 27,
            ErrorCode::InvalidCommitOffsetSize => 28,
            ErrorCode::TopicAuthorizationFailed => 29,
            ErrorCode::GroupAuthorizationFailed => 30,
            ErrorCode::ClusterAuthorizationFailed => 31,
            ErrorCode::InvalidTimestamp => 32,
            ErrorCode::UnsupportedSaslMechanism => 33,
            ErrorCode::IllegalSaslState => 34,
            ErrorCode::UnsupportedVersion => 35,
            ErrorCode::TopicAlreadyExists => 36,
            ErrorCode::InvalidPartitions => 37,
            ErrorCode::InvalidReplicationFactor => 38,
            ErrorCode::InvalidReplicaAssignment => 39,
            ErrorCode::InvalidConfig => 40,
            ErrorCode::NotController => 41,
            ErrorCode::InvalidRequest => 42,
            ErrorCode::UnsupportedForMessageFormat => 43,
            ErrorCode::PolicyViolation => 44,
            ErrorCode::OutOfOrderSequenceNumber => 45,
            ErrorCode::DuplicateSequenceNumber => 46,
            ErrorCode::InvalidProducerEpoch => 47,
            ErrorCode::InvalidTxnState => 48,
            ErrorCode::InvalidProducerIdMapping => 49,
            ErrorCode::InvalidTransactionTimeout => 50,
            ErrorCode::ConcurrentTransactions => 51,
            ErrorCode::TransactionCoordinatorFenced => 52,
            ErrorCode::TransactionalIdAuthorizationFailed => 53,
            ErrorCode::SecurityDisabled => 54,
            ErrorCode::OperationNotAttempted => 55,
            ErrorCode::KafkaStorageError => 56,
            ErrorCode::LogDirNotFound => 57,
            ErrorCode::SaslAuthenticationFailed => 58,
            ErrorCode::UnknownProducerId => 59,
            ErrorCode::ReassignmentInProgress => 60,
            ErrorCode::DelegationTokenAuthDisabled => 61,
            ErrorCode::DelegationTokenNotFound => 62,
            ErrorCode::DelegationTokenOwnerMismatch => 63,
            ErrorCode::DelegationTokenRequestNotAllowed => 64,
            ErrorCode::DelegationTokenAuthorizationFailed => 65,
            ErrorCode::DelegationTokenExpired => 66,
            ErrorCode::InvalidPrincipalType => 67,
            ErrorCode::NonEmptyGroup => 68,
            ErrorCode::GroupIdNotFound => 69,
            ErrorCode::FetchSessionIdNotFound => 70,
            ErrorCode::InvalidFetchSessionEpoch => 71,
            ErrorCode::ListenerNotFound => 72,
            ErrorCode::TopicDeletionDisabled => 73,
            ErrorCode::FencedLeaderEpoch => 74,
            ErrorCode::UnknownLeaderEpoch => 75,
            ErrorCode::UnsupportedCompressionType => 76,
            ErrorCode::StaleBrokerEpoch => 77,
            ErrorCode::OffsetNotAvailable => 78,
            ErrorCode::MemberIdRequired => 79,
            ErrorCode::PreferredLeaderNotAvailable => 80,
            ErrorCode::GroupMaxSizeReached => 81,
            ErrorCode::FencedInstanceId => 82,
            ErrorCode::EligibleLeadersNotAvailable => 83,
            ErrorCode::ElectionNotNeeded => 84,
            ErrorCode::NoReassignmentInProgress => 85,
            ErrorCode::GroupSubscribedToTopic => 86,
            ErrorCode::InvalidRecord => 87,
            ErrorCode::UnstableOffsetCommit => 88,
            ErrorCode::ThrottlingQuotaExceeded => 89,
            ErrorCode::ProducerFenced => 90,
            ErrorCode::ResourceNotFound => 91,
            ErrorCode::DuplicateResource => 92,
            ErrorCode::UnacceptableCredential => 93,
            ErrorCode::InconsistentVoterSet => 94,
            ErrorCode::InvalidUpdateVersion => 95,
            ErrorCode::FeatureUpdateFailed => 96,
            ErrorCode::PrincipalDeserializationFailure => 97,
            ErrorCode::SnapshotNotFound => 98,
            ErrorCode::PositionOutOfRange => 99,
            ErrorCode::UnknownTopicId => 100,
            ErrorCode::DuplicateBrokerRegistration => 101,
            ErrorCode::BrokerIdNotRegistered => 102,
            ErrorCode::InconsistentTopicId => 103,
            ErrorCode::InconsistentClusterId => 104,
            ErrorCode::TransactionalIdNotFound => 105,
            ErrorCode::FetchSessionTopicIdError => 106,
            ErrorCode::IneligibleReplica => 107,
            ErrorCode::NewLeaderElected => 108,
            ErrorCode::OffsetMovedToTieredStorage => 109,
            ErrorCode::FencedMemberEpoch => 110,
            ErrorCode::UnreleasedInstanceId => 111,
            ErrorCode::UnsupportedAssignor => 112,
            ErrorCode::StaleMemberEpoch => 113,
            ErrorCode::MismatchedEndpointType => 114,
            ErrorCode::UnsupportedEndpointType => 115,
            ErrorCode::UnknownControllerId => 116,
            ErrorCode::UnknownSubscriptionId => 117,
            ErrorCode::TelemetryTooLarge => 118,
            ErrorCode::InvalidRegistration => 119,
        }
    }
}

impl Display for ErrorCode {
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCode::UnknownServerError => f.write_str(
                "The server experienced an unexpected error when processing the request.",
            ),
            ErrorCode::None => f.write_str("No error."),
            ErrorCode::OffsetOutOfRange => f.write_str(
                "The requested offset is not within the range of offsets maintained by the server.",
            ),
            ErrorCode::CorruptMessage => f.write_str(
                "This message has failed its CRC checksum, exceeds the valid size, has a null key \
                 for a compacted topic, or is otherwise corrupt.",
            ),
            ErrorCode::UnknownTopicOrPartition => {
                f.write_str("This server does not host this topic-partition.")
            }
            ErrorCode::InvalidFetchSize => f.write_str("The requested fetch size is invalid."),
            ErrorCode::LeaderNotAvailable => f.write_str(
                "There is no leader for this topic-partition as we are in the middle of a \
                 leadership election.",
            ),
            ErrorCode::NotLeaderOrFollower => f.write_str(
                "For requests intended only for the leader, this error indicates that the broker \
                 is not the current leader. For requests intended for any replica, this error \
                 indicates that the broker is not a replica of the topic partition.",
            ),
            ErrorCode::RequestTimedOut => f.write_str("The request timed out."),
            ErrorCode::BrokerNotAvailable => f.write_str("The broker is not available."),
            ErrorCode::ReplicaNotAvailable => f.write_str(
                "The replica is not available for the requested topic-partition. Produce/Fetch \
                 requests and other requests intended only for the leader or follower return \
                 NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.",
            ),
            ErrorCode::MessageTooLarge => f.write_str(
                "The request included a message larger than the max message size the server will \
                 accept.",
            ),
            ErrorCode::StaleControllerEpoch => {
                f.write_str("The controller moved to another broker.")
            }
            ErrorCode::OffsetMetadataTooLarge => {
                f.write_str("The metadata field of the offset request was too large.")
            }
            ErrorCode::NetworkException => {
                f.write_str("The server disconnected before a response was received.")
            }
            ErrorCode::CoordinatorLoadInProgress => {
                f.write_str("The coordinator is loading and hence can't process requests.")
            }
            ErrorCode::CoordinatorNotAvailable => f.write_str("The coordinator is not available."),
            ErrorCode::NotCoordinator => f.write_str("This is not the correct coordinator."),
            ErrorCode::InvalidTopicException => {
                f.write_str("The request attempted to perform an operation on an invalid topic.")
            }
            ErrorCode::RecordListTooLarge => f.write_str(
                "The request included message batch larger than the configured segment size on \
                 the server.",
            ),
            ErrorCode::NotEnoughReplicas => f.write_str(
                "Messages are rejected since there are fewer in-sync replicas than required.",
            ),
            ErrorCode::NotEnoughReplicasAfterAppend => f.write_str(
                "Messages are written to the log, but to fewer in-sync replicas than required.",
            ),
            ErrorCode::InvalidRequiredAcks => {
                f.write_str("Produce request specified an invalid value for required acks.")
            }
            ErrorCode::IllegalGeneration => {
                f.write_str("Specified group generation id is not valid.")
            }
            ErrorCode::InconsistentGroupProtocol => f.write_str(
                "The group member's supported protocols are incompatible with those of existing \
                 members or first group member tried to join with empty protocol type or empty \
                 protocol list.",
            ),
            ErrorCode::InvalidGroupId => f.write_str("The configured groupId is invalid."),
            ErrorCode::UnknownMemberId => {
                f.write_str("The coordinator is not aware of this member.")
            }
            ErrorCode::InvalidSessionTimeout => f.write_str(
                "The session timeout is not within the range allowed by the broker (as configured \
                 by group.min.session.timeout.ms and group.max.session.timeout.ms).",
            ),
            ErrorCode::RebalanceInProgress => {
                f.write_str("The group is rebalancing, so a rejoin is needed.")
            }
            ErrorCode::InvalidCommitOffsetSize => {
                f.write_str("The committing offset data size is not valid.")
            }
            ErrorCode::TopicAuthorizationFailed => f.write_str("Topic authorization failed."),
            ErrorCode::GroupAuthorizationFailed => f.write_str("Group authorization failed."),
            ErrorCode::ClusterAuthorizationFailed => f.write_str("Cluster authorization failed."),
            ErrorCode::InvalidTimestamp => {
                f.write_str("The timestamp of the message is out of acceptable range.")
            }
            ErrorCode::UnsupportedSaslMechanism => {
                f.write_str("The broker does not support the requested SASL mechanism.")
            }
            ErrorCode::IllegalSaslState => {
                f.write_str("Request is not valid given the current SASL state.")
            }
            ErrorCode::UnsupportedVersion => f.write_str("The version of API is not supported."),
            ErrorCode::TopicAlreadyExists => f.write_str("Topic with this name already exists."),
            ErrorCode::InvalidPartitions => f.write_str("Number of partitions is below 1."),
            ErrorCode::InvalidReplicationFactor => f.write_str(
                "Replication factor is below 1 or larger than the number of available brokers.",
            ),
            ErrorCode::InvalidReplicaAssignment => f.write_str("Replica assignment is invalid."),
            ErrorCode::InvalidConfig => f.write_str("Configuration is invalid."),
            ErrorCode::NotController => {
                f.write_str("This is not the correct controller for this cluster.")
            }
            ErrorCode::InvalidRequest => f.write_str(
                "This most likely occurs because of a request being malformed by the client \
                 library or the message was sent to an incompatible broker. See the broker logs \
                 for more details.",
            ),
            ErrorCode::UnsupportedForMessageFormat => f.write_str(
                "The message format version on the broker does not support the request.",
            ),
            ErrorCode::PolicyViolation => {
                f.write_str("Request parameters do not satisfy the configured policy.")
            }
            ErrorCode::OutOfOrderSequenceNumber => {
                f.write_str("The broker received an out of order sequence number.")
            }
            ErrorCode::DuplicateSequenceNumber => {
                f.write_str("The broker received a duplicate sequence number.")
            }
            ErrorCode::InvalidProducerEpoch => {
                f.write_str("Producer attempted to produce with an old epoch.")
            }
            ErrorCode::InvalidTxnState => {
                f.write_str("The producer attempted a transactional operation in an invalid state.")
            }
            ErrorCode::InvalidProducerIdMapping => f.write_str(
                "The producer attempted to use a producer id which is not currently assigned to \
                 its transactional id.",
            ),
            ErrorCode::InvalidTransactionTimeout => f.write_str(
                "The transaction timeout is larger than the maximum value allowed by the broker \
                 (as configured by transaction.max.timeout.ms).",
            ),
            ErrorCode::ConcurrentTransactions => f.write_str(
                "The producer attempted to update a transaction while another concurrent \
                 operation on the same transaction was ongoing.",
            ),
            ErrorCode::TransactionCoordinatorFenced => f.write_str(
                "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer \
                 the current coordinator for a given producer.",
            ),
            ErrorCode::TransactionalIdAuthorizationFailed => {
                f.write_str("Transactional Id authorization failed.")
            }
            ErrorCode::SecurityDisabled => f.write_str("Security features are disabled."),
            ErrorCode::OperationNotAttempted => f.write_str(
                "The broker did not attempt to execute this operation. This may happen for \
                 batched RPCs where some operations in the batch failed, causing the broker to \
                 respond without trying the rest.",
            ),
            ErrorCode::KafkaStorageError => {
                f.write_str("Disk error when trying to access log file on the disk.")
            }
            ErrorCode::LogDirNotFound => {
                f.write_str("The user-specified log directory is not found in the broker config.")
            }
            ErrorCode::SaslAuthenticationFailed => f.write_str("SASL Authentication failed."),
            ErrorCode::UnknownProducerId => f.write_str(
                "This exception is raised by the broker if it could not locate the producer \
                 metadata associated with the producerId in question. This could happen if, for \
                 instance, the producer's records were deleted because their retention time had \
                 elapsed. Once the last records of the producerId are removed, the producer's \
                 metadata is removed from the broker, and future appends by the producer will \
                 return this exception.",
            ),
            ErrorCode::ReassignmentInProgress => {
                f.write_str("A partition reassignment is in progress.")
            }
            ErrorCode::DelegationTokenAuthDisabled => {
                f.write_str("Delegation Token feature is not enabled.")
            }
            ErrorCode::DelegationTokenNotFound => {
                f.write_str("Delegation Token is not found on server.")
            }
            ErrorCode::DelegationTokenOwnerMismatch => {
                f.write_str("Specified Principal is not valid Owner/Renewer.")
            }
            ErrorCode::DelegationTokenRequestNotAllowed => f.write_str(
                "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on \
                 delegation token authenticated channels.",
            ),
            ErrorCode::DelegationTokenAuthorizationFailed => {
                f.write_str("Delegation Token authorization failed.")
            }
            ErrorCode::DelegationTokenExpired => f.write_str("Delegation Token is expired."),
            ErrorCode::InvalidPrincipalType => {
                f.write_str("Supplied principalType is not supported.")
            }
            ErrorCode::NonEmptyGroup => f.write_str("The group is not empty."),
            ErrorCode::GroupIdNotFound => f.write_str("The group id does not exist."),
            ErrorCode::FetchSessionIdNotFound => f.write_str("The fetch session ID was not found."),
            ErrorCode::InvalidFetchSessionEpoch => {
                f.write_str("The fetch session epoch is invalid.")
            }
            ErrorCode::ListenerNotFound => f.write_str(
                "There is no listener on the leader broker that matches the listener on which \
                 metadata request was processed.",
            ),
            ErrorCode::TopicDeletionDisabled => f.write_str("Topic deletion is disabled."),
            ErrorCode::FencedLeaderEpoch => f.write_str(
                "The leader epoch in the request is older than the epoch on the broker.",
            ),
            ErrorCode::UnknownLeaderEpoch => f.write_str(
                "The leader epoch in the request is newer than the epoch on the broker.",
            ),
            ErrorCode::UnsupportedCompressionType => f.write_str(
                "The requesting client does not support the compression type of given partition.",
            ),
            ErrorCode::StaleBrokerEpoch => f.write_str("Broker epoch has changed."),
            ErrorCode::OffsetNotAvailable => f.write_str(
                "The leader high watermark has not caught up from a recent leader election so the \
                 offsets cannot be guaranteed to be monotonically increasing.",
            ),
            ErrorCode::MemberIdRequired => f.write_str(
                "The group member needs to have a valid member id before actually entering a \
                 consumer group.",
            ),
            ErrorCode::PreferredLeaderNotAvailable => {
                f.write_str("The preferred leader was not available.")
            }
            ErrorCode::GroupMaxSizeReached => {
                f.write_str("The consumer group has reached its max size.")
            }
            ErrorCode::FencedInstanceId => f.write_str(
                "The broker rejected this static consumer since another consumer with the same \
                 group.instance.id has registered with a different member.id.",
            ),
            ErrorCode::EligibleLeadersNotAvailable => {
                f.write_str("Eligible topic partition leaders are not available.")
            }
            ErrorCode::ElectionNotNeeded => {
                f.write_str("Leader election not needed for topic partition.")
            }
            ErrorCode::NoReassignmentInProgress => {
                f.write_str("No partition reassignment is in progress.")
            }
            ErrorCode::GroupSubscribedToTopic => f.write_str(
                "Deleting offsets of a topic is forbidden while the consumer group is actively \
                 subscribed to it.",
            ),
            ErrorCode::InvalidRecord => f.write_str(
                "This record has failed the validation on broker and hence will be rejected.",
            ),
            ErrorCode::UnstableOffsetCommit => {
                f.write_str("There are unstable offsets that need to be cleared.")
            }
            ErrorCode::ThrottlingQuotaExceeded => {
                f.write_str("The throttling quota has been exceeded.")
            }
            ErrorCode::ProducerFenced => f.write_str(
                "There is a newer producer with the same transactionalId which fences the current \
                 one.",
            ),
            ErrorCode::ResourceNotFound => {
                f.write_str("A request illegally referred to a resource that does not exist.")
            }
            ErrorCode::DuplicateResource => {
                f.write_str("A request illegally referred to the same resource twice.")
            }
            ErrorCode::UnacceptableCredential => {
                f.write_str("Requested credential would not meet criteria for acceptability.")
            }
            ErrorCode::InconsistentVoterSet => f.write_str(
                "Indicates that the either the sender or recipient of a voter-only request is not \
                 one of the expected voters",
            ),
            ErrorCode::InvalidUpdateVersion => f.write_str("The given update version was invalid."),
            ErrorCode::FeatureUpdateFailed => f.write_str(
                "Unable to update finalized features due to an unexpected server error.",
            ),
            ErrorCode::PrincipalDeserializationFailure => f.write_str(
                "Request principal deserialization failed during forwarding. This indicates an \
                 internal error on the broker cluster security setup.",
            ),
            ErrorCode::SnapshotNotFound => f.write_str("Requested snapshot was not found"),
            ErrorCode::PositionOutOfRange => f.write_str(
                "Requested position is not greater than or equal to zero, and less than the size \
                 of the snapshot.",
            ),
            ErrorCode::UnknownTopicId => f.write_str("This server does not host this topic ID."),
            ErrorCode::DuplicateBrokerRegistration => {
                f.write_str("This broker ID is already in use.")
            }
            ErrorCode::BrokerIdNotRegistered => {
                f.write_str("The given broker ID was not registered.")
            }
            ErrorCode::InconsistentTopicId => {
                f.write_str("The log's topic ID did not match the topic ID in the request")
            }
            ErrorCode::InconsistentClusterId => {
                f.write_str("The clusterId in the request does not match that found on the server")
            }
            ErrorCode::TransactionalIdNotFound => {
                f.write_str("The transactionalId could not be found")
            }
            ErrorCode::FetchSessionTopicIdError => {
                f.write_str("The fetch session encountered inconsistent topic ID usage")
            }
            ErrorCode::IneligibleReplica => {
                f.write_str("The new ISR contains at least one ineligible replica.")
            }
            ErrorCode::NewLeaderElected => f.write_str(
                "The AlterPartition request successfully updated the partition state but the \
                 leader has changed.",
            ),
            ErrorCode::OffsetMovedToTieredStorage => {
                f.write_str("The requested offset is moved to tiered storage.")
            }
            ErrorCode::FencedMemberEpoch => f.write_str(
                "The member epoch is fenced by the group coordinator. The member must abandon all \
                 its partitions and rejoin.",
            ),
            ErrorCode::UnreleasedInstanceId => f.write_str(
                "The instance ID is still used by another member in the consumer group. That \
                 member must leave first.",
            ),
            ErrorCode::UnsupportedAssignor => f.write_str(
                "The assignor or its version range is not supported by the consumer group.",
            ),
            ErrorCode::StaleMemberEpoch => f.write_str(
                "The member epoch is stale. The member must retry after receiving its updated \
                 member epoch via the ConsumerGroupHeartbeat API.",
            ),
            ErrorCode::MismatchedEndpointType => {
                f.write_str("The request was sent to an endpoint of the wrong type.")
            }
            ErrorCode::UnsupportedEndpointType => {
                f.write_str("This endpoint type is not supported yet.")
            }
            ErrorCode::UnknownControllerId => f.write_str("This controller ID is not known."),
            ErrorCode::UnknownSubscriptionId => f.write_str(
                "Client sent a push telemetry request with an invalid or outdated subscription ID.",
            ),
            ErrorCode::TelemetryTooLarge => f.write_str(
                "Client sent a push telemetry request larger than the maximum size the broker \
                 will accept.",
            ),
            ErrorCode::InvalidRegistration => {
                f.write_str("The controller has considered the broker registration to be invalid.")
            }
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Eq, Hash, Debug, Ord, PartialEq, PartialOrd)]
pub enum ErrorCode {
    UnknownServerError,
    None,
    OffsetOutOfRange,
    CorruptMessage,
    UnknownTopicOrPartition,
    InvalidFetchSize,
    LeaderNotAvailable,
    NotLeaderOrFollower,
    RequestTimedOut,
    BrokerNotAvailable,
    ReplicaNotAvailable,
    MessageTooLarge,
    StaleControllerEpoch,
    OffsetMetadataTooLarge,
    NetworkException,
    CoordinatorLoadInProgress,
    CoordinatorNotAvailable,
    NotCoordinator,
    InvalidTopicException,
    RecordListTooLarge,
    NotEnoughReplicas,
    NotEnoughReplicasAfterAppend,
    InvalidRequiredAcks,
    IllegalGeneration,
    InconsistentGroupProtocol,
    InvalidGroupId,
    UnknownMemberId,
    InvalidSessionTimeout,
    RebalanceInProgress,
    InvalidCommitOffsetSize,
    TopicAuthorizationFailed,
    GroupAuthorizationFailed,
    ClusterAuthorizationFailed,
    InvalidTimestamp,
    UnsupportedSaslMechanism,
    IllegalSaslState,
    UnsupportedVersion,
    TopicAlreadyExists,
    InvalidPartitions,
    InvalidReplicationFactor,
    InvalidReplicaAssignment,
    InvalidConfig,
    NotController,
    InvalidRequest,
    UnsupportedForMessageFormat,
    PolicyViolation,
    OutOfOrderSequenceNumber,
    DuplicateSequenceNumber,
    InvalidProducerEpoch,
    InvalidTxnState,
    InvalidProducerIdMapping,
    InvalidTransactionTimeout,
    ConcurrentTransactions,
    TransactionCoordinatorFenced,
    TransactionalIdAuthorizationFailed,
    SecurityDisabled,
    OperationNotAttempted,
    KafkaStorageError,
    LogDirNotFound,
    SaslAuthenticationFailed,
    UnknownProducerId,
    ReassignmentInProgress,
    DelegationTokenAuthDisabled,
    DelegationTokenNotFound,
    DelegationTokenOwnerMismatch,
    DelegationTokenRequestNotAllowed,
    DelegationTokenAuthorizationFailed,
    DelegationTokenExpired,
    InvalidPrincipalType,
    NonEmptyGroup,
    GroupIdNotFound,
    FetchSessionIdNotFound,
    InvalidFetchSessionEpoch,
    ListenerNotFound,
    TopicDeletionDisabled,
    FencedLeaderEpoch,
    UnknownLeaderEpoch,
    UnsupportedCompressionType,
    StaleBrokerEpoch,
    OffsetNotAvailable,
    MemberIdRequired,
    PreferredLeaderNotAvailable,
    GroupMaxSizeReached,
    FencedInstanceId,
    EligibleLeadersNotAvailable,
    ElectionNotNeeded,
    NoReassignmentInProgress,
    GroupSubscribedToTopic,
    InvalidRecord,
    UnstableOffsetCommit,
    ThrottlingQuotaExceeded,
    ProducerFenced,
    ResourceNotFound,
    DuplicateResource,
    UnacceptableCredential,
    InconsistentVoterSet,
    InvalidUpdateVersion,
    FeatureUpdateFailed,
    PrincipalDeserializationFailure,
    SnapshotNotFound,
    PositionOutOfRange,
    UnknownTopicId,
    DuplicateBrokerRegistration,
    BrokerIdNotRegistered,
    InconsistentTopicId,
    InconsistentClusterId,
    TransactionalIdNotFound,
    FetchSessionTopicIdError,
    IneligibleReplica,
    NewLeaderElected,
    OffsetMovedToTieredStorage,
    FencedMemberEpoch,
    UnreleasedInstanceId,
    UnsupportedAssignor,
    StaleMemberEpoch,
    MismatchedEndpointType,
    UnsupportedEndpointType,
    UnknownControllerId,
    UnknownSubscriptionId,
    TelemetryTooLarge,
    InvalidRegistration,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
}

impl TryFrom<i8> for IsolationLevel {
    type Error = Error;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::ReadUncommitted),
            1 => Ok(Self::ReadCommitted),
            _ => Err(Error::InvalidIsolationLevel(value)),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
enum Ack {
    None,
    Leader,
    FullIsr,
}

impl TryFrom<i16> for Ack {
    type Error = Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            -1 => Ok(Self::FullIsr),
            0 => Ok(Self::None),
            1 => Ok(Self::Leader),
            _ => Err(Error::InvalidAckValue(value)),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum Compression {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl TryFrom<i16> for Compression {
    type Error = Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value & 0b111i16 {
            0 => Ok(Self::None),
            1 => Ok(Self::Gzip),
            2 => Ok(Self::Snappy),
            3 => Ok(Self::Lz4),
            4 => Ok(Self::Zstd),
            otherwise => Err(Error::UnknownCompressionType(otherwise)),
        }
    }
}

impl From<Compression> for i16 {
    fn from(value: Compression) -> Self {
        match value {
            Compression::None => 0,
            Compression::Gzip => 1,
            Compression::Snappy => 2,
            Compression::Lz4 => 3,
            Compression::Zstd => 4,
        }
    }
}

impl Compression {
    fn inflator(&self, deflated: impl BufRead + 'static) -> Result<Box<dyn Read>> {
        match self {
            Compression::None => Ok(Box::new(deflated)),
            Compression::Gzip => Ok(Box::new(GzDecoder::new(deflated))),
            Compression::Snappy => Ok(Box::new(snap::read::FrameDecoder::new(deflated))),
            Compression::Lz4 => lz4::Decoder::new(deflated)
                .map(Box::new)
                .map(|boxed| boxed as Box<dyn Read>)
                .map_err(Into::into),
            Compression::Zstd => zstd::stream::read::Decoder::with_buffer(deflated)
                .map(Box::new)
                .map(|boxed| boxed as Box<dyn Read>)
                .map_err(Into::into),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum EndpointType {
    #[default]
    Unknown,
    Broker,
    Controller,
}

impl From<i8> for EndpointType {
    fn from(value: i8) -> Self {
        match value {
            1 => Self::Broker,
            2 => Self::Controller,
            _ => Self::Unknown,
        }
    }
}

impl From<EndpointType> for i8 {
    fn from(value: EndpointType) -> Self {
        match value {
            EndpointType::Unknown => 0,
            EndpointType::Broker => 1,
            EndpointType::Controller => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ScramMechanism {
    Scram256,
    Scram512,
}

impl TryFrom<i8> for ScramMechanism {
    type Error = Error;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Scram256),
            2 => Ok(Self::Scram512),
            otherwise => Err(Error::InvalidScramMechanism(otherwise)),
        }
    }
}

impl From<ScramMechanism> for i8 {
    fn from(value: ScramMechanism) -> Self {
        match value {
            ScramMechanism::Scram256 => 1,
            ScramMechanism::Scram512 => 2,
        }
    }
}

impl From<ScramMechanism> for i32 {
    fn from(value: ScramMechanism) -> Self {
        match value {
            ScramMechanism::Scram256 => 1,
            ScramMechanism::Scram512 => 2,
        }
    }
}

pub fn to_system_time(timestamp: i64) -> Result<SystemTime> {
    u64::try_from(timestamp)
        .map(|timestamp| SystemTime::UNIX_EPOCH + Duration::from_millis(timestamp))
        .map_err(Into::into)
}

pub fn to_timestamp(system_time: SystemTime) -> Result<i64> {
    system_time
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(Into::into)
        .map(|since_epoch| since_epoch.as_millis())
        .and_then(|since_epoch| i64::try_from(since_epoch).map_err(Into::into))
}

include!(concat!(env!("OUT_DIR"), "/generate.rs"));
