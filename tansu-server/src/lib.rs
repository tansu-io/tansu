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

use std::{
    fmt, io,
    num::TryFromIntError,
    result,
    str::Utf8Error,
    string::FromUtf8Error,
    sync::{Arc, PoisonError},
};

use jsonschema::ValidationError;
use opentelemetry::trace::TraceError;
use tansu_kafka_sans_io::ErrorCode;
use thiserror::Error;
use tracing_subscriber::filter::ParseError;
use url::Url;

pub mod broker;
pub mod coordinator;
pub mod otel;

#[derive(Error, Debug)]
pub enum Error {
    Api(ErrorCode),
    Custom(String),
    EmptyCoordinatorWrapper,
    EmptyJoinGroupRequestProtocol,
    ExpectedJoinGroupRequestProtocol(&'static str),
    Hyper(#[from] hyper::http::Error),
    Io(Arc<io::Error>),
    Json(#[from] serde_json::Error),
    KafkaProtocol(#[from] tansu_kafka_sans_io::Error),
    Message(String),
    Metric(#[from] opentelemetry_sdk::metrics::MetricError),
    Model(#[from] tansu_kafka_model::Error),
    ObjectStore(#[from] object_store::Error),
    ParseFilter(#[from] ParseError),
    ParseInt(#[from] std::num::ParseIntError),
    Poison,
    Pool(#[from] deadpool_postgres::PoolError),
    SchemaRegistry(#[from] tansu_schema_registry::Error),
    Storage(#[from] tansu_storage::Error),
    StringUtf8(#[from] FromUtf8Error),
    OpenTelemetryTrace(TraceError),
    Prometheus(#[from] prometheus::Error),
    TokioPostgres(#[from] tokio_postgres::error::Error),
    TryFromInt(#[from] TryFromIntError),
    UnsupportedStorageUrl(Url),
    Url(#[from] url::ParseError),
    Utf8(#[from] Utf8Error),
    Uuid(#[from] uuid::Error),
    SchemaValidation,
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl From<ValidationError<'_>> for Error {
    fn from(_value: ValidationError<'_>) -> Self {
        Self::SchemaValidation
    }
}

impl From<TraceError> for Error {
    fn from(value: TraceError) -> Self {
        Self::OpenTelemetryTrace(value)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(msg) => write!(f, "{}", msg),
            error => write!(f, "{:?}", error),
        }
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;
