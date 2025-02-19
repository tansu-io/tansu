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
    collections::HashMap,
    env::vars,
    fmt, io,
    num::TryFromIntError,
    result,
    str::{FromStr, Utf8Error},
    string::FromUtf8Error,
    sync::{Arc, LazyLock, PoisonError},
};

use jsonschema::ValidationError;
use opentelemetry::{global, metrics::Meter, trace::TraceError, InstrumentationScope};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use regex::{Regex, Replacer};
use tansu_kafka_sans_io::ErrorCode;
use thiserror::Error;
use tracing_subscriber::filter::ParseError;
use url::Url;

pub mod broker;
pub mod coordinator;
pub mod otel;

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

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
    Regex(#[from] regex::Error),
    TokioPostgres(#[from] tokio_postgres::error::Error),
    TryFromInt(#[from] TryFromIntError),
    UnsupportedStorageUrl(Url),
    UnsupportedTracingFormat(String),
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

#[derive(Copy, Clone, Debug)]
pub enum TracingFormat {
    Text,
    Json,
}

impl FromStr for TracingFormat {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            otherwise => Err(Error::UnsupportedTracingFormat(otherwise.to_owned())),
        }
    }
}

#[derive(Clone, Debug)]
pub struct VarRep(HashMap<String, String>);

impl From<HashMap<String, String>> for VarRep {
    fn from(value: HashMap<String, String>) -> Self {
        Self(value)
    }
}

impl VarRep {
    fn replace(&self, haystack: &str) -> Result<String> {
        Regex::new(r"\$\{(?<var>[^\}]+)\}")
            .map(|re| re.replace(haystack, self).into_owned())
            .map_err(Into::into)
    }
}

impl Replacer for &VarRep {
    fn replace_append(&mut self, caps: &regex::Captures<'_>, dst: &mut String) {
        if let Some(variable) = caps.name("var") {
            if let Some(value) = self.0.get(variable.as_str()) {
                dst.push_str(value);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct EnvVarExp<T>(T);

impl<T> EnvVarExp<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> FromStr for EnvVarExp<T>
where
    T: FromStr,
    Error: From<<T as FromStr>::Err>,
{
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        VarRep::from(vars().collect::<HashMap<_, _>>())
            .replace(s)
            .and_then(|s| T::from_str(&s).map_err(Into::into))
            .map(|t| Self(t))
    }
}
