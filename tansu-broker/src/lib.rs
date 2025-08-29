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

use std::{
    collections::HashMap,
    env::vars,
    fmt, io,
    net::AddrParseError,
    num::TryFromIntError,
    result,
    str::{FromStr, Utf8Error},
    string::FromUtf8Error,
    sync::{Arc, LazyLock, PoisonError},
    time::Duration,
};

use jsonschema::ValidationError;
use opentelemetry::{InstrumentationScope, global, metrics::Meter};
use opentelemetry_otlp::ExporterBuildError;
use opentelemetry_semantic_conventions::SCHEMA_URL;
use regex::{Regex, Replacer};
use tansu_sans_io::ErrorCode;
use thiserror::Error;
use tokio::{sync::broadcast::error::SendError, task::JoinError};
use tracing_subscriber::filter::ParseError;
use url::Url;

pub mod broker;
pub mod coordinator;
pub mod otel;
pub mod service;

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CancelKind {
    Interrupt,
    Terminate,
}

impl From<CancelKind> for Duration {
    fn from(cancellation: CancelKind) -> Self {
        Duration::from_millis(match cancellation {
            CancelKind::Interrupt => 0,
            CancelKind::Terminate => 5_000,
        })
    }
}

pub const NODE_ID: i32 = 111;

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
    AddrParse(#[from] AddrParseError),
    Api(ErrorCode),
    Custom(String),
    DuplicateApiService(i16),
    EmptyCoordinatorWrapper,
    EmptyJoinGroupRequestProtocol,
    ExpectedJoinGroupRequestProtocol(&'static str),
    ExporterBuild(#[from] ExporterBuildError),
    Hyper(#[from] hyper::http::Error),
    Io(Arc<io::Error>),
    Join(#[from] JoinError),
    Json(#[from] serde_json::Error),
    KafkaProtocol(#[from] tansu_sans_io::Error),
    Message(String),
    Model(#[from] tansu_model::Error),
    ObjectStore(#[from] object_store::Error),
    ParseFilter(#[from] ParseError),
    ParseInt(#[from] std::num::ParseIntError),
    Poison,
    Pool(#[from] deadpool_postgres::PoolError),
    SchemaRegistry(Box<tansu_schema::Error>),
    Storage(#[from] tansu_storage::Error),
    StringUtf8(#[from] FromUtf8Error),
    Regex(#[from] regex::Error),
    TokioPostgres(#[from] tokio_postgres::error::Error),
    TryFromInt(#[from] TryFromIntError),
    UnsupportedApiService(i16),
    UnsupportedStorageUrl(Url),
    UnsupportedTracingFormat(String),
    Url(#[from] url::ParseError),
    Utf8(#[from] Utf8Error),
    Uuid(#[from] uuid::Error),
    SchemaValidation,
    Send(#[from] SendError<CancelKind>),
}

impl From<tansu_schema::Error> for Error {
    fn from(value: tansu_schema::Error) -> Self {
        Self::SchemaRegistry(Box::new(value))
    }
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

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(msg) => write!(f, "{msg}"),
            error => write!(f, "{error:?}"),
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
        if let Some(variable) = caps.name("var")
            && let Some(value) = self.0.get(variable.as_str())
        {
            dst.push_str(value);
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
