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

use std::{env, fmt, io, result, sync::Arc, time::SystemTime};

use bytes::Bytes;
use jsonschema::ValidationError;
use object_store::{
    aws::AmazonS3Builder, local::LocalFileSystem, memory::InMemory, path::Path, DynObjectStore,
    ObjectStore,
};
use opentelemetry::{
    global,
    metrics::{Counter, Histogram},
    InstrumentationScope, KeyValue,
};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use tansu_kafka_sans_io::{record::inflated::Batch, ErrorCode};
use tracing::{debug, error};
use url::Url;

#[cfg(test)]
use tracing_subscriber::filter::ParseError;

mod json;
mod proto;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    Anyhow(#[from] anyhow::Error),
    Api(ErrorCode),
    Io(#[from] io::Error),
    KafkaSansIo(#[from] tansu_kafka_sans_io::Error),
    Message(String),
    ObjectStore(#[from] object_store::Error),

    #[cfg(test)]
    ParseFilter(#[from] ParseError),

    #[cfg(test)]
    ProtobufJsonMapping(#[from] protobuf_json_mapping::ParseError),

    Protobuf(#[from] protobuf::Error),

    ProtobufFileDescriptorMissing(Bytes),

    SchemaValidation,
    SerdeJson(#[from] serde_json::Error),
    UnsupportedSchemaRegistryUrl(Url),
}

impl From<ValidationError<'_>> for Error {
    fn from(_value: ValidationError<'_>) -> Self {
        Self::SchemaValidation
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

trait Validator {
    fn validate(&self, batch: &Batch) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct Registry {
    object_store: Arc<DynObjectStore>,
    validation_duration: Histogram<u64>,
    validation_error: Counter<u64>,
}

impl Registry {
    fn new(object_store: impl ObjectStore) -> Self {
        let meter = global::meter_with_scope(
            InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
                .with_version(env!("CARGO_PKG_VERSION"))
                .with_schema_url(SCHEMA_URL)
                .build(),
        );

        Self {
            object_store: Arc::new(object_store),
            validation_duration: meter
                .u64_histogram("registry_validation_duration")
                .with_unit("ms")
                .with_description("The registry validation request latencies in milliseconds")
                .build(),
            validation_error: meter
                .u64_counter("registry_validation_error")
                .with_description("The registry validation error count")
                .build(),
        }
    }

    async fn get(&self, location: &Path) -> Result<Bytes> {
        debug!(%location, object_store = ?self.object_store);

        let get_result = self
            .object_store
            .get(location)
            .await
            .inspect(|r| debug!(?r))
            .inspect_err(|err| error!(?err))?;

        get_result
            .bytes()
            .await
            .map_err(Into::into)
            .inspect(|r| debug!(?r))
            .inspect_err(|err| error!(?err))
    }

    pub async fn validate(&self, topic: &str, batch: &Batch) -> Result<()> {
        let list_result = self.object_store.list_with_delimiter(None).await?;
        debug!(common_prefixes = ?list_result.common_prefixes, objects = ?list_result.objects);

        if list_result
            .objects
            .iter()
            .any(|meta| meta.location == Path::from(format!("{topic}.proto")))
        {
            let validation_start = SystemTime::now();

            self.get(&Path::from(format!("{topic}.proto")))
                .await
                .and_then(proto::Schema::try_from)
                .and_then(|schema| schema.validate(batch))
                .inspect(|_| {
                    self.validation_duration.record(
                        validation_start
                            .elapsed()
                            .map_or(0, |duration| duration.as_millis() as u64),
                        &[
                            KeyValue::new("topic", topic.to_owned()),
                            KeyValue::new("schema", format!("{topic}.proto")),
                        ],
                    )
                })
                .inspect_err(|err| {
                    self.validation_error.add(
                        1,
                        &[
                            KeyValue::new("topic", topic.to_owned()),
                            KeyValue::new("reason", err.to_string()),
                        ],
                    )
                })
        } else if list_result
            .objects
            .iter()
            .any(|meta| meta.location == Path::from(format!("{topic}.json")))
        {
            let validation_start = SystemTime::now();

            self.get(&Path::from(format!("{topic}.json")))
                .await
                .and_then(json::Schema::try_from)
                .and_then(|schema| schema.validate(batch))
                .inspect(|_| {
                    self.validation_duration.record(
                        validation_start
                            .elapsed()
                            .map_or(0, |duration| duration.as_millis() as u64),
                        &[
                            KeyValue::new("topic", topic.to_owned()),
                            KeyValue::new("schema", format!("{topic}.json")),
                        ],
                    )
                })
                .inspect_err(|err| {
                    self.validation_error.add(
                        1,
                        &[
                            KeyValue::new("topic", topic.to_owned()),
                            KeyValue::new("schema", format!("{topic}.json")),
                            KeyValue::new("reason", err.to_string()),
                        ],
                    )
                })
        } else {
            Ok(())
        }
    }
}

impl TryFrom<Url> for Registry {
    type Error = Error;

    fn try_from(storage: Url) -> Result<Self, Self::Error> {
        debug!(%storage);

        match storage.scheme() {
            "s3" => {
                let bucket_name = storage.host_str().unwrap_or("schema");

                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket_name)
                    .build()
                    .map_err(Into::into)
                    .map(Registry::new)
            }

            "file" => {
                let mut path = env::current_dir().inspect(|current_dir| debug!(?current_dir))?;

                if let Some(domain) = storage.domain() {
                    path.push(domain);
                }

                if let Some(relative) = storage.path().strip_prefix("/") {
                    path.push(relative);
                } else {
                    path.push(storage.path());
                }

                debug!(?path);

                LocalFileSystem::new_with_prefix(path)
                    .map_err(Into::into)
                    .map(Registry::new)
            }

            "memory" => Ok(Registry::new(InMemory::new())),

            _unsupported => Err(Error::UnsupportedSchemaRegistryUrl(storage)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Result;
    use bytes::Bytes;
    use object_store::PutPayload;
    use serde_json::json;
    use std::{fs::File, sync::Arc, thread};
    use tansu_kafka_sans_io::record::Record;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    fn init_tracing() -> Result<DefaultGuard> {
        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(
                    EnvFilter::from_default_env()
                        .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
                )
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    static DEF_PROTO: &[u8] = br#"
      syntax = 'proto3';

      message Key {
        int32 id = 1;
      }

      message Value {
        string name = 1;
        string email = 2;
      }
    "#;

    async fn populate() -> Result<Registry> {
        let _guard = init_tracing()?;

        let object_store = InMemory::new();

        let location = Path::from("abc.json");
        let payload = serde_json::to_vec(&json!({
            "type": "object",
            "properties": {
                "key": {
                    "type": "number",
                    "multipleOf": 10
                }
            }
        }))
        .map(Bytes::from)
        .map(PutPayload::from)?;

        _ = object_store.put(&location, payload).await?;

        let location = Path::from("def.proto");
        let payload = PutPayload::from(Bytes::from_static(DEF_PROTO));
        _ = object_store.put(&location, payload).await?;

        Ok(Registry::new(object_store))
    }

    #[tokio::test]
    async fn abc_valid() -> Result<()> {
        let _guard = init_tracing()?;

        let registry = populate().await?;

        let key = Bytes::from_static(b"5450");

        let batch = Batch::builder()
            .record(Record::builder().key(key.clone().into()))
            .build()?;

        registry.validate("abc", &batch).await?;

        Ok(())
    }

    #[tokio::test]
    async fn abc_invalid() -> Result<()> {
        let _guard = init_tracing()?;
        let registry = populate().await?;

        let key = Bytes::from_static(b"545");

        let batch = Batch::builder()
            .record(Record::builder().key(key.clone().into()))
            .build()?;

        assert!(matches!(
            registry
                .validate("abc", &batch)
                .await
                .inspect_err(|err| error!(?err)),
            Err(Error::Api(ErrorCode::InvalidRecord))
        ));

        Ok(())
    }
}
