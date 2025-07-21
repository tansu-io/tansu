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

//! Schema
//!
//! Schema includes the following:
//! - Validation of Kafka messages with an AVRO, JSON or Protobuf schema

use std::{
    collections::BTreeMap,
    env::{self},
    io,
    num::TryFromIntError,
    result,
    string::FromUtf8Error,
    sync::{Arc, LazyLock, Mutex, PoisonError},
    time::{Duration, SystemTime},
};

use arrow::{datatypes::DataType, error::ArrowError, record_batch::RecordBatch};
use bytes::Bytes;
use datafusion::error::DataFusionError;
use deltalake::DeltaTableError;
use governor::InsufficientCapacity;
use iceberg::spec::DataFileBuilderError;
use jsonschema::ValidationError;
use object_store::{
    DynObjectStore, ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem, memory::InMemory,
    path::Path,
};
use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    metrics::{Counter, Histogram, Meter},
};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use parquet::errors::ParquetError;
use rhai::EvalAltResult;
use serde_json::Value;
use tansu_sans_io::{ErrorCode, record::inflated::Batch};
use tracing::{debug, error};
use tracing_subscriber::filter::ParseError;
use url::Url;

use crate::lake::LakeHouseType;

pub mod avro;
pub mod json;
pub mod lake;
pub mod proto;
pub(crate) mod sql;

pub(crate) const ARROW_LIST_FIELD_NAME: &str = "element";

/// Error
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{:?}", self)]
    Anyhow(#[from] anyhow::Error),

    #[error("{:?}", self)]
    Api(ErrorCode),

    #[error("{:?}", self)]
    Arrow(#[from] ArrowError),

    #[error("{:?}", self)]
    Avro(Box<apache_avro::Error>),

    #[error("{:?}", self)]
    AvroToJson(apache_avro::types::Value),

    #[error("{:?}", self)]
    BadDowncast { field: String },

    #[error("{:?}", self)]
    EvalAlt(#[from] Box<EvalAltResult>),

    #[error("{:?}", self)]
    BuilderExhausted,

    #[error("{:?}", self)]
    ChronoParse(#[from] chrono::ParseError),

    #[error("{:?}", self)]
    DataFileBuilder(#[from] DataFileBuilderError),

    #[error("{:?}", self)]
    DataFusion(#[from] DataFusionError),

    #[error("{:?}", self)]
    DeltaTable(#[from] DeltaTableError),

    #[error("{:?}", self)]
    Downcast,

    #[error("{:?}", self)]
    FromUtf8(#[from] FromUtf8Error),

    #[error("{:?}", self)]
    Iceberg(#[from] ::iceberg::Error),

    #[error("{:?}", self)]
    InvalidValue(apache_avro::types::Value),

    #[error("{:?}", self)]
    InsufficientCapacity(#[from] InsufficientCapacity),

    #[error("{:?}", self)]
    Io(#[from] io::Error),

    #[error("{:?}", self)]
    JsonToAvro(Box<apache_avro::Schema>, Box<Value>),

    #[error("field: {field}, not found in: {value} with schema: {schema}")]
    JsonToAvroFieldNotFound {
        schema: Box<apache_avro::Schema>,
        value: Box<Value>,
        field: String,
    },

    #[error("{:?}", self)]
    KafkaSansIo(#[from] tansu_sans_io::Error),

    #[error("{:?}", self)]
    Message(String),

    #[error("{:?}", self)]
    NoCommonType(Vec<DataType>),

    #[error("{:?}", self)]
    ObjectStore(#[from] object_store::Error),

    #[error("{:?}", self)]
    Parquet(#[from] ParquetError),

    #[error("{:?}", self)]
    ParseFilter(#[from] ParseError),

    #[error("{:?}", self)]
    ParseUrl(#[from] url::ParseError),

    #[error("{:?}", self)]
    Poison,

    #[error("{:?}", self)]
    ProtobufJsonMapping(#[from] protobuf_json_mapping::ParseError),

    #[error("{:?}", self)]
    ProtobufJsonMappingPrint(#[from] protobuf_json_mapping::PrintError),

    #[error("{:?}", self)]
    Protobuf(#[from] protobuf::Error),

    #[error("{:?}", self)]
    ProtobufFileDescriptorMissing(Bytes),

    #[error("{:?}", self)]
    SchemaValidation,

    #[error("{:?}", self)]
    SerdeJson(#[from] serde_json::Error),

    #[error("{:?}", self)]
    SqlParser(#[from] datafusion::logical_expr::sqlparser::parser::ParserError),

    #[error("{:?}", self)]
    TryFromInt(#[from] TryFromIntError),

    #[error("{:?}", self)]
    UnsupportedIcebergCatalogUrl(Url),

    #[error("{:?}", self)]
    UnsupportedLakeHouseUrl(Url),

    #[error("{:?}", self)]
    UnsupportedSchemaRegistryUrl(Url),

    #[error("{:?}", self)]
    UnsupportedSchemaRuntimeValue(DataType, Value),

    #[error("{:?}", self)]
    Uuid(#[from] uuid::Error),
}

impl From<apache_avro::Error> for Error {
    fn from(value: apache_avro::Error) -> Self {
        Self::Avro(Box::new(value))
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

pub type Result<T, E = Error> = result::Result<T, E>;

/// Validate a Batch with a Schema
pub trait Validator {
    fn validate(&self, batch: &Batch) -> Result<()>;
}

/// Represent a Batch in the Arrow columnar data format
pub trait AsArrow {
    fn as_arrow(
        &self,
        partition: i32,
        batch: &Batch,
        lake_type: LakeHouseType,
    ) -> Result<RecordBatch>;
}

/// Convert a JSON message into a Kafka record
pub trait AsKafkaRecord {
    fn as_kafka_record(&self, value: &Value) -> Result<tansu_sans_io::record::Builder>;
}

/// Convert a Batch into a JSON value
pub trait AsJsonValue {
    fn as_json_value(&self, batch: &Batch) -> Result<Value>;
}

/// Generate a record
pub trait Generator {
    fn generate(&self) -> Result<tansu_sans_io::record::Builder>;
}

// Schema
//
// This is wrapper enumeration of the supported schema types
#[derive(Clone, Debug)]
pub enum Schema {
    Avro(Box<avro::Schema>),
    Json(Arc<json::Schema>),
    Proto(Box<proto::Schema>),
}

#[derive(Clone, Debug)]
struct CachedSchema {
    loaded_at: SystemTime,
    schema: Schema,
}

impl CachedSchema {
    fn new(schema: Schema) -> Self {
        Self {
            schema,
            loaded_at: SystemTime::now(),
        }
    }
}

impl AsKafkaRecord for Schema {
    fn as_kafka_record(&self, value: &Value) -> Result<tansu_sans_io::record::Builder> {
        debug!(?value);

        match self {
            Self::Avro(schema) => schema.as_kafka_record(value),
            Self::Json(schema) => schema.as_kafka_record(value),
            Self::Proto(schema) => schema.as_kafka_record(value),
        }
    }
}

impl Validator for Schema {
    fn validate(&self, batch: &Batch) -> Result<()> {
        debug!(?batch);

        match self {
            Self::Avro(schema) => schema.validate(batch),
            Self::Json(schema) => schema.validate(batch),
            Self::Proto(schema) => schema.validate(batch),
        }
    }
}

impl AsArrow for Schema {
    fn as_arrow(
        &self,
        partition: i32,
        batch: &Batch,
        lake_type: LakeHouseType,
    ) -> Result<RecordBatch> {
        debug!(?batch);

        match self {
            Self::Avro(schema) => schema.as_arrow(partition, batch, lake_type),
            Self::Json(schema) => schema.as_arrow(partition, batch, lake_type),
            Self::Proto(schema) => schema.as_arrow(partition, batch, lake_type),
        }
    }
}

impl AsJsonValue for Schema {
    fn as_json_value(&self, batch: &Batch) -> Result<Value> {
        debug!(?batch);

        match self {
            Self::Avro(schema) => schema.as_json_value(batch),
            Self::Json(schema) => schema.as_json_value(batch),
            Self::Proto(schema) => schema.as_json_value(batch),
        }
    }
}

impl Generator for Schema {
    fn generate(&self) -> Result<tansu_sans_io::record::Builder> {
        match self {
            Schema::Avro(schema) => schema.generate(),
            Schema::Json(schema) => schema.generate(),
            Schema::Proto(schema) => schema.generate(),
        }
    }
}

type SchemaCache = Arc<Mutex<BTreeMap<String, CachedSchema>>>;

// Schema Registry
#[derive(Clone, Debug)]
pub struct Registry {
    object_store: Arc<DynObjectStore>,
    schemas: SchemaCache,
    cache_expiry_after: Option<Duration>,
}

// Schema Registry builder
#[derive(Clone, Debug)]
pub struct Builder {
    object_store: Arc<DynObjectStore>,
    cache_expiry_after: Option<Duration>,
}

impl TryFrom<&Url> for Builder {
    type Error = Error;

    fn try_from(storage: &Url) -> Result<Self, Self::Error> {
        debug!(%storage);

        match storage.scheme() {
            "s3" => {
                let bucket_name = storage.host_str().unwrap_or("schema");

                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket_name)
                    .build()
                    .map_err(Into::into)
                    .map(Self::new)
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
                    .map(Self::new)
            }

            "memory" => Ok(Self::new(InMemory::new())),

            _unsupported => Err(Error::UnsupportedSchemaRegistryUrl(storage.to_owned())),
        }
    }
}

impl From<Builder> for Registry {
    fn from(builder: Builder) -> Self {
        Self {
            object_store: builder.object_store,
            schemas: Arc::new(Mutex::new(BTreeMap::new())),
            cache_expiry_after: builder.cache_expiry_after,
        }
    }
}

impl Builder {
    pub fn new(object_store: impl ObjectStore) -> Self {
        Self {
            object_store: Arc::new(object_store),
            cache_expiry_after: None,
        }
    }

    pub fn with_cache_expiry_after(self, cache_expiry_after: Option<Duration>) -> Self {
        Self {
            cache_expiry_after,
            ..self
        }
    }

    pub fn build(self) -> Registry {
        Registry::from(self)
    }
}

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

static VALIDATION_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("registry_validation_duration")
        .with_unit("ms")
        .with_description("The registry validation request latencies in milliseconds")
        .build()
});

static VALIDATION_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("registry_validation_error")
        .with_description("The registry validation error count")
        .build()
});

static AS_ARROW_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("registry_as_arrow_duration")
        .with_unit("ms")
        .with_description("The registry as Apache Arrow latencies in milliseconds")
        .build()
});

impl Registry {
    pub fn new(object_store: impl ObjectStore) -> Self {
        Builder::new(object_store).build()
    }

    pub fn builder(object_store: impl ObjectStore) -> Builder {
        Builder::new(object_store)
    }

    pub fn builder_try_from_url(url: &Url) -> Result<Builder> {
        Builder::try_from(url)
    }

    pub fn as_arrow(
        &self,
        topic: &str,
        partition: i32,
        batch: &Batch,
        lake_type: LakeHouseType,
    ) -> Result<Option<RecordBatch>> {
        debug!(topic, partition, ?batch);

        let start = SystemTime::now();

        self.schemas
            .lock()
            .map_err(Into::into)
            .and_then(|guard| {
                guard
                    .get(topic)
                    .map(|cached| cached.schema.as_arrow(partition, batch, lake_type))
                    .transpose()
            })
            .inspect(|record_batch| {
                debug!(
                    rows = record_batch
                        .as_ref()
                        .map(|record_batch| record_batch.num_rows())
                );
                AS_ARROW_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[KeyValue::new("topic", topic.to_owned())],
                )
            })
            .inspect_err(|err| debug!(?err))
    }

    pub async fn schema(&self, topic: &str) -> Result<Option<Schema>> {
        debug!(?topic);

        let proto = Path::from(format!("{topic}.proto"));
        let json = Path::from(format!("{topic}.json"));
        let avro = Path::from(format!("{topic}.avsc"));

        if let Some(cached) = self.schemas.lock().map(|guard| guard.get(topic).cloned())? {
            if self.cache_expiry_after.is_some_and(|cache_expiry_after| {
                SystemTime::now()
                    .duration_since(cached.loaded_at)
                    .unwrap_or_default()
                    > cache_expiry_after
            }) {
                return Ok(Some(cached.schema));
            } else {
                debug!(cache_expiry = topic);
            }
        }

        if let Ok(get_result) = self
            .object_store
            .get(&proto)
            .await
            .inspect(|get_result| debug!(?get_result))
            .inspect_err(|err| debug!(?err))
        {
            get_result
                .bytes()
                .await
                .map_err(Into::into)
                .and_then(proto::Schema::try_from)
                .map(Box::new)
                .map(Schema::Proto)
                .and_then(|schema| {
                    self.schemas
                        .lock()
                        .map_err(Into::into)
                        .map(|mut guard| {
                            guard.insert(topic.to_owned(), CachedSchema::new(schema.clone()))
                        })
                        .and(Ok(Some(schema)))
                })
        } else if let Ok(get_result) = self.object_store.get(&json).await {
            get_result
                .bytes()
                .await
                .map_err(Into::into)
                .and_then(json::Schema::try_from)
                .map(Arc::new)
                .map(Schema::Json)
                .and_then(|schema| {
                    self.schemas
                        .lock()
                        .map_err(Into::into)
                        .map(|mut guard| {
                            guard.insert(topic.to_owned(), CachedSchema::new(schema.clone()))
                        })
                        .and(Ok(Some(schema)))
                })
        } else if let Ok(get_result) = self.object_store.get(&avro).await {
            get_result
                .bytes()
                .await
                .map_err(Into::into)
                .and_then(avro::Schema::try_from)
                .map(Box::new)
                .map(Schema::Avro)
                .and_then(|schema| {
                    self.schemas
                        .lock()
                        .map_err(Into::into)
                        .map(|mut guard| {
                            guard.insert(topic.to_owned(), CachedSchema::new(schema.clone()))
                        })
                        .and(Ok(Some(schema)))
                })
        } else {
            Ok(None)
        }
    }

    pub async fn validate(&self, topic: &str, batch: &Batch) -> Result<()> {
        debug!(%topic, ?batch);

        let validation_start = SystemTime::now();

        let Some(schema) = self.schema(topic).await? else {
            debug!(no_schema_for_topic = %topic);
            return Ok(());
        };

        schema
            .validate(batch)
            .inspect(|_| {
                VALIDATION_DURATION.record(
                    validation_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[KeyValue::new("topic", topic.to_owned())],
                )
            })
            .inspect_err(|err| {
                VALIDATION_ERROR.add(
                    1,
                    &[
                        KeyValue::new("topic", topic.to_owned()),
                        KeyValue::new("reason", err.to_string()),
                    ],
                )
            })
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
    use tansu_sans_io::record::Record;
    use tracing::{error, subscriber::DefaultGuard};
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

    const DEF_PROTO: &[u8] = br#"
      syntax = 'proto3';

      message Key {
        int32 id = 1;
      }

      message Value {
        string name = 1;
        string email = 2;
      }
    "#;

    const PQR_AVRO: &[u8] = br#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "a", "type": "long", "default": 42},
                {"name": "b", "type": "string"},
                {"name": "c", "type": "long", "default": 43}
            ]
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

        let location = Path::from("pqr.avsc");
        let payload = PutPayload::from(Bytes::from_static(PQR_AVRO));
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

    #[tokio::test]
    async fn pqr_valid() -> Result<()> {
        let _guard = init_tracing()?;
        let registry = populate().await?;

        let key = Bytes::from_static(b"5450");

        let batch = Batch::builder()
            .record(Record::builder().key(key.clone().into()))
            .build()?;

        registry.validate("pqr", &batch).await?;

        Ok(())
    }
}
