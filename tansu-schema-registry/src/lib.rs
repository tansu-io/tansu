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
    collections::{BTreeMap, HashMap},
    env::{self},
    io,
    num::TryFromIntError,
    result,
    string::FromUtf8Error,
    sync::{Arc, Mutex, PoisonError},
    time::SystemTime,
};

use ::arrow::{datatypes::DataType, error::ArrowError, record_batch::RecordBatch};
use berg::env_s3_props;
use bytes::Bytes;
use datafusion::error::DataFusionError;
use iceberg::{
    Catalog, NamespaceIdent, TableCreation, TableIdent,
    spec::{DataFileBuilderError, DataFileFormat::Parquet, Schema as IcebergSchema},
    transaction::Transaction,
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
        },
    },
};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use jsonschema::ValidationError;
use object_store::{
    DynObjectStore, ObjectStore, PutMode, PutOptions, PutPayload, aws::AmazonS3Builder,
    local::LocalFileSystem, memory::InMemory, path::Path,
};
use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    metrics::{Counter, Histogram},
};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use parquet::{arrow::AsyncArrowWriter, errors::ParquetError, file::properties::WriterProperties};
use serde_json::Value;
use tansu_kafka_sans_io::{ErrorCode, record::inflated::Batch};
use tracing::{debug, error};
use url::Url;

#[cfg(test)]
use tracing_subscriber::filter::ParseError;
use uuid::Uuid;

mod arrow;
mod avro;
mod berg;
mod json;
mod proto;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{:?}", self)]
    Anyhow(#[from] anyhow::Error),

    #[error("{:?}", self)]
    Api(ErrorCode),

    #[error("{:?}", self)]
    Arrow(#[from] ArrowError),

    #[error("{:?}", self)]
    Avro(#[from] apache_avro::Error),

    #[error("{:?}", self)]
    AvroToJson(apache_avro::types::Value),

    #[error("{:?}", self)]
    BadDowncast { field: String },

    #[error("{:?}", self)]
    BuilderExhausted,

    #[error("{:?}", self)]
    ChronoParse(#[from] chrono::ParseError),

    #[error("{:?}", self)]
    DataFileBuilder(#[from] DataFileBuilderError),

    #[error("{:?}", self)]
    DataFusion(#[from] DataFusionError),

    #[error("{:?}", self)]
    Downcast,

    #[error("{:?}", self)]
    FromUtf8(#[from] FromUtf8Error),

    #[error("{:?}", self)]
    Iceberg(#[from] ::iceberg::Error),

    #[error("{:?}", self)]
    InvalidValue(apache_avro::types::Value),

    #[error("{:?}", self)]
    Io(#[from] io::Error),

    #[error("{:?}", self)]
    JsonToAvro(apache_avro::Schema, serde_json::Value),

    #[error("field: {field}, not found in: {value} with schema: {schema}")]
    JsonToAvroFieldNotFound {
        schema: apache_avro::Schema,
        value: serde_json::Value,
        field: String,
    },

    #[error("{:?}", self)]
    KafkaSansIo(#[from] tansu_kafka_sans_io::Error),

    #[error("{:?}", self)]
    Message(String),

    #[error("{:?}", self)]
    NoCommonType(Vec<DataType>),

    #[error("{:?}", self)]
    ObjectStore(#[from] object_store::Error),

    #[error("{:?}", self)]
    Parquet(#[from] ParquetError),

    #[cfg(test)]
    #[error("{:?}", self)]
    ParseFilter(#[from] ParseError),

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
    TryFromInt(#[from] TryFromIntError),

    #[error("{:?}", self)]
    UnsupportedSchemaRegistryUrl(Url),

    #[error("{:?}", self)]
    UnsupportedSchemaRuntimeValue(DataType, serde_json::Value),

    #[error("{:?}", self)]
    Uuid(#[from] uuid::Error),
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

trait Validator {
    fn validate(&self, batch: &Batch) -> Result<()>;
}

trait AsArrow {
    fn as_arrow(&self, batch: &Batch) -> Result<RecordBatch>;
}

pub trait AsKafkaRecord {
    fn as_kafka_record(&self, value: &Value) -> Result<tansu_kafka_sans_io::record::Builder>;
}

pub trait AsJsonValue {
    fn as_json_value(&self, batch: &Batch) -> Result<Value>;
}

#[derive(Clone, Debug)]
pub enum Schema {
    Avro(avro::Schema),
    Json(Arc<json::Schema>),
    Proto(proto::Schema),
}

impl AsKafkaRecord for Schema {
    fn as_kafka_record(&self, value: &Value) -> Result<tansu_kafka_sans_io::record::Builder> {
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
    fn as_arrow(&self, batch: &Batch) -> Result<RecordBatch> {
        debug!(?batch);

        match self {
            Schema::Avro(schema) => schema.as_arrow(batch),
            Schema::Json(schema) => schema.as_arrow(batch),
            Schema::Proto(schema) => schema.as_arrow(batch),
        }
    }
}

impl AsJsonValue for Schema {
    fn as_json_value(&self, batch: &Batch) -> Result<Value> {
        debug!(?batch);

        match self {
            Schema::Avro(schema) => schema.as_json_value(batch),
            Schema::Json(schema) => schema.as_json_value(batch),
            Schema::Proto(schema) => schema.as_json_value(batch),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Registry {
    object_store: Arc<DynObjectStore>,
    schemas: Arc<Mutex<BTreeMap<String, Schema>>>,
    validation_duration: Histogram<u64>,
    validation_error: Counter<u64>,
}

impl Registry {
    pub fn new(storage: impl ObjectStore) -> Self {
        let meter = global::meter_with_scope(
            InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
                .with_version(env!("CARGO_PKG_VERSION"))
                .with_schema_url(SCHEMA_URL)
                .build(),
        );

        Self {
            object_store: Arc::new(storage),
            schemas: Arc::new(Mutex::new(BTreeMap::new())),
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

    pub async fn store_as_iceberg(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        batch: &Batch,
        lake: &str,
    ) -> Result<()> {
        debug!(topic, partition, offset, lake);

        if let Some(record_batch) = self
            .schemas
            .lock()
            .map_err(Into::into)
            .and_then(|guard| {
                guard
                    .get(topic)
                    .map(|schema| schema.as_arrow(batch))
                    .transpose()
            })
            .inspect(|record_batch| debug!(?record_batch))
            .inspect_err(|err| debug!(?err))?
        {
            let catalog_config = RestCatalogConfig::builder()
                .uri(format!("http://{}:{}", "localhost", "8181"))
                .props(env_s3_props().collect())
                .build();

            let catalog = RestCatalog::new(catalog_config);
            debug!(?catalog);

            let namespace = "tansu";
            let namespace_ident = NamespaceIdent::new(namespace.into());

            if !catalog.namespace_exists(&namespace_ident).await? {
                catalog
                    .create_namespace(&namespace_ident, HashMap::new())
                    .await
                    .inspect(|namespace| debug!(?namespace))
                    .inspect_err(|err| debug!(?err))?;
            }

            let iceberg_schema = IcebergSchema::try_from(record_batch.schema().as_ref())
                .inspect(|schema| debug!(?schema))
                .inspect_err(|err| debug!(?err))?;

            let table = if catalog
                .table_exists(&TableIdent::new(namespace_ident.clone(), topic.into()))
                .await?
            {
                catalog
                    .load_table(&TableIdent::from_strs([namespace, topic])?)
                    .await
                    .inspect_err(|err| debug!(?err))
            } else {
                catalog
                    .create_table(
                        &namespace_ident,
                        TableCreation::builder()
                            .name(topic.into())
                            .schema(iceberg_schema.clone())
                            .build(),
                    )
                    .await
                    .inspect(|table| debug!(?table))
                    .inspect_err(|err| debug!(?err))
            }?;

            let writer = ParquetWriterBuilder::new(
                WriterProperties::default(),
                table.metadata().current_schema().clone(),
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata().clone())?,
                DefaultFileNameGenerator::new(
                    topic.to_owned(),
                    Some(format!("{partition:0>10}-{offset:0>20}")),
                    Parquet,
                ),
            );

            let mut data_file_writer = DataFileWriterBuilder::new(writer, None, 0).build().await?;

            data_file_writer.write(record_batch).await?;

            let data_files = data_file_writer
                .close()
                .await
                .inspect(|data_files| debug!(?data_files))
                .inspect_err(|err| debug!(?err))?;

            let commit_uuid = Uuid::now_v7();
            debug!(%commit_uuid);
            let key_metadata = format!("{topic}/{partition}/{offset}").into_bytes();

            let tx = Transaction::new(&table);

            let mut fast_append = tx
                .fast_append(Some(commit_uuid), key_metadata)
                .inspect_err(|err| debug!(?err))?;

            fast_append
                .add_data_files(data_files)
                .inspect_err(|err| debug!(?err))?;

            let tx = fast_append.apply().await.inspect_err(|err| debug!(?err))?;
            let _table = tx.commit(&catalog).await.inspect_err(|err| debug!(?err))?;
        }

        Ok(())
    }

    pub async fn store_as_parquet(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        batch: &Batch,
        object_store: &impl ObjectStore,
    ) -> Result<()> {
        debug!(%topic, partition, offset, ?object_store);
        let _ = batch;

        if let Some(record_batch) = self.schemas.lock().map_err(Into::into).and_then(|guard| {
            guard
                .get(topic)
                .map(|schema| schema.as_arrow(batch))
                .transpose()
        })? {
            let payload = {
                let mut buffer = Vec::new();
                let mut writer =
                    AsyncArrowWriter::try_new(&mut buffer, record_batch.schema(), None)?;
                writer.write(&record_batch).await?;
                writer.close().await?;
                PutPayload::from(Bytes::from(buffer))
            };

            let location = Path::from(format!(
                "{}/{:0>10}/{:0>20}.parquet",
                topic, partition, offset,
            ));

            _ = object_store
                .put_opts(
                    &location,
                    payload,
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await
                .inspect_err(|err| error!(?err, topic, partition, ?batch))
                .inspect(|result| debug!(%location, ?result))?
        }

        Ok(())
    }

    pub async fn schema(&self, topic: &str) -> Result<Option<Schema>> {
        debug!(?topic);

        let proto = Path::from(format!("{topic}.proto"));
        let json = Path::from(format!("{topic}.json"));
        let avro = Path::from(format!("{topic}.avsc"));

        if let Some(schema) = self.schemas.lock().map(|guard| guard.get(topic).cloned())? {
            Ok(Some(schema))
        } else if let Ok(get_result) = self
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
                .map(Schema::Proto)
                .and_then(|schema| {
                    self.schemas
                        .lock()
                        .map_err(Into::into)
                        .map(|mut guard| guard.insert(topic.to_owned(), schema.clone()))
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
                        .map(|mut guard| guard.insert(topic.to_owned(), schema.clone()))
                        .and(Ok(Some(schema)))
                })
        } else if let Ok(get_result) = self.object_store.get(&avro).await {
            get_result
                .bytes()
                .await
                .map_err(Into::into)
                .and_then(avro::Schema::try_from)
                .map(Schema::Avro)
                .and_then(|schema| {
                    self.schemas
                        .lock()
                        .map_err(Into::into)
                        .map(|mut guard| guard.insert(topic.to_owned(), schema.clone()))
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
                self.validation_duration.record(
                    validation_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[KeyValue::new("topic", topic.to_owned())],
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
    }
}

impl TryFrom<Url> for Registry {
    type Error = Error;

    fn try_from(storage: Url) -> Result<Self, Self::Error> {
        Self::try_from(&storage)
    }
}

impl TryFrom<&Url> for Registry {
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

            _unsupported => Err(Error::UnsupportedSchemaRegistryUrl(storage.to_owned())),
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
