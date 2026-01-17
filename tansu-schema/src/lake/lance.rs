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
    io::Cursor,
    marker::PhantomData,
    sync::{Arc, LazyLock, Mutex},
    time::SystemTime,
};

use crate::{
    AsArrow as _, Error, METER, Registry, Result,
    lake::{LakeHouse, LakeHouseType},
};
use async_trait::async_trait;
use lance::dataset::{Dataset, WriteMode, WriteParams};
// Use lance's re-exported arrow types to avoid version mismatch
use lance::deps::arrow_array::{RecordBatch as LanceRecordBatch, RecordBatchIterator};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use tansu_sans_io::{describe_configs_response::DescribeConfigsResult, record::inflated::Batch};
use tracing::{debug, instrument};
use url::Url;

use super::House;

#[derive(Clone, Debug, Default)]
pub struct Builder<L = PhantomData<Url>, R = PhantomData<Registry>> {
    location: L,
    schema_registry: R,
    database: Option<String>,
}

impl<L, R> Builder<L, R> {
    pub fn location(self, location: Url) -> Builder<Url, R> {
        Builder {
            location,
            schema_registry: self.schema_registry,
            database: self.database,
        }
    }

    pub fn schema_registry(self, schema_registry: Registry) -> Builder<L, Registry> {
        Builder {
            location: self.location,
            schema_registry,
            database: self.database,
        }
    }

    pub fn database(self, database: Option<String>) -> Self {
        Self { database, ..self }
    }
}

impl Builder<Url, Registry> {
    pub fn build(self) -> Result<House> {
        Lance::try_from(self).map(House::Lance)
    }
}

static RECORD_BATCH_ROWS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("lance_record_batch_rows")
        .with_description("The row count of records written in a batch")
        .build()
});

static WRITE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("lance_write_duration")
        .with_unit("ms")
        .with_description("The Lance write latencies in milliseconds")
        .build()
});

static COMPACT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("lance_compact_duration")
        .with_unit("ms")
        .with_description("Lance compact latency in milliseconds")
        .build()
});

#[derive(Clone, Debug)]
pub struct Lance {
    location: Url,
    schema_registry: Registry,
    datasets: Arc<Mutex<HashMap<String, Dataset>>>,
    database: String,
}

/// Convert arrow v57 RecordBatch to lance-compatible arrow v56 RecordBatch via IPC serialization.
/// This bridges the gap between tansu's arrow version (v57) and lance's arrow version (v56).
fn convert_record_batch(batch: &arrow::array::RecordBatch) -> Result<LanceRecordBatch> {
    use arrow::ipc::writer::StreamWriter;
    use arrow_ipc_v56::reader::StreamReader;

    // Serialize with tansu's arrow version (v57)
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, batch.schema().as_ref())?;
        writer.write(batch)?;
        writer.finish()?;
    }

    // Deserialize with lance's arrow version (v56) via arrow-ipc_v56
    let cursor = Cursor::new(buffer);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| Error::Message(format!("Arrow IPC error: {}", e)))?;

    reader
        .into_iter()
        .next()
        .ok_or_else(|| Error::Message("Failed to convert RecordBatch".into()))?
        .map_err(|e| Error::Message(format!("Arrow error: {}", e)))
}

impl Lance {
    fn dataset_uri(&self, name: &str) -> String {
        format!("{}/{}.{name}", self.location, self.database)
    }

    async fn open_or_create_dataset(
        &self,
        name: &str,
        batch: &LanceRecordBatch,
    ) -> Result<Dataset> {
        let uri = self.dataset_uri(name);
        debug!(?uri, ?name);

        // Try to get from cache first
        if let Some(dataset) = self.datasets.lock().map(|guard| guard.get(name).cloned())? {
            return Ok(dataset);
        }

        // Try to open existing dataset
        let dataset = match Dataset::open(&uri).await {
            Ok(dataset) => {
                debug!(opened = ?uri);
                dataset
            }
            Err(_) => {
                // Dataset doesn't exist, create it
                debug!(creating = ?uri);
                let schema = batch.schema();
                let batches = vec![batch.clone()];
                let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

                let write_params = WriteParams {
                    mode: WriteMode::Create,
                    ..Default::default()
                };

                Dataset::write(reader, &uri, Some(write_params)).await?
            }
        };

        // Cache the dataset
        _ = self
            .datasets
            .lock()
            .map(|mut guard| guard.insert(name.to_owned(), dataset.clone()))?;

        Ok(dataset)
    }

    async fn append(&self, name: &str, batch: LanceRecordBatch) -> Result<()> {
        let uri = self.dataset_uri(name);
        let start = SystemTime::now();
        let num_rows = batch.num_rows() as u64;

        let schema = batch.schema();
        let batches = vec![batch];
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

        let write_params = WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        };

        let dataset = Dataset::write(reader, &uri, Some(write_params)).await?;

        let properties = [KeyValue::new("dataset_uri", uri.clone())];

        RECORD_BATCH_ROWS.add(num_rows, &properties);

        WRITE_DURATION.record(
            start
                .elapsed()
                .map_or(0, |duration| duration.as_millis() as u64),
            &properties,
        );

        // Update cached dataset
        _ = self
            .datasets
            .lock()
            .map(|mut guard| guard.insert(name.to_owned(), dataset))?;

        Ok(())
    }

    async fn compact(&self, name: &str) -> Result<()> {
        let start = SystemTime::now();
        let uri = self.dataset_uri(name);

        if let Some(dataset) = self.datasets.lock().map(|guard| guard.get(name).cloned())? {
            // Lance datasets are self-compacting during writes
            // Full optimization can be done with external tools
            debug!(compact = %name, version = dataset.version().version);

            let properties = [KeyValue::new("dataset_uri", uri)];

            COMPACT_DURATION.record(
                start
                    .elapsed()
                    .map_or(0, |duration| duration.as_millis() as u64),
                &properties,
            );
        }

        Ok(())
    }
}

#[async_trait]
impl LakeHouse for Lance {
    #[instrument(skip(self, inflated, config), ret)]
    async fn store(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        inflated: &Batch,
        config: DescribeConfigsResult,
    ) -> Result<()> {
        let _ = config;

        // Get RecordBatch from schema registry (tansu's arrow v57)
        let record_batch = self
            .schema_registry
            .as_arrow(topic, partition, inflated, LakeHouseType::Lance)
            .await?;

        let num_rows = record_batch.num_rows();
        debug!(%topic, partition, offset, rows = num_rows, columns = record_batch.num_columns());

        // Convert to lance's arrow version (v56)
        let lance_batch = convert_record_batch(&record_batch)?;

        // Try to open or create the dataset
        let _ = self.open_or_create_dataset(topic, &lance_batch).await?;

        // Append the data
        self.append(topic, lance_batch).await
    }

    #[instrument(skip(self), ret)]
    async fn maintain(&self) -> Result<()> {
        debug!(?self);

        let names = self
            .datasets
            .lock()
            .map(|guard| guard.keys().map(|name| name.to_owned()).collect::<Vec<_>>())
            .inspect(|names| debug!(?names))
            .inspect_err(|err| debug!(?err))?;

        for name in names {
            debug!(name);
            self.compact(&name).await?;
        }

        Ok(())
    }

    #[instrument(skip(self), ret)]
    async fn lake_type(&self) -> Result<LakeHouseType> {
        Ok(LakeHouseType::Lance)
    }
}

impl TryFrom<Builder<Url, Registry>> for Lance {
    type Error = Error;

    fn try_from(value: Builder<Url, Registry>) -> Result<Self, Self::Error> {
        Ok(Self {
            location: value.location,
            schema_registry: value.schema_registry,
            database: value.database.unwrap_or(String::from("tansu")),
            datasets: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::StreamExt;
    use object_store::{ObjectStoreExt as _, PutPayload, memory::InMemory, path::Path};
    use serde_json::json;
    use std::{fs::File, marker::PhantomData, str::FromStr as _, sync::Arc, thread};
    use tansu_sans_io::{
        ConfigResource, ErrorCode,
        record::{Record, inflated::Batch},
    };
    use tempfile::tempdir;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use crate::proto::{MessageKind, Schema};

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

    #[tokio::test]
    async fn taxi_plain() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "taxi";

        let schema = Schema::try_from(Bytes::from_static(include_bytes!(
            "../../../../tansu/etc/schema/taxi.proto"
        )))?;

        let value = schema.encode_from_value(
            MessageKind::Value,
            &json!({
              "vendor_id": 1,
              "trip_id": 1000371,
              "trip_distance": 1.8,
              "fare_amount": 15.32,
              "store_and_fwd": "N"
            }),
        )?;

        let partition = 32123;

        let record_batch = Batch::builder()
            .record(Record::builder().value(value.into()))
            .base_timestamp(119_731_017_000)
            .build()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let location = format!("file://{}", temp_dir.path().to_str().unwrap());
        let database = "pqr";

        let schema_registry = Registry::from_str("file://../../../etc/schema")?;

        schema_registry.validate(topic, &record_batch).await?;

        let lake_house =
            Url::parse(location.as_ref())
                .map_err(Into::into)
                .and_then(|location| {
                    Builder::<PhantomData<Url>, PhantomData<Registry>>::default()
                        .location(location)
                        .database(Some(database.into()))
                        .schema_registry(schema_registry)
                        .build()
                })?;

        let config = DescribeConfigsResult::default()
            .error_code(ErrorCode::None.into())
            .error_message(None)
            .resource_type(ConfigResource::Topic.into())
            .resource_name(topic.into())
            .configs(Some(vec![]));

        let offset = 543212345;

        lake_house
            .store(topic, partition, offset, &record_batch, config)
            .await
            .inspect(|result| debug!(?result))
            .inspect_err(|err| debug!(?err))?;

        // Read back and verify
        let dataset = Dataset::open(&format!("{location}/{database}.{topic}")).await?;
        let scanner = dataset.scan();
        let mut batch_stream = scanner.try_into_stream().await?;

        let mut batches = vec![];
        while let Some(batch_result) = batch_stream.next().await {
            batches.push(batch_result?);
        }

        assert!(!batches.is_empty());
        assert!(batches[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn message_descriptor_singular_to_field() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "abc";

        let proto = Bytes::from_static(
            br#"
            syntax = 'proto3';

            message Key {
                int32 id = 1;
            }

            message Value {
                double a = 1;
                float b = 2;
                int32 c = 3;
                int64 d = 4;
                uint32 e = 5;
                uint64 f = 6;
                sint32 g = 7;
                sint64 h = 8;
                fixed32 i = 9;
                fixed64 j = 10;
                sfixed32 k = 11;
                sfixed64 l = 12;
                bool m = 13;
                string n = 14;
            }
            "#,
        );

        let object_store = InMemory::new();

        let location = Path::from(format!("{topic}.proto"));
        _ = object_store
            .put(&location, PutPayload::from(proto.clone()))
            .await?;
        let schema_registry = Registry::new(object_store);

        let kv = [(
            json!({"id": 32123}),
            json!({"a": 567.65,
                    "b": 45.654,
                    "c": -6,
                    "d": -66,
                    "e": 23432,
                    "f": 34543,
                    "g": 45654,
                    "h": 67876,
                    "i": 78987,
                    "j": 89098,
                    "k": 90109,
                    "l": 12321,
                    "m": true,
                    "n": "Hello World!"}),
        )];

        let partition = 32123;

        let schema = Schema::try_from(proto)?;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(119_731_017_000);

            for (delta, (key, value)) in kv.iter().enumerate() {
                batch = batch.record(
                    Record::builder()
                        .key(schema.encode_from_value(MessageKind::Key, key)?.into())
                        .value(schema.encode_from_value(MessageKind::Value, value)?.into())
                        .timestamp_delta(delta as i64)
                        .offset_delta(delta as i32),
                );
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let location = format!("file://{}", temp_dir.path().to_str().unwrap());
        let database = "pqr";

        let lake_house =
            Url::parse(location.as_ref())
                .map_err(Into::into)
                .and_then(|location| {
                    Builder::<PhantomData<Url>, PhantomData<Registry>>::default()
                        .location(location)
                        .database(Some(database.into()))
                        .schema_registry(schema_registry)
                        .build()
                })?;

        let config = DescribeConfigsResult::default()
            .error_code(ErrorCode::None.into())
            .error_message(None)
            .resource_type(ConfigResource::Topic.into())
            .resource_name(topic.into())
            .configs(Some(vec![]));

        let offset = 543212345;

        lake_house
            .store(topic, partition, offset, &record_batch, config)
            .await
            .inspect(|result| debug!(?result))
            .inspect_err(|err| debug!(?err))?;

        // Read back and verify
        let dataset = Dataset::open(&format!("{location}/{database}.{topic}")).await?;
        let scanner = dataset.scan();
        let mut batch_stream = scanner.try_into_stream().await?;

        let mut batches = vec![];
        while let Some(batch_result) = batch_stream.next().await {
            batches.push(batch_result?);
        }

        assert!(!batches.is_empty());
        assert!(batches[0].num_rows() > 0);

        Ok(())
    }
}
