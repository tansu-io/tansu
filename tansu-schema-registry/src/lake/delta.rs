// Copyright ⓒ 2025 Peter Morgan <peter.james.morgan@gmail.com>
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
    sync::{Arc, Mutex},
};

use crate::{Error, Result};
use arrow::{array::RecordBatch, datatypes::Schema as ArrowSchema};
use async_trait::async_trait;
use deltalake::{
    DeltaOps, DeltaTable, aws,
    kernel::{ColumnMetadataKey, DataType, StructField},
    operations::optimize::OptimizeType,
    protocol::SaveMode,
    writer::{DeltaWriter, RecordBatchWriter},
};
use parquet::file::properties::WriterProperties;
use serde_json::json;
use tansu_kafka_sans_io::describe_configs_response::DescribeConfigsResult;
use tracing::debug;
use url::Url;

use super::{House, LakeHouse};

#[derive(Clone, Debug, Default)]
pub struct Builder<L> {
    location: L,
    database: Option<String>,
}

impl<L> Builder<L> {
    pub fn location(self, location: Url) -> Builder<Url> {
        Builder {
            location,
            database: self.database,
        }
    }

    pub fn database(self, database: Option<String>) -> Self {
        Self { database, ..self }
    }
}

impl Builder<Url> {
    pub fn build(self) -> Result<House> {
        Delta::try_from(self).map(House::Delta)
    }
}

#[derive(Clone, Debug)]
pub struct Delta {
    location: Url,
    tables: Arc<Mutex<HashMap<String, DeltaTable>>>,
    database: String,
}

impl Delta {
    fn table_uri(&self, name: &str) -> String {
        format!("{}/{}.{name}", self.location, self.database)
    }

    async fn create_initialized_table(
        &self,
        name: &str,
        schema: &ArrowSchema,
        config: DescribeConfigsResult,
    ) -> Result<DeltaTable> {
        debug!(?name, ?schema);

        if let Some(table) = self.tables.lock().map(|guard| guard.get(name).cloned())? {
            return Ok(table);
        }

        let partitions = config
            .configs
            .and_then(|configs| {
                configs.iter().find_map(|config| {
                    if config.name == "tansu.lake.partition" {
                        config.value.clone()
                    } else {
                        None
                    }
                })
            })
            .inspect(|partitions| debug!(?partitions));

        let columns = schema
            .fields()
            .iter()
            .map(|field| StructField::try_from(field.as_ref()).map_err(Into::into))
            .collect::<Result<Vec<_>>>()
            .inspect(|columns| debug!(?columns))
            .inspect_err(|err| debug!(?err))?;

        let table = DeltaOps::try_from_uri(&self.table_uri(name))
            .await
            .inspect_err(|err| debug!(?err))?
            .create()
            .with_save_mode(SaveMode::Ignore)
            .with_column(
                "date",
                DataType::DATE,
                true,
                Some(HashMap::from_iter(
                    [(
                        ColumnMetadataKey::GenerationExpression.as_ref().into(),
                        json!("cast(meta.timestamp as date)"),
                    )]
                    .into_iter(),
                )),
            )
            .with_columns(columns)
            .with_partition_columns(partitions)
            .await
            .inspect(|table| debug!(?table))
            .inspect_err(|err| debug!(?err))?;

        _ = self
            .tables
            .lock()
            .map(|mut guard| guard.insert(name.to_owned(), table.clone()))?;

        Ok(table)
    }

    async fn write_with_datafusion(
        &self,
        name: &str,
        batches: impl Iterator<Item = RecordBatch>,
    ) -> Result<DeltaTable> {
        DeltaOps::try_from_uri(&self.table_uri(name))
            .await
            .inspect_err(|err| debug!(?err))?
            .write(batches)
            .await
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
    }

    #[allow(dead_code)]
    async fn write(&self, table: &mut DeltaTable, batch: RecordBatch) -> Result<i64> {
        let writer_properties = WriterProperties::default();

        let mut writer = RecordBatchWriter::for_table(table)
            .map(|batch_writer| batch_writer.with_writer_properties(writer_properties))
            .inspect_err(|err| debug!(?err))?;

        writer.write(batch).await.inspect_err(|err| debug!(?err))?;

        writer
            .flush_and_commit(table)
            .await
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
    }

    async fn compact(&self, name: &str) -> Result<()> {
        DeltaOps::try_from_uri(&self.table_uri(name))
            .await
            .inspect_err(|err| debug!(?err))?
            .optimize()
            .with_type(OptimizeType::Compact)
            .await
            .inspect(|(table, metrics)| debug!(?table, ?metrics))
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
            .and(Ok(()))
    }
}

#[async_trait]
impl LakeHouse for Delta {
    async fn store(
        &self,
        topic: &str,
        _partition: i32,
        _offset: i64,
        record_batch: RecordBatch,
        config: DescribeConfigsResult,
    ) -> Result<()> {
        _ = self
            .create_initialized_table(topic, record_batch.schema().as_ref(), config)
            .await?;

        _ = self
            .write_with_datafusion(topic, [record_batch].into_iter())
            .await
            .inspect(|table| debug!(?table))
            .inspect_err(|err| debug!(?err))?;

        debug!(scheme = ?self.location.scheme());

        if self.location.scheme() == "memory" {
            Ok(())
        } else {
            self.compact(topic).await.inspect_err(|err| debug!(?err))
        }
    }
}

impl TryFrom<Builder<Url>> for Delta {
    type Error = Error;

    fn try_from(value: Builder<Url>) -> Result<Self, Self::Error> {
        aws::register_handlers(None);

        Ok(Self {
            location: value.location,
            database: value.database.unwrap_or(String::from("tansu")),
            tables: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, marker::PhantomData, sync::Arc, thread};

    use arrow::util::pretty::pretty_format_batches;
    use bytes::Bytes;
    use datafusion::execution::context::SessionContext;
    use deltalake::DeltaTableBuilder;
    use tansu_kafka_sans_io::{
        ConfigResource, ErrorCode,
        record::{Record, inflated::Batch},
    };
    use tempfile::tempdir;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use crate::{AsArrow, Error};

    use super::*;

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

    mod proto {
        use super::*;
        use crate::proto::{MessageKind, Schema};

        #[tokio::test]
        async fn proto() -> Result<()> {
            let _guard = init_tracing()?;

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
                    bytes o = 15;
                }
                "#,
            );

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
                        "n": "Hello World!",
                        "o": "YWJjMTIzIT8kKiYoKSctPUB+"}),
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

                batch
                    .build()
                    .map_err(Into::into)
                    .and_then(|batch| schema.as_arrow(partition, &batch))
            }?;

            let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
            let location = format!("file://{}", temp_dir.path().to_str().unwrap());
            let database = "pqr";

            let lake_house =
                Url::parse(location.as_ref())
                    .map_err(Into::into)
                    .and_then(|location| {
                        Builder::<PhantomData<Url>>::default()
                            .location(location)
                            .database(Some(database.into()))
                            .build()
                    })?;

            let topic = "abc";

            let config = DescribeConfigsResult {
                error_code: ErrorCode::None.into(),
                error_message: None,
                resource_type: ConfigResource::Topic.into(),
                resource_name: topic.into(),
                configs: Some(vec![]),
            };

            let offset = 543212345;

            lake_house
                .store(topic, partition, offset, record_batch, config)
                .await
                .inspect(|result| debug!(?result))
                .inspect_err(|err| debug!(?err))?;

            let table = {
                let mut table =
                    DeltaTableBuilder::from_uri(format!("{location}/{database}.{topic}"))
                        .build()?;
                table.load().await?;
                table
            };

            let ctx = SessionContext::new();

            ctx.register_table("t", Arc::new(table))?;

            let df = ctx.sql("select * from t").await?;
            let results = df.collect().await?;

            let pretty_results = pretty_format_batches(&results)?.to_string();

            let expected = vec![
                "+------------+-----------+---------------------+-------+--------+--------+----+-----+-------+-------+-------+-------+-------+-------+-------+-------+------+--------------+--------------------------------------+",
                "| date       | partition | timestamp           | id    | a      | b      | c  | d   | e     | f     | g     | h     | i     | j     | k     | l     | m    | n            | o                                    |",
                "+------------+-----------+---------------------+-------+--------+--------+----+-----+-------+-------+-------+-------+-------+-------+-------+-------+------+--------------+--------------------------------------+",
                "| 1973-10-17 | 32123     | 1973-10-17T18:36:57 | 32123 | 567.65 | 45.654 | -6 | -66 | 23432 | 34543 | 45654 | 67876 | 78987 | 89098 | 90109 | 12321 | true | Hello World! | 616263313233213f242a262829272d3d407e |",
                "+------------+-----------+---------------------+-------+--------+--------+----+-----+-------+-------+-------+-------+-------+-------+-------+-------+------+--------------+--------------------------------------+",
            ];

            assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

            Ok(())
        }
    }

    mod avro {
        use super::*;
        use crate::avro::{Schema, r, schema_write};

        #[tokio::test]
        async fn record_of_primitive_data_types() -> Result<()> {
            let _guard = init_tracing()?;

            let schema = Schema::from(json!({
                "type": "record",
                "name": "Message",
                "fields": [
                    {"name": "value", "type": "record", "fields": [
                    {"name": "b", "type": "boolean"},
                    {"name": "c", "type": "int"},
                    {"name": "d", "type": "long"},
                    {"name": "e", "type": "float"},
                    {"name": "f", "type": "double"},
                    {"name": "g", "type": "bytes"},
                    {"name": "h", "type": "string"}
                    ]}
                ]
            }));

            let partition = 32123;

            let record_batch = {
                let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

                let values = [r(
                    schema.value.as_ref().unwrap(),
                    [
                        ("b", false.into()),
                        ("c", i32::MAX.into()),
                        ("d", i64::MAX.into()),
                        ("e", f32::MAX.into()),
                        ("f", f64::MAX.into()),
                        ("g", Vec::from(&b"abcdef"[..]).into()),
                        ("h", "pqr".into()),
                    ],
                )];

                for value in values {
                    batch =
                        batch.record(Record::builder().value(
                            schema_write(schema.value.as_ref().unwrap(), value.into())?.into(),
                        ))
                }

                batch
                    .build()
                    .map_err(Into::into)
                    .and_then(|batch| schema.as_arrow(partition, &batch))
            }?;

            let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
            let location = format!("file://{}", temp_dir.path().to_str().unwrap());
            let database = "pqr";

            let lake_house =
                Url::parse(location.as_ref())
                    .map_err(Into::into)
                    .and_then(|location| {
                        Builder::<PhantomData<Url>>::default()
                            .location(location)
                            .database(Some(database.into()))
                            .build()
                    })?;

            let topic = "abc";

            let config = DescribeConfigsResult {
                error_code: ErrorCode::None.into(),
                error_message: None,
                resource_type: ConfigResource::Topic.into(),
                resource_name: topic.into(),
                configs: Some(vec![]),
            };

            let offset = 543212345;

            lake_house
                .store(topic, partition, offset, record_batch, config)
                .await
                .inspect(|result| debug!(?result))
                .inspect_err(|err| debug!(?err))?;

            let table = {
                let mut table =
                    DeltaTableBuilder::from_uri(format!("{location}/{database}.{topic}"))
                        .build()?;
                table.load().await?;
                table
            };

            let ctx = SessionContext::new();

            ctx.register_table("t", Arc::new(table))?;

            let df = ctx.sql("select * from t").await?;
            let results = df.collect().await?;

            let pretty_results = pretty_format_batches(&results)?.to_string();

            let expected = vec![
                "+------------+------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------+",
                "| date       | value                                                                                                                  | meta                                               |",
                "+------------+------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------+",
                "| 2009-02-13 | {b: false, c: 2147483647, d: 9223372036854775807, e: 3.4028235e38, f: 1.7976931348623157e308, g: 616263646566, h: pqr} | {partition: 32123, timestamp: 2009-02-13T23:31:30} |",
                "+------------+------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------+",
            ];

            assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

            Ok(())
        }
    }
}
