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
    collections::{HashMap, VecDeque},
    num::NonZeroU32,
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, SystemTime},
};

use crate::{Error, METER, Result, lake::LakeHouseType, sql::typeof_sql_expr};
use arrow::{
    array::RecordBatch,
    datatypes::{Field, Schema as ArrowSchema},
};
use async_trait::async_trait;
use deltalake::{
    DeltaOps, DeltaTable, DeltaTableBuilder, aws,
    kernel::{ColumnMetadataKey, StructField},
    operations::optimize::OptimizeType,
    protocol::SaveMode,
    writer::{DeltaWriter, RecordBatchWriter},
};
use governor::{
    DefaultDirectRateLimiter, Jitter, Quota, RateLimiter, clock::QuantaInstant,
    middleware::NoOpMiddleware,
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use parquet::file::properties::WriterProperties;
use tansu_sans_io::describe_configs_response::DescribeConfigsResult;
use tracing::{debug, warn};
use url::Url;

use super::{House, LakeHouse};

#[derive(Clone, Debug, Default)]
pub struct Builder<L> {
    location: L,
    database: Option<String>,
    records_per_second: Option<u32>,
}

impl<L> Builder<L> {
    pub fn location(self, location: Url) -> Builder<Url> {
        Builder {
            location,
            database: self.database,
            records_per_second: self.records_per_second,
        }
    }

    pub fn database(self, database: Option<String>) -> Self {
        Self { database, ..self }
    }

    pub fn records_per_second(self, records_per_second: Option<u32>) -> Self {
        Self {
            records_per_second,
            ..self
        }
    }
}

impl Builder<Url> {
    pub fn build(self) -> Result<House> {
        Delta::try_from(self).map(House::Delta)
    }
}

static RECORD_BATCH_ROWS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_record_batch_rows")
        .with_description("The row count of records written in a batch")
        .build()
});

static WRITE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("deltalake_write_duration")
        .with_unit("ms")
        .with_description("The Delta Lake write latencies in milliseconds")
        .build()
});

static FLUSH_AND_COMMIT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("deltalake_flush_and_commit_duration")
        .with_unit("ms")
        .with_description("Delta Lake record batch flush and commit latency in milliseconds")
        .build()
});

static WRITE_WITH_DATAFUSION_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("deltalake_write_with_datafusion_duration")
        .with_unit("ms")
        .with_description("The Delta Lake write with datafusion latencies in milliseconds")
        .build()
});

static RATE_LIMIT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("deltalake_rate_limit_duration")
        .with_unit("ms")
        .with_description("Delta Lake Rate limit latencies in milliseconds")
        .build()
});

static OPTIMIZE_NUM_FILES_ADDED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_optimize_num_files_added")
        .with_description("Number of optimized files added")
        .build()
});

static OPTIMIZE_NUM_FILES_REMOVED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_optimize_num_files_removed")
        .with_description("Number of unoptimized files removed")
        .build()
});

static OPTIMIZE_PARTITIONS_OPTIMIZED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_optimize_partitions_optimized")
        .with_description("Number of partitions that had at least one file optimized")
        .build()
});

static OPTIMIZE_NUM_BATCHES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_optimize_num_batches")
        .with_description("The number of batches written")
        .build()
});

static OPTIMIZE_TOTAL_CONSIDERED_FILES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_optimize_total_considered_files")
        .with_description("How many files were considered during optimization. Not every file considered is optimized")
        .build()
});

static OPTIMIZE_TOTAL_FILES_SKIPPED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("deltalake_optimize_total_files_skipped")
        .with_description("How many files were considered for optimization but were skipped")
        .build()
});

#[derive(Clone, Debug)]
pub struct Delta {
    location: Url,
    tables: Arc<Mutex<HashMap<String, Table>>>,
    database: String,
    rate_limiter: Option<Arc<DefaultDirectRateLimiter<NoOpMiddleware<QuantaInstant>>>>,
}

#[derive(Clone, Debug)]
struct Table {
    config: Config,
    delta_table: DeltaTable,
}

#[derive(Clone, Debug)]
struct Config(Vec<(String, String)>);

impl From<DescribeConfigsResult> for Config {
    fn from(config: DescribeConfigsResult) -> Self {
        Self(config.configs.map_or(vec![], |configs| {
            configs
                .into_iter()
                .filter_map(|config| config.value.map(|value| (config.name, value)))
                .collect::<Vec<(String, String)>>()
        }))
    }
}

impl Config {
    fn as_columns(&self, name: &str) -> Vec<String> {
        self.0
            .iter()
            .flat_map(|(key, value)| {
                if key == name {
                    value
                        .split(",")
                        .map(str::trim)
                        .map(String::from)
                        .collect::<Vec<_>>()
                        .into_iter()
                } else {
                    vec![].into_iter()
                }
            })
            .collect::<Vec<_>>()
    }

    fn partition(&self) -> Vec<String> {
        self.as_columns("tansu.lake.partition")
    }

    fn z_order(&self) -> Vec<String> {
        self.as_columns("tansu.lake.z_order")
    }

    fn quote(s: &str) -> String {
        format!("\"{s}\"")
    }

    fn generated_fields(&self) -> Vec<Arc<Field>> {
        self.0
            .iter()
            .filter_map(|(name, value)| {
                name.strip_prefix("tansu.lake.generate.")
                    .and_then(|suffix| {
                        typeof_sql_expr(value)
                            .map(|data_type| {
                                Arc::new(Field::new(suffix, data_type, true).with_metadata(
                                    HashMap::from_iter([(
                                        ColumnMetadataKey::GenerationExpression.as_ref().into(),
                                        Self::quote(value),
                                    )]),
                                ))
                            })
                            .inspect_err(|err| debug!(?err, %value))
                            .ok()
                    })
            })
            .inspect(|generated| debug!(?generated))
            .collect::<Vec<_>>()
    }

    fn generated(&self) -> Result<Vec<StructField>> {
        self.generated_fields()
            .iter()
            .map(|field| StructField::try_from(field.as_ref()).map_err(Into::into))
            .collect::<Result<Vec<_>>>()
    }
}

impl Delta {
    fn table_uri(&self, name: &str) -> String {
        format!("{}/{}.{name}", self.location, self.database)
    }

    async fn create_initialized_table(
        &self,
        name: &str,
        schema: &ArrowSchema,
        config: Config,
    ) -> Result<DeltaTable> {
        debug!(?name, ?schema);

        let columns = schema
            .fields()
            .iter()
            .inspect(|field| debug!(?field))
            .map(|field| StructField::try_from(field.as_ref()).map_err(Into::into))
            .inspect(|struct_field| debug!(?struct_field))
            .collect::<Result<Vec<_>>>()
            .inspect(|columns| debug!(?columns))
            .inspect_err(|err| debug!(?err))?;

        let table = match DeltaOps::try_from_uri(&self.table_uri(name))
            .await
            .inspect_err(|err| debug!(?err))?
            .create()
            .with_save_mode(SaveMode::Ignore)
            .with_columns(columns.into_iter().chain(config.generated()?.into_iter()))
            .with_partition_columns(config.partition())
            .await
            .inspect(|table| debug!(?table))
            .inspect_err(|err| debug!(?err))
        {
            Err(deltalake::DeltaTableError::VersionAlreadyExists(_)) => {
                if let Some(table) = self.tables.lock().map(|guard| guard.get(name).cloned())? {
                    return Ok(table.delta_table);
                }

                let mut table = DeltaTableBuilder::from_uri(self.table_uri(name)).build()?;
                table
                    .load()
                    .await
                    .inspect(|table| debug!(?table))
                    .inspect_err(|err| debug!(?err))
                    .and(Ok(table))
            }

            otherwise => otherwise,
        }?;

        self.tables.lock().map(|mut guard| {
            _ = guard
                .entry(name.to_owned())
                .and_modify(|existing| {
                    if table.version() > existing.delta_table.version() {
                        existing.delta_table = table.to_owned()
                    } else {
                        warn!(
                            name,
                            existing = existing.delta_table.version(),
                            current = table.version()
                        );
                    }
                })
                .or_insert(Table {
                    delta_table: table.clone(),
                    config,
                });
        })?;

        Ok(table)
    }

    async fn write_with_datafusion(
        &self,
        name: &str,
        batches: impl Iterator<Item = RecordBatch>,
    ) -> Result<()> {
        let start = SystemTime::now();

        let table = DeltaOps::try_from_uri(&self.table_uri(name))
            .await
            .inspect_err(|err| debug!(?err))?
            .write(batches)
            .await
            .inspect_err(|err| debug!(?err))
            .inspect(|table| {
                WRITE_WITH_DATAFUSION_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[KeyValue::new("table_uri", table.table_uri())],
                )
            })?;

        self.tables.lock().map(|mut guard| {
            _ = guard.entry(name.to_string()).and_modify(|existing| {
                if table.version() > existing.delta_table.version() {
                    existing.delta_table = table.to_owned();
                } else {
                    warn!(
                        name,
                        existing = existing.delta_table.version(),
                        current = table.version()
                    );
                }
            });
        })?;

        Ok(())
    }

    async fn rate_limit(&self, table_uri: String, n_ready: NonZeroU32) -> Result<()> {
        if let Some(ref rate_limiter) = self.rate_limiter {
            let start = SystemTime::now();

            let attributes = [KeyValue::new("table_uri", table_uri)];

            rate_limiter
                .until_n_ready_with_jitter(n_ready, Jitter::up_to(Duration::from_millis(50)))
                .await
                .inspect(|_| {
                    RATE_LIMIT_DURATION.record(
                        start
                            .elapsed()
                            .map_or(0, |duration| duration.as_millis() as u64),
                        &attributes,
                    )
                })
                .map_err(Into::into)
        } else {
            Ok(())
        }
    }

    async fn write(&self, name: &str, mut table: DeltaTable, batch: RecordBatch) -> Result<()> {
        let properties = [KeyValue::new("table_uri", table.table_uri())];

        if let Some(num_rows) = NonZeroU32::new(batch.num_rows() as u32) {
            self.rate_limit(table.table_uri(), num_rows).await?;
        }

        let num_rows = batch.num_rows() as u64;

        let mut writer = RecordBatchWriter::for_table(&table)
            .map(|batch_writer| batch_writer.with_writer_properties(WriterProperties::default()))
            .inspect_err(|err| debug!(?err))?;

        {
            let start = SystemTime::now();

            writer
                .write(batch)
                .await
                .inspect_err(|err| debug!(?err))
                .inspect(|_| {
                    RECORD_BATCH_ROWS.add(num_rows, &properties);

                    WRITE_DURATION.record(
                        start
                            .elapsed()
                            .map_or(0, |duration| duration.as_millis() as u64),
                        &properties,
                    )
                })?
        }

        {
            let start = SystemTime::now();

            _ = writer
                .flush_and_commit(&mut table)
                .await
                .inspect_err(|err| debug!(?err))
                .inspect(|_| {
                    FLUSH_AND_COMMIT_DURATION.record(
                        start
                            .elapsed()
                            .map_or(0, |duration| duration.as_millis() as u64),
                        &[KeyValue::new("table", table.table_uri())],
                    )
                })?;
        }

        self.tables.lock().map(|mut guard| {
            _ = guard.entry(name.to_string()).and_modify(|existing| {
                if table.version() > existing.delta_table.version() {
                    existing.delta_table = table;
                } else {
                    warn!(
                        name,
                        existing = existing.delta_table.version(),
                        current = table.version()
                    );
                }
            });
        })?;

        Ok(())
    }

    async fn z_order(&self, name: &str) -> Result<()> {
        debug!(%name);

        let Some(table) = self.tables.lock().map(|guard| guard.get(name).cloned())? else {
            return Ok(());
        };

        self.optimize(name, OptimizeType::ZOrder(table.config.z_order()))
            .await
    }

    async fn compact(&self, name: &str) -> Result<()> {
        self.optimize(name, OptimizeType::Compact).await
    }

    async fn optimize(&self, name: &str, optimize_type: OptimizeType) -> Result<()> {
        debug!(%name, ?optimize_type);

        let optimize_type_label = KeyValue::new(
            "optimize_type",
            match optimize_type {
                OptimizeType::Compact => "compact",
                OptimizeType::ZOrder(..) => "z_order",
            },
        );

        let (table, metrics) = DeltaOps::try_from_uri(&self.table_uri(name))
            .await?
            .optimize()
            .with_type(optimize_type)
            .await?;

        let properties = [
            KeyValue::new("table_uri", table.table_uri()),
            optimize_type_label,
        ];

        OPTIMIZE_NUM_FILES_ADDED.add(metrics.num_files_added, &properties);
        OPTIMIZE_NUM_FILES_REMOVED.add(metrics.num_files_removed, &properties);
        OPTIMIZE_PARTITIONS_OPTIMIZED.add(metrics.partitions_optimized, &properties);
        OPTIMIZE_NUM_BATCHES.add(metrics.num_batches, &properties);
        OPTIMIZE_TOTAL_CONSIDERED_FILES.add(metrics.total_considered_files as u64, &properties);
        OPTIMIZE_TOTAL_FILES_SKIPPED.add(metrics.total_files_skipped as u64, &properties);

        Ok(())
    }

    async fn migrate_schema(&self, table: DeltaTable, schema: &ArrowSchema) -> Result<DeltaTable> {
        let mut expected = schema
            .fields()
            .iter()
            .inspect(|field| debug!(?field))
            .map(|field| StructField::try_from(field.as_ref()).map_err(Into::into))
            .inspect(|struct_field| debug!(?struct_field))
            .collect::<Result<VecDeque<_>>>()
            .inspect(|columns| debug!(?columns))
            .inspect_err(|err| debug!(?err))?;

        let mut actual = table
            .schema()
            .map(|schema| schema.fields().collect::<VecDeque<_>>())
            .unwrap_or_default();
        debug!(?actual);

        let additional = {
            let mut additional = vec![];

            while !expected.is_empty() {
                match (expected.pop_front(), actual.pop_front()) {
                    (Some(expected_field), Some(actual_field))
                        if expected_field.name == actual_field.name =>
                    {
                        debug!(unchanged = expected_field.name);

                        continue;
                    }

                    (Some(expected_field), None) => {
                        debug!(additional = expected_field.name);
                        additional.push(expected_field);
                    }

                    (Some(expected_field), Some(actual_field)) => {
                        debug!(expected = expected_field.name, actual = actual_field.name);
                        todo!("{expected_field:?}, {actual_field:?}")
                    }

                    (expected_field, actual_field) => {
                        todo!("{expected_field:?}, {actual_field:?}")
                    }
                }
            }

            additional
        };

        if additional.is_empty() {
            Ok(table)
        } else {
            DeltaOps::from(table)
                .add_columns()
                .with_fields(additional.into_iter())
                .await
                .map_err(Into::into)
        }
    }
}

#[async_trait]
impl LakeHouse for Delta {
    async fn store(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        record_batch: RecordBatch,
        config: DescribeConfigsResult,
    ) -> Result<()> {
        debug!(%topic, partition, offset, rows = record_batch.num_rows(), columns = record_batch.num_columns(), ?config);

        let config = Config::from(config);

        let table =
            if let Some(table) = self.tables.lock().map(|guard| guard.get(topic).cloned())? {
                table.delta_table
            } else {
                self.create_initialized_table(topic, record_batch.schema().as_ref(), config.clone())
                    .await?
            };

        let table = self
            .migrate_schema(table, record_batch.schema().as_ref())
            .await?;

        if config.generated_fields().is_empty() {
            _ = self.write(topic, table, record_batch).await?;
        } else {
            _ = self
                .write_with_datafusion(topic, [record_batch].into_iter())
                .await
                .inspect(|delta_table| debug!(?delta_table))
                .inspect_err(|err| debug!(?err))?;
        }

        Ok(())
    }

    async fn maintain(&self) -> Result<()> {
        debug!(?self);

        let names = self
            .tables
            .lock()
            .map(|guard| guard.keys().map(|name| name.to_owned()).collect::<Vec<_>>())
            .inspect(|names| debug!(?names))
            .inspect_err(|err| debug!(?err))?;

        for name in names {
            debug!(name);

            self.compact(&name).await?;
            self.z_order(&name).await?;
        }

        Ok(())
    }

    async fn lake_type(&self) -> Result<LakeHouseType> {
        Ok(LakeHouseType::Delta)
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
            rate_limiter: value
                .records_per_second
                .and_then(NonZeroU32::new)
                .map(Quota::per_second)
                .map(RateLimiter::direct)
                .map(Arc::new)
                .inspect(|rate_limiter| debug!(?rate_limiter)),
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
    use serde_json::json;
    use tansu_sans_io::{
        ConfigResource, ErrorCode,
        describe_configs_response::DescribeConfigsResourceResult,
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

    mod sql {
        use datafusion::{
            logical_expr::sqlparser::ast::Expr,
            sql::sqlparser::{
                ast::{CastKind, DataType as SqlDataType},
                dialect::GenericDialect,
                parser::Parser,
            },
        };

        use super::*;

        #[test]
        fn sql_parser_cast() -> Result<()> {
            let _guard = init_tracing()?;

            let dialect = GenericDialect {};

            let sql = "cast(meta.timestamp as date)";

            let expr = Parser::new(&dialect)
                .try_with_sql(sql)?
                .parse_expr()
                .inspect(|ast| debug!(?ast))?;

            assert!(matches!(
                expr,
                Expr::Cast {
                    kind: CastKind::Cast,
                    data_type: SqlDataType::Date,
                    format: None,
                    ..
                }
            ));

            Ok(())
        }
    }

    mod proto {
        use super::*;
        use crate::{
            Generator,
            proto::{MessageKind, Schema},
        };

        #[tokio::test]
        async fn message_descriptor_singular_to_field() -> Result<()> {
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

                batch
                    .build()
                    .map_err(Into::into)
                    .and_then(|batch| schema.as_arrow(partition, &batch, LakeHouseType::Delta))
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

            let config = DescribeConfigsResult::default()
                .error_code(ErrorCode::None.into())
                .error_message(None)
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic.into())
                .configs(Some(vec![
                    DescribeConfigsResourceResult::default()
                        .name(String::from("tansu.lake.generate.date"))
                        .value(Some(String::from("cast(meta.timestamp as date)")))
                        .read_only(true)
                        .is_default(None)
                        .config_source(None)
                        .is_sensitive(false)
                        .synonyms(None)
                        .config_type(None)
                        .documentation(None),
                ]));

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

            _ = ctx.register_table("t", Arc::new(table))?;

            let df = ctx.sql("select * from t").await?;
            let results = df.collect().await?;

            let pretty_results = pretty_format_batches(&results)?.to_string();

            let expected = vec![
                "+------------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+------------+",
                "| meta                                                                               | key         | value                                                                                                                                           | date       |",
                "+------------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+------------+",
                "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {id: 32123} | {a: 567.65, b: 45.654, c: -6, d: -66, e: 23432, f: 34543, g: 45654, h: 67876, i: 78987, j: 89098, k: 90109, l: 12321, m: true, n: Hello World!} | 1973-10-17 |",
                "+------------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+------------+",
            ];

            assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

            Ok(())
        }

        #[tokio::test]
        async fn taxi_plain() -> Result<()> {
            let _guard = init_tracing()?;

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
                .build()
                .map_err(Into::into)
                .and_then(|batch| schema.as_arrow(partition, &batch, LakeHouseType::Delta))?;

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

            let topic = "taxi";

            let config = DescribeConfigsResult::default()
                .error_code(ErrorCode::None.into())
                .error_message(None)
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic.into())
                .configs(Some(vec![]));

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

            _ = ctx.register_table("t", Arc::new(table))?;

            let df = ctx.sql("select * from t").await?;
            let results = df.collect().await?;

            let pretty_results = pretty_format_batches(&results)?.to_string();

            let expected = vec![
                "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+",
                "| meta                                                                               | value                                                                                      |",
                "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+",
                "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {vendor_id: 1, trip_id: 1000371, trip_distance: 1.8, fare_amount: 15.32, store_and_fwd: 0} |",
                "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+",
            ];

            assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

            Ok(())
        }

        #[tokio::test]
        async fn taxi_normalized() -> Result<()> {
            let _guard = init_tracing()?;

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
                .build()
                .map_err(Into::into)
                .and_then(|batch| schema.as_arrow(partition, &batch, LakeHouseType::Delta))?;

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

            let topic = "taxi";

            let config = DescribeConfigsResult::default()
                .error_code(ErrorCode::None.into())
                .error_message(None)
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic.into())
                .configs(Some(vec![
                    DescribeConfigsResourceResult::default()
                        .name(String::from("tansu.lake.normalize"))
                        .value(Some(String::from("true")))
                        .read_only(true)
                        .is_default(None)
                        .config_source(None)
                        .is_sensitive(false)
                        .synonyms(None)
                        .config_type(None)
                        .documentation(None),
                ]));

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

            _ = ctx.register_table("t", Arc::new(table))?;

            let df = ctx.sql("select * from t").await?;
            let results = df.collect().await?;

            let pretty_results = pretty_format_batches(&results)?.to_string();

            let expected = vec![
                "+----------------+---------------------+-----------+------------+----------+-----------------+---------------+---------------------+-------------------+---------------------+",
                "| meta.partition | meta.timestamp      | meta.year | meta.month | meta.day | value.vendor_id | value.trip_id | value.trip_distance | value.fare_amount | value.store_and_fwd |",
                "+----------------+---------------------+-----------+------------+----------+-----------------+---------------+---------------------+-------------------+---------------------+",
                "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | 1               | 1000371       | 1.8                 | 15.32             | 0                   |",
                "+----------------+---------------------+-----------+------------+----------+-----------------+---------------+---------------------+-------------------+---------------------+",
            ];

            assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

            Ok(())
        }

        #[tokio::test]
        async fn taxi_normalized_with_separator() -> Result<()> {
            let _guard = init_tracing()?;

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
                .build()
                .map_err(Into::into)
                .and_then(|batch| schema.as_arrow(partition, &batch, LakeHouseType::Delta))?;

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

            let topic = "taxi";

            let config = DescribeConfigsResult::default()
                .error_code(ErrorCode::None.into())
                .error_message(None)
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic.into())
                .configs(Some(vec![
                    DescribeConfigsResourceResult::default()
                        .name(String::from("tansu.lake.normalize"))
                        .value(Some(String::from("true")))
                        .read_only(true)
                        .is_default(None)
                        .config_source(None)
                        .is_sensitive(false)
                        .synonyms(None)
                        .config_type(None)
                        .documentation(None),
                    DescribeConfigsResourceResult::default()
                        .name(String::from("tansu.lake.normalize.separator"))
                        .value(Some(String::from("_")))
                        .read_only(true)
                        .is_default(None)
                        .config_source(None)
                        .is_sensitive(false)
                        .synonyms(None)
                        .config_type(None)
                        .documentation(None),
                ]));

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

            _ = ctx.register_table("t", Arc::new(table))?;

            let df = ctx.sql("select * from t").await?;
            let results = df.collect().await?;

            let pretty_results = pretty_format_batches(&results)?.to_string();

            let expected = vec![
                "+----------------+---------------------+-----------+------------+----------+-----------------+---------------+---------------------+-------------------+---------------------+",
                "| meta_partition | meta_timestamp      | meta_year | meta_month | meta_day | value_vendor_id | value_trip_id | value_trip_distance | value_fare_amount | value_store_and_fwd |",
                "+----------------+---------------------+-----------+------------+----------+-----------------+---------------+---------------------+-------------------+---------------------+",
                "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | 1               | 1000371       | 1.8                 | 15.32             | 0                   |",
                "+----------------+---------------------+-----------+------------+----------+-----------------+---------------+---------------------+-------------------+---------------------+",
            ];

            assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

            Ok(())
        }

        #[tokio::test]
        async fn taxi_normalized_partition_on_value_dot_vendor_id() -> Result<()> {
            let _guard = init_tracing()?;

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
                .build()
                .map_err(Into::into)
                .and_then(|batch| schema.as_arrow(partition, &batch, LakeHouseType::Delta))?;

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

            let topic = "taxi";

            let config = DescribeConfigsResult::default()
                .error_code(ErrorCode::None.into())
                .error_message(None)
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic.into())
                .configs(Some(vec![
                    DescribeConfigsResourceResult::default()
                        .name(String::from("tansu.lake.normalize"))
                        .value(Some(String::from("true")))
                        .read_only(true)
                        .is_default(None)
                        .config_source(None)
                        .is_sensitive(false)
                        .synonyms(None)
                        .config_type(None)
                        .documentation(None),
                    DescribeConfigsResourceResult::default()
                        .name(String::from("tansu.lake.partition"))
                        .value(Some(String::from("value.vendor_id")))
                        .read_only(true)
                        .is_default(None)
                        .config_source(None)
                        .is_sensitive(false)
                        .synonyms(None)
                        .config_type(None)
                        .documentation(None),
                ]));

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

            _ = ctx.register_table("t", Arc::new(table))?;

            let df = ctx.sql("select * from t").await?;
            let results = df.collect().await?;

            let pretty_results = pretty_format_batches(&results)?.to_string();

            let expected = vec![
                "+----------------+---------------------+-----------+------------+----------+---------------+---------------------+-------------------+---------------------+-----------------+",
                "| meta.partition | meta.timestamp      | meta.year | meta.month | meta.day | value.trip_id | value.trip_distance | value.fare_amount | value.store_and_fwd | value.vendor_id |",
                "+----------------+---------------------+-----------+------------+----------+---------------+---------------------+-------------------+---------------------+-----------------+",
                "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | 1000371       | 1.8                 | 15.32             | 0                   | 1               |",
                "+----------------+---------------------+-----------+------------+----------+---------------+---------------------+-------------------+---------------------+-----------------+",
            ];

            assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

            Ok(())
        }

        #[tokio::test]
        async fn taxi_date_generated_field() -> Result<()> {
            let _guard = init_tracing()?;

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
                .build()
                .map_err(Into::into)
                .and_then(|batch| schema.as_arrow(partition, &batch, LakeHouseType::Delta))?;

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

            let topic = "taxi";

            let config = DescribeConfigsResult::default()
                .error_code(ErrorCode::None.into())
                .error_message(None)
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic.into())
                .configs(Some(vec![
                    DescribeConfigsResourceResult::default()
                        .name(String::from("tansu.lake.generate.date"))
                        .value(Some(String::from("cast(meta.timestamp as date)")))
                        .read_only(true)
                        .is_default(None)
                        .config_source(None)
                        .is_sensitive(false)
                        .synonyms(None)
                        .config_type(None)
                        .documentation(None),
                ]));

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

            _ = ctx.register_table("t", Arc::new(table))?;

            let df = ctx.sql("select * from t").await?;
            let results = df.collect().await?;

            let pretty_results = pretty_format_batches(&results)?.to_string();

            let expected = vec![
                "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+------------+",
                "| meta                                                                               | value                                                                                      | date       |",
                "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+------------+",
                "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {vendor_id: 1, trip_id: 1000371, trip_distance: 1.8, fare_amount: 15.32, store_and_fwd: 0} | 1973-10-17 |",
                "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+------------+",
            ];

            assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

            Ok(())
        }

        #[tokio::test]
        async fn taxi_partition_on_date_generated_field() -> Result<()> {
            let _guard = init_tracing()?;

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
                .build()
                .map_err(Into::into)
                .and_then(|batch| schema.as_arrow(partition, &batch, LakeHouseType::Delta))?;

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

            let topic = "taxi";

            let config = DescribeConfigsResult::default()
                .error_code(ErrorCode::None.into())
                .error_message(None)
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic.into())
                .configs(Some(vec![
                    DescribeConfigsResourceResult::default()
                        .name(String::from("tansu.lake.generate.date"))
                        .value(Some(String::from("cast(meta.timestamp as date)")))
                        .read_only(true)
                        .is_default(None)
                        .config_source(None)
                        .is_sensitive(false)
                        .synonyms(None)
                        .config_type(None)
                        .documentation(None),
                    DescribeConfigsResourceResult::default()
                        .name(String::from("tansu.lake.partition"))
                        .value(Some(String::from("date")))
                        .read_only(true)
                        .is_default(None)
                        .config_source(None)
                        .is_sensitive(false)
                        .synonyms(None)
                        .config_type(None)
                        .documentation(None),
                ]));

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

            _ = ctx.register_table("t", Arc::new(table))?;

            let df = ctx.sql("select * from t").await?;
            let results = df.collect().await?;

            let pretty_results = pretty_format_batches(&results)?.to_string();

            let expected = vec![
                "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+------------+",
                "| meta                                                                               | value                                                                                      | date       |",
                "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+------------+",
                "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {vendor_id: 1, trip_id: 1000371, trip_distance: 1.8, fare_amount: 15.32, store_and_fwd: 0} | 1973-10-17 |",
                "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+------------+",
            ];

            assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

            Ok(())
        }

        #[tokio::test]
        async fn taxi_partition_on_value_vendor_id_is_an_error() -> Result<()> {
            let _guard = init_tracing()?;

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
                .build()
                .map_err(Into::into)
                .and_then(|batch| schema.as_arrow(partition, &batch, LakeHouseType::Delta))?;

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

            let topic = "taxi";

            let config = DescribeConfigsResult::default()
                .error_code(ErrorCode::None.into())
                .error_message(None)
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic.into())
                .configs(Some(vec![
                    DescribeConfigsResourceResult::default()
                        .name(String::from("tansu.lake.partition"))
                        .value(Some(String::from("value.vendor_id")))
                        .read_only(true)
                        .is_default(None)
                        .config_source(None)
                        .is_sensitive(false)
                        .synonyms(None)
                        .config_type(None)
                        .documentation(None),
                ]));

            let offset = 543212345;

            let not_found_in_schema =
                String::from("Partition column value.vendor_id not found in schema");

            assert!(matches!(
                lake_house
                    .store(topic, partition, offset, record_batch, config)
                    .await
                    .inspect(|result| debug!(?result))
                    .inspect_err(|err| debug!(?err)),
                Err(Error::DeltaTable(
                    deltalake::errors::DeltaTableError::Generic(error)
                )) if error == not_found_in_schema
            ));

            Ok(())
        }

        #[tokio::test]
        async fn taxi_partition_on_vendor_id_generated_field() -> Result<()> {
            let _guard = init_tracing()?;

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
                .build()
                .map_err(Into::into)
                .and_then(|batch| schema.as_arrow(partition, &batch, LakeHouseType::Delta))?;

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

            let topic = "taxi";

            let config = DescribeConfigsResult::default()
                .error_message(None)
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic.into())
                .configs(Some(vec![
                    DescribeConfigsResourceResult::default()
                        .name(String::from("tansu.lake.generate.year"))
                        .value(Some(String::from("cast(meta.year as integer)")))
                        .read_only(true)
                        .is_default(None)
                        .config_source(None)
                        .is_sensitive(false)
                        .synonyms(None)
                        .config_type(None)
                        .documentation(None),
                    DescribeConfigsResourceResult::default()
                        .name(String::from("tansu.lake.generate.month"))
                        .value(Some(String::from("cast(meta.month as integer)")))
                        .read_only(true)
                        .is_default(None)
                        .config_source(None)
                        .is_sensitive(false)
                        .synonyms(None)
                        .config_type(None)
                        .documentation(None),
                    DescribeConfigsResourceResult::default()
                        .name(String::from("tansu.lake.generate.day"))
                        .value(Some(String::from("cast(meta.day as integer)")))
                        .read_only(true)
                        .is_default(None)
                        .config_source(None)
                        .is_sensitive(false)
                        .synonyms(None)
                        .config_type(None)
                        .documentation(None),
                    DescribeConfigsResourceResult::default()
                        .name(String::from("tansu.lake.generate.vendor_id"))
                        .value(Some(String::from("cast(value.vendor_id as integer)")))
                        .read_only(true)
                        .is_default(None)
                        .config_source(None)
                        .is_sensitive(false)
                        .synonyms(None)
                        .config_type(None)
                        .documentation(None),
                    DescribeConfigsResourceResult::default()
                        .name(String::from("tansu.lake.partition"))
                        .value(Some(String::from("year,month,day,vendor_id")))
                        .read_only(true)
                        .is_default(None)
                        .config_source(None)
                        .is_sensitive(false)
                        .synonyms(None)
                        .config_type(None)
                        .documentation(None),
                ]));

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

            _ = ctx.register_table("t", Arc::new(table))?;

            let df = ctx.sql("select * from t").await?;
            let results = df.collect().await?;

            let pretty_results = pretty_format_batches(&results)?.to_string();

            let expected = vec![
                "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+------+-------+-----+-----------+",
                "| meta                                                                               | value                                                                                      | year | month | day | vendor_id |",
                "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+------+-------+-----+-----------+",
                "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {vendor_id: 1, trip_id: 1000371, trip_distance: 1.8, fare_amount: 15.32, store_and_fwd: 0} | 1973 | 10    | 17  | 1         |",
                "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+------+-------+-----+-----------+",
            ];

            assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

            Ok(())
        }

        #[tokio::test]
        async fn repeated_string() -> Result<()> {
            let _guard = init_tracing()?;

            let schema = Schema::try_from(Bytes::from_static(include_bytes!(
                "../../tests/repeated-string.proto"
            )))?;

            let value = schema.encode_from_value(
                MessageKind::Value,
                &json!({
                  "id": 12321,
                  "industry": ["abc", "def", "pqr"],
                }),
            )?;

            let partition = 32123;

            let record_batch = Batch::builder()
                .record(Record::builder().value(value.into()))
                .base_timestamp(119_731_017_000)
                .build()
                .map_err(Into::into)
                .and_then(|batch| schema.as_arrow(partition, &batch, LakeHouseType::Delta))?;

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

            let topic = "t";

            let config = DescribeConfigsResult::default()
                .error_code(ErrorCode::None.into())
                .error_message(None)
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic.into())
                .configs(Some(vec![]));

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

            _ = ctx.register_table("t", Arc::new(table))?;

            let df = ctx.sql("select * from t").await?;
            let results = df.collect().await?;

            let pretty_results = pretty_format_batches(&results)?.to_string();

            let expected = vec![
                "+------------------------------------------------------------------------------------+----------------------------------------+",
                "| meta                                                                               | value                                  |",
                "+------------------------------------------------------------------------------------+----------------------------------------+",
                "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {id: 12321, industry: [abc, def, pqr]} |",
                "+------------------------------------------------------------------------------------+----------------------------------------+",
            ];

            assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

            Ok(())
        }

        #[tokio::test]
        async fn customer_schema_migration() -> Result<()> {
            let _guard = init_tracing()?;

            let partition = 32123;

            let record_batch_001 = {
                let schema = Schema::try_from(Bytes::from_static(include_bytes!(
                    "../../tests/migrate-001.proto"
                )))?;

                Batch::builder()
                    .record(schema.generate()?)
                    .base_timestamp(119_731_017_000)
                    .build()
                    .map_err(Into::into)
                    .and_then(|batch| schema.as_arrow(partition, &batch, LakeHouseType::Delta))?
            };

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

            let topic = "t";

            let config = DescribeConfigsResult::default()
                .error_code(ErrorCode::None.into())
                .error_message(None)
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic.into())
                .configs(Some(vec![
                    DescribeConfigsResourceResult::default()
                        .name(String::from("tansu.lake.normalize"))
                        .value(Some(String::from("true")))
                        .read_only(true),
                ]));

            let offset = 543212345;

            lake_house
                .store(topic, partition, offset, record_batch_001, config.clone())
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
            _ = ctx.register_table("t", Arc::new(table))?;

            let df = ctx.sql("select * from t").await?;
            let results = df.collect().await?;

            let pretty_results = pretty_format_batches(&results)?.to_string();

            let expected = vec![
                "+----------------+---------------------+-----------+------------+----------+---------------------+",
                "| meta.partition | meta.timestamp      | meta.year | meta.month | meta.day | value.email_address |",
                "+----------------+---------------------+-----------+------------+----------+---------------------+",
                "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | lorem               |",
                "+----------------+---------------------+-----------+------------+----------+---------------------+",
            ];

            assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

            let record_batch_002 = {
                let schema = Schema::try_from(Bytes::from_static(include_bytes!(
                    "../../tests/migrate-002.proto"
                )))?;

                Batch::builder()
                    .record(schema.generate()?)
                    .base_timestamp(119_731_017_000)
                    .build()
                    .map_err(Into::into)
                    .and_then(|batch| schema.as_arrow(partition, &batch, LakeHouseType::Delta))?
            };

            let offset = 654323456;

            lake_house
                .store(topic, partition, offset, record_batch_002, config.clone())
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
            _ = ctx.register_table("t", Arc::new(table))?;

            let df = ctx.sql("select * from t").await?;
            let results = df.collect().await?;

            let pretty_results = pretty_format_batches(&results)?.to_string();

            let expected = vec![
                "+----------------+---------------------+-----------+------------+----------+---------------------+-----------------+",
                "| meta.partition | meta.timestamp      | meta.year | meta.month | meta.day | value.email_address | value.full_name |",
                "+----------------+---------------------+-----------+------------+----------+---------------------+-----------------+",
                "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | lorem               | ipsum           |",
                "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | lorem               |                 |",
                "+----------------+---------------------+-----------+------------+----------+---------------------+-----------------+",
            ];

            assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

            let record_batch_003 = {
                let schema = Schema::try_from(Bytes::from_static(include_bytes!(
                    "../../tests/migrate-003.proto"
                )))?;

                Batch::builder()
                    .record(schema.generate()?)
                    .base_timestamp(119_731_017_000)
                    .build()
                    .map_err(Into::into)
                    .and_then(|batch| schema.as_arrow(partition, &batch, LakeHouseType::Delta))?
            };

            let offset = 765434567;

            lake_house
                .store(topic, partition, offset, record_batch_003, config)
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
            _ = ctx.register_table("t", Arc::new(table))?;

            let df = ctx.sql("select * from t").await?;
            let results = df.collect().await?;

            let pretty_results = pretty_format_batches(&results)?.to_string();

            let expected = vec![
                "+----------------+---------------------+-----------+------------+----------+---------------------+-----------------+----------------------------+------------------------+-----------------+----------------------+-------------------------+--------------------+",
                "| meta.partition | meta.timestamp      | meta.year | meta.month | meta.day | value.email_address | value.full_name | value.home.building_number | value.home.street_name | value.home.city | value.home.post_code | value.home.country_name | value.industry     |",
                "+----------------+---------------------+-----------+------------+----------+---------------------+-----------------+----------------------------+------------------------+-----------------+----------------------+-------------------------+--------------------+",
                "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | lorem               | ipsum           | dolor                      | sit                    | amet            | consectetur          | adipiscing              | [elit, elit, elit] |",
                "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | lorem               | ipsum           |                            |                        |                 |                      |                         |                    |",
                "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | lorem               |                 |                            |                        |                 |                      |                         |                    |",
                "+----------------+---------------------+-----------+------------+----------+---------------------+-----------------+----------------------------+------------------------+-----------------+----------------------+-------------------------+--------------------+",
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
                    .and_then(|batch| schema.as_arrow(partition, &batch, LakeHouseType::Delta))
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

            let config = DescribeConfigsResult::default()
                .error_code(ErrorCode::None.into())
                .error_message(None)
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic.into())
                .configs(Some(vec![
                    DescribeConfigsResourceResult::default()
                        .name(String::from("tansu.lake.generate.date"))
                        .value(Some(String::from("cast(meta.timestamp as date)")))
                        .read_only(true)
                        .is_default(None)
                        .config_source(None)
                        .is_sensitive(false)
                        .synonyms(None)
                        .config_type(None)
                        .documentation(None),
                ]));

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

            _ = ctx.register_table("t", Arc::new(table))?;

            let df = ctx.sql("select * from t").await?;
            let results = df.collect().await?;

            let pretty_results = pretty_format_batches(&results)?.to_string();

            let expected = vec![
                "+-------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+------------+",
                "| value                                                                                                 | meta                                                                              | date       |",
                "+-------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+------------+",
                "| {b: false, c: 2147483647, d: 9223372036854775807, e: 3.4028235e38, f: 1.7976931348623157e308, h: pqr} | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} | 2009-02-13 |",
                "+-------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+------------+",
            ];

            assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

            Ok(())
        }
    }

    mod json {
        use serde_json::Value;

        use super::*;
        use crate::json::Schema;

        #[tokio::test]
        async fn grade() -> Result<()> {
            let _guard = init_tracing()?;

            let schema = Schema::try_from(Bytes::from_static(include_bytes!(
                "../../../../tansu/etc/schema/grade.json"
            )))?;

            let kv = if let Value::Array(values) = serde_json::from_slice::<Value>(include_bytes!(
                "../../../../tansu/etc/data/grades.json"
            ))? {
                values
                    .into_iter()
                    .map(|value| {
                        (
                            value.get("key").cloned().unwrap(),
                            value.get("value").cloned().unwrap(),
                        )
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![]
            };

            let partition = 32123;

            let record_batch = {
                let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

                for (ref key, ref value) in kv {
                    debug!(?key, ?value);

                    batch = batch.record(
                        Record::builder()
                            .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                            .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
                    );
                }

                batch
                    .build()
                    .map_err(Into::into)
                    .and_then(|batch| schema.as_arrow(partition, &batch, LakeHouseType::Delta))
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

            let config = DescribeConfigsResult::default()
                .error_code(ErrorCode::None.into())
                .error_message(None)
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic.into())
                .configs(Some(vec![]));

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

            _ = ctx.register_table("t", Arc::new(table))?;

            let df = ctx.sql("select * from t").await?;
            let results = df.collect().await?;

            let pretty_results = pretty_format_batches(&results)?.to_string();

            let expected = vec![
                "+-----------------------------------------------------------------------------------------+-------------+---------------------------------------------------------------------------------------------------------------+",
                "| meta                                                                                    | key         | value                                                                                                         |",
                "+-----------------------------------------------------------------------------------------+-------------+---------------------------------------------------------------------------------------------------------------+",
                "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 123-45-6789 | {final: 49.0, first: Aloysius, grade: D-, last: Alfalfa, test1: 40.0, test2: 90.0, test3: 100.0, test4: 83.0} |",
                "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 123-12-1234 | {final: 48.0, first: University, grade: D+, last: Alfred, test1: 41.0, test2: 97.0, test3: 96.0, test4: 97.0} |",
                "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 567-89-0123 | {final: 44.0, first: Gramma, grade: C, last: Gerty, test1: 41.0, test2: 80.0, test3: 60.0, test4: 40.0}       |",
                "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 087-65-4321 | {final: 47.0, first: Electric, grade: B-, last: Android, test1: 42.0, test2: 23.0, test3: 36.0, test4: 45.0}  |",
                "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 456-78-9012 | {final: 45.0, first: Fred, grade: A-, last: Bumpkin, test1: 43.0, test2: 78.0, test3: 88.0, test4: 77.0}      |",
                "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 234-56-7890 | {final: 46.0, first: Betty, grade: C-, last: Rubble, test1: 44.0, test2: 90.0, test3: 80.0, test4: 90.0}      |",
                "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 345-67-8901 | {final: 43.0, first: Cecil, grade: F, last: Noshow, test1: 45.0, test2: 11.0, test3: -1.0, test4: 4.0}        |",
                "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 632-79-9939 | {final: 50.0, first: Bif, grade: B+, last: Buff, test1: 46.0, test2: 20.0, test3: 30.0, test4: 40.0}          |",
                "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 223-45-6789 | {final: 83.0, first: Andrew, grade: A, last: Airpump, test1: 49.0, test2: 1.0, test3: 90.0, test4: 100.0}     |",
                "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 143-12-1234 | {final: 97.0, first: Jim, grade: A+, last: Backus, test1: 48.0, test2: 1.0, test3: 97.0, test4: 96.0}         |",
                "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 565-89-0123 | {final: 40.0, first: Art, grade: D+, last: Carnivore, test1: 44.0, test2: 1.0, test3: 80.0, test4: 60.0}      |",
                "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 087-75-4321 | {final: 45.0, first: Jim, grade: C+, last: Dandy, test1: 47.0, test2: 1.0, test3: 23.0, test4: 36.0}          |",
                "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 456-71-9012 | {final: 77.0, first: Ima, grade: B-, last: Elephant, test1: 45.0, test2: 1.0, test3: 78.0, test4: 88.0}       |",
                "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 234-56-2890 | {final: 90.0, first: Benny, grade: B-, last: Franklin, test1: 50.0, test2: 1.0, test3: 90.0, test4: 80.0}     |",
                "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 345-67-3901 | {final: 4.0, first: Boy, grade: B, last: George, test1: 40.0, test2: 1.0, test3: 11.0, test4: -1.0}           |",
                "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 632-79-9439 | {final: 40.0, first: Harvey, grade: C, last: Heffalump, test1: 30.0, test2: 1.0, test3: 20.0, test4: 30.0}    |",
                "+-----------------------------------------------------------------------------------------+-------------+---------------------------------------------------------------------------------------------------------------+",
            ];

            assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

            Ok(())
        }
    }
}
