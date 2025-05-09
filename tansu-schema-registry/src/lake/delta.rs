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
use arrow::{array::RecordBatch, datatypes::Schema};
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
        schema: &Schema,
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
                        json!("cast(timestamp as date)"),
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

        self.compact(topic).await
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
