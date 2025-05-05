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

use std::collections::HashMap;

use crate::{Error, Result};
use arrow::{array::RecordBatch, datatypes::Schema};
use async_trait::async_trait;
use deltalake::{
    DeltaOps, DeltaTable, aws,
    kernel::StructField,
    writer::{DeltaWriter, RecordBatchWriter},
};
use parquet::file::properties::WriterProperties;
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
}

impl Delta {
    async fn create_initialized_table(&self, table: &str, schema: &Schema) -> Result<DeltaTable> {
        debug!(?table, ?schema);

        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                StructField::try_from(field.as_ref())
                    .map_err(Into::into)
                    .map(|mut sf| {
                        sf.metadata = HashMap::new();
                        sf
                    })
            })
            .collect::<Result<Vec<_>>>()
            .inspect(|columns| debug!(?columns))
            .inspect_err(|err| debug!(?err))?;

        let table_uri = format!("{}/{table}", self.location);

        DeltaOps::try_from_uri(&table_uri)
            .await
            .inspect_err(|err| debug!(?err))?
            .create()
            .with_columns(columns)
            .await
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
    }

    async fn write_with_datafusion(
        &self,
        table: &str,
        batches: impl Iterator<Item = RecordBatch>,
    ) -> Result<DeltaTable> {
        let table_uri = format!("{}/{table}", self.location);

        DeltaOps::try_from_uri(&table_uri)
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
}

#[async_trait]
impl LakeHouse for Delta {
    async fn store(
        &self,
        topic: &str,
        _partition: i32,
        _offset: i64,
        record_batch: RecordBatch,
    ) -> Result<()> {
        let mut _table = self
            .create_initialized_table(topic, record_batch.schema().as_ref())
            .await?;

        self.write_with_datafusion(topic, [record_batch].into_iter())
            .await
            .and(Ok(()))
    }
}

impl TryFrom<Builder<Url>> for Delta {
    type Error = Error;

    fn try_from(value: Builder<Url>) -> Result<Self, Self::Error> {
        aws::register_handlers(None);

        Ok(Self {
            location: value.location,
        })
    }
}
