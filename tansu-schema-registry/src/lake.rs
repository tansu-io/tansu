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

use crate::Result;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use std::{fmt::Debug, marker::PhantomData};
use tracing::debug;
use url::Url;

mod berg;
mod delta;
mod quet;

#[derive(Clone, Debug)]
pub enum House {
    Delta(delta::Delta),
    Iceberg(berg::Iceberg),
    Parquet(quet::Parquet),
}

#[async_trait]
pub trait LakeHouse: Clone + Debug + Send + Sync + 'static {
    async fn store(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        record_batch: RecordBatch,
    ) -> Result<()>;
}

#[async_trait]
impl LakeHouse for House {
    async fn store(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        record_batch: RecordBatch,
    ) -> Result<()> {
        debug!(?topic, ?partition, ?offset, ?record_batch);

        match self {
            House::Delta(inner) => inner.store(topic, partition, offset, record_batch).await,
            House::Iceberg(inner) => inner.store(topic, partition, offset, record_batch).await,
            House::Parquet(inner) => inner.store(topic, partition, offset, record_batch).await,
        }
    }
}

impl House {
    pub fn iceberg() -> berg::Builder<PhantomData<Url>, PhantomData<Url>> {
        berg::Builder::default()
    }

    pub fn delta() -> delta::Builder<PhantomData<Url>> {
        delta::Builder::default()
    }

    pub fn parquet() -> quet::Builder<PhantomData<Url>> {
        quet::Builder::default()
    }
}
