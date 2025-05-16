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

use crate::{METER, Result};
use arrow::array::RecordBatch;
use async_trait::async_trait;
use opentelemetry::{KeyValue, metrics::Histogram};
use std::{fmt::Debug, marker::PhantomData, sync::LazyLock, time::SystemTime};
use tansu_kafka_sans_io::describe_configs_response::DescribeConfigsResult;
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
        config: DescribeConfigsResult,
    ) -> Result<()>;

    async fn maintain(&self) -> Result<()>;
}

static STORE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("lakehouse_store_duration")
        .with_unit("ms")
        .with_description("The Lake House store latencies in milliseconds")
        .build()
});

static MAINTENANCE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("lakehouse_maintenance_duration")
        .with_unit("ms")
        .with_description("The Lake House maintenance latencies in milliseconds")
        .build()
});

#[async_trait]
impl LakeHouse for House {
    async fn store(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        record_batch: RecordBatch,
        configs: DescribeConfigsResult,
    ) -> Result<()> {
        debug!(?topic, ?partition, ?offset, ?record_batch);

        let start = SystemTime::now();

        match self {
            House::Delta(inner) => {
                inner
                    .store(topic, partition, offset, record_batch, configs)
                    .await
            }
            House::Iceberg(inner) => {
                inner
                    .store(topic, partition, offset, record_batch, configs)
                    .await
            }
            House::Parquet(inner) => {
                inner
                    .store(topic, partition, offset, record_batch, configs)
                    .await
            }
        }
        .inspect(|_| {
            STORE_DURATION.record(
                start
                    .elapsed()
                    .map_or(0, |duration| duration.as_millis() as u64),
                &[KeyValue::new("topic", topic.to_owned())],
            )
        })
    }

    async fn maintain(&self) -> Result<()> {
        debug!(?self);

        let start = SystemTime::now();

        match self {
            House::Delta(inner) => inner.maintain().await,
            House::Iceberg(inner) => inner.maintain().await,
            House::Parquet(inner) => inner.maintain().await,
        }
        .inspect(|_| {
            MAINTENANCE_DURATION.record(
                start
                    .elapsed()
                    .map_or(0, |duration| duration.as_millis() as u64),
                &[],
            )
        })
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
