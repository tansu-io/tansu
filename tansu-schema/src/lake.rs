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

//! Data Lake: Delta, Iceberg or Parquet

use crate::{METER, Result};
use arrow::array::RecordBatch;
use async_trait::async_trait;
use opentelemetry::{KeyValue, metrics::Histogram};
use std::{fmt::Debug, marker::PhantomData, sync::LazyLock, time::SystemTime};
use tansu_sans_io::describe_configs_response::DescribeConfigsResult;
use tracing::{debug, warn};
use url::Url;

pub mod berg;
pub mod delta;
pub mod quet;

/// House
///
/// Wrapper enum for the each Data Lake implementation
#[derive(Clone, Debug)]
pub enum House {
    Delta(delta::Delta),
    Iceberg(berg::Iceberg),
    Parquet(quet::Parquet),
}

/// Lake House Type
///
/// While Parquet is a common format used by both Delta and Iceberg,
/// there are some minor differences.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum LakeHouseType {
    Delta,
    Iceberg,
    Parquet,
}

impl From<&House> for LakeHouseType {
    fn from(house: &House) -> Self {
        match house {
            House::Delta(_) => Self::Delta,
            House::Iceberg(_) => Self::Iceberg,
            House::Parquet(_) => Self::Parquet,
        }
    }
}

impl LakeHouseType {
    pub fn is_delta(&self) -> bool {
        matches!(self, Self::Delta)
    }

    pub fn is_iceberg(&self) -> bool {
        matches!(self, Self::Iceberg)
    }

    pub fn is_parquet(&self) -> bool {
        matches!(self, Self::Parquet)
    }
}

/// Lake House
///
/// This trait is implemented by [`delta::Delta`], [`berg::Iceberg`] and [`quet::Parquet`].
#[async_trait]
pub trait LakeHouse: Clone + Debug + Send + Sync + 'static {
    /// Store a batch of records in this lake house
    async fn store(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        record_batch: RecordBatch,
        config: DescribeConfigsResult,
    ) -> Result<()>;

    /// Run periodic maintenance on this lake house
    async fn maintain(&self) -> Result<()>;

    /// Query the underlying type of this lake house
    async fn lake_type(&self) -> Result<LakeHouseType>;
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
    fn is_normalized(&self) -> bool {
        self.0
            .iter()
            .find_map(|(name, value)| {
                (name == "tansu.lake.normalize").then(|| value.parse().ok().unwrap_or_default())
            })
            .unwrap_or(false)
    }

    fn normalize_separator(&self) -> &str {
        self.0
            .iter()
            .find(|(name, _)| name == "tansu.lake.normalize.separator")
            .map(|(_, value)| value.as_str())
            .unwrap_or(".")
    }
}

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
        debug!(
            ?topic,
            ?partition,
            ?offset,
            rows = record_batch.num_rows(),
            columns = record_batch.num_columns(),
            ?record_batch
        );

        let start = SystemTime::now();

        let config = Config::from(configs.clone());

        let record_batch = if LakeHouseType::from(self).is_iceberg() & config.is_normalized() {
            warn!(iceberg_normalized = config.is_normalized());
            record_batch
        } else if config.is_normalized() {
            record_batch.normalize(config.normalize_separator(), None)?
        } else {
            record_batch
        };

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

    async fn lake_type(&self) -> Result<LakeHouseType> {
        Ok(LakeHouseType::from(self))
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
