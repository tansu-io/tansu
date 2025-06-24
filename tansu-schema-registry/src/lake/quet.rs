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

use std::{env, sync::Arc};

use arrow::array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use object_store::{
    DynObjectStore, PutMode, PutOptions, PutPayload,
    aws::{AmazonS3Builder, S3ConditionalPut},
    local::LocalFileSystem,
    path::Path,
};
use parquet::arrow::AsyncArrowWriter;
use tansu_kafka_sans_io::describe_configs_response::DescribeConfigsResult;
use tracing::debug;
use url::Url;

use crate::{
    Error, Result,
    lake::{LakeHouse, LakeHouseType},
};

use super::House;

#[derive(Clone, Debug, Default)]
pub struct Builder<L> {
    location: L,
}

impl<L> Builder<L> {
    pub fn location(self, location: Url) -> Builder<Url> {
        Builder { location }
    }
}

impl Builder<Url> {
    pub fn build(self) -> Result<House> {
        Parquet::try_from(self).map(House::Parquet)
    }
}

#[derive(Clone, Debug)]
pub struct Parquet {
    object_store: Arc<DynObjectStore>,
}

#[async_trait]
impl LakeHouse for Parquet {
    async fn store(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        record_batch: RecordBatch,
        _config: DescribeConfigsResult,
    ) -> Result<()> {
        let payload = {
            let mut buffer = Vec::new();
            let mut writer = AsyncArrowWriter::try_new(&mut buffer, record_batch.schema(), None)?;
            writer.write(&record_batch).await?;
            writer.close().await?;
            PutPayload::from(Bytes::from(buffer))
        };

        let location = Path::from(format!(
            "{}/{:0>10}/{:0>20}.parquet",
            topic, partition, offset,
        ));

        self.object_store
            .put_opts(
                &location,
                payload,
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .inspect(|put_result| debug!(?put_result))
            .and(Ok(()))
            .map_err(Into::into)
    }

    async fn maintain(&self) -> Result<()> {
        Ok(())
    }

    async fn lake_type(&self) -> Result<LakeHouseType> {
        Ok(LakeHouseType::Parquet)
    }
}

impl TryFrom<Builder<Url>> for Parquet {
    type Error = Error;

    fn try_from(value: Builder<Url>) -> Result<Self, Self::Error> {
        match value.location.scheme() {
            "s3" => {
                let bucket_name = value.location.host_str().unwrap_or("lake");

                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket_name)
                    .with_conditional_put(S3ConditionalPut::ETagMatch)
                    .build()
                    .map(Arc::new)
                    .map(|object_store| Self { object_store })
                    .map_err(Into::into)
            }

            "file" => {
                let mut path = env::current_dir().inspect(|current_dir| debug!(?current_dir))?;

                if let Some(domain) = value.location.domain() {
                    path.push(domain);
                }

                if let Some(relative) = value.location.path().strip_prefix("/") {
                    path.push(relative);
                } else {
                    path.push(value.location.path());
                }

                debug!(?path);

                LocalFileSystem::new_with_prefix(path)
                    .map(Arc::new)
                    .map(|object_store| Self { object_store })
                    .map_err(Into::into)
            }

            _unsupported => Err(Error::UnsupportedLakeHouseUrl(value.location.to_owned())),
        }
    }
}
