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

use std::marker::PhantomData;

use crate::{Error, Result};

use futures::SinkExt;
use tansu_client::{Client, Manager};
use tansu_sans_io::{
    ErrorCode,
    fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    record::inflated,
};
use tansu_schema::{AsJsonValue, Registry};
use tokio::io::stdout;
use tokio_util::codec::{FramedWrite, LinesCodec};
use tracing::debug;
use url::Url;

#[derive(Clone, Debug, Default)]
pub struct Builder<B, T, P, S> {
    broker: B,
    topic: T,
    partition: P,
    schema_registry: S,
    max_wait_time_ms: i32,
    min_bytes: i32,
    max_bytes: Option<i32>,
    fetch_offset: i64,
    partition_max_bytes: i32,
}

pub(crate) type PhantomBuilder =
    Builder<PhantomData<Url>, PhantomData<String>, PhantomData<i32>, PhantomData<Option<Url>>>;

impl<B, T, P, S> Builder<B, T, P, S> {
    pub fn broker(self, broker: impl Into<Url>) -> Builder<Url, T, P, S> {
        Builder {
            broker: broker.into(),
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            max_wait_time_ms: self.max_wait_time_ms,
            min_bytes: self.min_bytes,
            max_bytes: self.max_bytes,
            fetch_offset: self.fetch_offset,
            partition_max_bytes: self.partition_max_bytes,
        }
    }

    pub fn topic(self, topic: impl Into<String>) -> Builder<B, String, P, S> {
        Builder {
            broker: self.broker,
            topic: topic.into(),
            partition: self.partition,
            schema_registry: self.schema_registry,
            max_wait_time_ms: self.max_wait_time_ms,
            min_bytes: self.min_bytes,
            max_bytes: self.max_bytes,
            fetch_offset: self.fetch_offset,
            partition_max_bytes: self.partition_max_bytes,
        }
    }

    pub fn partition(self, partition: i32) -> Builder<B, T, i32, S> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition,
            schema_registry: self.schema_registry,
            max_wait_time_ms: self.max_wait_time_ms,
            min_bytes: self.min_bytes,
            max_bytes: self.max_bytes,
            fetch_offset: self.fetch_offset,
            partition_max_bytes: self.partition_max_bytes,
        }
    }

    pub fn schema_registry(self, schema_registry: Option<Url>) -> Builder<B, T, P, Option<Url>> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry,
            max_wait_time_ms: self.max_wait_time_ms,
            min_bytes: self.min_bytes,
            max_bytes: self.max_bytes,
            fetch_offset: self.fetch_offset,
            partition_max_bytes: self.partition_max_bytes,
        }
    }

    pub fn max_wait_time_ms(self, max_wait_time_ms: i32) -> Self {
        Self {
            max_wait_time_ms,
            ..self
        }
    }

    pub fn min_bytes(self, min_bytes: i32) -> Self {
        Self { min_bytes, ..self }
    }

    pub fn max_bytes(self, max_bytes: Option<i32>) -> Self {
        Self { max_bytes, ..self }
    }

    pub fn fetch_offset(self, fetch_offset: i64) -> Self {
        Self {
            fetch_offset,
            ..self
        }
    }

    pub fn partition_max_bytes(self, partition_max_bytes: i32) -> Self {
        Self {
            partition_max_bytes,
            ..self
        }
    }
}

impl Builder<Url, String, i32, Option<Url>> {
    pub fn build(self) -> super::Cat {
        super::Cat::Consume(Box::new(Configuration::from(self)))
    }
}

impl From<Builder<Url, String, i32, Option<Url>>> for Configuration {
    fn from(builder: Builder<Url, String, i32, Option<Url>>) -> Self {
        Self {
            broker: builder.broker,
            topic: builder.topic,
            partition: builder.partition,
            schema_registry: builder.schema_registry,
            max_wait_time_ms: builder.max_wait_time_ms,
            min_bytes: builder.min_bytes,
            max_bytes: builder.max_bytes,
            fetch_offset: builder.fetch_offset,
            partition_max_bytes: builder.partition_max_bytes,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Configuration {
    broker: Url,
    topic: String,
    partition: i32,
    schema_registry: Option<Url>,
    max_wait_time_ms: i32,
    min_bytes: i32,
    max_bytes: Option<i32>,
    fetch_offset: i64,
    partition_max_bytes: i32,
}

#[derive(Clone, Debug)]
pub(crate) struct Consume {
    configuration: Configuration,
    registry: Option<Registry>,
}

impl TryFrom<Configuration> for Consume {
    type Error = Error;

    fn try_from(configuration: Configuration) -> Result<Self, Self::Error> {
        configuration
            .schema_registry
            .as_ref()
            .map(|url| Registry::builder_try_from_url(url).map(|builder| builder.build()))
            .transpose()
            .map(|registry| Self {
                configuration,
                registry,
            })
            .map_err(Into::into)
    }
}

impl Consume {
    pub(crate) async fn main(self) -> Result<ErrorCode> {
        let stdout = stdout();

        let mut writer = FramedWrite::new(stdout, LinesCodec::new());

        let schema = if let Some(ref registry) = self.registry {
            registry
                .schema(&self.configuration.topic)
                .await
                .inspect(|schema| debug!(?schema))?
        } else {
            None
        };

        let client = Manager::builder(self.configuration.broker.clone())
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .build()
            .await
            .inspect(|pool| debug!(?pool))
            .map(Client::new)?;

        let response = client
            .call(
                FetchRequest::default()
                    .replica_id(Some(-1))
                    .max_wait_ms(self.configuration.max_wait_time_ms)
                    .min_bytes(self.configuration.min_bytes)
                    .max_bytes(self.configuration.max_bytes)
                    .isolation_level(Some(1))
                    .topics(Some(vec![
                        FetchTopic::default()
                            .topic(Some(self.configuration.topic))
                            .partitions(Some(vec![
                                FetchPartition::default()
                                    .partition(self.configuration.partition)
                                    .log_start_offset(Some(self.configuration.fetch_offset))
                                    .partition_max_bytes(4096),
                            ])),
                    ])),
            )
            .await?;

        for response in response.responses.unwrap_or_default() {
            debug!(?response);

            for partition in response.partitions.unwrap_or_default() {
                debug!(?partition);

                if let Some(frame) = partition.records {
                    debug!(?frame);

                    let frame = inflated::Frame::try_from(frame)?;
                    for batch in frame.batches {
                        debug!(?batch);

                        if let Some(ref schema) = schema {
                            writer
                                .send(schema.as_json_value(&batch).map(|kv| kv.to_string())?)
                                .await?;
                        }
                    }
                }
            }
        }

        Ok(ErrorCode::None)
    }
}
