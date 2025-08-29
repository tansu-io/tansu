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

use futures::StreamExt;
use serde_json::Value;
use tansu_client::{Client, Manager};
use tansu_sans_io::{
    ErrorCode, ProduceRequest, ProduceResponse,
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    record::{deflated, inflated},
};
use tansu_schema::{AsKafkaRecord, Registry};
use tokio::{
    fs::File,
    io::{self, AsyncReadExt},
};
use tokio_util::codec::{FramedRead, LinesCodec};
use tracing::debug;
use url::Url;

#[derive(Clone, Debug, Default)]
pub struct Builder<B, T, P, S, F> {
    broker: B,
    topic: T,
    partition: P,
    schema_registry: S,
    file_name: F,
}

pub(crate) type PhantomBuilder = Builder<
    PhantomData<Url>,
    PhantomData<String>,
    PhantomData<i32>,
    PhantomData<Option<Url>>,
    PhantomData<String>,
>;

impl<B, T, P, S, F> Builder<B, T, P, S, F> {
    pub fn broker(self, broker: impl Into<Url>) -> Builder<Url, T, P, S, F> {
        Builder {
            broker: broker.into(),
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            file_name: self.file_name,
        }
    }

    pub fn topic(self, topic: impl Into<String>) -> Builder<B, String, P, S, F> {
        Builder {
            broker: self.broker,
            topic: topic.into(),
            partition: self.partition,
            schema_registry: self.schema_registry,
            file_name: self.file_name,
        }
    }

    pub fn partition(self, partition: i32) -> Builder<B, T, i32, S, F> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition,
            schema_registry: self.schema_registry,
            file_name: self.file_name,
        }
    }

    pub fn schema_registry(self, schema_registry: Option<Url>) -> Builder<B, T, P, Option<Url>, F> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry,
            file_name: self.file_name,
        }
    }

    pub fn file_name(self, file_name: String) -> Builder<B, T, P, S, String> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            file_name,
        }
    }
}

impl Builder<Url, String, i32, Option<Url>, String> {
    pub fn build(self) -> super::Cat {
        super::Cat::Produce(Box::new(Configuration {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            file_name: self.file_name,
        }))
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Configuration {
    pub broker: Url,
    pub topic: String,
    pub partition: i32,
    pub schema_registry: Option<Url>,
    pub file_name: String,
}

#[derive(Clone, Debug)]
pub(crate) struct Produce {
    configuration: Configuration,
    registry: Option<Registry>,
}

impl TryFrom<Configuration> for Produce {
    type Error = Error;

    fn try_from(configuration: Configuration) -> Result<Self, Self::Error> {
        configuration
            .schema_registry
            .as_ref()
            .map(Registry::try_from)
            .transpose()
            .map(|registry| Self {
                configuration,
                registry,
            })
            .map_err(Into::into)
    }
}

impl Produce {
    pub(crate) async fn main(self) -> Result<ErrorCode> {
        debug!(%self.configuration.file_name);

        let schema = if let Some(ref registry) = self.registry {
            registry
                .schema(&self.configuration.topic)
                .await
                .inspect(|schema| debug!(?schema))?
        } else {
            None
        };

        let frame = {
            let mut batch = inflated::Batch::builder();
            let mut offset_delta = 0;

            if self.configuration.file_name == "-" {
                let stdin = io::stdin();
                let mut reader = FramedRead::new(stdin, LinesCodec::new());

                while let Some(line) = reader.next().await.transpose()? {
                    if line.trim().is_empty() {
                        continue;
                    }

                    debug!(%line);

                    let v = serde_json::from_str::<Value>(&line).inspect(|value| debug!(?value))?;
                    debug!(%v);

                    if let Some(ref schema) = schema {
                        batch = batch.record(
                            schema
                                .as_kafka_record(&v)
                                .map(|record| record.offset_delta(offset_delta))?,
                        );
                    }

                    offset_delta += 1;
                }
            } else {
                let mut file = File::open(self.configuration.file_name).await?;

                let mut contents = vec![];
                _ = file.read_to_end(&mut contents).await?;

                let v = serde_json::from_slice::<Value>(&contents[..])?;

                if let Some(records) = v.as_array() {
                    for record in records {
                        debug!(%record);

                        if let Some(ref schema) = schema {
                            batch = batch.record(
                                schema
                                    .as_kafka_record(record)
                                    .map(|record| record.offset_delta(offset_delta))?,
                            );
                        }

                        offset_delta += 1;
                    }
                }
            }

            batch
                .build()
                .map(|batch| inflated::Frame {
                    batches: vec![batch],
                })
                .and_then(deflated::Frame::try_from)?
        };

        debug!(?frame);

        let req = ProduceRequest::default()
            .transactional_id(None)
            .acks(-1)
            .timeout_ms(1_500)
            .topic_data(Some(
                [TopicProduceData::default()
                    .name(self.configuration.topic)
                    .partition_data(Some(
                        [PartitionProduceData::default()
                            .index(self.configuration.partition)
                            .records(Some(frame))]
                        .into(),
                    ))]
                .into(),
            ));

        let client = Manager::builder(self.configuration.broker.clone())
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .build()
            .await
            .inspect(|pool| debug!(?pool))
            .map(Client::new)?;

        let ProduceResponse { responses, .. } = client.call(req).await?;
        let responses = responses.unwrap_or_default();
        assert_eq!(1, responses.len());

        let TopicProduceResponse {
            partition_responses,
            ..
        } = responses.first().expect("responses: {responses:?}");
        let partition_responses = partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partition_responses.len());

        let PartitionProduceResponse { error_code, .. } = partition_responses
            .first()
            .expect("partition_responses: {partition_responses:?}");

        ErrorCode::try_from(*error_code).map_err(Into::into)
    }
}
