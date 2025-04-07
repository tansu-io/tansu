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

use crate::{Error, Result};

use futures::StreamExt;
use serde_json::Value;
use tansu_kafka_sans_io::{
    Body, ErrorCode, Frame, Header,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{deflated, inflated},
};
use tansu_schema_registry::{AsKafkaRecord, Registry};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_util::codec::{FramedRead, LinesCodec};
use tracing::debug;
use url::Url;

#[derive(Clone, Default)]
pub struct Builder<B, T, P, S> {
    broker: B,
    topic: T,
    partition: P,
    schema_registry: S,
}

impl<B, T, P, S> Builder<B, T, P, S> {
    pub fn broker(self, broker: impl Into<Url>) -> Builder<Url, T, P, S> {
        Builder {
            broker: broker.into(),
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
        }
    }

    pub fn topic(self, topic: impl Into<String>) -> Builder<B, String, P, S> {
        Builder {
            broker: self.broker,
            topic: topic.into(),
            partition: self.partition,
            schema_registry: self.schema_registry,
        }
    }

    pub fn partition(self, partition: i32) -> Builder<B, T, i32, S> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition,
            schema_registry: self.schema_registry,
        }
    }

    pub fn schema_registry(self, schema_registry: Option<Url>) -> Builder<B, T, P, Option<Url>> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry,
        }
    }
}

impl Builder<Url, String, i32, Option<Url>> {
    pub fn build(self) -> super::Cat {
        super::Cat::Produce(Box::new(Configuration {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
        }))
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Configuration {
    pub broker: Url,
    pub topic: String,
    pub partition: i32,
    pub schema_registry: Option<Url>,
}

#[derive(Clone, Debug)]
pub struct Produce {
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
    pub async fn main(self) -> Result<ErrorCode> {
        let stdin = io::stdin();

        let schema = if let Some(ref registry) = self.registry {
            registry
                .schema(&self.configuration.topic)
                .await
                .inspect(|schema| debug!(?schema))?
        } else {
            None
        };

        let mut reader = FramedRead::new(stdin, LinesCodec::new());

        let frame = {
            let mut batch = inflated::Batch::builder();

            let mut offset_delta = 0;

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

            batch
                .build()
                .map(|batch| inflated::Frame {
                    batches: vec![batch],
                })
                .and_then(deflated::Frame::try_from)?
        };

        debug!(?frame);

        let mut connection = Connection::open(&self.configuration.broker).await?;

        connection
            .produce(
                self.configuration.topic.as_str(),
                self.configuration.partition,
                frame,
            )
            .await
    }
}

#[derive(Debug)]
struct Connection {
    broker: TcpStream,
    correlation_id: i32,
}

impl Connection {
    async fn open(broker: &Url) -> Result<Self> {
        debug!(%broker);

        TcpStream::connect(format!(
            "{}:{}",
            broker.host_str().unwrap(),
            broker.port().unwrap()
        ))
        .await
        .map(|broker| Self {
            broker,
            correlation_id: 0,
        })
        .map_err(Into::into)
    }

    async fn produce(
        &mut self,
        topic: &str,
        partition: i32,
        frame: deflated::Frame,
    ) -> Result<ErrorCode> {
        debug!(%topic, partition, ?frame);

        let api_key = 0;
        let api_version = 9;

        let header = Header::Request {
            api_key,
            api_version,
            correlation_id: self.correlation_id,
            client_id: Some("tansu".into()),
        };

        let body = Body::ProduceRequest {
            transactional_id: None,
            acks: -1,
            timeout_ms: 1_500,
            topic_data: Some(
                [TopicProduceData {
                    name: topic.into(),
                    partition_data: Some(
                        [PartitionProduceData {
                            index: partition,
                            records: Some(frame),
                        }]
                        .into(),
                    ),
                }]
                .into(),
            ),
        };

        let encoded = Frame::request(header, body)?;

        self.broker
            .write_all(&encoded[..])
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut size = [0u8; 4];
        _ = self.broker.read_exact(&mut size).await?;

        let mut response_buffer: Vec<u8> = vec![0u8; Self::frame_length(size)];
        response_buffer[0..size.len()].copy_from_slice(&size[..]);
        _ = self
            .broker
            .read_exact(&mut response_buffer[size.len()..])
            .await
            .inspect_err(|err| debug!(?err))?;

        let response = Frame::response_from_bytes(&response_buffer, api_key, api_version)
            .inspect_err(|err| debug!(?err))?;

        debug!(?response);

        Ok(ErrorCode::None)
    }

    fn frame_length(encoded: [u8; 4]) -> usize {
        i32::from_be_bytes(encoded) as usize + encoded.len()
    }
}
