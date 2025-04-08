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

use std::marker::PhantomData;

use crate::{Error, Result};

use futures::SinkExt;
use tansu_kafka_sans_io::{
    Body, ErrorCode, Frame, Header,
    fetch_request::{FetchPartition, FetchTopic},
    fetch_response::FetchableTopicResponse,
    record::inflated,
};
use tansu_schema_registry::{AsJsonValue, Registry};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_util::codec::{FramedWrite, LinesCodec};
use tracing::debug;
use url::Url;

#[derive(Clone, Default)]
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

pub type PhantomBuilder =
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
pub struct Consume {
    configuration: Configuration,
    registry: Option<Registry>,
}

impl TryFrom<Configuration> for Consume {
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

impl Consume {
    pub async fn main(self) -> Result<ErrorCode> {
        let stdout = io::stdout();

        let mut writer = FramedWrite::new(stdout, LinesCodec::new());

        let schema = if let Some(ref registry) = self.registry {
            registry
                .schema(&self.configuration.topic)
                .await
                .inspect(|schema| debug!(?schema))?
        } else {
            None
        };

        let mut connection = Connection::open(&self.configuration.broker).await?;

        for response in connection
            .consume(
                self.configuration.topic.as_str(),
                self.configuration.partition,
                self.configuration.fetch_offset,
                self.configuration.max_wait_time_ms,
                self.configuration.min_bytes,
                self.configuration.max_bytes,
            )
            .await?
        {
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

    async fn consume(
        &mut self,
        topic: &str,
        partition: i32,
        fetch_offset: i64,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: Option<i32>,
    ) -> Result<Vec<FetchableTopicResponse>> {
        let _ = (fetch_offset, max_wait_ms, min_bytes, max_bytes);

        debug!(%topic, partition);

        let api_key = 1;
        let api_version = 6;

        let header = Header::Request {
            api_key,
            api_version,
            correlation_id: self.correlation_id,
            client_id: None,
        };

        let body = Body::FetchRequest {
            cluster_id: None,
            replica_state: None,
            replica_id: Some(-1),
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level: Some(1),
            session_id: None,
            session_epoch: None,
            topics: Some(
                [FetchTopic {
                    topic: Some(topic.into()),
                    topic_id: None,
                    partitions: Some(
                        [FetchPartition {
                            partition: 0,
                            current_leader_epoch: None,
                            fetch_offset: 0,
                            last_fetched_epoch: None,
                            log_start_offset: Some(0),
                            partition_max_bytes: 4096,
                            replica_directory_id: None,
                        }]
                        .into(),
                    ),
                }]
                .into(),
            ),
            forgotten_topics_data: None,
            rack_id: None,
        };

        debug!(?header, ?body);

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

        match response {
            Frame {
                body:
                    Body::FetchResponse {
                        responses: Some(responses),
                        ..
                    },
                ..
            } => Ok(responses),

            Frame {
                body:
                    Body::FetchResponse {
                        error_code: Some(error_code),
                        ..
                    },
                ..
            } => Err(Error::Api(ErrorCode::try_from(error_code)?)),

            frame @ Frame { .. } => unreachable!("{frame:?}"),
        }
    }

    fn frame_length(encoded: [u8; 4]) -> usize {
        i32::from_be_bytes(encoded) as usize + encoded.len()
    }
}
