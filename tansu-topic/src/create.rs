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

use tansu_sans_io::{
    Body, ErrorCode, Frame, Header,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    create_topics_response::CreatableTopicResult,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::debug;
use url::Url;

use crate::{Error, Result};

use super::Topic;

#[derive(Clone, Default)]
pub struct Builder<B, N, P> {
    broker: B,
    name: N,
    partitions: P,
    configs: HashMap<String, String>,
}

impl<B, N, P> Builder<B, N, P> {
    pub fn broker(self, broker: Url) -> Builder<Url, N, P> {
        Builder {
            broker,
            name: self.name,
            partitions: self.partitions,
            configs: self.configs,
        }
    }

    pub fn name(self, name: impl Into<String>) -> Builder<B, String, P> {
        Builder {
            broker: self.broker,
            name: name.into(),
            partitions: self.partitions,
            configs: self.configs,
        }
    }

    pub fn partitions(self, partitions: i32) -> Builder<B, N, i32> {
        Builder {
            broker: self.broker,
            name: self.name,
            partitions,
            configs: self.configs,
        }
    }

    pub fn config(self, configs: HashMap<String, String>) -> Builder<B, N, P> {
        Self { configs, ..self }
    }
}

impl Builder<Url, String, i32> {
    pub fn build(self) -> Topic {
        Topic::Create(Configuration {
            broker: self.broker,
            name: self.name,
            partitions: self.partitions,
            configs: self.configs,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Configuration {
    broker: Url,
    name: String,
    partitions: i32,
    configs: HashMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Create {
    configuration: Configuration,
}

impl TryFrom<Configuration> for Create {
    type Error = Error;

    fn try_from(configuration: Configuration) -> Result<Self, Self::Error> {
        Ok(Create { configuration })
    }
}

impl Create {
    pub async fn main(self) -> Result<ErrorCode> {
        let mut connection = Connection::open(&self.configuration.broker).await?;

        connection
            .create(
                self.configuration.name.as_str(),
                self.configuration.partitions,
                self.configuration.configs,
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

    async fn create(
        &mut self,
        topic: &str,
        partitions: i32,
        configs: HashMap<String, String>,
    ) -> Result<ErrorCode> {
        debug!(%topic, partitions);

        let api_key = 19;
        let api_version = 7;

        let header = Header::Request {
            api_key,
            api_version,
            correlation_id: self.correlation_id,
            client_id: Some("tansu".into()),
        };

        let timeout_ms = 30_000;
        let validate_only = Some(false);

        let body = Body::CreateTopicsRequest {
            topics: Some(
                [CreatableTopic {
                    name: topic.into(),
                    num_partitions: partitions,
                    replication_factor: -1,
                    assignments: Some([].into()),
                    configs: Some(
                        configs
                            .into_iter()
                            .map(|(name, value)| CreatableTopicConfig {
                                name,
                                value: Some(value),
                            })
                            .collect(),
                    ),
                }]
                .into(),
            ),
            timeout_ms,
            validate_only,
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

        match Frame::response_from_bytes(&response_buffer, api_key, api_version)
            .inspect(|response| debug!(?response))
            .inspect_err(|err| debug!(?err))?
        {
            Frame {
                body:
                    Body::CreateTopicsResponse {
                        topics: Some(topics),
                        ..
                    },
                ..
            } => match topics.as_slice() {
                [CreatableTopicResult { error_code, .. }] => {
                    ErrorCode::try_from(error_code).map_err(Into::into)
                }
                otherwise => unreachable!("{otherwise:?}"),
            },

            otherwise => unreachable!("{otherwise:?}"),
        }
    }

    fn frame_length(encoded: [u8; 4]) -> usize {
        i32::from_be_bytes(encoded) as usize + encoded.len()
    }
}
