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

use tansu_sans_io::{
    ApiKey as _, Body, DeleteTopicsRequest, DeleteTopicsResponse, ErrorCode, Frame, Header,
    NULL_TOPIC_ID, delete_topics_request::DeleteTopicState,
    delete_topics_response::DeletableTopicResult,
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
pub struct Builder<B, N> {
    broker: B,
    name: N,
}

impl<B, N> Builder<B, N> {
    pub fn broker(self, broker: Url) -> Builder<Url, N> {
        Builder {
            broker,
            name: self.name,
        }
    }

    pub fn name(self, name: impl Into<String>) -> Builder<B, String> {
        Builder {
            broker: self.broker,
            name: name.into(),
        }
    }
}

impl Builder<Url, String> {
    pub fn build(self) -> Topic {
        Topic::Delete(Configuration {
            broker: self.broker,
            name: self.name,
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Configuration {
    broker: Url,
    name: String,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Delete {
    configuration: Configuration,
}

impl TryFrom<Configuration> for Delete {
    type Error = Error;

    fn try_from(configuration: Configuration) -> Result<Self, Self::Error> {
        Ok(Delete { configuration })
    }
}

impl Delete {
    pub async fn main(self) -> Result<ErrorCode> {
        let mut connection = Connection::open(&self.configuration.broker).await?;
        connection.delete(self.configuration.name.as_str()).await
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

    async fn delete(&mut self, topic: &str) -> Result<ErrorCode> {
        debug!(%topic);

        let api_key = DeleteTopicsRequest::KEY;
        let api_version = 6;

        let header = Header::Request {
            api_key,
            api_version,
            correlation_id: self.correlation_id,
            client_id: Some("tansu".into()),
        };

        let timeout_ms = 30_000;

        let delete_topics_request = DeleteTopicsRequest::default()
            .topics(Some(
                [DeleteTopicState::default()
                    .name(Some(topic.into()))
                    .topic_id(NULL_TOPIC_ID)]
                .into(),
            ))
            .topic_names(None)
            .timeout_ms(timeout_ms);

        let encoded = Frame::request(header, delete_topics_request.into())?;

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
                    Body::DeleteTopicsResponse(DeleteTopicsResponse {
                        responses: Some(responses),
                        ..
                    }),
                ..
            } => match responses.as_slice() {
                [DeletableTopicResult { error_code, .. }] => {
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
