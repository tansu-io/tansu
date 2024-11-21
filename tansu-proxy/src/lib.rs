// Copyright ⓒ 2024 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::{
    fmt,
    io::{self, ErrorKind},
    result,
    sync::Arc,
};
use tansu_kafka_sans_io::{Frame, Header};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::{debug, error, info, span, Instrument, Level};
use url::Url;

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Error, Debug)]
pub enum Error {
    Io(Arc<io::Error>),
    Protocol(#[from] tansu_kafka_sans_io::Error),
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub struct Proxy {
    listener: Url,
    origin: Url,
}

impl Proxy {
    pub fn new(listener: Url, origin: Url) -> Self {
        Self { listener, origin }
    }

    pub async fn listen(&self) -> Result<()> {
        debug!(%self.listener);

        let listener = TcpListener::bind(format!(
            "{}:{}",
            self.listener.host_str().unwrap(),
            self.listener.port().unwrap()
        ))
        .await?;

        loop {
            let (stream, addr) = listener.accept().await?;

            let mut connection = Connection::open(&self.origin, stream).await?;

            _ = tokio::spawn(async move {
                let span = span!(Level::DEBUG, "peer", addr = %addr);

                async move {
                    match connection.stream_handler().await {
                        Err(ref error @ Error::Io(ref io))
                            if io.kind() == ErrorKind::UnexpectedEof =>
                        {
                            info!(?error);
                        }

                        Err(error) => {
                            error!(?error);
                        }

                        Ok(_) => {}
                    }
                }
                .instrument(span)
                .await
            });
        }
    }
}

struct Connection {
    proxy: TcpStream,
    origin: TcpStream,
}

impl Connection {
    async fn open(origin: &Url, proxy: TcpStream) -> Result<Self> {
        debug!(%origin, ?proxy);

        TcpStream::connect(format!(
            "{}:{}",
            origin.host_str().unwrap(),
            origin.port().unwrap()
        ))
        .await
        .map(|origin| Self { proxy, origin })
        .map_err(Into::into)
    }

    fn frame_length(encoded: [u8; 4]) -> usize {
        i32::from_be_bytes(encoded) as usize + encoded.len()
    }

    async fn stream_handler(&mut self) -> Result<()> {
        let mut size = [0u8; 4];

        loop {
            _ = self.proxy.read_exact(&mut size).await?;

            let mut request_buffer: Vec<u8> = vec![0u8; Self::frame_length(size)];
            request_buffer[0..size.len()].copy_from_slice(&size[..]);
            _ = self.proxy.read_exact(&mut request_buffer[4..]).await?;

            debug!(?request_buffer);

            self.origin.write_all(&request_buffer).await?;

            _ = self.origin.read_exact(&mut size).await?;

            let mut response_buffer: Vec<u8> = vec![0u8; Self::frame_length(size)];
            response_buffer[0..size.len()].copy_from_slice(&size[..]);
            _ = self
                .origin
                .read_exact(&mut response_buffer[size.len()..])
                .await?;
            debug!(?response_buffer);

            self.proxy.write_all(&response_buffer).await?;

            if let Ok(
                request @ Frame {
                    header:
                        Header::Request {
                            api_key,
                            api_version,
                            ..
                        },
                    ..
                },
            ) = Frame::request_from_bytes(&request_buffer)
            {
                debug!(?request);
                debug!(body = ?request.body);

                let response = Frame::response_from_bytes(&response_buffer, api_key, api_version)?;
                debug!(?response);
                debug!(body = ?response.body);
            }
        }
    }
}
