// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use bytes::Bytes;
use rama::{Context, context::Extensions, matcher::Matcher, tcp::TcpStream};
use std::fmt::Debug;
use tansu_kafka_sans_io::{Body, Frame, Header};
use tokio::io::AsyncReadExt;
use tracing::debug;

use crate::Error;

pub mod metadata;
pub mod produce;

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiKey(pub i16);

impl AsRef<i16> for ApiKey {
    fn as_ref(&self) -> &i16 {
        &self.0
    }
}

pub type ApiVersion = i16;
pub type CorrelationId = i32;
pub type ClientId = Option<String>;

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct ApiRequest {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,
    pub client_id: ClientId,
    pub body: Body,
}

impl TryFrom<ApiRequest> for Bytes {
    type Error = Error;

    fn try_from(api_request: ApiRequest) -> Result<Self, Self::Error> {
        Frame::request(
            Header::Request {
                api_key: api_request.api_key.0,
                api_version: api_request.api_version,
                correlation_id: api_request.correlation_id,
                client_id: api_request.client_id,
            },
            api_request.body,
        )
        .map(Bytes::from)
        .map_err(Into::into)
    }
}

impl<State> Matcher<State, ApiRequest> for ApiKey
where
    State: Debug,
{
    fn matches(
        &self,
        ext: Option<&mut Extensions>,
        ctx: &Context<State>,
        req: &ApiRequest,
    ) -> bool {
        debug!(?ext, ?ctx, api_key = self.0, ?req);
        self.0 == req.api_key.0
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct ApiResponse {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,
    pub body: Body,
}

impl TryFrom<ApiResponse> for Bytes {
    type Error = Error;

    fn try_from(api_response: ApiResponse) -> Result<Self, Self::Error> {
        Frame::response(
            Header::Response {
                correlation_id: api_response.correlation_id,
            },
            api_response.body,
            api_response.api_key.0,
            api_response.api_version,
        )
        .map(Bytes::from)
        .map_err(Into::into)
    }
}

fn frame_length(encoded: [u8; 4]) -> usize {
    i32::from_be_bytes(encoded) as usize + encoded.len()
}

pub async fn read_frame(tcp_stream: &mut TcpStream) -> Result<Bytes, Error> {
    let mut size = [0u8; 4];
    tcp_stream.read_exact(&mut size).await?;

    let mut buffer: Vec<u8> = vec![0u8; frame_length(size)];
    buffer[0..size.len()].copy_from_slice(&size[..]);
    _ = tcp_stream.read_exact(&mut buffer[4..]).await?;
    Ok(Bytes::from(buffer))
}

pub fn read_api_request(buffer: Bytes) -> Result<ApiRequest, Error> {
    match Frame::request_from_bytes(&buffer[..])? {
        Frame {
            header:
                Header::Request {
                    api_key,
                    api_version,
                    correlation_id,
                    client_id,
                },
            body,
            ..
        } => Ok(ApiRequest {
            api_key: ApiKey(api_key),
            api_version,
            correlation_id,
            client_id,
            body,
        }),

        frame => Err(Error::UnexpectedType(Box::new(frame))),
    }
}

pub fn read_api_response(
    buffer: Bytes,
    api_key: ApiKey,
    api_version: ApiVersion,
) -> Result<ApiResponse, Error> {
    match Frame::response_from_bytes(&buffer[..], api_key.0, api_version)? {
        Frame {
            header: Header::Response { correlation_id },
            body,
            ..
        } => Ok(ApiResponse {
            api_key,
            api_version,
            correlation_id,
            body,
        }),

        frame => Err(Error::UnexpectedType(Box::new(frame))),
    }
}
