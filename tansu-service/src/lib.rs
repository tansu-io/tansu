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

use std::{result, sync::LazyLock};

use bytes::Bytes;
use opentelemetry::{InstrumentationScope, global, metrics::Meter};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use rama::{error::BoxError, tcp::TcpStream};
use tokio::io::AsyncReadExt as _;

pub mod api;
pub mod service;

pub type Result<T, E = BoxError> = result::Result<T, E>;

fn frame_length(encoded: [u8; 4]) -> usize {
    i32::from_be_bytes(encoded) as usize + encoded.len()
}

pub async fn read_frame(tcp_stream: &mut TcpStream) -> Result<Bytes> {
    let mut size = [0u8; 4];
    tcp_stream.read_exact(&mut size).await?;

    let mut buffer: Vec<u8> = vec![0u8; frame_length(size)];
    buffer[0..size.len()].copy_from_slice(&size[..]);
    _ = tcp_stream.read_exact(&mut buffer[4..]).await?;
    Ok(Bytes::from(buffer))
}

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});
