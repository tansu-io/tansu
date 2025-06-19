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

use std::result;

use bytes::Bytes;
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
