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

#![deny(
    nonstandard_style,
    rust_2018_idioms,
    rustdoc::broken_intra_doc_links,
    rustdoc::private_intra_doc_links,
    elided_lifetimes_in_paths
)]
#![forbid(non_ascii_idents, unsafe_code)]
#![warn(
    deprecated_in_future,
    missing_copy_implementations,
    missing_debug_implementations,
    unreachable_pub,
    unused_crate_dependencies,
    unused_import_braces,
    unused_labels,
    unused_lifetimes,
    unused_qualifications,
    unused_results
)]

use std::result;
use std::sync::Arc;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use condtype::num::Usize64;
use tokio::io::AsyncReadExt;

#[derive(Clone, thiserror::Error, Debug)]
pub enum Error {
    #[error("io")]
    Io(Arc<std::io::Error>),

    #[error("varint truncated")]
    VarIntTruncated,
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

pub type Result<R, E = Error> = result::Result<R, E>;

const CONTINUATION: u8 = 0b1000_0000;
const MASK: u8 = 0b0111_1111;

pub fn get_usize64(bytes: &mut Bytes) -> Result<Usize64> {
    let mut shift = 0u8;
    let mut accumulator = 0;
    let mut done = false;

    while !done {
        if !bytes.has_remaining() {
            return Err(Error::VarIntTruncated);
        }

        let byte = bytes.get_u8();

        if byte & CONTINUATION == CONTINUATION {
            let intermediate = (byte & MASK) as Usize64;
            accumulator += intermediate << shift;
            shift += 7;
        } else {
            accumulator += (byte as Usize64) << shift;
            done = true;
        }
    }

    Ok(accumulator)
}

pub fn get_u64(bytes: &mut Bytes) -> Result<u64> {
    let mut shift = 0u8;
    let mut accumulator = 0u64;
    let mut done = false;

    while !done {
        if !bytes.has_remaining() {
            return Err(Error::VarIntTruncated);
        }

        let byte = bytes.get_u8();

        if byte & CONTINUATION == CONTINUATION {
            let intermediate = (byte & MASK) as u64;
            accumulator += intermediate << shift;
            shift += 7;
        } else {
            accumulator += (byte as u64) << shift;
            done = true;
        }
    }

    Ok(accumulator)
}

pub async fn read_usize64<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<(Usize64, Bytes)> {
    let mut shift = 0u8;
    let mut accumulator = 0;
    let mut done = false;
    let mut bytes = BytesMut::new();

    while !done {
        let byte = r.read_u8().await?;
        bytes.put_u8(byte);

        if byte & CONTINUATION == CONTINUATION {
            let intermediate = (byte & MASK) as Usize64;
            accumulator += intermediate << shift;
            shift += 7;
        } else {
            accumulator += (byte as Usize64) << shift;
            done = true;
        }
    }

    Ok((accumulator, Bytes::from(bytes)))
}

pub async fn read_usize<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<(usize, Bytes)> {
    let mut shift = 0u8;
    let mut accumulator = 0;
    let mut done = false;
    let mut bytes = BytesMut::new();

    while !done {
        let byte = r.read_u8().await?;
        bytes.put_u8(byte);

        if byte & CONTINUATION == CONTINUATION {
            let intermediate = (byte & MASK) as usize;
            accumulator += intermediate << shift;
            shift += 7;
        } else {
            accumulator += (byte as usize) << shift;
            done = true;
        }
    }

    Ok((accumulator, Bytes::from(bytes)))
}

pub fn get_usize(bytes: &mut Bytes) -> Result<usize> {
    let mut shift = 0u8;
    let mut accumulator = 0usize;
    let mut done = false;

    while !done {
        if !bytes.has_remaining() {
            return Err(Error::VarIntTruncated);
        }

        let byte = bytes.get_u8();

        if byte & CONTINUATION == CONTINUATION {
            let intermediate = (byte & MASK) as usize;
            accumulator += intermediate << shift;
            shift += 7;
        } else {
            accumulator += (byte as usize) << shift;
            done = true;
        }
    }

    Ok(accumulator)
}

pub fn put_usize64_into(mut value: Usize64, encoded: &mut BytesMut) -> usize {
    let mut size = 1;
    while value >= CONTINUATION as Usize64 {
        encoded.put_u8(value as u8 | CONTINUATION);
        value >>= 7;
        size += 1;
    }

    encoded.put_u8(value as u8);
    size
}

pub fn put_u64_into(mut value: u64, encoded: &mut BytesMut) -> usize {
    let mut size = 1;
    while value >= CONTINUATION as u64 {
        encoded.put_u8(value as u8 | CONTINUATION);
        value >>= 7;
        size += 1;
    }

    encoded.put_u8(value as u8);
    size
}

pub fn put_usize64(value: Usize64) -> Bytes {
    let mut bytes = BytesMut::new();
    _ = put_usize64_into(value, &mut bytes);
    Bytes::from(bytes)
}

pub fn put_u64(value: u64) -> Bytes {
    let mut bytes = BytesMut::new();
    _ = put_u64_into(value, &mut bytes);
    Bytes::from(bytes)
}

pub fn put_usize_into(mut value: usize, encoded: &mut BytesMut) -> usize {
    let mut size = 1;
    while value >= CONTINUATION as usize {
        encoded.put_u8(value as u8 | CONTINUATION);
        value >>= 7;
        size += 1;
    }

    encoded.put_u8(value as u8);
    size
}

pub fn put_usize(value: usize) -> Bytes {
    let mut bytes = BytesMut::new();
    _ = put_usize_into(value, &mut bytes);
    Bytes::from(bytes)
}

pub mod blocking {
    use std::io::Read;
    use std::io::Write;

    use bytes::BufMut;
    use bytes::Bytes;
    use bytes::BytesMut;
    use condtype::num::Usize64;

    use super::Result;
    use super::CONTINUATION;
    use super::MASK;
    use crate::put_usize;
    use crate::put_usize64;

    pub fn read_usize64<R: Read>(r: &mut R) -> Result<(Usize64, Bytes)> {
        let mut shift = 0u8;
        let mut accumulator = 0;
        let mut done = false;
        let mut bytes = BytesMut::new();

        while !done {
            let mut byte = [0u8; 1];
            _ = r.read(&mut byte)?;
            bytes.put_u8(byte[0]);

            if byte[0] & CONTINUATION == CONTINUATION {
                let intermediate = (byte[0] & MASK) as Usize64;
                accumulator += intermediate << shift;
                shift += 7;
            } else {
                accumulator += (byte[0] as Usize64) << shift;
                done = true;
            }
        }

        Ok((accumulator, Bytes::from(bytes)))
    }

    pub fn read_usize<R: Read>(r: &mut R) -> Result<(usize, Bytes)> {
        let mut shift = 0u8;
        let mut accumulator = 0usize;
        let mut done = false;
        let mut bytes = BytesMut::new();

        while !done {
            let mut byte = [0u8; 1];
            _ = r.read(&mut byte)?;
            bytes.put_u8(byte[0]);

            if byte[0] & CONTINUATION == CONTINUATION {
                let intermediate = (byte[0] & MASK) as usize;
                accumulator += intermediate << shift;
                shift += 7;
            } else {
                accumulator += (byte[0] as usize) << shift;
                done = true;
            }
        }

        Ok((accumulator, Bytes::from(bytes)))
    }

    pub fn write_usize64<W: Write>(w: &mut W, value: Usize64) -> Result<usize> {
        let bytes = put_usize64(value);
        w.write_all(&bytes)?;
        Ok(bytes.len())
    }

    pub fn write_usize<W: Write>(w: &mut W, value: usize) -> Result<usize> {
        let bytes = put_usize(value);
        w.write_all(&bytes)?;
        Ok(bytes.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_u64_empty() {
        assert!(matches!(
            get_u64(&mut Bytes::new()),
            Err(Error::VarIntTruncated)
        ))
    }

    #[test]
    fn put_get_min_u64() -> Result<()> {
        let expected = u64::MIN;
        assert_eq!(expected, get_u64(&mut put_u64(expected))?);
        Ok(())
    }

    #[test]
    fn put_get_max_u64() -> Result<()> {
        let expected = u64::MAX;
        assert_eq!(expected, get_u64(&mut put_u64(expected))?);
        Ok(())
    }
}
