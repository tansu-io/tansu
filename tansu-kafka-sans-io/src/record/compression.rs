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
    fmt::Formatter,
    io::{BufRead, Read},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use flate2::read::GzDecoder;
use serde::{
    de::{self, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use tracing::debug;

use crate::{Decoder, Error, Result};

use super::Record;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Batch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub record_count: u32,
    pub record_data: Bytes,
}

impl Batch {
    fn compression(&self) -> Result<Compression> {
        Compression::try_from(self.attributes)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum Compression {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl TryFrom<i16> for Compression {
    type Error = Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value & 0b111i16 {
            0 => Ok(Self::None),
            1 => Ok(Self::Gzip),
            2 => Ok(Self::Snappy),
            3 => Ok(Self::Lz4),
            4 => Ok(Self::Zstd),
            otherwise => Err(Error::UnknownCompressionType(otherwise)),
        }
    }
}

impl From<Compression> for i16 {
    fn from(value: Compression) -> Self {
        match value {
            Compression::None => 0,
            Compression::Gzip => 1,
            Compression::Snappy => 2,
            Compression::Lz4 => 3,
            Compression::Zstd => 4,
        }
    }
}

impl Compression {
    fn inflator(&self, deflated: impl BufRead + 'static) -> Result<Box<dyn Read>> {
        match self {
            Compression::None => Ok(Box::new(deflated)),
            Compression::Gzip => Ok(Box::new(GzDecoder::new(deflated))),
            Compression::Snappy => Ok(Box::new(snap::read::FrameDecoder::new(deflated))),
            Compression::Lz4 => lz4::Decoder::new(deflated)
                .map(Box::new)
                .map(|boxed| boxed as Box<dyn Read>)
                .map_err(Into::into),
            Compression::Zstd => zstd::stream::read::Decoder::with_buffer(deflated)
                .map(Box::new)
                .map(|boxed| boxed as Box<dyn Read>)
                .map_err(Into::into),
        }
    }
}

impl TryFrom<Batch> for Vec<Record> {
    type Error = Error;

    fn try_from(batch: Batch) -> Result<Self, Self::Error> {
        let record_count = usize::try_from(batch.record_count)?;

        let mut reader = batch
            .compression()
            .and_then(|compression| compression.inflator(batch.record_data.reader()))?;

        let mut decoder = Decoder::new(&mut reader);
        let mut records = Vec::with_capacity(record_count);

        for i in 0..record_count {
            let record = Record::deserialize(&mut decoder)?;
            records.push(record);
        }

        Ok(records)
    }
}

const FIXED_BATCH_LENGTH: usize =
    // partition leader epoch
    size_of::<i32>()
    // magic
    + size_of::<i8>()
    // CRC
    + size_of::<u32>()
    // attributes
    + size_of::<i16>()
    // last_offset_delta
    + size_of::<i32>()
    // base timestamp
    + size_of::<i64>()
    // max timestamp
    + size_of::<i64>()
    // producer id
    + size_of::<i64>()
    // producer epoch
    + size_of::<i16>()
    // base sequence
    + size_of::<i32>()
    // record count
    + size_of::<u32>();

impl<'de> Deserialize<'de> for Batch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = Batch;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(stringify!(Batch))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let base_offset = seq
                    .next_element::<i64>()?
                    .ok_or(<A::Error as de::Error>::custom("base_offset"))?;
                let batch_length = seq
                    .next_element::<i32>()?
                    .ok_or(<A::Error as de::Error>::custom("batch_length"))?;
                let partition_leader_epoch = seq
                    .next_element::<i32>()?
                    .ok_or(<A::Error as de::Error>::custom("partition_leader_epoch"))?;
                let magic = seq
                    .next_element::<i8>()?
                    .ok_or(<A::Error as de::Error>::custom("magic"))?;
                let crc = seq
                    .next_element::<u32>()?
                    .ok_or(<A::Error as de::Error>::custom("crc"))?;
                let attributes = seq
                    .next_element::<i16>()?
                    .ok_or(<A::Error as de::Error>::custom("attributes"))?;
                let last_offset_delta = seq
                    .next_element::<i32>()?
                    .ok_or(<A::Error as de::Error>::custom("last_offset_delta"))?;
                let base_timestamp = seq
                    .next_element::<i64>()?
                    .ok_or(<A::Error as de::Error>::custom("base_timestamp"))?;
                let max_timestamp = seq
                    .next_element::<i64>()?
                    .ok_or(<A::Error as de::Error>::custom("max_timestamp"))?;
                let producer_id = seq
                    .next_element::<i64>()?
                    .ok_or(<A::Error as de::Error>::custom("producer_id"))?;
                let producer_epoch = seq
                    .next_element::<i16>()?
                    .ok_or(<A::Error as de::Error>::custom("producer_epoch"))?;
                let base_sequence = seq
                    .next_element::<i32>()?
                    .ok_or(<A::Error as de::Error>::custom("base_sequence"))?;
                let record_count = seq
                    .next_element::<u32>()?
                    .ok_or(<A::Error as de::Error>::custom("record_count"))?;

                let record_data_size = usize::try_from(batch_length)
                    .map_err(|e| {
                        <A::Error as de::Error>::custom(format!(
                            "base_offset: {base_offset}, caused: {e:?}"
                        ))
                    })
                    .map(|batch_length| batch_length - FIXED_BATCH_LENGTH)?;

                debug!(?record_data_size);

                let mut record_data = BytesMut::with_capacity(record_data_size);

                for n in 0..record_data_size {
                    let byte = seq
                        .next_element::<u8>()?
                        .ok_or(<A::Error as de::Error>::custom("record_data: {n}"))?;

                    record_data.put_u8(byte);
                }

                let record_data = Bytes::from(record_data);

                let batch = Batch {
                    base_offset,
                    batch_length,
                    partition_leader_epoch,
                    magic,
                    crc,
                    attributes,
                    last_offset_delta,
                    base_timestamp,
                    max_timestamp,
                    producer_id,
                    producer_epoch,
                    base_sequence,
                    record_count,
                    record_data,
                };

                Ok(batch)
            }
        }

        deserializer.deserialize_seq(V)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    use tracing::subscriber::DefaultGuard;

    #[cfg(miri)]
    fn init_tracing() -> Result<()> {
        Ok(())
    }

    #[cfg(not(miri))]
    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        use tracing_subscriber::fmt::format::FmtSpan;

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_max_level(tracing::Level::DEBUG)
                .with_span_events(FmtSpan::ACTIVE)
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME")))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    const LOREM: &[u8] = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do \
    eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad \
    minim veniam, quis nostrud exercitation ullamco laboris nisi ut \
    aliquip ex ea commodo consequat. Duis aute irure dolor in \
    reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla \
    pariatur. Excepteur sint occaecat cupidatat non proident, sunt in \
    culpa qui officia deserunt mollit anim id est laborum.";

    #[test]
    fn decode_gzip() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 89, 0, 0, 0, 0, 2, 198, 48, 56, 83, 0, 1, 0, 0, 0, 0,
            0, 0, 1, 145, 183, 231, 239, 158, 0, 0, 1, 145, 183, 231, 239, 158, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 31, 139, 8, 0, 0, 0, 0,
            0, 0, 19, 53, 144, 205, 81, 67, 49, 12, 132, 31, 23, 104, 99, 11, 200, 188, 42, 224,
            198, 149, 2, 132, 172, 4, 205, 248, 47, 182, 148, 73, 9, 169, 153, 19, 50, 15, 110,
            150, 37, 173, 118, 191, 199, 203, 182, 109, 79, 223, 207, 239, 109, 72, 129, 246, 233,
            5, 169, 229, 54, 48, 213, 64, 69, 236, 4, 110, 117, 10, 155, 152, 15, 80, 210, 174,
            147, 181, 94, 32, 89, 163, 57, 37, 197, 2, 68, 125, 150, 150, 96, 82, 122, 44, 107,
            101, 77, 154, 188, 26, 220, 144, 233, 51, 228, 33, 118, 72, 11, 10, 93, 42, 129, 178,
            94, 157, 118, 124, 24, 164, 106, 9, 109, 20, 93, 143, 91, 148, 84, 78, 184, 186, 78,
            212, 54, 109, 120, 130, 220, 101, 176, 26, 153, 182, 10, 207, 153, 10, 183, 67, 121,
            13, 233, 212, 117, 233, 87, 82, 123, 12, 67, 40, 140, 151, 240, 212, 142, 0, 113, 202,
            118, 188, 46, 73, 114, 19, 232, 240, 112, 114, 100, 213, 138, 33, 125, 200, 151, 212,
            36, 35, 130, 199, 199, 173, 101, 239, 113, 78, 194, 78, 36, 133, 204, 41, 96, 205, 249,
            159, 80, 4, 114, 156, 253, 162, 100, 168, 203, 16, 58, 141, 40, 124, 236, 120, 187,
            179, 116, 19, 95, 24, 131, 65, 99, 38, 225, 152, 99, 239, 154, 200, 214, 70, 164, 232,
            163, 105, 146, 186, 40, 46, 82, 113, 148, 61, 119, 90, 185, 209, 206, 103, 101, 37, 36,
            153, 50, 86, 183, 180, 188, 108, 208, 2, 164, 129, 99, 254, 113, 245, 178, 111, 63,
            143, 62, 223, 101, 198, 1, 0, 0, 0, 0, 0,
        ];

        let mut c = Cursor::new(encoded);

        let mut decoder = Decoder::new(&mut c);

        let batch = Batch::deserialize(&mut decoder)?;
        assert_eq!(Compression::Gzip, Compression::try_from(batch.attributes)?);

        let records: Vec<Record> = batch.try_into()?;

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            records
        );

        Ok(())
    }

    #[test]
    fn decode_zstd() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 91, 0, 0, 0, 0, 2, 200, 21, 172, 244, 0, 4, 0, 0, 0,
            0, 0, 0, 1, 145, 183, 250, 201, 221, 0, 0, 1, 145, 183, 250, 201, 221, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 40, 181, 47, 253, 0,
            88, 13, 9, 0, 70, 217, 64, 36, 160, 37, 73, 7, 255, 255, 255, 255, 143, 174, 211, 102,
            147, 114, 239, 182, 165, 188, 148, 244, 91, 75, 123, 39, 146, 211, 241, 3, 167, 201,
            64, 245, 234, 24, 45, 9, 56, 0, 53, 0, 53, 0, 197, 44, 90, 146, 147, 15, 123, 209, 29,
            99, 44, 57, 147, 242, 238, 145, 18, 167, 14, 240, 197, 53, 216, 71, 250, 57, 169, 162,
            68, 227, 112, 178, 27, 29, 160, 77, 21, 159, 138, 174, 28, 169, 201, 116, 94, 99, 116,
            4, 0, 36, 40, 40, 138, 34, 0, 130, 229, 38, 220, 115, 204, 62, 221, 154, 126, 195, 84,
            41, 89, 187, 99, 225, 7, 114, 194, 37, 137, 48, 157, 53, 61, 15, 29, 152, 186, 25, 81,
            115, 187, 41, 169, 154, 155, 139, 5, 74, 168, 60, 188, 84, 203, 12, 101, 106, 116, 141,
            206, 64, 149, 40, 177, 142, 234, 180, 74, 73, 43, 214, 13, 89, 104, 186, 46, 229, 163,
            78, 201, 197, 23, 35, 106, 44, 60, 89, 14, 81, 123, 241, 200, 196, 124, 192, 232, 24,
            213, 94, 163, 68, 175, 49, 171, 233, 41, 160, 4, 172, 60, 28, 57, 99, 110, 52, 5, 193,
            100, 114, 201, 107, 110, 85, 124, 41, 103, 138, 150, 200, 201, 164, 245, 241, 66, 225,
            250, 60, 180, 78, 201, 97, 153, 58, 64, 51, 249, 161, 83, 59, 43, 22, 22, 249, 201, 81,
            172, 225, 26, 227, 138, 246, 84, 148, 198, 227, 145, 24, 80, 66, 8, 0, 220, 218, 44,
            15, 30, 45, 186, 100, 95, 73, 49, 124, 17, 109, 0, 43, 70, 43, 93, 140, 227, 122, 67,
            136, 2, 0, 0, 0,
        ];

        let mut c = Cursor::new(encoded);

        let mut decoder = Decoder::new(&mut c);

        let batch = Batch::deserialize(&mut decoder)?;
        assert_eq!(Compression::Zstd, Compression::try_from(batch.attributes)?);

        let records: Vec<Record> = batch.try_into()?;

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            records
        );

        Ok(())
    }

    #[test]
    fn decode_lz4() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 237, 0, 0, 0, 0, 2, 43, 216, 167, 237, 0, 3, 0, 0, 0,
            0, 0, 0, 1, 145, 184, 77, 37, 242, 0, 0, 1, 145, 184, 77, 37, 242, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 4, 34, 77, 24, 96, 64,
            130, 173, 1, 0, 0, 242, 95, 136, 7, 0, 0, 0, 1, 250, 6, 76, 111, 114, 101, 109, 32,
            105, 112, 115, 117, 109, 32, 100, 111, 108, 111, 114, 32, 115, 105, 116, 32, 97, 109,
            101, 116, 44, 32, 99, 111, 110, 115, 101, 99, 116, 101, 116, 117, 114, 32, 97, 100,
            105, 112, 105, 115, 99, 105, 110, 103, 32, 101, 108, 105, 116, 44, 32, 115, 101, 100,
            32, 100, 111, 32, 101, 105, 117, 115, 109, 111, 100, 32, 116, 101, 109, 112, 111, 114,
            32, 105, 110, 99, 105, 100, 105, 100, 117, 110, 116, 32, 117, 116, 32, 108, 97, 98,
            111, 114, 101, 32, 101, 116, 91, 0, 240, 14, 101, 32, 109, 97, 103, 110, 97, 32, 97,
            108, 105, 113, 117, 97, 46, 32, 85, 116, 32, 101, 110, 105, 109, 32, 97, 100, 32, 109,
            105, 9, 0, 242, 26, 118, 101, 110, 105, 97, 109, 44, 32, 113, 117, 105, 115, 32, 110,
            111, 115, 116, 114, 117, 100, 32, 101, 120, 101, 114, 99, 105, 116, 97, 116, 105, 111,
            110, 32, 117, 108, 108, 97, 109, 99, 111, 90, 0, 0, 37, 0, 98, 105, 115, 105, 32, 117,
            116, 83, 0, 242, 1, 105, 112, 32, 101, 120, 32, 101, 97, 32, 99, 111, 109, 109, 111,
            100, 111, 193, 0, 112, 113, 117, 97, 116, 46, 32, 68, 83, 0, 162, 97, 117, 116, 101,
            32, 105, 114, 117, 114, 101, 145, 0, 240, 2, 32, 105, 110, 32, 114, 101, 112, 114, 101,
            104, 101, 110, 100, 101, 114, 105, 116, 17, 0, 176, 118, 111, 108, 117, 112, 116, 97,
            116, 101, 32, 118, 234, 0, 164, 32, 101, 115, 115, 101, 32, 99, 105, 108, 108, 34, 1,
            208, 101, 32, 101, 117, 32, 102, 117, 103, 105, 97, 116, 32, 110, 145, 0, 240, 4, 32,
            112, 97, 114, 105, 97, 116, 117, 114, 46, 32, 69, 120, 99, 101, 112, 116, 101, 117, 71,
            1, 240, 4, 110, 116, 32, 111, 99, 99, 97, 101, 99, 97, 116, 32, 99, 117, 112, 105, 100,
            97, 116, 50, 0, 160, 111, 110, 32, 112, 114, 111, 105, 100, 101, 110, 70, 1, 0, 42, 1,
            128, 105, 110, 32, 99, 117, 108, 112, 97, 248, 0, 224, 32, 111, 102, 102, 105, 99, 105,
            97, 32, 100, 101, 115, 101, 114, 30, 0, 64, 109, 111, 108, 108, 147, 1, 0, 33, 1, 240,
            1, 105, 100, 32, 101, 115, 116, 32, 108, 97, 98, 111, 114, 117, 109, 46, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];

        let mut c = Cursor::new(encoded);

        let mut decoder = Decoder::new(&mut c);

        let batch = Batch::deserialize(&mut decoder)?;
        assert_eq!(Compression::Lz4, Compression::try_from(batch.attributes)?);

        let records: Vec<Record> = batch.try_into()?;

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            records
        );

        Ok(())
    }

    #[ignore]
    #[test]
    fn decode_snappy() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 218, 0, 0, 0, 0, 2, 228, 189, 111, 249, 0, 2, 0, 0, 0,
            0, 0, 0, 1, 145, 184, 92, 90, 192, 0, 0, 1, 145, 184, 92, 90, 192, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 198, 3, 240, 111, 136, 7,
            0, 0, 0, 1, 250, 6, 76, 111, 114, 101, 109, 32, 105, 112, 115, 117, 109, 32, 100, 111,
            108, 111, 114, 32, 115, 105, 116, 32, 97, 109, 101, 116, 44, 32, 99, 111, 110, 115,
            101, 99, 116, 101, 116, 117, 114, 32, 97, 100, 105, 112, 105, 115, 99, 105, 110, 103,
            32, 101, 108, 105, 116, 44, 32, 115, 101, 100, 32, 100, 111, 32, 101, 105, 117, 115,
            109, 111, 100, 32, 116, 101, 109, 112, 111, 114, 32, 105, 110, 99, 105, 100, 105, 100,
            117, 110, 116, 32, 117, 116, 32, 108, 97, 98, 111, 114, 101, 32, 101, 116, 32, 100, 1,
            91, 112, 101, 32, 109, 97, 103, 110, 97, 32, 97, 108, 105, 113, 117, 97, 46, 32, 85,
            116, 32, 101, 110, 105, 109, 32, 97, 100, 32, 109, 105, 1, 9, 160, 118, 101, 110, 105,
            97, 109, 44, 32, 113, 117, 105, 115, 32, 110, 111, 115, 116, 114, 117, 100, 32, 101,
            120, 101, 114, 99, 105, 116, 97, 116, 105, 111, 110, 32, 117, 108, 108, 97, 109, 99,
            111, 9, 90, 1, 37, 8, 105, 115, 105, 1, 106, 5, 83, 60, 105, 112, 32, 101, 120, 32,
            101, 97, 32, 99, 111, 109, 109, 111, 100, 111, 9, 193, 24, 113, 117, 97, 116, 46, 32,
            68, 1, 83, 36, 97, 117, 116, 101, 32, 105, 114, 117, 114, 101, 13, 236, 60, 105, 110,
            32, 114, 101, 112, 114, 101, 104, 101, 110, 100, 101, 114, 105, 116, 1, 17, 40, 118,
            111, 108, 117, 112, 116, 97, 116, 101, 32, 118, 1, 234, 36, 32, 101, 115, 115, 101, 32,
            99, 105, 108, 108, 49, 34, 232, 101, 32, 101, 117, 32, 102, 117, 103, 105, 97, 116, 32,
            110, 117, 108, 108, 97, 32, 112, 97, 114, 105, 97, 116, 117, 114, 46, 32, 69, 120, 99,
            101, 112, 116, 101, 117, 114, 32, 115, 105, 110, 116, 32, 111, 99, 99, 97, 101, 99, 97,
            116, 32, 99, 117, 112, 105, 100, 97, 116, 1, 50, 60, 111, 110, 32, 112, 114, 111, 105,
            100, 101, 110, 116, 44, 32, 115, 117, 110, 5, 117, 88, 99, 117, 108, 112, 97, 32, 113,
            117, 105, 32, 111, 102, 102, 105, 99, 105, 97, 32, 100, 101, 115, 101, 114, 1, 30, 12,
            109, 111, 108, 108, 33, 147, 33, 33, 60, 105, 100, 32, 101, 115, 116, 32, 108, 97, 98,
            111, 114, 117, 109, 46, 0, 0, 0, 0,
        ];

        let mut c = Cursor::new(encoded);

        let mut decoder = Decoder::new(&mut c);

        let batch = Batch::deserialize(&mut decoder)?;
        assert_eq!(
            Compression::Snappy,
            Compression::try_from(batch.attributes)?
        );

        let records: Vec<Record> = batch.try_into()?;

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            records
        );

        Ok(())
    }
}
