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

use super::ByteSize;
use crate::{Error, Result};
use serde::{
    de::{self, SeqAccess, Visitor},
    ser::SerializeSeq,
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::{any::type_name_of_val, fmt::Formatter, ops::Deref};
use tracing::debug;

const CONTINUATION: u8 = 0b1000_0000;
const MASK: u8 = 0b0111_1111;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct VarInt(pub i32);

impl Deref for VarInt {
    type Target = i32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl VarInt {
    #[allow(clippy::cast_sign_loss)]
    fn en_zigzag(decoded: i32) -> u32 {
        ((decoded << 1) ^ (decoded >> 31)) as u32
    }

    #[allow(clippy::cast_possible_wrap)]
    fn de_zigzag(encoded: u32) -> i32 {
        ((encoded >> 1) as i32) ^ -((encoded & 1) as i32)
    }

    pub fn serialize<S>(i: &i32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        debug!(?i);

        let mut v = Self::en_zigzag(*i);
        let mut s = serializer.serialize_seq(None)?;

        while v >= u32::from(CONTINUATION) {
            #[allow(clippy::cast_possible_truncation)]
            s.serialize_element(&(v as u8 | CONTINUATION))?;
            v >>= 7;
        }

        #[allow(clippy::cast_possible_truncation)]
        s.serialize_element(&(v as u8))?;
        s.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<i32, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VarIntVisitor;

        impl<'de> Visitor<'de> for VarIntVisitor {
            type Value = i32;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(stringify!(VarInt))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut shift = 0u8;
                let mut accumulator = 0u32;
                let mut done = false;

                while !done {
                    let byte = seq
                        .next_element::<u8>()?
                        .ok_or_else(|| de::Error::custom("u8"))?;

                    if byte & CONTINUATION == CONTINUATION {
                        let intermediate = u32::from(byte & MASK);
                        accumulator += intermediate << shift;
                        shift += 7;
                    } else {
                        accumulator += u32::from(byte) << shift;
                        done = true;
                    }
                }

                let i = VarInt::de_zigzag(accumulator);
                debug!("i: {i}");
                Ok(i)
            }
        }

        deserializer.deserialize_seq(VarIntVisitor)
    }

    pub fn size_inclusive(bs: &impl ByteSize) -> Result<usize> {
        bs.size_in_bytes().and_then(|size| {
            i32::try_from(size)
                .map_err(Into::into)
                .map(Self)
                .and_then(|vsz| vsz.size_in_bytes().map(|vsz| vsz + size))
        })
    }
}

impl ByteSize for VarInt {
    fn size_in_bytes(&self) -> Result<usize> {
        let mut bytes = 1;
        let mut v = Self::en_zigzag(self.0);
        while v >= u32::from(CONTINUATION) {
            v >>= 7;
            bytes += 1;
        }
        Ok(bytes)
    }
}

impl From<i32> for VarInt {
    fn from(value: i32) -> Self {
        Self(value)
    }
}

impl TryFrom<usize> for VarInt {
    type Error = Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        let v = i32::try_from(value)?;
        Ok(Self(v))
    }
}

impl Serialize for VarInt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Self::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for VarInt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::deserialize(deserializer).map(Self)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LongVarInt(pub i64);

impl Deref for LongVarInt {
    type Target = i64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl LongVarInt {
    #[allow(clippy::cast_sign_loss)]
    fn en_zigzag(decoded: i64) -> u64 {
        ((decoded << 1) ^ (decoded >> 63)) as u64
    }

    #[allow(clippy::cast_possible_wrap)]
    fn de_zigzag(encoded: u64) -> i64 {
        ((encoded >> 1) as i64) ^ -((encoded & 1) as i64)
    }

    pub fn serialize<S>(i: &i64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut v = Self::en_zigzag(*i);
        let mut s = serializer.serialize_seq(None)?;

        while v >= u64::from(CONTINUATION) {
            #[allow(clippy::cast_possible_truncation)]
            s.serialize_element(&(v as u8 | CONTINUATION))?;
            v >>= 7;
        }

        #[allow(clippy::cast_possible_truncation)]
        s.serialize_element(&(v as u8))?;
        s.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<i64, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct LongVarIntVisitor;

        impl<'de> Visitor<'de> for LongVarIntVisitor {
            type Value = i64;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(stringify!(LongVarInt))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut shift = 0u8;
                let mut accumulator = 0u64;
                let mut done = false;

                while !done {
                    let byte = seq
                        .next_element::<u8>()?
                        .ok_or_else(|| de::Error::custom("u8"))?;

                    if byte & CONTINUATION == CONTINUATION {
                        let intermediate = u64::from(byte & MASK);
                        accumulator += intermediate << shift;
                        shift += 7;
                    } else {
                        accumulator += u64::from(byte) << shift;
                        done = true;
                    }
                }

                Ok(LongVarInt::de_zigzag(accumulator))
            }
        }

        deserializer.deserialize_seq(LongVarIntVisitor)
    }

    pub fn size_inclusive(bs: &impl ByteSize) -> Result<usize> {
        bs.size_in_bytes().and_then(|size| {
            i64::try_from(size)
                .map_err(Into::into)
                .map(Self)
                .and_then(|vsz| vsz.size_in_bytes().map(|vsz| vsz + size))
        })
    }
}

impl ByteSize for LongVarInt {
    fn size_in_bytes(&self) -> Result<usize> {
        let mut bytes = 1;
        let mut v = Self::en_zigzag(self.0);
        while v >= u64::from(CONTINUATION) {
            v >>= 7;
            bytes += 1;
        }
        Ok(bytes)
    }
}

impl From<i64> for LongVarInt {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl TryFrom<usize> for LongVarInt {
    type Error = Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        let v = i64::try_from(value)?;
        Ok(Self(v))
    }
}

impl Serialize for LongVarInt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Self::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for LongVarInt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::deserialize(deserializer).map(Self)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct UnsignedVarInt(pub u32);

impl Deref for UnsignedVarInt {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl UnsignedVarInt {
    pub fn serialize<S>(i: &u32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut v = *i;
        let mut s = serializer.serialize_seq(None)?;

        while v >= u32::from(CONTINUATION) {
            #[allow(clippy::cast_possible_truncation)]
            s.serialize_element(&(v as u8 | CONTINUATION))?;
            v >>= 7;
        }

        #[allow(clippy::cast_possible_truncation)]
        s.serialize_element(&(v as u8))?;
        s.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u32, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = u32;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(stringify!(UnsignedVarInt))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                debug!("seq: {}", type_name_of_val(&seq));

                let mut shift = 0u8;
                let mut accumulator = 0u32;
                let mut done = false;

                while !done {
                    let byte = seq
                        .next_element::<u8>()?
                        .ok_or_else(|| de::Error::custom("byte"))?;

                    debug!("byte: {byte}");

                    if byte & CONTINUATION == CONTINUATION {
                        let intermediate = u32::from(byte & MASK);
                        accumulator += intermediate << shift;
                        shift += 7;
                    } else {
                        accumulator += u32::from(byte) << shift;
                        done = true;
                    }
                }

                debug!("accumulator: {accumulator}");

                Ok(accumulator)
            }
        }

        debug!("deserializer: {}", type_name_of_val(&deserializer));

        deserializer.deserialize_seq(V)
    }

    pub fn size_inclusive(bs: &impl ByteSize) -> Result<usize> {
        bs.size_in_bytes().and_then(|size| {
            u32::try_from(size)
                .map_err(Into::into)
                .map(Self)
                .and_then(|vsz| vsz.size_in_bytes().map(|vsz| vsz + size))
        })
    }
}

impl ByteSize for UnsignedVarInt {
    fn size_in_bytes(&self) -> Result<usize> {
        let mut bytes = 1;
        let mut v = self.0;
        while v >= u32::from(CONTINUATION) {
            v >>= 7;
            bytes += 1;
        }
        Ok(bytes)
    }
}

impl From<u32> for UnsignedVarInt {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<UnsignedVarInt> for u32 {
    fn from(value: UnsignedVarInt) -> Self {
        value.0
    }
}

impl From<UnsignedVarInt> for usize {
    fn from(value: UnsignedVarInt) -> Self {
        value.0 as usize
    }
}

impl TryFrom<usize> for UnsignedVarInt {
    type Error = Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        let v = u32::try_from(value)?;
        Ok(Self(v))
    }
}

impl Serialize for UnsignedVarInt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Self::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for UnsignedVarInt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::deserialize(deserializer).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ser::Encoder;

    // #[test]
    // fn serde_varint() -> Result<()> {
    //     check(&VarInt(-1))?;
    //     check(&VarInt(0))?;
    //     check(&VarInt(1))?;
    //     check(&VarInt(i32::MIN))?;
    //     check(&VarInt(i32::MAX))?;
    //     Ok(())
    // }

    // #[test]
    // fn serde_longvarint() -> Result<()> {
    //     check(&LongVarInt(-1))?;
    //     check(&LongVarInt(0))?;
    //     check(&LongVarInt(1))?;
    //     check(&LongVarInt(i64::from(i32::MIN)))?;
    //     check(&LongVarInt(i64::from(i32::MAX)))?;
    //     check(&LongVarInt(i64::MIN))?;
    //     check(&LongVarInt(i64::MAX))?;
    //     Ok(())
    // }

    // #[test]
    // fn serde_unsigned_varint() -> Result<()> {
    //     check(&UnsignedVarInt(0))?;
    //     check(&UnsignedVarInt(1))?;
    //     check(&UnsignedVarInt(u32::MAX))?;
    //     Ok(())
    // }

    #[test]
    fn encode_varint_signed_one() -> Result<()> {
        let mut encoded = Vec::new();
        let mut serializer = Encoder::new(&mut encoded);
        let decoded = VarInt(1);
        decoded.serialize(&mut serializer)?;

        assert_eq!(vec![2u8], encoded);
        Ok(())
    }

    #[test]
    fn encode_varint_unsigned_one() -> Result<()> {
        let mut encoded = Vec::new();
        let mut serializer = Encoder::new(&mut encoded);
        let decoded = UnsignedVarInt(1);
        decoded.serialize(&mut serializer)?;

        assert_eq!(vec![1u8], encoded);
        Ok(())
    }

    #[test]
    fn encode_varint_signed_zero() -> Result<()> {
        let mut encoded = Vec::new();
        let mut serializer = Encoder::new(&mut encoded);
        let decoded = VarInt(0);
        decoded.serialize(&mut serializer)?;

        assert_eq!(vec![0u8], encoded);
        Ok(())
    }

    // #[test]
    // fn decode_varint_minus_one() -> Result<()> {
    //     assert_eq!(-1, Decoder::decode::<VarInt>(&[1u8]).map(|v| v.0)?);
    //     Ok(())
    // }
}
