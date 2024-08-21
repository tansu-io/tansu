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

use crate::{
    primitive::{
        varint::{UnsignedVarInt, VarInt},
        ByteSize,
    },
    Result,
};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{
    de::{self, SeqAccess, Visitor},
    ser::{self, SerializeSeq, SerializeStruct},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::{
    fmt::{self, Formatter},
    marker::PhantomData,
};
use tracing::debug;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Octets(pub Option<Bytes>);

impl ByteSize for Octets {
    fn size_in_bytes(&self) -> Result<usize> {
        self.0.as_ref().map_or_else(
            || VarInt(-1).size_in_bytes(),
            |bytes| {
                VarInt::try_from(bytes.len())
                    .and_then(|v| v.size_in_bytes())
                    .map(|vlen| vlen + bytes.len())
            },
        )
    }
}

impl Octets {
    #[must_use]
    pub fn empty() -> Self {
        Self(None)
    }

    pub fn serialize<S>(i: &Option<Bytes>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match i {
            None => {
                let mut s = serializer.serialize_struct(stringify!(Octets), 1)?;
                s.serialize_field("length", &VarInt(-1))?;
                s.end()
            }

            Some(bytes) => {
                let mut s = serializer.serialize_struct(stringify!(Octets), 2)?;

                i32::try_from(bytes.len())
                    .map_err(|e| ser::Error::custom(e.to_string()))
                    .map(VarInt)
                    .and_then(|length| s.serialize_field("length", &length))
                    .and(s.serialize_field("data", &bytes))
                    .and(s.end())
            }
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Bytes>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = Option<Bytes>;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str(stringify!(Bytes))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut length = seq
                    .next_element::<VarInt>()?
                    .ok_or_else(|| de::Error::custom("length"))?
                    .0;

                debug!(?length);

                if length == -1 {
                    Ok(None)
                } else {
                    let mut r = usize::try_from(length)
                        .map(BytesMut::with_capacity)
                        .map_err(|error| de::Error::custom(format!("{error:?}")))?;

                    while length >= 1 {
                        let char = seq
                            .next_element::<u8>()?
                            .ok_or_else(|| de::Error::custom("byte"))?;

                        r.put_u8(char);
                        length -= 1;
                    }

                    debug!(?r);

                    Ok(Some(r.into()))
                }
            }
        }

        deserializer.deserialize_seq(V)
    }
}

impl From<Octets> for Option<Bytes> {
    fn from(value: Octets) -> Self {
        value.0
    }
}

impl From<Bytes> for Octets {
    fn from(value: Bytes) -> Self {
        Self(Some(value))
    }
}

impl From<Option<Bytes>> for Octets {
    fn from(value: Option<Bytes>) -> Self {
        Self(value)
    }
}

impl From<Option<Vec<u8>>> for Octets {
    fn from(value: Option<Vec<u8>>) -> Self {
        Self(value.map(Into::into))
    }
}

impl From<Vec<u8>> for Octets {
    fn from(value: Vec<u8>) -> Self {
        Self(Some(value.into()))
    }
}

impl From<&[u8]> for Octets {
    fn from(value: &[u8]) -> Self {
        Self(Some(Bytes::copy_from_slice(value)))
    }
}

impl Serialize for Octets {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Self::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for Octets {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::deserialize(deserializer).map(Self)
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct VarIntSequence<T>(pub Vec<T>);

impl<T> IntoIterator for VarIntSequence<T> {
    type Item = T;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T> FromIterator<T> for VarIntSequence<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut c = Self(vec![]);
        for i in iter {
            c.0.push(i);
        }
        c
    }
}

impl<T> From<Vec<T>> for VarIntSequence<T> {
    fn from(value: Vec<T>) -> Self {
        Self(value)
    }
}

impl<T> ByteSize for VarIntSequence<T>
where
    T: ByteSize,
{
    fn size_in_bytes(&self) -> Result<usize> {
        self.0.iter().try_fold(
            i32::try_from(self.0.len())
                .map_err(Into::into)
                .map(VarInt)
                .and_then(|vlen| vlen.size_in_bytes())?,
            |acc, t| t.size_in_bytes().map(|tlen| acc + tlen),
        )
    }
}

impl<T> VarIntSequence<T> {
    pub fn serialize<S>(v: &Vec<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize,
    {
        let mut s = serializer.serialize_seq(Some(v.len()))?;

        i32::try_from(v.len())
            .map_err(|e| ser::Error::custom(e.to_string()))
            .map(VarInt)
            .and_then(|length| s.serialize_element(&length))?;

        for i in v {
            s.serialize_element(&i)?;
        }
        s.end()
    }
}

impl<T> VarIntSequence<T>
where
    T: Serialize,
{
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        struct V<T>(PhantomData<T>);

        impl<'de, T> Visitor<'de> for V<T>
        where
            T: Deserialize<'de>,
        {
            type Value = Vec<T>;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str(stringify!(Sequence))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                seq.next_element::<VarInt>()?
                    .ok_or_else(|| <A::Error as de::Error>::custom("length"))
                    .map(|v| v.0)
                    .inspect(|length| debug!("length: {length}"))
                    .and_then(|length| {
                        (0..length).try_fold(
                            Vec::with_capacity(length.try_into().map_err(|e| {
                                <A::Error as de::Error>::custom(format!(
                                    "length: {length}, caused: {e:?}"
                                ))
                            })?),
                            |mut acc, _| {
                                seq.next_element::<T>()?
                                    .ok_or_else(|| <A::Error as de::Error>::custom("item"))
                                    .map(|t| {
                                        acc.push(t);
                                        acc
                                    })
                            },
                        )
                    })
            }
        }

        deserializer.deserialize_seq(V(PhantomData))
    }
}

impl<T: Serialize> Serialize for VarIntSequence<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Self::serialize(&self.0, serializer)
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Sequence<T>(pub Vec<T>);

impl<T> IntoIterator for Sequence<T> {
    type Item = T;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T> FromIterator<T> for Sequence<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut c = Self(vec![]);
        for i in iter {
            c.0.push(i);
        }
        c
    }
}

impl<T> From<Vec<T>> for Sequence<T> {
    fn from(value: Vec<T>) -> Self {
        Self(value)
    }
}

impl<T: Serialize> Sequence<T> {
    pub fn serialize<S>(v: &Vec<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_seq(Some(v.len()))?;

        u32::try_from(v.len())
            .map_err(|e| ser::Error::custom(e.to_string()))
            .and_then(|length| s.serialize_element(&length))?;

        for i in v {
            s.serialize_element(&i)?;
        }
        s.end()
    }
}

impl<T> Sequence<T> {
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        struct V<T>(PhantomData<T>);

        impl<'de, T> Visitor<'de> for V<T>
        where
            T: Deserialize<'de>,
        {
            type Value = Vec<T>;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str(stringify!(Sequence))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                seq.next_element::<i32>()?
                    .ok_or_else(|| <A::Error as de::Error>::custom("length"))
                    .inspect(|length| debug!("length: {length}"))
                    .and_then(|length| {
                        (0..length).try_fold(
                            Vec::with_capacity(length.try_into().map_err(|e| {
                                <A::Error as de::Error>::custom(format!(
                                    "length: {length}, caused: {e:?}"
                                ))
                            })?),
                            |mut acc, _| {
                                seq.next_element::<T>()?
                                    .ok_or_else(|| <A::Error as de::Error>::custom("item"))
                                    .map(|t| {
                                        acc.push(t);
                                        acc
                                    })
                            },
                        )
                    })
            }
        }

        deserializer.deserialize_seq(V(PhantomData))
    }
}

impl<T> ByteSize for Sequence<T>
where
    T: ByteSize,
{
    fn size_in_bytes(&self) -> Result<usize> {
        self.0.iter().try_fold(size_of::<u32>(), |acc, i| {
            UnsignedVarInt::size_inclusive(i).map(|sz| acc + sz)
        })
    }
}

impl<T> Extend<T> for Sequence<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for i in iter {
            self.0.push(i);
        }
    }
}

impl<T: Serialize> Serialize for Sequence<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Self::serialize(&self.0, serializer)
    }
}

impl<'de, T> Deserialize<'de> for Sequence<T>
where
    T: Deserialize<'de>,
    T: Default,
{
    #[allow(clippy::too_many_lines)]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::deserialize(deserializer).map(Self)
    }
}
