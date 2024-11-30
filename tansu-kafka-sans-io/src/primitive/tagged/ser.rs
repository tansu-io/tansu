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

use crate::{Error, Result};
use serde::{
    ser::{
        SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
    Serialize, Serializer,
};
use std::{fmt, io::Write};
use tracing::debug;

pub(crate) struct Encoder<'a> {
    writer: &'a mut dyn Write,
}

impl fmt::Debug for Encoder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Self)).finish()
    }
}

impl<'a> Encoder<'a> {
    pub(crate) fn new(writer: &'a mut dyn Write) -> Self {
        Self { writer }
    }

    pub(crate) fn encode<T>(decoded: &T) -> Result<Vec<u8>>
    where
        T: Serialize,
    {
        let mut encoded = Vec::new();
        let mut serializer = Encoder::new(&mut encoded);
        decoded.serialize(&mut serializer)?;
        Ok(encoded)
    }

    fn unsigned_varint(&mut self, mut v: u32) -> Result<()> {
        const CONTINUATION: u8 = 0b1000_0000;

        while v >= u32::from(CONTINUATION) {
            #[allow(clippy::cast_possible_truncation)]
            self.writer.write_all(&[(v as u8 | CONTINUATION)])?;
            v >>= 7;
        }

        #[allow(clippy::cast_possible_truncation)]
        self.writer.write_all(&[(v as u8)])?;
        Ok(())
    }
}

impl Serializer for &mut Encoder<'_> {
    type Ok = ();

    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        debug!(?v);

        let buf: [u8; 1] = [u8::from(v); 1];
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        debug!(?v);

        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        debug!(?v);

        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        debug!(?v);

        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        debug!(?v);

        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        debug!(?v);

        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        debug!(?v);

        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        debug!(?v);

        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        debug!(?v);

        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        debug!(?v);

        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        debug!(?v);

        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        debug!(?v);
        todo!()
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        debug!(?v);
        (v.len() + 1)
            .try_into()
            .map_err(Into::into)
            .and_then(|len| self.unsigned_varint(len))?;
        self.writer.write_all(v.as_bytes()).map_err(Into::into)
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        debug!(?v);
        (v.len() + 1)
            .try_into()
            .map_err(Into::into)
            .and_then(|len| self.unsigned_varint(len))?;
        self.writer.write_all(v).map_err(Into::into)
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.unsigned_varint(0)
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        debug!(?name);
        todo!()
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        debug!(?name, ?variant_index, ?variant);
        todo!()
    }

    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        debug!(?name);
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        debug!(?name, ?variant_index, ?variant);
        let _ = value;
        todo!()
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        debug!(?len);

        if let Some(len) = len {
            (len + 1)
                .try_into()
                .map_err(Into::into)
                .and_then(|l| self.unsigned_varint(l))?;
        }
        Ok(self)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        debug!(?len);

        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        debug!(?name, ?len);
        Ok(self)
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        debug!(?name, ?variant_index, ?variant, ?len);
        Ok(self)
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        debug!(?len);
        Ok(self)
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        debug!(?name, ?len);
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        debug!(?name, ?variant_index, ?variant, ?len);
        Ok(self)
    }
}

impl SerializeSeq for &mut Encoder<'_> {
    type Ok = ();

    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTuple for &mut Encoder<'_> {
    type Ok = ();

    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTupleStruct for &mut Encoder<'_> {
    type Ok = ();

    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        let _ = value;
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl SerializeTupleVariant for &mut Encoder<'_> {
    type Ok = ();

    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        let _ = value;
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl SerializeMap for &mut Encoder<'_> {
    type Ok = ();

    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        let _ = key;
        todo!()
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        let _ = value;
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl SerializeStruct for &mut Encoder<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        let _ = key;
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeStructVariant for &mut Encoder<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        let _ = key;
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}
