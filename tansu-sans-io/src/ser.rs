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

use std::{
    collections::VecDeque,
    fmt,
    io::{Cursor, Write},
};

use serde::{
    Serialize, Serializer,
    ser::{
        SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
};
use tansu_model::{FieldMeta, MessageMeta};
use tracing::debug;

use crate::{Error, Result, RootMessageMeta};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum Kind {
    Request,
    Response,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum Container {
    Struct {
        name: &'static str,
        len: usize,
    },

    StructVariant {
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    },
}

impl Container {
    fn name(&self) -> String {
        match self {
            Self::Struct { name, .. } => (*name).to_string(),
            Self::StructVariant { name, variant, .. } => format!("{name}::{variant}"),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct FieldLookup(&'static [(&'static str, &'static FieldMeta)]);

impl From<&'static [(&'static str, &'static FieldMeta)]> for FieldLookup {
    fn from(value: &'static [(&'static str, &'static FieldMeta)]) -> Self {
        Self(value)
    }
}

impl FieldLookup {
    #[must_use]
    pub(crate) fn field(&self, name: &str) -> Option<&'static FieldMeta> {
        self.0
            .iter()
            .find(|(found, _)| name == *found)
            .map(|(_, meta)| *meta)
    }
}

const PARSE_DEPTH: usize = 6;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Meta {
    message: Option<&'static MessageMeta>,
    field: Option<&'static FieldMeta>,
    parse: VecDeque<FieldLookup>,
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            message: Default::default(),
            field: Default::default(),
            parse: VecDeque::with_capacity(PARSE_DEPTH),
        }
    }
}

/// Serialize the serde data model into the Kafka protocol.
pub struct Encoder<'a> {
    writer: &'a mut dyn Write,
    containers: VecDeque<Container>,
    field: Option<&'static str>,
    kind: Option<Kind>,
    api_key: Option<i16>,
    api_version: Option<i16>,
    meta: Meta,
}

impl fmt::Debug for Encoder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Self)).finish()
    }
}

impl<'a> Encoder<'a> {
    pub(crate) fn request(writer: &'a mut dyn Write) -> Self {
        Self {
            writer,
            containers: VecDeque::with_capacity(PARSE_DEPTH),
            kind: Some(Kind::Request),
            field: None,
            api_key: None,
            api_version: None,
            meta: Meta::default(),
        }
    }

    pub(crate) fn response(writer: &'a mut dyn Write, api_key: i16, api_version: i16) -> Self {
        Self {
            writer,
            containers: VecDeque::with_capacity(PARSE_DEPTH),
            kind: Some(Kind::Response),
            field: None,
            api_key: Some(api_key),
            api_version: Some(api_version),
            meta: RootMessageMeta::messages()
                .responses()
                .get(&api_key)
                .map_or_else(Meta::default, |meta| {
                    let mut parse = VecDeque::with_capacity(PARSE_DEPTH);
                    parse.push_front(meta.fields.into());

                    Meta {
                        message: Some(*meta),
                        parse,
                        ..Default::default()
                    }
                }),
        }
    }

    pub fn new(writer: &'a mut dyn Write) -> Self {
        Self {
            writer,
            containers: VecDeque::with_capacity(PARSE_DEPTH),
            kind: None,
            field: None,
            api_key: None,
            api_version: None,
            meta: Meta::default(),
        }
    }

    fn field_meta(&self, name: &str) -> Option<&'static FieldMeta> {
        debug!(
            name,
            parse_front = ?self.meta.parse.front().and_then(|front| front.field(name)),
            meta = ?self.meta.message.and_then(|mm| mm.field(name))
        );

        self.meta
            .parse
            .front()
            .and_then(|front| front.field(name))
            .or(self.meta.message.and_then(|mm| mm.field(name)))
    }

    #[allow(dead_code)]
    fn field_name(&self) -> String {
        self.containers.iter().fold(
            self.field.map_or_else(String::new, str::to_owned),
            |acc, container| {
                if acc.is_empty() {
                    container.name()
                } else {
                    format!("{}.{acc}", container.name())
                }
            },
        )
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

    fn in_header(&self) -> bool {
        matches!(
            self.containers.front(),
            Some(Container::StructVariant {
                name: "HeaderMezzanine",
                ..
            })
        )
    }

    #[must_use]
    fn is_flexible(&self) -> bool {
        debug!(
            "api_key: {:?}, api_version: {:?}, in_header: {}, is_client_id: {}",
            self.api_key,
            self.api_version,
            self.in_header(),
            self.field.is_some_and(|field| field == "client_id")
        );

        if self.in_header()
            && ((self.kind.is_some_and(|kind| kind == Kind::Request)
                && self.field.is_some_and(|field| field == "client_id"))
                || (self.kind.is_some_and(|kind| kind == Kind::Response)
                    && self.api_key.is_some_and(|api_key| api_key == 18)))
        {
            false
        } else {
            self.meta.message.is_some_and(|meta| {
                self.api_version
                    .is_some_and(|api_version| meta.is_flexible(api_version))
            })
        }
    }

    fn is_nullable(&self) -> bool {
        self.api_version.is_some_and(|api_version| {
            self.meta
                .field
                .is_some_and(|field| field.is_nullable(api_version))
        })
    }

    #[must_use]
    fn is_valid(&self) -> bool {
        self.api_version.is_some_and(|api_version| {
            self.meta
                .field
                .is_some_and(|field| field.version.within(api_version))
        })
    }

    #[must_use]
    fn is_sequence(&self) -> bool {
        self.meta
            .field
            .is_some_and(|field| field.kind.is_sequence())
    }

    #[must_use]
    fn is_structure(&self) -> bool {
        self.meta.field.is_some_and(|field| field.is_structure())
    }

    #[must_use]
    fn is_string(&self) -> bool {
        self.in_header() && self.field.is_some_and(|field| field == "client_id")
            || self.meta.field.is_some_and(|field| field.kind.is_string())
    }

    #[must_use]
    fn is_records(&self) -> bool {
        self.meta.field.is_some_and(|field| field.kind.is_records())
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
        let buf: [u8; 1] = [u8::from(v); 1];
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        match (self.containers.front(), self.field) {
            (
                Some(Container::StructVariant {
                    name: "HeaderMezzanine",
                    variant: "Request",
                    ..
                }),
                Some("api_key"),
            ) => {
                _ = self.api_key.replace(v);

                if let Some(meta) = RootMessageMeta::messages().requests().get(&v) {
                    self.meta.message = Some(*meta);
                    self.meta.parse.push_front(meta.fields.into());
                }
            }

            (
                Some(Container::StructVariant {
                    name: "HeaderMezzanine",
                    variant: "Request",
                    ..
                }),
                Some("api_version"),
            ) => {
                _ = self.api_version.replace(v);
            }

            _ => (),
        }

        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        if self.in_header()
            && self.kind.is_some_and(|kind| kind == Kind::Request)
            && self.field.is_some_and(|field| field == "client_id")
        {
            v.len()
                .try_into()
                .map_err(Into::into)
                .and_then(|len| self.serialize_i16(len))
                .and(self.writer.write_all(v.as_bytes()).map_err(Into::into))
        } else if self.is_valid() {
            if self.is_flexible() {
                (v.len() + 1)
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|len| self.unsigned_varint(len))
                    .and(self.writer.write_all(v.as_bytes()).map_err(Into::into))
            } else {
                v.len()
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|len| self.serialize_i16(len))
                    .and(self.writer.write_all(v.as_bytes()).map_err(Into::into))
            }
        } else {
            Ok(())
        }
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        if self.is_valid() {
            if self.is_flexible() {
                (v.len() + 1)
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|len| self.unsigned_varint(len))?;
            } else {
                v.len()
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|len| self.serialize_u32(len))?;
            }
        }

        self.writer.write_all(v).map_err(Into::into)
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        debug!(
            name = self.field_name(),
            is_valid = self.is_valid(),
            is_nullable = self.is_nullable(),
            is_structure = self.is_structure(),
            is_sequence = self.is_sequence(),
            is_flexible = self.is_flexible(),
        );

        if self.in_header()
            && self.kind.is_some_and(|kind| kind == Kind::Request)
            && self.field.is_some_and(|field| field == "client_id")
        {
            self.serialize_i16(-1)
        } else if self.is_valid() && self.is_records() {
            if self.is_flexible() {
                self.unsigned_varint(1)
            } else {
                self.serialize_i32(0)
            }
        } else if self.is_valid() && self.is_nullable() {
            if self.is_structure() && !self.is_sequence() {
                self.serialize_i8(-1)
            } else if self.is_flexible() {
                self.unsigned_varint(0)
            } else if self.is_sequence() {
                self.serialize_i32(-1)
            } else if self.is_string() {
                self.serialize_i16(-1)
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        if self.field.is_some_and(|field| field == "tag_buffer") && !self.is_flexible() {
            Ok(())
        } else if self.is_records() {
            let mut c = Cursor::new(vec![]);
            let mut e = Encoder::new(&mut c);
            value.serialize(&mut e)?;

            u32::try_from(c.position())
                .map_err(Into::into)
                .and_then(|length| {
                    if self.is_flexible() {
                        self.unsigned_varint(length + 1)
                    } else {
                        let buf = length.to_be_bytes();
                        self.writer.write_all(&buf).map_err(Into::into)
                    }
                })?;

            self.writer.write_all(&c.into_inner()).map_err(Into::into)
        } else {
            value.serialize(self)
        }
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        unimplemented!("{name}")
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        unimplemented!("{name}, {variant_index}, {variant}")
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
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        debug!(?len);

        if self.is_valid()
            && let Some(len) = len
        {
            if self.is_flexible() {
                (len + 1)
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|l| self.unsigned_varint(l))?;
            } else {
                len.try_into()
                    .map_err(Into::into)
                    .and_then(|l| self.serialize_i32(l))?;
            }
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

        self.containers.push_front(Container::Struct { name, len });

        if let Some(fm) = self.field_meta(name) {
            self.meta.field = Some(fm);
            self.meta.parse.push_front(fm.fields.into());
        }

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

        self.containers.push_front(Container::StructVariant {
            name,
            variant_index,
            variant,
            len,
        });

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
        unimplemented!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
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
        unimplemented!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
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
        unimplemented!()
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        unimplemented!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
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
        _ = self.field.replace(key);

        if let Some(fm) = self.field_meta(key) {
            debug!(field = self.field_name(), meta = ?fm, is_valid = self.is_valid());

            _ = self.meta.field.replace(fm);
            self.meta.parse.push_front(fm.fields.into());
            let outcome = if self.is_valid() {
                value.serialize(&mut **self)
            } else {
                Ok(())
            };
            _ = self.meta.parse.pop_front();
            _ = self.meta.field.take();
            outcome
        } else {
            debug!(field = self.field_name());

            _ = self.meta.field.take();
            self.meta.parse.push_front(FieldLookup::default());
            let outcome = value.serialize(&mut **self);
            _ = self.meta.parse.pop_front();
            outcome
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        _ = self.containers.pop_front();
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
        debug!(?key);

        _ = self.field.replace(key);

        if let Some(fm) = self.field_meta(key) {
            if self
                .api_version
                .is_some_and(|api_version| fm.version.within(api_version))
            {
                debug!("field name: {}, meta: {fm:?}", self.field_name());

                _ = self.meta.field.replace(fm);
                self.meta.parse.push_front(fm.fields.into());
                let outcome = value.serialize(&mut **self);
                _ = self.meta.parse.pop_front();
                _ = self.meta.field.take();
                outcome
            } else {
                debug!(
                    "field name: {}, meta: {fm:?}, is not required in v: {:?}",
                    self.field_name(),
                    self.api_version
                );
                Ok(())
            }
        } else {
            _ = self.meta.field.take();
            self.meta.parse.push_front(FieldLookup::default());
            let outcome = value.serialize(&mut **self);
            _ = self.meta.parse.pop_front();
            outcome
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        _ = self.containers.pop_front();
        Ok(())
    }
}
