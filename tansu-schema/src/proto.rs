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

//! Protocol Buffer message schema

use std::{
    collections::BTreeMap,
    io::Write,
    ops::{Deref, RangeInclusive},
    sync::LazyLock,
};

use crate::{
    ARROW_LIST_FIELD_NAME, AsArrow, AsJsonValue, AsKafkaRecord, Error, Generator, Result,
    Validator, lake::LakeHouseType,
};
use arrow::{
    array::{
        ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
        LargeBinaryBuilder, ListBuilder, MapBuilder, StringBuilder, StructBuilder,
        TimestampMicrosecondBuilder,
    },
    datatypes::{DataType, Field, FieldRef, Fields, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Datelike};
use fake::Fake;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use protobuf::{
    CodedInputStream, MessageDyn, UnknownValueRef,
    descriptor::{self, FieldDescriptorProto},
    reflect::{
        EnumDescriptor, FieldDescriptor, FileDescriptor, MessageDescriptor, ReflectValueBox,
        ReflectValueRef, RuntimeFieldType, RuntimeType,
    },
    well_known_types,
};
use protobuf_json_mapping::{parse_dyn_from_str, print_to_string};
use rand::prelude::*;
use rhai::{Engine, packages::Package};
use rhai_rand::RandomPackage;
use serde_json::{Map, Value, json};
use tansu_sans_io::{ErrorCode, record::inflated::Batch};
use tempfile::{NamedTempFile, tempdir};
use tracing::{debug, error};

const NULLABLE: bool = true;
const SORTED_MAP_KEYS: bool = false;

const KEY: &str = "Key";
const META: &str = "Meta";
const VALUE: &str = "Value";

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum MessageKind {
    Key,
    Meta,
    Value,
}

impl AsRef<str> for MessageKind {
    fn as_ref(&self) -> &str {
        match self {
            MessageKind::Key => "Key",
            MessageKind::Meta => "Meta",
            MessageKind::Value => "Value",
        }
    }
}

const GOOGLE_PROTOBUF_TIMESTAMP: &str = "google.protobuf.Timestamp";

fn append<'a>(path: &[&'a str], name: &'a str) -> Vec<&'a str> {
    let mut path = Vec::from(path);
    path.push(name);
    path
}

#[derive(Default)]
struct RecordBuilder {
    meta: Option<Box<dyn ArrayBuilder>>,
    key: Option<Box<dyn ArrayBuilder>>,
    value: Option<Box<dyn ArrayBuilder>>,
}

impl RecordBuilder {
    fn new(ids: &BTreeMap<String, i32>, schema: &Schema) -> RecordBuilder {
        Self {
            meta: schema.message_by_package_relative_name_array_builder(ids, MessageKind::Meta),
            key: schema.message_by_package_relative_name_array_builder(ids, MessageKind::Key),
            value: schema.message_by_package_relative_name_array_builder(ids, MessageKind::Value),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Schema {
    file_descriptors: Vec<FileDescriptor>,
}

impl Schema {
    fn new_list_field(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        data_type: DataType,
    ) -> Field {
        self.new_field(ids, path, ARROW_LIST_FIELD_NAME, data_type)
    }

    fn new_field(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        name: &str,
        data_type: DataType,
    ) -> Field {
        self.new_nullable_field(ids, path, name, data_type, NULLABLE)
    }

    fn new_nullable_field(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> Field {
        debug!(?path, name, ?data_type, ?nullable, ?ids);

        let path = append(path, name).join(".");

        Field::new(name.to_owned(), data_type, nullable).with_metadata(
            ids.get(path.as_str())
                .inspect(|field_id| debug!(?path, field_id))
                .map(|field_id| (PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string()))
                .into_iter()
                .collect(),
        )
    }

    fn field(&self, ids: &BTreeMap<String, i32>, message_kind: MessageKind) -> Option<Field> {
        debug!(?message_kind);

        self.message_by_package_relative_name(message_kind)
            .inspect(|descriptor| debug!(?descriptor))
            .map(|descriptor| {
                let name = message_kind.as_ref().to_lowercase();

                self.new_nullable_field(
                    ids,
                    &[],
                    &name,
                    DataType::Struct(Fields::from(self.message_descriptor_to_fields(
                        ids,
                        &[&name],
                        &descriptor,
                    ))),
                    NULLABLE,
                )
            })
    }

    fn message_by_package_relative_name(
        &self,
        message_kind: MessageKind,
    ) -> Option<MessageDescriptor> {
        self.file_descriptors
            .iter()
            .find_map(|fd| fd.message_by_package_relative_name(message_kind.as_ref()))
    }

    fn value_to_message(
        &self,
        message_kind: MessageKind,
        json: &Value,
    ) -> Result<Box<dyn MessageDyn>> {
        self.file_descriptors
            .iter()
            .find_map(|fd| fd.message_by_package_relative_name(message_kind.as_ref()))
            .ok_or(Error::Message(format!(
                "message {message_kind:?} not found"
            )))
            .and_then(|message_descriptor| {
                serde_json::to_string(json)
                    .map_err(Error::from)
                    .and_then(|json| {
                        parse_dyn_from_str(&message_descriptor, json.as_str()).map_err(Into::into)
                    })
            })
    }

    pub fn encode_from_value(&self, message_kind: MessageKind, json: &Value) -> Result<Bytes> {
        self.value_to_message(message_kind, json)
            .and_then(message_to_bytes)
    }

    fn message_generator(&self) -> Option<MessageGenerator> {
        self.file_descriptors
            .iter()
            .find_map(|fd| fd.message_by_package_relative_name("Generator"))
            .map(|generator_descriptor| MessageGenerator {
                generator_descriptor,
            })
    }

    fn generate_message_kind(&self, message_kind: MessageKind) -> Result<Option<Bytes>> {
        debug!(?message_kind);

        let engine = {
            let mut engine = Engine::new();

            _ = engine
                .register_fn("first_name", || {
                    fake::faker::name::raw::FirstName(fake::locales::EN).fake::<String>()
                })
                .register_fn("last_name", || {
                    fake::faker::name::raw::LastName(fake::locales::EN).fake::<String>()
                })
                .register_fn("safe_email", || {
                    fake::faker::internet::raw::SafeEmail(fake::locales::EN).fake::<String>()
                })
                .register_fn("building_number", || {
                    fake::faker::address::raw::BuildingNumber(fake::locales::EN).fake::<String>()
                })
                .register_fn("street_name", || {
                    fake::faker::address::raw::StreetName(fake::locales::EN).fake::<String>()
                })
                .register_fn("city_name", || {
                    fake::faker::address::raw::CityName(fake::locales::EN).fake::<String>()
                })
                .register_fn("post_code", || {
                    fake::faker::address::raw::PostCode(fake::locales::EN).fake::<String>()
                })
                .register_fn("country_name", || {
                    fake::faker::address::raw::CountryName(fake::locales::EN).fake::<String>()
                })
                .register_fn("industry", || {
                    fake::faker::company::raw::Industry(fake::locales::EN).fake::<String>()
                });

            let random = RandomPackage::new();
            _ = random.register_into_engine(&mut engine);
            engine
        };

        self.message_by_package_relative_name(message_kind)
            .map_or(Ok(None), |message_descriptor| {
                self.message_generator()
                    .map_or(Ok(None), |message_generator| {
                        message_generator
                            .generate(&engine, &message_descriptor)
                            .and_then(message_to_bytes)
                            .map(Some)
                    })
            })
    }

    fn message_value_as_bytes(
        &self,
        message_kind: MessageKind,
        json: &Value,
    ) -> Result<Option<Bytes>> {
        self.message_by_package_relative_name(message_kind)
            .map(|message_descriptor| {
                serde_json::to_string(json)
                    .map_err(Error::from)
                    .inspect(|json| debug!(%json))
                    .and_then(|json| {
                        parse_dyn_from_str(&message_descriptor, json.as_str()).map_err(Into::into)
                    })
                    .inspect(|message| debug!(%message))
                    .and_then(|message| {
                        let mut w = BytesMut::new().writer();
                        message
                            .write_to_writer_dyn(&mut w)
                            .map(|()| Bytes::from(w.into_inner()))
                            .map_err(Into::into)
                    })
                    .inspect_err(|err| error!(?err))
            })
            .transpose()
    }

    fn runtime_type_to_data_type(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        runtime_type: &RuntimeType,
    ) -> DataType {
        debug!(?path, ?runtime_type);

        match runtime_type {
            RuntimeType::U32 | RuntimeType::I32 | RuntimeType::Enum(_) => DataType::Int32,
            RuntimeType::U64 | RuntimeType::I64 => DataType::Int64,
            RuntimeType::F32 => DataType::Float32,
            RuntimeType::F64 => DataType::Float64,
            RuntimeType::Bool => DataType::Boolean,
            RuntimeType::String => DataType::Utf8,
            RuntimeType::VecU8 => DataType::LargeBinary,
            RuntimeType::Message(descriptor) => {
                if descriptor.full_name() == GOOGLE_PROTOBUF_TIMESTAMP {
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                } else {
                    DataType::Struct(Fields::from(
                        self.message_descriptor_to_fields(ids, path, descriptor),
                    ))
                }
            }
        }
    }

    fn message_descriptor_to_fields(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        descriptor: &MessageDescriptor,
    ) -> Vec<Field> {
        debug!(?path, ?ids, descriptor_full_name = ?descriptor.full_name());

        descriptor
            .fields()
            .inspect(|field| {
                debug!(
                    name = field.name(),
                    full_name = field.full_name(),
                    type_name = field.proto().type_name()
                )
            })
            .map(|field| match field.runtime_field_type() {
                RuntimeFieldType::Singular(ref singular) => {
                    debug!(
                        descriptor = descriptor.name(),
                        field_name = field.name(),
                        ?singular
                    );

                    self.new_nullable_field(
                        ids,
                        path,
                        field.name(),
                        self.runtime_type_to_data_type(
                            ids,
                            &append(path, field.name())[..],
                            singular,
                        ),
                        !field.is_required(),
                    )
                }

                RuntimeFieldType::Repeated(ref repeated) => {
                    debug!(
                        descriptor = descriptor.name(),
                        field_name = field.name(),
                        ?repeated
                    );

                    self.new_nullable_field(
                        ids,
                        path,
                        field.name(),
                        {
                            let path = &append(path, field.name())[..];

                            DataType::List(FieldRef::new(self.new_list_field(
                                ids,
                                path,
                                self.runtime_type_to_data_type(
                                    ids,
                                    &append(path, ARROW_LIST_FIELD_NAME)[..],
                                    repeated,
                                ),
                            )))
                        },
                        !field.is_required(),
                    )
                }

                RuntimeFieldType::Map(ref key, ref value) => {
                    debug!(
                        descriptor = descriptor.name(),
                        field_name = field.name(),
                        ?key,
                        ?value
                    );

                    self.new_nullable_field(
                        ids,
                        path,
                        field.name(),
                        {
                            let path = &append(path, field.name())[..];

                            DataType::Map(
                                FieldRef::new(self.new_nullable_field(
                                    ids,
                                    path,
                                    "entries",
                                    DataType::Struct({
                                        let path = &append(path, "entries")[..];

                                        Fields::from_iter([
                                            self.new_nullable_field(
                                                ids,
                                                path,
                                                "keys",
                                                self.runtime_type_to_data_type(
                                                    ids,
                                                    append(path, "keys").as_slice(),
                                                    key,
                                                ),
                                                !NULLABLE,
                                            ),
                                            self.new_field(
                                                ids,
                                                path,
                                                "values",
                                                self.runtime_type_to_data_type(
                                                    ids,
                                                    append(path, "values").as_slice(),
                                                    value,
                                                ),
                                            ),
                                        ])
                                    }),
                                    !NULLABLE,
                                )),
                                SORTED_MAP_KEYS,
                            )
                        },
                        !field.is_required(),
                    )
                }
            })
            .collect::<Vec<_>>()
    }

    fn message_by_package_relative_name_array_builder(
        &self,
        ids: &BTreeMap<String, i32>,
        message_kind: MessageKind,
    ) -> Option<Box<dyn ArrayBuilder>> {
        debug!(?message_kind);
        self.message_by_package_relative_name(message_kind)
            .map(|descriptor| {
                self.message_descriptor_to_array_builder(
                    ids,
                    &[&message_kind.as_ref().to_lowercase()],
                    &descriptor,
                )
            })
    }

    fn message_descriptor_to_array_builder(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        descriptor: &MessageDescriptor,
    ) -> Box<dyn ArrayBuilder> {
        debug!(?path, descriptor = descriptor.name());
        let fields = self.message_descriptor_to_fields(ids, path, descriptor);
        let builders = self.message_descriptor_array_builders(ids, path, descriptor);

        Box::new(StructBuilder::new(fields, builders)) as Box<dyn ArrayBuilder>
    }

    fn runtime_type_to_array_builder(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        runtime_type: &RuntimeType,
    ) -> Box<dyn ArrayBuilder> {
        debug!(?path, ?runtime_type, ?ids);

        match runtime_type {
            RuntimeType::U32 | RuntimeType::I32 | RuntimeType::Enum(_) => {
                Box::new(Int32Builder::new())
            }
            RuntimeType::U64 | RuntimeType::I64 => Box::new(Int64Builder::new()),
            RuntimeType::F32 => Box::new(Float32Builder::new()),
            RuntimeType::F64 => Box::new(Float64Builder::new()),
            RuntimeType::Bool => Box::new(BooleanBuilder::new()),
            RuntimeType::String => Box::new(StringBuilder::new()),
            RuntimeType::VecU8 => Box::new(LargeBinaryBuilder::new()),

            RuntimeType::Message(descriptor) => {
                if descriptor.full_name() == GOOGLE_PROTOBUF_TIMESTAMP {
                    Box::new(TimestampMicrosecondBuilder::new())
                } else {
                    let (fields, builders) = descriptor
                        .fields()
                        .map(|field| match field.runtime_field_type() {
                            RuntimeFieldType::Singular(ref singular) => {
                                debug!(
                                    descriptor = descriptor.name(),
                                    field_name = field.name(),
                                    ?singular
                                );

                                (
                                    self.new_nullable_field(
                                        ids,
                                        path,
                                        field.name(),
                                        self.runtime_type_to_data_type(
                                            ids,
                                            &append(path, field.name())[..],
                                            singular,
                                        ),
                                        !field.is_required(),
                                    ),
                                    self.runtime_type_to_array_builder(ids, path, singular),
                                )
                            }

                            RuntimeFieldType::Repeated(ref repeated) => {
                                debug!(
                                    descriptor = descriptor.name(),
                                    field_name = field.name(),
                                    ?repeated
                                );

                                (
                                    self.new_nullable_field(
                                        ids,
                                        path,
                                        field.name(),
                                        {
                                            let path = &append(path, field.name())[..];

                                            DataType::List(FieldRef::new(self.new_list_field(
                                                ids,
                                                path,
                                                self.runtime_type_to_data_type(
                                                    ids,
                                                    &append(path, ARROW_LIST_FIELD_NAME)[..],
                                                    repeated,
                                                ),
                                            )))
                                        },
                                        !field.is_required(),
                                    ),
                                    {
                                        let path = &append(path, field.name())[..];

                                        Box::new(
                                            ListBuilder::new(self.runtime_type_to_array_builder(
                                                ids,
                                                &append(path, ARROW_LIST_FIELD_NAME)[..],
                                                repeated,
                                            ))
                                            .with_field(self.new_list_field(
                                                ids,
                                                path,
                                                self.runtime_type_to_data_type(
                                                    ids,
                                                    &append(path, ARROW_LIST_FIELD_NAME)[..],
                                                    repeated,
                                                ),
                                            )),
                                        )
                                            as Box<dyn ArrayBuilder>
                                    },
                                )
                            }

                            RuntimeFieldType::Map(ref key, ref value) => {
                                debug!(
                                    descriptor = descriptor.name(),
                                    field_name = field.name(),
                                    ?key,
                                    ?value
                                );

                                (
                                    self.new_nullable_field(
                                        ids,
                                        path,
                                        field.name(),
                                        {
                                            let path = &append(path, field.name())[..];

                                            DataType::Map(
                                                FieldRef::new(self.new_nullable_field(
                                                    ids,
                                                    path,
                                                    "entries",
                                                    DataType::Struct({
                                                        let path = &append(path, "entries")[..];

                                                        Fields::from_iter([
                                                            self.new_nullable_field(
                                                                ids,
                                                                path,
                                                                "keys",
                                                                self.runtime_type_to_data_type(
                                                                    ids,
                                                                    &append(path, "keys")[..],
                                                                    key,
                                                                ),
                                                                !NULLABLE,
                                                            ),
                                                            self.new_field(
                                                                ids,
                                                                path,
                                                                "values",
                                                                self.runtime_type_to_data_type(
                                                                    ids,
                                                                    &append(path, "values")[..],
                                                                    value,
                                                                ),
                                                            ),
                                                        ])
                                                    }),
                                                    !NULLABLE,
                                                )),
                                                SORTED_MAP_KEYS,
                                            )
                                        },
                                        !field.is_required(),
                                    ),
                                    Box::new(MapBuilder::new(
                                        None,
                                        self.runtime_type_to_array_builder(ids, path, key),
                                        self.runtime_type_to_array_builder(ids, path, value),
                                    )) as Box<dyn ArrayBuilder>,
                                )
                            }
                        })
                        .collect::<(Vec<_>, Vec<_>)>();

                    Box::new(StructBuilder::new(fields, builders))
                }
            }
        }
    }

    fn message_descriptor_array_builders(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        descriptor: &MessageDescriptor,
    ) -> Vec<Box<dyn ArrayBuilder>> {
        debug!(?path, descriptor = descriptor.full_name(), ?ids);

        descriptor
            .fields()
            .map(|field| {
                let inside = &append(path, field.name())[..];

                match field.runtime_field_type() {
                    RuntimeFieldType::Singular(ref singular) => {
                        debug!(
                            descriptor = descriptor.name(),
                            field_name = field.name(),
                            ?singular,
                            ?inside
                        );
                        self.runtime_type_to_array_builder(ids, inside, singular)
                    }

                    RuntimeFieldType::Repeated(ref repeated) => {
                        debug!(
                            descriptor = descriptor.name(),
                            field_name = field.name(),
                            ?repeated,
                            ?inside
                        );
                        Box::new(
                            ListBuilder::new(self.runtime_type_to_array_builder(
                                ids,
                                &append(inside, ARROW_LIST_FIELD_NAME)[..],
                                repeated,
                            ))
                            .with_field(self.new_list_field(
                                ids,
                                inside,
                                self.runtime_type_to_data_type(
                                    ids,
                                    &append(inside, ARROW_LIST_FIELD_NAME)[..],
                                    repeated,
                                ),
                            )),
                        )
                    }

                    RuntimeFieldType::Map(ref key, ref value) => {
                        debug!(
                            descriptor = descriptor.name(),
                            field_name = field.name(),
                            ?key,
                            ?value,
                            ?inside
                        );

                        let path = &append(inside, "entries");

                        Box::new(
                            MapBuilder::new(
                                None,
                                self.runtime_type_to_array_builder(ids, path, key),
                                self.runtime_type_to_array_builder(ids, path, value),
                            )
                            .with_keys_field(self.new_nullable_field(
                                ids,
                                &path[..],
                                "keys",
                                self.runtime_type_to_data_type(ids, &append(path, "keys")[..], key),
                                !NULLABLE,
                            ))
                            .with_values_field(self.new_field(
                                ids,
                                &path[..],
                                "values",
                                self.runtime_type_to_data_type(
                                    ids,
                                    &append(path, "values")[..],
                                    value,
                                ),
                            )),
                        )
                    }
                }
            })
            .collect::<Vec<_>>()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct MessageGenerator {
    generator_descriptor: MessageDescriptor,
}

impl MessageGenerator {
    fn generate(
        &self,
        engine: &Engine,
        message_descriptor: &MessageDescriptor,
    ) -> Result<Box<dyn MessageDyn>> {
        debug!(message_descriptor = message_descriptor.full_name());

        let mut message_dyn = message_descriptor.new_instance();

        for (field_proto, field) in message_descriptor
            .proto()
            .field
            .iter()
            .zip(message_dyn.descriptor_dyn().fields())
        {
            let field_generator = FieldGenerator {
                generator_descriptor: &self.generator_descriptor,
                configuration: FieldGeneratorConfiguration::with_field_generator(
                    field_proto,
                    &self.generator_descriptor,
                ),
            };

            if field_generator.configuration.skip() {
                continue;
            }

            match field.runtime_field_type() {
                RuntimeFieldType::Singular(ref singular) => field_generator
                    .singular_value(engine, singular)
                    .map(|value| field.set_singular_field(message_dyn.as_mut(), value))?,

                RuntimeFieldType::Repeated(ref repeated) => {
                    let mut r = field.mut_repeated(message_dyn.as_mut());
                    for element in field_generator.repeated_value(engine, repeated)? {
                        r.push(element);
                    }
                }

                RuntimeFieldType::Map(key, value) => todo!("key={key:?} value={value:?}"),
            }
        }

        Ok(message_dyn)
    }
}

struct FieldGenerator<'a> {
    generator_descriptor: &'a MessageDescriptor,
    configuration: FieldGeneratorConfiguration,
}

impl<'a> FieldGenerator<'a> {
    fn singular_value(
        &self,
        engine: &Engine,
        runtime_type: &RuntimeType,
    ) -> Result<ReflectValueBox> {
        let mut rng = rand::rng();
        match runtime_type {
            RuntimeType::I32 => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<i32>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::I64 => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<i64>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::U32 => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<u32>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::U64 => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<u64>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::F32 => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<f32>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::F64 => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<f64>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::Bool => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<bool>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::String => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(String::from("abc")), |script| {
                    engine
                        .eval::<String>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::VecU8 => todo!(),

            RuntimeType::Enum(descriptor) => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(ReflectValueBox::Enum(descriptor.clone(), 1)), |script| {
                    engine
                        .eval::<String>(script)
                        .map(|name| {
                            descriptor
                                .value_by_name(&name[..])
                                .map(|value_descriptor| {
                                    ReflectValueBox::Enum(
                                        descriptor.clone(),
                                        value_descriptor.value(),
                                    )
                                })
                                .inspect(|value| debug!(?value))
                                .unwrap()
                        })
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map_err(Into::into),

            RuntimeType::Message(message_descriptor) => {
                let generator = MessageGenerator {
                    generator_descriptor: self.generator_descriptor.to_owned(),
                };

                generator
                    .generate(engine, message_descriptor)
                    .map(ReflectValueBox::Message)
            }
        }
    }

    fn repeated_value(
        &self,
        engine: &Engine,
        runtime_type: &RuntimeType,
    ) -> Result<Vec<ReflectValueBox>> {
        let upper = self.configuration.repeated_len().unwrap_or_else(|| {
            rand::rng().random_range(self.configuration.repeated_range().unwrap_or(0..=1))
        });

        (0..upper)
            .inspect(|i| debug!(i))
            .map(|_| self.singular_value(engine, runtime_type))
            .collect::<Result<Vec<_>>>()
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
enum FieldGeneratorConfiguration {
    Bool(bool),
    Bytes(Bytes),
    F32(f32),
    F64(f64),
    I32(i32),
    I64(i64),
    List(Vec<FieldGeneratorConfiguration>),
    Message(BTreeMap<String, FieldGeneratorConfiguration>),
    String(String),
    U32(u32),
    U64(u64),
}

impl Default for FieldGeneratorConfiguration {
    fn default() -> Self {
        Self::Message(Default::default())
    }
}

impl FieldGeneratorConfiguration {
    fn with_field_generator(
        field: &FieldDescriptorProto,
        generator: &MessageDescriptor,
    ) -> FieldGeneratorConfiguration {
        debug!(field = field.name(), generator = generator.full_name(),);

        field
            .options
            .special_fields
            .unknown_fields()
            .iter()
            .find_map(|(id, unknown)| {
                if id != 51215 {
                    None
                } else if let UnknownValueRef::LengthDelimited(items) = unknown {
                    let mut message = generator.new_instance();

                    _ = message
                        .merge_from_bytes_dyn(items)
                        .inspect_err(|err| debug!(?err))
                        .ok();

                    Some(Self::from(message.as_ref()))
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }

    fn skip(&self) -> bool {
        self.get("skip")
            .cloned()
            .and_then(|value| value.as_bool())
            .inspect(|skip| debug!(?skip))
            .unwrap_or_default()
    }

    fn script(&self) -> Option<&str> {
        self.get("script")
            .inspect(|script| debug!(?script))
            .and_then(|value| value.as_str())
            .inspect(|script| debug!(?script))
            .or(self.repeated_script())
    }

    fn repeated_range(&self) -> Option<RangeInclusive<u32>> {
        self.get("repeated")
            .inspect(|repeated| debug!(?repeated))
            .and_then(|repeated| {
                repeated
                    .get("range")
                    .inspect(|range| debug!(?range))
                    .and_then(|range| {
                        range
                            .get("min")
                            .inspect(|min| debug!(?min))
                            .and_then(|min| min.as_u32())
                            .and_then(|min| {
                                range
                                    .get("max")
                                    .inspect(|max| debug!(?max))
                                    .and_then(|max| max.as_u32())
                                    .map(|max| min..=max)
                            })
                    })
            })
    }

    fn repeated_len(&self) -> Option<u32> {
        self.get("repeated")
            .and_then(|repeated| repeated.get("len"))
            .and_then(|len| len.as_u32())
    }

    fn repeated_script(&self) -> Option<&str> {
        self.get("repeated")
            .and_then(|repeated| repeated.get("script"))
            .and_then(|script| script.as_str())
    }

    fn get(&self, key: &str) -> Option<&FieldGeneratorConfiguration> {
        if let Self::Message(message) = self {
            message.get(key)
        } else {
            None
        }
    }

    fn as_bool(&self) -> Option<bool> {
        if let Self::Bool(flag) = self {
            Some(*flag)
        } else {
            None
        }
    }

    fn as_str(&self) -> Option<&str> {
        if let Self::String(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn as_u32(&self) -> Option<u32> {
        if let Self::U32(value) = self {
            Some(*value)
        } else {
            None
        }
    }
}

impl From<EnumDescriptor> for FieldGeneratorConfiguration {
    fn from(_value: EnumDescriptor) -> Self {
        todo!()
    }
}

impl<'a> From<ReflectValueRef<'a>> for FieldGeneratorConfiguration {
    fn from(value: ReflectValueRef<'a>) -> Self {
        match value {
            ReflectValueRef::U32(value) => Self::U32(value),
            ReflectValueRef::U64(value) => Self::U64(value),
            ReflectValueRef::I32(value) => Self::I32(value),
            ReflectValueRef::I64(value) => Self::I64(value),
            ReflectValueRef::F32(value) => Self::F32(value),
            ReflectValueRef::F64(value) => Self::F64(value),
            ReflectValueRef::Bool(value) => Self::Bool(value),
            ReflectValueRef::String(value) => Self::String(value.to_owned()),
            ReflectValueRef::Bytes(items) => Self::Bytes(Bytes::copy_from_slice(items)),
            ReflectValueRef::Enum(enum_descriptor, _) => Self::from(enum_descriptor),
            ReflectValueRef::Message(message_ref) => Self::from(message_ref.deref()),
        }
    }
}

impl From<&dyn MessageDyn> for FieldGeneratorConfiguration {
    fn from(message: &dyn MessageDyn) -> Self {
        debug!(%message);

        Self::Message(
            message
                .descriptor_dyn()
                .fields()
                .inspect(|field| debug!(field = field.name()))
                .filter_map(|field| match field.runtime_field_type() {
                    RuntimeFieldType::Singular(singular) => {
                        debug!(?singular);
                        field
                            .get_singular(message)
                            .inspect(|value| debug!(field = field.name(), ?value))
                            .map(|value| (field.name().to_owned(), Self::from(value)))
                    }

                    RuntimeFieldType::Repeated(repeated) => {
                        debug!(?repeated);
                        Some((
                            field.name().to_owned(),
                            Self::List(
                                field
                                    .get_repeated(message)
                                    .into_iter()
                                    .map(Self::from)
                                    .inspect(|configuration| debug!(?configuration))
                                    .collect::<Vec<_>>(),
                            ),
                        ))
                    }

                    RuntimeFieldType::Map(key, value) => todo!("key={key:?} value={value:?}"),
                })
                .inspect(|(field, value)| debug!(field, ?value))
                .collect::<BTreeMap<String, FieldGeneratorConfiguration>>(),
        )
    }
}

fn message_to_bytes(message: Box<dyn MessageDyn>) -> Result<Bytes> {
    let mut w = BytesMut::new().writer();
    message
        .write_to_writer_dyn(&mut w)
        .and(Ok(Bytes::from(w.into_inner())))
        .map_err(Into::into)
}

impl AsKafkaRecord for Schema {
    fn as_kafka_record(&self, value: &Value) -> Result<tansu_sans_io::record::Builder> {
        debug!(?value);

        let mut builder = tansu_sans_io::record::Record::builder();

        if let Some(value) = value.get("key") {
            debug!(?value);

            if let Some(encoded) = self.message_value_as_bytes(MessageKind::Key, value)? {
                builder = builder.key(encoded.into());
            }
        };

        if let Some(value) = value.get("value") {
            debug!(?value);

            if let Some(encoded) = self.message_value_as_bytes(MessageKind::Value, value)? {
                builder = builder.value(encoded.into());
            }
        };

        Ok(builder)
    }
}

impl Generator for Schema {
    fn generate(&self) -> Result<tansu_sans_io::record::Builder> {
        let mut builder = tansu_sans_io::record::Record::builder();

        if let Some(generated) = self.generate_message_kind(MessageKind::Key)? {
            builder = builder.key(generated.into());
        }

        if let Some(generated) = self.generate_message_kind(MessageKind::Value)? {
            builder = builder.value(generated.into());
        }

        Ok(builder)
    }
}

fn fields(ids: &BTreeMap<String, i32>, schema: &Schema) -> Fields {
    let mut fields = vec![];

    if let Some(field) = schema.field(ids, MessageKind::Meta) {
        fields.push(field);
    }

    if let Some(field) = schema.field(ids, MessageKind::Key) {
        fields.push(field);
    }

    if let Some(field) = schema.field(ids, MessageKind::Value) {
        fields.push(field);
    }

    fields.into()
}

fn arrow_schema(ids: &BTreeMap<String, i32>, schema: &Schema) -> ArrowSchema {
    ArrowSchema::new(fields(ids, schema))
}

fn decode(
    message_descriptor: Option<MessageDescriptor>,
    encoded: Option<Bytes>,
) -> Result<Option<Box<dyn MessageDyn>>> {
    debug!(?message_descriptor, ?encoded);

    message_descriptor.map_or(Ok(None), |message_descriptor| {
        encoded.map_or(Err(Error::Api(ErrorCode::InvalidRecord)), |encoded| {
            let mut message = message_descriptor.new_instance();

            message
                .merge_from_dyn(&mut CodedInputStream::from_tokio_bytes(&encoded))
                .inspect_err(|err| error!(?err))
                .map_err(|_err| Error::Api(ErrorCode::InvalidRecord))
                .and(Ok(Some(message)))
                .inspect(|message| debug!(?message))
        })
    })
}

fn validate(message_descriptor: Option<MessageDescriptor>, encoded: Option<Bytes>) -> Result<()> {
    decode(message_descriptor, encoded).and(Ok(()))
}

impl Validator for Schema {
    fn validate(&self, batch: &Batch) -> Result<()> {
        debug!(?batch);

        for record in &batch.records {
            debug!(?record);

            validate(
                self.message_by_package_relative_name(MessageKind::Key),
                record.key.clone(),
            )
            .and(validate(
                self.message_by_package_relative_name(MessageKind::Value),
                record.value.clone(),
            ))
            .inspect_err(|err| error!(?err))?
        }

        Ok(())
    }
}

impl TryFrom<Bytes> for Schema {
    type Error = Error;

    fn try_from(proto: Bytes) -> Result<Self, Self::Error> {
        make_fd(proto)
            .map(|mut protos| {
                debug!(
                    protos = ?protos
                        .iter()
                        .flat_map(|proto| {
                            proto
                                .messages()
                                .map(|message| message.name_to_package().to_owned())
                        })
                        .collect::<Vec<_>>()
                );

                if let Some(mut meta) = META_FILE_DESCRIPTOR.clone() {
                    debug!(
                        meta = ?meta
                            .iter()
                            .flat_map(|proto| {
                                proto
                                    .messages()
                                    .map(|message| message.name_to_package().to_owned())
                            })
                            .collect::<Vec<_>>()
                    );

                    protos.append(&mut meta);
                }

                protos
            })
            .map(|file_descriptors| Self { file_descriptors })
    }
}

static WELL_KNOWN_TYPES: LazyLock<Vec<FileDescriptor>> = LazyLock::new(|| {
    vec![
        descriptor::file_descriptor().to_owned(),
        well_known_types::duration::file_descriptor().to_owned(),
        well_known_types::empty::file_descriptor().to_owned(),
        well_known_types::source_context::file_descriptor().to_owned(),
        well_known_types::timestamp::file_descriptor().to_owned(),
        well_known_types::wrappers::file_descriptor().to_owned(),
    ]
});

static META_FILE_DESCRIPTOR: LazyLock<Option<Vec<FileDescriptor>>> =
    LazyLock::new(|| make_fd(Bytes::from_static(include_bytes!("meta.proto"))).ok());

fn make_fd(proto: Bytes) -> Result<Vec<FileDescriptor>> {
    tempdir().map_err(Into::into).and_then(|temp_dir| {
        NamedTempFile::new_in(&temp_dir)
            .inspect(|temp_dir| debug!(?temp_dir))
            .map_err(Into::into)
            .and_then(|mut temp_file| {
                temp_file.write_all(&proto).map_err(Into::into).and(
                    protobuf_parse::Parser::new()
                        .pure()
                        .input(&temp_file)
                        .include(&temp_dir)
                        .parse_and_typecheck()
                        .inspect_err(|err| debug!(?err))
                        .map_err(Into::into)
                        .and_then(|parsed| {
                            parsed
                                .file_descriptors
                                .into_iter()
                                .map(|file_descriptor_proto| {
                                    FileDescriptor::new_dynamic(
                                        file_descriptor_proto,
                                        &WELL_KNOWN_TYPES[..],
                                    )
                                    .inspect_err(|err| debug!(?err))
                                    .map_err(Into::into)
                                })
                                .collect::<Result<Vec<_>>>()
                        }),
                )
            })
    })
}

fn field_ids(schemas: &[FileDescriptor]) -> BTreeMap<String, i32> {
    fn field_ids_with_path(
        path: &[&str],
        schemas: &[MessageDescriptor],
        id: &mut i32,
    ) -> BTreeMap<String, i32> {
        debug!(?path, ?schemas, ?id);

        let mut ids = BTreeMap::new();

        if path.is_empty() {
            for schema in schemas {
                _ = ids.insert(schema.name().to_lowercase(), *id);
                *id += 1;
            }
        }

        debug!(?ids);

        for schema in schemas {
            let name = schema.name().to_lowercase();

            let path = if path.is_empty() {
                Vec::from([&name[..]])
            } else {
                Vec::from(path)
            };

            for field in schema.fields() {
                debug!(path = ?path.join("."), field_name = ?field.name());
                let name = field.name().to_string();

                let path = {
                    let mut path = path.clone();
                    path.push(&name[..]);
                    path
                };

                _ = ids.insert(path.join("."), *id);
                *id += 1;
            }

            for field in schema.fields() {
                debug!(path = ?path.join("."), field_name = ?field.name());
                let name = field.name().to_string();

                let path = {
                    let mut path = path.clone();
                    path.push(&name[..]);
                    path
                };

                match field.runtime_field_type() {
                    RuntimeFieldType::Singular(singular) => {
                        debug!(?path, ?singular);

                        if let RuntimeType::Message(message_descriptor) = singular {
                            debug!(?path, ?message_descriptor);

                            if message_descriptor.full_name() != GOOGLE_PROTOBUF_TIMESTAMP {
                                ids.extend(
                                    field_ids_with_path(&path[..], &[message_descriptor], id)
                                        .into_iter(),
                                )
                            }
                        }
                    }

                    RuntimeFieldType::Repeated(repeated) => {
                        debug!(?path, ?repeated);

                        let path = {
                            let mut path = path.clone();
                            path.push(ARROW_LIST_FIELD_NAME);
                            path
                        };

                        _ = ids.insert(path.join("."), *id);
                        *id += 1;

                        if let RuntimeType::Message(message_descriptor) = repeated {
                            debug!(?path, ?message_descriptor);

                            ids.extend(
                                field_ids_with_path(&path[..], &[message_descriptor], id)
                                    .into_iter(),
                            )
                        }
                    }

                    RuntimeFieldType::Map(keys, values) => {
                        debug!(?path, ?keys, ?values);

                        let path = {
                            let mut path = path.clone();
                            path.push("entries");
                            path
                        };

                        _ = ids.insert(path.join("."), *id);
                        *id += 1;

                        {
                            let path = {
                                let mut path = path.clone();
                                path.push("keys");
                                path
                            };

                            _ = ids.insert(path.join("."), *id);
                            *id += 1;

                            if let RuntimeType::Message(message_descriptor) = keys {
                                debug!(?path, ?message_descriptor);

                                ids.extend(
                                    field_ids_with_path(&path[..], &[message_descriptor], id)
                                        .into_iter(),
                                )
                            }
                        }

                        {
                            let path = {
                                let mut path = path.clone();
                                path.push("values");
                                path
                            };

                            _ = ids.insert(path.join("."), *id);
                            *id += 1;

                            if let RuntimeType::Message(message_descriptor) = values {
                                debug!(?path, ?message_descriptor);

                                ids.extend(
                                    field_ids_with_path(&path[..], &[message_descriptor], id)
                                        .into_iter(),
                                )
                            }
                        }
                    }
                }
            }
        }

        debug!(?ids);
        ids
    }

    let descriptors = schemas
        .iter()
        .find_map(|fd| fd.message_by_package_relative_name(META))
        .into_iter()
        .chain(
            schemas
                .iter()
                .find_map(|fd| fd.message_by_package_relative_name(KEY))
                .into_iter()
                .chain(
                    schemas
                        .iter()
                        .find_map(|fd| fd.message_by_package_relative_name(VALUE)),
                ),
        )
        .collect::<Vec<_>>();

    field_ids_with_path(&[], &descriptors[..], &mut 1)
}

fn append_struct_builder(message: &dyn MessageDyn, builder: &mut StructBuilder) -> Result<()> {
    debug!(%message, ?builder);
    for (index, ref field) in message.descriptor_dyn().fields().enumerate() {
        debug!(field_name = field.name());

        match field.runtime_field_type() {
            RuntimeFieldType::Singular(singular) => {
                debug!(?singular);

                match field.get_singular_field_or_default(message) {
                    ReflectValueRef::U32(value) => builder
                        .field_builder::<Int32Builder>(index)
                        .ok_or(Error::Downcast)
                        .and_then(|values| {
                            i32::try_from(value)
                                .map_err(Into::into)
                                .map(|value| values.append_value(value))
                        })?,

                    ReflectValueRef::U64(value) => builder
                        .field_builder::<Int64Builder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .and_then(|values| {
                            i64::try_from(value)
                                .map_err(Into::into)
                                .map(|value| values.append_value(value))
                        })?,

                    ReflectValueRef::I32(value) | ReflectValueRef::Enum(_, value) => builder
                        .field_builder::<Int32Builder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::I64(value) => builder
                        .field_builder::<Int64Builder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::F32(value) => builder
                        .field_builder::<Float32Builder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::F64(value) => builder
                        .field_builder::<Float64Builder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::Bool(value) => builder
                        .field_builder::<BooleanBuilder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::String(value) => builder
                        .field_builder::<StringBuilder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::Bytes(value) => builder
                        .field_builder::<LargeBinaryBuilder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::Message(message_ref) => {
                        if message_ref.deref().descriptor_dyn().full_name()
                            == GOOGLE_PROTOBUF_TIMESTAMP
                        {
                            let message = print_to_string(message_ref.deref())?;
                            debug!(message = message.trim_matches('"'));

                            let value = DateTime::parse_from_rfc3339(message.trim_matches('"'))
                                .inspect(|dt| debug!(?dt))
                                .map(|dt| dt.timestamp_micros())?;
                            debug!(?value);

                            builder
                                .field_builder::<TimestampMicrosecondBuilder>(index)
                                .ok_or(Error::BadDowncast {
                                    field: field.name().to_owned(),
                                })
                                .map(|builder| builder.append_value(value))
                                .inspect_err(|err| debug!(?err, ?message_ref, ?builder))?
                        } else {
                            builder
                                .field_builder::<StructBuilder>(index)
                                .ok_or(Error::BadDowncast {
                                    field: field.name().to_owned(),
                                })
                                .and_then(|builder| {
                                    append_struct_builder(message_ref.deref(), builder)
                                })
                                .inspect_err(|err| debug!(?err, ?message_ref, ?builder))?
                        }
                    }
                }
            }

            RuntimeFieldType::Repeated(repeated) => {
                debug!(?repeated);

                let builder = builder
                    .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(index)
                    .ok_or(Error::Downcast)
                    .inspect_err(|err| error!(?err, ?repeated))?;

                let values = builder.values().as_any_mut();

                for value in field.get_repeated(message) {
                    match value {
                        ReflectValueRef::U32(value) => values
                            .downcast_mut::<Int32Builder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .and_then(|builder| {
                                i32::try_from(value)
                                    .map_err(Into::into)
                                    .map(|value| builder.append_value(value))
                            })?,

                        ReflectValueRef::U64(value) => values
                            .downcast_mut::<Int64Builder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .and_then(|builder| {
                                i64::try_from(value)
                                    .map_err(Into::into)
                                    .map(|value| builder.append_value(value))
                            })?,

                        ReflectValueRef::I32(value) | ReflectValueRef::Enum(_, value) => values
                            .downcast_mut::<Int32Builder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::I64(value) => values
                            .downcast_mut::<Int64Builder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::F32(value) => values
                            .downcast_mut::<Float32Builder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::F64(value) => values
                            .downcast_mut::<Float64Builder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::Bool(value) => values
                            .downcast_mut::<BooleanBuilder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::String(value) => values
                            .downcast_mut::<StringBuilder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::Bytes(value) => values
                            .downcast_mut::<LargeBinaryBuilder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::Message(message_ref) => {
                            if message_ref.deref().descriptor_dyn().full_name()
                                == GOOGLE_PROTOBUF_TIMESTAMP
                            {
                                let message = print_to_string(message_ref.deref())?;
                                debug!(message = message.trim_matches('"'));

                                let value = DateTime::parse_from_rfc3339(message.trim_matches('"'))
                                    .inspect(|dt| debug!(?dt))
                                    .map(|dt| dt.timestamp_micros())?;
                                debug!(?value);

                                values
                                    .downcast_mut::<TimestampMicrosecondBuilder>()
                                    .ok_or(Error::BadDowncast {
                                        field: field.name().to_owned(),
                                    })
                                    .map(|builder| builder.append_value(value))?
                            } else {
                                values
                                    .downcast_mut::<StructBuilder>()
                                    .ok_or(Error::BadDowncast {
                                        field: field.name().to_owned(),
                                    })
                                    .inspect_err(|err| error!(?err, ?message_ref))
                                    .and_then(|builder| {
                                        append_struct_builder(message_ref.deref(), builder)
                                    })?
                            }
                        }
                    }
                }

                builder.append(true);
            }

            RuntimeFieldType::Map(key, value) => {
                debug!(?key, ?value);

                builder
                    .as_any_mut()
                    .downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
                    .ok_or(Error::BadDowncast {
                        field: field.name().to_owned(),
                    })
                    .and_then(|builder| append_map_builder(message, field, builder))?
            }
        }
    }

    builder.append(true);

    Ok(())
}

fn append_map_builder(
    message: &dyn MessageDyn,
    field: &FieldDescriptor,
    builder: &mut MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>,
) -> Result<()> {
    for (key, value) in &field.get_map(message) {
        decode_value(key, builder.keys())?;
        decode_value(value, builder.values())?;
    }

    builder.append(true).map_err(Into::into)
}

fn decode_value(value: ReflectValueRef<'_>, builder: &mut dyn ArrayBuilder) -> Result<()> {
    debug!(?value);

    match value {
        ReflectValueRef::U32(value) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or(Error::Downcast)
            .and_then(|builder| {
                i32::try_from(value)
                    .map_err(Into::into)
                    .map(|value| builder.append_value(value))
            }),

        ReflectValueRef::U64(value) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .and_then(|builder| {
                i64::try_from(value)
                    .map_err(Into::into)
                    .map(|value| builder.append_value(value))
            }),

        ReflectValueRef::I32(value) | ReflectValueRef::Enum(_, value) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::I64(value) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::F32(value) => builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::F64(value) => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::Bool(value) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::String(value) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::Bytes(value) => builder
            .as_any_mut()
            .downcast_mut::<LargeBinaryBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::Message(message_ref) => {
            if message_ref.descriptor_dyn().full_name() == GOOGLE_PROTOBUF_TIMESTAMP {
                let message = print_to_string(message_ref.deref())?;
                debug!(message = message.trim_matches('"'));

                let value = DateTime::parse_from_rfc3339(message.trim_matches('"'))
                    .inspect(|dt| debug!(?dt))
                    .map(|dt| dt.timestamp_micros())?;
                debug!(?value);

                builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMicrosecondBuilder>()
                    .ok_or(Error::Downcast)
                    .map(|builder| builder.append_value(value))
            } else {
                builder
                    .as_any_mut()
                    .downcast_mut::<StructBuilder>()
                    .ok_or(Error::Downcast)
                    .inspect_err(|err| error!(?err, ?message_ref))
                    .and_then(|builder| append_struct_builder(message_ref.deref(), builder))
            }
        }
    }
}

fn process_message_descriptor<'a, T>(
    descriptor: Option<MessageDescriptor>,
    encoded: Option<Bytes>,
    builders: &mut T,
) -> Result<()>
where
    T: Iterator<Item = &'a mut Box<dyn ArrayBuilder>>,
{
    let Some(descriptor) = descriptor else {
        return Ok(());
    };

    debug!(descriptor = descriptor.name(), ?encoded,);

    let message = {
        let mut message = descriptor.new_instance();
        encoded.map_or(Err(Error::Api(ErrorCode::InvalidRecord)), |encoded| {
            message
                .merge_from_dyn(&mut CodedInputStream::from_tokio_bytes(&encoded))
                .inspect_err(|err| error!(?err))
                .map_err(|_err| Error::Api(ErrorCode::InvalidRecord))
        })?;

        message
    };

    builders
        .next()
        .ok_or(Error::BuilderExhausted)
        .map(|column| column.as_any_mut())
        .inspect(|column| debug!(?column))
        .and_then(|column| {
            column
                .downcast_mut::<StructBuilder>()
                .ok_or(Error::Downcast)
                .inspect_err(|err| debug!(?err))
        })
        .and_then(|column| append_struct_builder(message.as_ref(), column))
        .inspect_err(|err| debug!(?err))
}

impl AsArrow for Schema {
    fn as_arrow(
        &self,
        partition: i32,
        batch: &Batch,
        lake_type: LakeHouseType,
    ) -> Result<RecordBatch> {
        debug!(?batch, ?lake_type);

        let ids = if lake_type.is_iceberg() {
            field_ids(&self.file_descriptors)
        } else {
            BTreeMap::new()
        };

        let schema = arrow_schema(&ids, self);
        let mut record_builder = RecordBuilder::new(&ids, self);

        for record in batch.records.iter() {
            debug!(?record);

            process_message_descriptor(
                self.message_by_package_relative_name(MessageKind::Meta),
                self.encode_from_value(
                    MessageKind::Meta,
                    &DateTime::from_timestamp_millis(batch.base_timestamp + record.timestamp_delta)
                        .map_or(json!({"partition": partition}), |date_time| {
                            json!({
                            "partition": partition,
                            "timestamp": date_time.to_rfc3339(),
                            "year": date_time.date_naive().year(),
                            "month": date_time.date_naive().month(),
                            "day": date_time.date_naive().day()})
                        }),
                )
                .map(Some)?,
                &mut record_builder.meta.iter_mut(),
            )
            .inspect_err(|err| debug!(?err))?;

            process_message_descriptor(
                self.message_by_package_relative_name(MessageKind::Key),
                record.key(),
                &mut record_builder.key.iter_mut(),
            )
            .inspect_err(|err| debug!(?err))?;

            process_message_descriptor(
                self.message_by_package_relative_name(MessageKind::Value),
                record.value(),
                &mut record_builder.value.iter_mut(),
            )
            .inspect_err(|err| debug!(?err))?;
        }

        debug!(
            meta_rows = ?record_builder.meta.iter().map(|rows| rows.len()).collect::<Vec<_>>(),
            key_rows = ?record_builder.key.iter().map(|rows| rows.len()).collect::<Vec<_>>(),
            value_rows = ?record_builder.value.iter().map(|rows| rows.len()).collect::<Vec<_>>()
        );

        let mut columns = vec![];

        if let Some(meta) = record_builder.meta {
            columns.push(meta);
        }

        if let Some(key) = record_builder.key {
            columns.push(key);
        }

        if let Some(value) = record_builder.value {
            columns.push(value);
        }

        debug!(columns = columns.len(), ?schema);

        RecordBatch::try_new(
            schema.into(),
            columns.iter_mut().map(|builder| builder.finish()).collect(),
        )
        .inspect_err(|err| debug!(?err))
        .inspect(|record_batch| debug!(?record_batch))
        .map_err(Into::into)
    }
}

impl Schema {
    fn to_json_value(
        &self,
        message_kind: MessageKind,
        encoded: Option<Bytes>,
    ) -> Result<(String, Value)> {
        decode(self.message_by_package_relative_name(message_kind), encoded)
            .inspect(|decoded| debug!(?decoded))
            .and_then(|decoded| {
                decoded.map_or(
                    Ok((message_kind.as_ref().to_lowercase(), Value::Null)),
                    |message| {
                        print_to_string(message.as_ref())
                            .inspect(|s| debug!(s))
                            .map_err(Into::into)
                            .and_then(|s| serde_json::from_str::<Value>(&s).map_err(Into::into))
                            .map(|value| (message_kind.as_ref().to_lowercase(), value))
                            .inspect(|(k, v)| debug!(k, ?v))
                    },
                )
            })
    }
}

impl AsJsonValue for Schema {
    fn as_json_value(&self, batch: &Batch) -> Result<Value> {
        Ok(Value::Array(
            batch
                .records
                .iter()
                .inspect(|record| debug!(?record))
                .map(|record| {
                    Value::Object(Map::from_iter(
                        self.to_json_value(MessageKind::Key, record.key.clone())
                            .into_iter()
                            .chain(self.to_json_value(MessageKind::Value, record.value.clone())),
                    ))
                })
                .collect::<Vec<_>>(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::Registry;

    use super::*;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::prelude::*;
    use iceberg::{
        io::FileIOBuilder,
        spec::{
            DataFile, DataFileFormat::Parquet, Schema as IcebergSchema,
            SchemaRef as IcebergSchemaRef,
        },
        writer::{
            IcebergWriter, IcebergWriterBuilder,
            base_writer::data_file_writer::DataFileWriterBuilder,
            file_writer::{
                ParquetWriterBuilder,
                location_generator::{DefaultFileNameGenerator, LocationGenerator},
            },
        },
    };
    use object_store::{ObjectStore, PutPayload, memory::InMemory, path::Path};
    use parquet::file::properties::WriterProperties;
    use serde_json::json;
    use std::{fs::File, sync::Arc, thread};
    use tansu_sans_io::record::Record;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    fn init_tracing() -> Result<DefaultGuard> {
        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(
                    EnvFilter::from_default_env()
                        .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
                )
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    #[tokio::test]
    async fn key_only_invalid_record() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let proto = Bytes::from_static(
            br#"
            syntax = 'proto3';

            message Key {
              int32 id = 1;
            }

            message Value {
              string name = 1;
              string email = 2;
            }
            "#,
        );

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.proto"));
        let payload = PutPayload::from(proto.clone());
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let key = Schema::try_from(proto.clone())
            .and_then(|schema| schema.encode_from_value(MessageKind::Key, &json!({"id": 12321})))?;

        let batch = Batch::builder()
            .record(Record::builder().key(key.clone().into()))
            .build()?;

        assert!(matches!(
            registry.validate(topic, &batch).await,
            Err(Error::Api(ErrorCode::InvalidRecord))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn value_only_invalid_record() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let proto = Bytes::from_static(
            br#"
                syntax = 'proto3';

                message Key {
                  int32 id = 1;
                }

                message Value {
                  string name = 1;
                  string email = 2;
                }
                "#,
        );

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.proto"));
        let payload = PutPayload::from(proto.clone());
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let value = Schema::try_from(proto).and_then(|schema| {
            schema.encode_from_value(
                MessageKind::Value,
                &json!({
                    "name": "alice",
                    "email": "alice@example.com"
                }),
            )
        })?;

        let batch = Batch::builder()
            .record(Record::builder().value(value.clone().into()))
            .build()?;

        assert!(matches!(
            registry.validate(topic, &batch).await,
            Err(Error::Api(ErrorCode::InvalidRecord))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn key_and_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let proto = Bytes::from_static(
            br#"
                syntax = 'proto3';

                message Key {
                  int32 id = 1;
                }

                message Value {
                  string name = 1;
                  string email = 2;
                }
                "#,
        );

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.proto"));
        let payload = PutPayload::from(proto.clone());
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let schema = Schema::try_from(proto.clone())?;

        let key = schema.encode_from_value(MessageKind::Key, &json!({"id": 12321}))?;
        let value = schema.encode_from_value(
            MessageKind::Value,
            &json!({
                "name": "alice",
                "email": "alice@example.com"
            }),
        )?;

        let batch = Batch::builder()
            .record(
                Record::builder()
                    .key(key.clone().into())
                    .value(value.clone().into()),
            )
            .build()?;

        registry.validate(topic, &batch).await
    }

    #[tokio::test]
    async fn enumeration() -> Result<()> {
        let _guard = init_tracing()?;
        let proto = Bytes::from_static(
            br#"
            syntax = 'proto3';

            message Key {
                int32 id = 1;
            }

            enum Corpus {
              CORPUS_UNSPECIFIED = 0;
              CORPUS_UNIVERSAL = 1;
              CORPUS_WEB = 2;
              CORPUS_IMAGES = 3;
              CORPUS_LOCAL = 4;
              CORPUS_NEWS = 5;
              CORPUS_PRODUCTS = 6;
              CORPUS_VIDEO = 7;
            }

            message Value {
              string query = 1;
              int32 page_number = 2;
              int32 results_per_page = 3;
              Corpus corpus = 4;
            }
            "#,
        );

        let kv = [
            (
                &json!({"id": 32123}),
                &json!({"query": "abc/def", "pageNumber": 6, "resultsPerPage": 13, "corpus": "CORPUS_WEB"}),
            ),
            (
                &json!({"id": 45654}),
                &json!({"query": "pqr/stu", "pageNumber": 42, "resultsPerPage": 5, "corpus": "CORPUS_PRODUCTS"}),
            ),
        ];

        let schema = Schema::try_from(proto)?;

        let batch = {
            let mut batch = Batch::builder().base_timestamp(119_731_017_000);

            for (delta, (key, value)) in kv.iter().enumerate() {
                batch = batch.record(
                    Record::builder()
                        .key(schema.encode_from_value(MessageKind::Key, key)?.into())
                        .value(schema.encode_from_value(MessageKind::Value, value)?.into())
                        .timestamp_delta(delta as i64)
                        .offset_delta(delta as i32),
                );
            }

            batch.build()?
        };

        let record_batch = schema.as_arrow(0, &batch, LakeHouseType::Iceberg)?;

        let data_files = iceberg_write(record_batch.clone()).await?;
        assert_eq!(1, data_files.len());
        assert_eq!(2, data_files[0].record_count());

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch.clone())?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+------------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------+",
            "| meta                                                                               | key         | value                                                             |",
            "+------------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------+",
            "| {partition: 0, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17}     | {id: 32123} | {query: abc/def, page_number: 6, results_per_page: 13, corpus: 2} |",
            "| {partition: 0, timestamp: 1973-10-17T18:36:57.001, year: 1973, month: 10, day: 17} | {id: 45654} | {query: pqr/stu, page_number: 42, results_per_page: 5, corpus: 6} |",
            "+------------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        assert_eq!(
            json!([{"key": {"id": 32123},
                    "value": {
                        "query": "abc/def",
                        "pageNumber": 6,
                        "resultsPerPage": 13,
                        "corpus": "CORPUS_WEB"}},
                    {"key": {"id": 45654},
                     "value": {
                         "query": "pqr/stu",
                         "pageNumber": 42,
                         "resultsPerPage": 5,
                         "corpus": "CORPUS_PRODUCTS"}}]),
            schema.as_json_value(&batch)?
        );

        Ok(())
    }

    async fn iceberg_write(record_batch: RecordBatch) -> Result<Vec<DataFile>> {
        let iceberg_schema = IcebergSchema::try_from(record_batch.schema().as_ref())
            .map(IcebergSchemaRef::new)
            .inspect(|schema| debug!(?schema))
            .inspect_err(|err| debug!(?err))?;

        let memory = FileIOBuilder::new("memory").build()?;

        #[derive(Clone)]
        struct Location;

        impl LocationGenerator for Location {
            fn generate_location(&self, file_name: &str) -> String {
                format!("abc/{file_name}")
            }
        }

        let writer = ParquetWriterBuilder::new(
            WriterProperties::default(),
            iceberg_schema,
            memory,
            Location,
            DefaultFileNameGenerator::new("pqr".into(), None, Parquet),
        );

        let mut data_file_writer = DataFileWriterBuilder::new(writer, None, 0)
            .build()
            .await
            .inspect_err(|err| error!(?err))?;

        data_file_writer
            .write(record_batch)
            .await
            .inspect_err(|err| debug!(?err))?;

        data_file_writer
            .close()
            .await
            .inspect(|data_files| debug!(?data_files))
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
    }

    #[tokio::test]
    async fn message_descriptor_singular_to_field() -> Result<()> {
        let _guard = init_tracing()?;

        let proto = Bytes::from_static(
            br#"
            syntax = 'proto3';

            message Key {
                int32 id = 1;
            }

            message Value {
                double a = 1;
                float b = 2;
                int32 c = 3;
                int64 d = 4;
                uint32 e = 5;
                uint64 f = 6;
                sint32 g = 7;
                sint64 h = 8;
                fixed32 i = 9;
                fixed64 j = 10;
                sfixed32 k = 11;
                sfixed64 l = 12;
                bool m = 13;
                string n = 14;
                bytes o = 15;
            }
            "#,
        );

        let kv = [(
            json!({"id": 32123}),
            json!({"a": 567.65,
                    "b": 45.654,
                    "c": -6,
                    "d": -66,
                    "e": 23432,
                    "f": 34543,
                    "g": 45654,
                    "h": 67876,
                    "i": 78987,
                    "j": 89098,
                    "k": 90109,
                    "l": 12321,
                    "m": true,
                    "n": "Hello World!",
                    "o": "YWJjMTIzIT8kKiYoKSctPUB+"}),
        )];

        let schema = Schema::try_from(proto)?;

        let batch = {
            let mut batch = Batch::builder().base_timestamp(119_731_017_000);

            for (delta, (key, value)) in kv.iter().enumerate() {
                batch = batch.record(
                    Record::builder()
                        .key(schema.encode_from_value(MessageKind::Key, key)?.into())
                        .value(schema.encode_from_value(MessageKind::Value, value)?.into())
                        .timestamp_delta(delta as i64)
                        .offset_delta(delta as i32),
                );
            }

            batch.build()?
        };

        let record_batch = schema.as_arrow(0, &batch, LakeHouseType::Iceberg)?;

        let data_files = iceberg_write(record_batch.clone()).await?;
        assert_eq!(1, data_files.len());
        assert_eq!(1, data_files[0].record_count());

        let ctx = SessionContext::new();

        _ = ctx.register_batch("ty", record_batch)?;
        let df = ctx.sql("select * from ty").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+--------------------------------------------------------------------------------+-------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| meta                                                                           | key         | value                                                                                                                                                                                    |",
            "+--------------------------------------------------------------------------------+-------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| {partition: 0, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {id: 32123} | {a: 567.65, b: 45.654, c: -6, d: -66, e: 23432, f: 34543, g: 45654, h: 67876, i: 78987, j: 89098, k: 90109, l: 12321, m: true, n: Hello World!, o: 616263313233213f242a262829272d3d407e} |",
            "+--------------------------------------------------------------------------------+-------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn key_and_value_as_arrow() -> Result<()> {
        let _guard = init_tracing()?;

        let proto = Bytes::from_static(
            br#"
            syntax = 'proto3';

            message Key {
                int32 id = 1;
            }

            message Value {
                string name = 1;
                string email = 2;
            }
            "#,
        );

        let kv = [
            (
                json!({"id": 12321}),
                json!({
                    "name": "alice",
                    "email": "alice@example.com"
                }),
            ),
            (
                json!({"id": 32123}),
                json!({
                    "name": "bob",
                    "email": "bob@example.com"
                }),
            ),
        ];

        let schema = Schema::try_from(proto)?;

        let batch = {
            let mut batch = Batch::builder().base_timestamp(119_731_017_000);

            for (delta, (key, value)) in kv.iter().enumerate() {
                batch = batch.record(
                    Record::builder()
                        .key(schema.encode_from_value(MessageKind::Key, key)?.into())
                        .value(schema.encode_from_value(MessageKind::Value, value)?.into())
                        .timestamp_delta(delta as i64)
                        .offset_delta(delta as i32),
                );
            }

            batch.build()?
        };

        let record_batch = schema.as_arrow(0, &batch, LakeHouseType::Iceberg)?;

        let data_files = iceberg_write(record_batch.clone()).await?;
        assert_eq!(1, data_files.len());
        assert_eq!(2, data_files[0].record_count());

        let ctx = SessionContext::new();

        let topic = "abc";

        _ = ctx.register_batch(topic, record_batch)?;
        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+------------------------------------------------------------------------------------+-------------+-----------------------------------------+",
            "| meta                                                                               | key         | value                                   |",
            "+------------------------------------------------------------------------------------+-------------+-----------------------------------------+",
            "| {partition: 0, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17}     | {id: 12321} | {name: alice, email: alice@example.com} |",
            "| {partition: 0, timestamp: 1973-10-17T18:36:57.001, year: 1973, month: 10, day: 17} | {id: 32123} | {name: bob, email: bob@example.com}     |",
            "+------------------------------------------------------------------------------------+-------------+-----------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);
        Ok(())
    }

    #[tokio::test]
    async fn taxi() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = Schema::try_from(Bytes::from_static(include_bytes!(
            "../../../tansu/etc/schema/taxi.proto"
        )))?;

        let value = schema.encode_from_value(
            MessageKind::Value,
            &json!({
              "vendor_id": 1,
              "trip_id": 1000371,
              "trip_distance": 1.8,
              "fare_amount": 15.32,
              "store_and_fwd": "N"
            }),
        )?;

        let batch = Batch::builder()
            .record(Record::builder().value(value.into()))
            .base_timestamp(119_731_017_000)
            .build()?;

        let record_batch = schema.as_arrow(0, &batch, LakeHouseType::Iceberg)?;

        let data_files = iceberg_write(record_batch.clone()).await?;
        assert_eq!(1, data_files.len());
        assert_eq!(1, data_files[0].record_count());

        let ctx = SessionContext::new();

        let topic = "taxi";
        _ = ctx.register_batch(topic, record_batch)?;
        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+--------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+",
            "| meta                                                                           | value                                                                                      |",
            "+--------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+",
            "| {partition: 0, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {vendor_id: 1, trip_id: 1000371, trip_distance: 1.8, fare_amount: 15.32, store_and_fwd: 0} |",
            "+--------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn simple_map() -> Result<()> {
        let _guard = init_tracing()?;

        let proto = Bytes::from_static(
            br#"
            syntax = 'proto3';

            message Value {
                map<string, int32> kv = 1;
            }
            "#,
        );

        let schema = Schema::try_from(proto)?;

        let value = schema.encode_from_value(
            MessageKind::Value,
            &json!({
                "kv": {"a": 31234, "b": 56765, "c": 12321}
            }),
        )?;

        let batch = Batch::builder()
            .record(Record::builder().value(value.into()))
            .build()?;

        let record_batch = schema.as_arrow(0, &batch, LakeHouseType::Parquet)?;

        let ctx = SessionContext::new();

        let topic = "snippets";
        _ = ctx.register_batch(topic, record_batch)?;
        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty = pretty_format_batches(&results)?.to_string();
        debug!(pretty);

        let kv = pretty.trim().lines().collect::<Vec<_>>()[3];
        debug!(kv);

        assert!(
            kv == "| {a: 31234, b: 56765, c: 12321} |"
                || kv == "| {a: 31234, c: 12321, b: 56765} |"
                || kv == "| {b: 56765, c: 12321, a: 31234} |"
                || kv == "| {b: 56765, a: 31234, c: 12321} |"
                || kv == "| {c: 12321, a: 31234, b: 56765} |"
                || kv == "| {c: 12321, b: 56765, a: 31234} |"
        );

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn map_other_type() -> Result<()> {
        let _guard = init_tracing()?;

        let proto = Bytes::from_static(
            br#"
            syntax = 'proto3';

            message Project {
                string name = 1;
                float complete = 2;
            }

            message Value {
                map<string, Project> kv = 1;
            }
            "#,
        );

        let schema = Schema::try_from(proto)?;

        let value = schema.encode_from_value(
            MessageKind::Value,
            &json!({
                "kv": {"a": {"name": "xyz", "complete": 0.99}}
            }),
        )?;

        let batch = Batch::builder()
            .record(Record::builder().value(value.into()))
            .base_timestamp(119_731_017_000)
            .build()?;

        let record_batch = schema.as_arrow(0, &batch, LakeHouseType::Parquet)?;

        let ctx = SessionContext::new();

        let topic = "snippets";
        _ = ctx.register_batch(topic, record_batch)?;
        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+----------------------------------+",
            "| kv                               |",
            "+----------------------------------+",
            "| {a: {name: xyz, complete: 0.99}} |",
            "+----------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn value_message_ref() -> Result<()> {
        let _guard = init_tracing()?;

        let proto = Bytes::from_static(
            br#"
                syntax = 'proto3';

                message Project {
                    string name = 1;
                    float complete = 2;
                }

                message Value {
                    Project project = 1;
                    string title = 2;
                }
                "#,
        );

        let schema = Schema::try_from(proto)?;

        let value = schema.encode_from_value(
            MessageKind::Value,
            &json!({
                "project": {"name": "xyz", "complete": 0.99},
                "title": "abc",
            }),
        )?;

        let batch = Batch::builder()
            .base_timestamp(119_731_017_000)
            .record(Record::builder().value(value.into()))
            .build()?;

        let record_batch = schema.as_arrow(0, &batch, LakeHouseType::Iceberg)?;

        let data_files = iceberg_write(record_batch.clone()).await?;
        assert_eq!(1, data_files.len());
        assert_eq!(1, data_files[0].record_count());

        let ctx = SessionContext::new();

        let topic = "snippets";
        _ = ctx.register_batch(topic, record_batch)?;
        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+--------------------------------------------------------------------------------+----------------------------------------------------+",
            "| meta                                                                           | value                                              |",
            "+--------------------------------------------------------------------------------+----------------------------------------------------+",
            "| {partition: 0, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {project: {name: xyz, complete: 0.99}, title: abc} |",
            "+--------------------------------------------------------------------------------+----------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn simple_repeated() -> Result<()> {
        let _guard = init_tracing()?;

        let proto = Bytes::from_static(
            br#"
            syntax = 'proto3';

            message Value {
              string url = 1;
              string title = 2;
              repeated string snippets = 3;
            }
            "#,
        );

        let schema = Schema::try_from(proto)?;

        let value = schema.encode_from_value(
            MessageKind::Value,
            &json!({
                "url": "https://example.com/a", "title": "a", "snippets": ["p", "q", "r"]
            }),
        )?;

        let batch = Batch::builder()
            .base_timestamp(119_731_017_000)
            .record(Record::builder().value(value.into()))
            .build()?;

        let record_batch = schema.as_arrow(0, &batch, LakeHouseType::Iceberg)?;
        let data_files = iceberg_write(record_batch.clone()).await?;
        assert_eq!(1, data_files.len());
        assert_eq!(1, data_files[0].record_count());

        let ctx = SessionContext::new();

        let topic = "snippets";
        _ = ctx.register_batch(topic, record_batch)?;
        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+--------------------------------------------------------------------------------+-------------------------------------------------------------+",
            "| meta                                                                           | value                                                       |",
            "+--------------------------------------------------------------------------------+-------------------------------------------------------------+",
            "| {partition: 0, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {url: https://example.com/a, title: a, snippets: [p, q, r]} |",
            "+--------------------------------------------------------------------------------+-------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn repeated() -> Result<()> {
        let _guard = init_tracing()?;

        let proto = Bytes::from_static(
            br#"
                syntax = 'proto3';

                message Value {
                  repeated Result results = 1;
                }

                message Result {
                  string url = 1;
                  string title = 2;
                  repeated string snippets = 3;
                }
                "#,
        );

        let schema = Schema::try_from(proto)?;

        let value = schema.encode_from_value(
            MessageKind::Value,
            &json!({
                "results": [{"url": "https://example.com/abc", "title": "a", "snippets": ["p", "q", "r"]},
                            {"url": "https://example.com/def", "title": "b", "snippets": ["x", "y", "z"]}]
            }),
        )?;

        let batch = Batch::builder()
            .record(Record::builder().value(value.into()))
            .base_timestamp(119_731_017_000)
            .build()?;

        let record_batch = schema.as_arrow(0, &batch, LakeHouseType::Iceberg)?;
        let data_files = iceberg_write(record_batch.clone()).await?;
        assert_eq!(1, data_files.len());
        assert_eq!(1, data_files[0].record_count());

        let ctx = SessionContext::new();

        let topic = "snippets";
        _ = ctx.register_batch(topic, record_batch)?;
        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+--------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------+",
            "| meta                                                                           | value                                                                                                                                     |",
            "+--------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------+",
            "| {partition: 0, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {results: [{url: https://example.com/abc, title: a, snippets: [p, q, r]}, {url: https://example.com/def, title: b, snippets: [x, y, z]}]} |",
            "+--------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn no_schema() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let registry = Registry::new(InMemory::new());

        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");
        let value = Bytes::from_static(b"Consectetur adipiscing elit");

        let batch = Batch::builder()
            .record(
                Record::builder()
                    .key(key.clone().into())
                    .value(value.clone().into()),
            )
            .build()?;

        registry.validate(topic, &batch).await
    }

    #[tokio::test]
    async fn empty_schema() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let proto = Bytes::from_static(br#"syntax = 'proto3';"#);

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.proto"));
        let payload = PutPayload::from(proto.clone());
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");
        let value = Bytes::from_static(b"Consectetur adipiscing elit");

        let batch = Batch::builder()
            .record(
                Record::builder()
                    .key(key.clone().into())
                    .value(value.clone().into()),
            )
            .build()?;

        registry.validate(topic, &batch).await
    }

    #[tokio::test]
    async fn key_schema_only() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let proto = Bytes::from_static(
            br#"
                syntax = 'proto3';

                message Key {
                  int32 id = 1;
                }
                "#,
        );

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.proto"));
        let payload = PutPayload::from(proto.clone());
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let schema = Schema::try_from(proto)?;

        let key = schema.encode_from_value(MessageKind::Key, &json!({"id": 12321}))?;
        let value = Bytes::from_static(b"Consectetur adipiscing elit");

        let batch = Batch::builder()
            .record(
                Record::builder()
                    .key(key.clone().into())
                    .value(value.clone().into()),
            )
            .build()?;

        registry.validate(topic, &batch).await
    }

    #[tokio::test]
    async fn bad_key() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let proto = Bytes::from_static(
            br#"
                syntax = 'proto3';

                message Key {
                  int32 id = 1;
                }
                "#,
        );

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.proto"));
        let payload = PutPayload::from(proto.clone());
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");

        let batch = Batch::builder()
            .record(Record::builder().key(key.clone().into()))
            .build()?;

        assert!(matches!(
            registry.validate(topic, &batch).await,
            Err(Error::Api(ErrorCode::InvalidRecord))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn value_schema_only() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let proto = Bytes::from_static(
            br#"
            syntax = 'proto3';

            message Value {
                string name = 1;
                string email = 2;
            }
            "#,
        );

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.proto"));
        let payload = PutPayload::from(proto.clone());
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let schema = Schema::try_from(proto)?;

        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");

        let value = schema.encode_from_value(
            MessageKind::Value,
            &json!({
                "name": "alice",
                "email": "alice@example.com"
            }),
        )?;

        let batch = Batch::builder()
            .record(
                Record::builder()
                    .key(key.clone().into())
                    .value(value.clone().into()),
            )
            .build()?;

        registry.validate(topic, &batch).await
    }

    #[tokio::test]
    async fn bad_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let proto = Bytes::from_static(
            br#"
                syntax = 'proto3';

                message Value {
                  string name = 1;
                  string email = 2;
                }
                "#,
        );

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.proto"));
        let payload = PutPayload::from(proto.clone());
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let value = Bytes::from_static(b"Consectetur adipiscing elit");

        let batch = Batch::builder()
            .record(Record::builder().value(value.clone().into()))
            .build()?;

        assert!(matches!(
            registry.validate(topic, &batch).await,
            Err(Error::Api(ErrorCode::InvalidRecord))
        ));

        Ok(())
    }

    #[test]
    fn timestamp_well_known_type() -> Result<()> {
        let _guard = init_tracing()?;
        let proto = Bytes::from_static(
            br#"
            syntax = "proto3";

            import "google/protobuf/timestamp.proto";

            message Value {
                google.protobuf.Timestamp timestamp = 1;
            }
            "#,
        );

        let _file_descriptor = make_fd(proto).inspect(|fds| debug!(?fds))?;

        Ok(())
    }

    #[tokio::test]
    async fn customer_001() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = Schema::try_from(Bytes::from_static(include_bytes!(
            "../tests/customer-001.proto"
        )))?;

        let topic = "t";
        let ctx = SessionContext::new();

        _ = schema
            .generate()
            .and_then(|record| {
                Batch::builder()
                    .record(record)
                    .base_timestamp(119_731_017_000)
                    .build()
                    .map_err(Into::into)
            })
            .and_then(|batch| schema.as_arrow(0, &batch, LakeHouseType::Parquet))
            .and_then(|record_batch| ctx.register_batch(topic, record_batch).map_err(Into::into))?;

        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+--------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| meta                                                                           | value                                                                                                                                                    |",
            "+--------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| {partition: 0, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {email_address: lorem, full_name: ipsum, home: {building_number: dolor, street_name: sit, city: amet, post_code: consectetur, country_name: adipiscing}} |",
            "+--------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn customer_002_user_id() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = Schema::try_from(Bytes::from_static(include_bytes!(
            "../tests/customer-002.proto"
        )))?;

        let message_generator = schema.message_generator().unwrap();

        let user_id = schema
            .message_by_package_relative_name(MessageKind::Value)
            .and_then(|message_descriptor| message_descriptor.field_by_name("user_id"))
            .unwrap();

        let configuration = FieldGeneratorConfiguration::with_field_generator(
            user_id.proto(),
            &message_generator.generator_descriptor,
        );

        assert!(configuration.skip());

        Ok(())
    }

    #[tokio::test]
    async fn customer_002_email_address() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = Schema::try_from(Bytes::from_static(include_bytes!(
            "../tests/customer-002.proto"
        )))?;

        let message_generator = schema.message_generator().unwrap();

        let user_id = schema
            .message_by_package_relative_name(MessageKind::Value)
            .and_then(|message_descriptor| message_descriptor.field_by_name("email_address"))
            .unwrap();

        let configuration = FieldGeneratorConfiguration::with_field_generator(
            user_id.proto(),
            &message_generator.generator_descriptor,
        );

        assert!(!configuration.skip());
        assert_eq!(Some("\"lorem\""), configuration.script());

        Ok(())
    }

    #[tokio::test]
    async fn customer_002_industry() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = Schema::try_from(Bytes::from_static(include_bytes!(
            "../tests/customer-002.proto"
        )))?;

        let message_generator = schema.message_generator().unwrap();

        let user_id = schema
            .message_by_package_relative_name(MessageKind::Value)
            .and_then(|message_descriptor| message_descriptor.field_by_name("industry"))
            .unwrap();

        let configuration = FieldGeneratorConfiguration::with_field_generator(
            user_id.proto(),
            &message_generator.generator_descriptor,
        );

        assert!(!configuration.skip());
        assert_eq!(Some(3), configuration.repeated_len());
        assert_eq!(Some("\"elit\""), configuration.repeated_script());

        Ok(())
    }

    #[tokio::test]
    async fn customer_002() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = Schema::try_from(Bytes::from_static(include_bytes!(
            "../tests/customer-002.proto"
        )))?;

        let topic = "t";
        let ctx = SessionContext::new();

        _ = schema
            .generate()
            .and_then(|record| {
                Batch::builder()
                    .record(record)
                    .base_timestamp(119_731_017_000)
                    .build()
                    .map_err(Into::into)
            })
            .and_then(|batch| schema.as_arrow(0, &batch, LakeHouseType::Parquet))
            .and_then(|record_batch| ctx.register_batch(topic, record_batch).map_err(Into::into))?;

        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+--------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| meta                                                                           | value                                                                                                                                                                                              |",
            "+--------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| {partition: 0, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {user_id: 0, email_address: lorem, full_name: ipsum, home: {building_number: dolor, street_name: sit, city: amet, post_code: consectetur, country_name: adipiscing}, industry: [elit, elit, elit]} |",
            "+--------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn customer_003_industry() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = Schema::try_from(Bytes::from_static(include_bytes!(
            "../tests/customer-003.proto"
        )))?;

        let message_generator = schema.message_generator().unwrap();

        let user_id = schema
            .message_by_package_relative_name(MessageKind::Value)
            .and_then(|message_descriptor| message_descriptor.field_by_name("industry"))
            .unwrap();

        let configuration = FieldGeneratorConfiguration::with_field_generator(
            user_id.proto(),
            &message_generator.generator_descriptor,
        );

        assert!(!configuration.skip());
        assert_eq!(Some(1..=3), configuration.repeated_range());
        assert_eq!(Some("\"elit\""), configuration.repeated_script());

        Ok(())
    }
}
