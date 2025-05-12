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

use std::{collections::HashMap, io::Write, ops::Deref, sync::LazyLock};

use crate::{ARROW_LIST_FIELD_NAME, AsArrow, AsJsonValue, AsKafkaRecord, Error, Result, Validator};
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
use chrono::DateTime;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use protobuf::{
    CodedInputStream, MessageDyn, descriptor,
    reflect::{
        FieldDescriptor, FileDescriptor, MessageDescriptor, ReflectValueRef, RuntimeFieldType,
        RuntimeType,
    },
    well_known_types,
};
use protobuf_json_mapping::{parse_dyn_from_str, print_to_string};
use serde_json::{Map, Value, json};
use tansu_kafka_sans_io::{ErrorCode, record::inflated::Batch};
use tempfile::{NamedTempFile, tempdir};
use tracing::{debug, error};

const NULLABLE: bool = true;
const SORTED_MAP_KEYS: bool = false;

const KEY: &str = "Key";
const META: &str = "Meta";
const VALUE: &str = "Value";

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum MessageKind {
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
    meta: Vec<Box<dyn ArrayBuilder>>,
    keys: Vec<Box<dyn ArrayBuilder>>,
    values: Vec<Box<dyn ArrayBuilder>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Schema {
    file_descriptors: Vec<FileDescriptor>,
    ids: HashMap<String, i32>,
}

impl Schema {
    fn new_list_field(&self, path: &[&str], data_type: DataType) -> Field {
        self.new_field(path, ARROW_LIST_FIELD_NAME, data_type)
    }

    fn new_field(&self, path: &[&str], name: &str, data_type: DataType) -> Field {
        self.new_nullable_field(path, name, data_type, NULLABLE)
    }

    fn new_nullable_field(
        &self,
        path: &[&str],
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> Field {
        debug!(?path, name, ?data_type, ?nullable, ids = ?self.ids);

        let path = append(path, name).join(".");

        Field::new(name.to_owned(), data_type, nullable).with_metadata(
            self.ids
                .get(path.as_str())
                .inspect(|field_id| debug!(?path, field_id))
                .map(|field_id| (PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string()))
                .into_iter()
                .collect(),
        )
    }

    fn message_by_package_relative_name(
        &self,
        message_type: MessageKind,
    ) -> Option<MessageDescriptor> {
        self.file_descriptors
            .iter()
            .find_map(|fd| fd.message_by_package_relative_name(message_type.as_ref()))
    }

    fn value_to_message(
        &self,
        message_type: MessageKind,
        json: &Value,
    ) -> Result<Box<dyn MessageDyn>> {
        self.file_descriptors
            .iter()
            .find_map(|fd| fd.message_by_package_relative_name(message_type.as_ref()))
            .ok_or(Error::Message(format!(
                "message {message_type:?} not found"
            )))
            .and_then(|message_descriptor| {
                serde_json::to_string(json)
                    .map_err(Error::from)
                    .and_then(|json| {
                        parse_dyn_from_str(&message_descriptor, json.as_str()).map_err(Into::into)
                    })
            })
    }

    fn message_to_bytes(message: Box<dyn MessageDyn>) -> Result<Bytes> {
        let mut w = BytesMut::new().writer();
        message
            .write_to_writer_dyn(&mut w)
            .and(Ok(Bytes::from(w.into_inner())))
            .map_err(Into::into)
    }

    pub(crate) fn encode_from_value(
        &self,
        message_type: MessageKind,
        json: &Value,
    ) -> Result<Bytes> {
        self.value_to_message(message_type, json)
            .and_then(Self::message_to_bytes)
    }

    fn message_value_as_bytes(
        &self,
        message_type: MessageKind,
        json: &Value,
    ) -> Result<Option<Bytes>> {
        self.message_by_package_relative_name(message_type)
            .map(|message_descriptor| {
                serde_json::to_string(json)
                    .map_err(Error::from)
                    .inspect(|json| debug!(%json))
                    .and_then(|json| {
                        parse_dyn_from_str(&message_descriptor, json.as_str()).map_err(Into::into)
                    })
                    .inspect(|message| debug!(?message))
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

    fn runtime_type_to_data_type(&self, path: &[&str], runtime_type: &RuntimeType) -> DataType {
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
                        self.message_descriptor_to_fields(path, descriptor),
                    ))
                }
            }
        }
    }

    fn message_descriptor_to_fields(
        &self,
        path: &[&str],
        descriptor: &MessageDescriptor,
    ) -> Vec<Field> {
        debug!(?path, descriptor_full_name = ?descriptor.full_name());

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
                        path,
                        field.name(),
                        self.runtime_type_to_data_type(&append(path, field.name())[..], singular),
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
                        path,
                        field.name(),
                        {
                            let path = &append(path, field.name())[..];

                            DataType::List(FieldRef::new(self.new_list_field(
                                path,
                                self.runtime_type_to_data_type(
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
                        path,
                        field.name(),
                        {
                            let path = &append(path, field.name())[..];

                            DataType::Map(
                                FieldRef::new(self.new_nullable_field(
                                    path,
                                    "entries",
                                    DataType::Struct({
                                        let path = &append(path, "entries")[..];

                                        Fields::from_iter([
                                            self.new_nullable_field(
                                                path,
                                                "keys",
                                                self.runtime_type_to_data_type(
                                                    append(path, "keys").as_slice(),
                                                    key,
                                                ),
                                                !NULLABLE,
                                            ),
                                            self.new_field(
                                                path,
                                                "values",
                                                self.runtime_type_to_data_type(
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

    fn runtime_type_to_array_builder(
        &self,
        path: &[&str],
        runtime_type: &RuntimeType,
    ) -> Box<dyn ArrayBuilder> {
        debug!(?path, ?runtime_type);

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
                                        path,
                                        field.name(),
                                        self.runtime_type_to_data_type(
                                            &append(path, field.name())[..],
                                            singular,
                                        ),
                                        !field.is_required(),
                                    ),
                                    self.runtime_type_to_array_builder(path, singular),
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
                                        path,
                                        field.name(),
                                        {
                                            let path = &append(path, field.name())[..];

                                            DataType::List(FieldRef::new(self.new_list_field(
                                                path,
                                                self.runtime_type_to_data_type(
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
                                                &append(path, ARROW_LIST_FIELD_NAME)[..],
                                                repeated,
                                            ))
                                            .with_field(self.new_list_field(
                                                path,
                                                self.runtime_type_to_data_type(
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
                                        path,
                                        field.name(),
                                        {
                                            let path = &append(path, field.name())[..];

                                            DataType::Map(
                                                FieldRef::new(self.new_nullable_field(
                                                    path,
                                                    "entries",
                                                    DataType::Struct({
                                                        let path = &append(path, "entries")[..];

                                                        Fields::from_iter([
                                                            self.new_nullable_field(
                                                                path,
                                                                "keys",
                                                                self.runtime_type_to_data_type(
                                                                    &append(path, "keys")[..],
                                                                    key,
                                                                ),
                                                                !NULLABLE,
                                                            ),
                                                            self.new_field(
                                                                path,
                                                                "values",
                                                                self.runtime_type_to_data_type(
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
                                        self.runtime_type_to_array_builder(path, key),
                                        self.runtime_type_to_array_builder(path, value),
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
        path: &[&str],
        descriptor: &MessageDescriptor,
    ) -> Vec<Box<dyn ArrayBuilder>> {
        debug!(?path, ?descriptor);

        descriptor
            .fields()
            .map(|field| {
                let path = &append(path, field.name())[..];

                match field.runtime_field_type() {
                    RuntimeFieldType::Singular(ref singular) => {
                        debug!(
                            descriptor = descriptor.name(),
                            field_name = field.name(),
                            ?singular,
                            ?path
                        );
                        self.runtime_type_to_array_builder(path, singular)
                    }

                    RuntimeFieldType::Repeated(ref repeated) => {
                        debug!(
                            descriptor = descriptor.name(),
                            field_name = field.name(),
                            ?repeated,
                            ?path
                        );
                        Box::new(
                            ListBuilder::new(self.runtime_type_to_array_builder(
                                &append(path, ARROW_LIST_FIELD_NAME)[..],
                                repeated,
                            ))
                            .with_field(self.new_list_field(
                                path,
                                self.runtime_type_to_data_type(
                                    &append(path, ARROW_LIST_FIELD_NAME)[..],
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
                            ?path
                        );

                        let path = &append(path, "entries");

                        Box::new(
                            MapBuilder::new(
                                None,
                                self.runtime_type_to_array_builder(path, key),
                                self.runtime_type_to_array_builder(path, value),
                            )
                            .with_keys_field(self.new_nullable_field(
                                &path[..],
                                "keys",
                                self.runtime_type_to_data_type(&append(path, "keys")[..], key),
                                !NULLABLE,
                            ))
                            .with_values_field(self.new_field(
                                &path[..],
                                "values",
                                self.runtime_type_to_data_type(&append(path, "values")[..], value),
                            )),
                        )
                    }
                }
            })
            .collect::<Vec<_>>()
    }
}

impl AsKafkaRecord for Schema {
    fn as_kafka_record(&self, value: &Value) -> Result<tansu_kafka_sans_io::record::Builder> {
        debug!(?value);

        let mut builder = tansu_kafka_sans_io::record::Record::builder();

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

impl From<&Schema> for Vec<Box<dyn ArrayBuilder>> {
    fn from(schema: &Schema) -> Self {
        debug!(?schema);

        let mut builders = vec![];

        if let Some(ref descriptor) = schema.message_by_package_relative_name(MessageKind::Key) {
            debug!(?descriptor);
            builders.append(
                &mut schema
                    .message_descriptor_array_builders(&[MessageKind::Key.as_ref()], descriptor),
            );
        }

        if let Some(ref descriptor) = schema.message_by_package_relative_name(MessageKind::Value) {
            debug!(?descriptor);
            builders.append(
                &mut schema
                    .message_descriptor_array_builders(&[MessageKind::Value.as_ref()], descriptor),
            );
        }

        builders
    }
}

impl From<&Schema> for RecordBuilder {
    fn from(schema: &Schema) -> Self {
        let meta = {
            let mut meta = vec![];
            if let Some(ref descriptor) = schema.message_by_package_relative_name(MessageKind::Meta)
            {
                debug!(descriptor = descriptor.name());
                meta.append(
                    &mut schema.message_descriptor_array_builders(
                        &[MessageKind::Meta.as_ref()],
                        descriptor,
                    ),
                )
            }

            meta
        };

        let keys = {
            let mut keys = vec![];

            if let Some(ref descriptor) = schema.message_by_package_relative_name(MessageKind::Key)
            {
                debug!(descriptor = descriptor.name());
                keys.append(
                    &mut schema.message_descriptor_array_builders(
                        &[MessageKind::Key.as_ref()],
                        descriptor,
                    ),
                );
            }

            keys
        };

        let values =
            {
                let mut values = vec![];

                if let Some(ref descriptor) =
                    schema.message_by_package_relative_name(MessageKind::Value)
                {
                    debug!(descriptor = descriptor.name());
                    values.append(&mut schema.message_descriptor_array_builders(
                        &[MessageKind::Value.as_ref()],
                        descriptor,
                    ));
                }

                values
            };

        Self { meta, keys, values }
    }
}

impl From<&Schema> for Fields {
    fn from(schema: &Schema) -> Self {
        let mut fields = vec![];

        if let Some(ref descriptor) = schema
            .message_by_package_relative_name(MessageKind::Meta)
            .inspect(|descriptor| debug!(?descriptor))
        {
            fields.append(
                &mut schema.message_descriptor_to_fields(&[MessageKind::Meta.as_ref()], descriptor),
            );
        }

        if let Some(ref descriptor) = schema
            .message_by_package_relative_name(MessageKind::Key)
            .inspect(|descriptor| debug!(?descriptor))
        {
            fields.append(
                &mut schema.message_descriptor_to_fields(&[MessageKind::Key.as_ref()], descriptor),
            );
        }

        if let Some(ref descriptor) = schema
            .message_by_package_relative_name(MessageKind::Value)
            .inspect(|descriptor| debug!(?descriptor))
        {
            fields.append(
                &mut schema
                    .message_descriptor_to_fields(&[MessageKind::Value.as_ref()], descriptor),
            );
        }

        fields.into()
    }
}

impl From<&Schema> for ArrowSchema {
    fn from(schema: &Schema) -> Self {
        ArrowSchema::new(Fields::from(schema))
    }
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
            .map(|file_descriptors| Self {
                ids: field_ids(&file_descriptors),
                file_descriptors,
            })
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
    tempdir()
        .map_err(Into::into)
        .and_then(|temp_dir| {
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
                                    .inspect(|parsed| debug!(?parsed))
                                    .map(|file_descriptor_proto| {
                                        FileDescriptor::new_dynamic(
                                            file_descriptor_proto,
                                            &WELL_KNOWN_TYPES[..],
                                        )
                                        .inspect(|fd| debug!(?fd))
                                        .inspect_err(|err| debug!(?err))
                                        .map_err(Into::into)
                                    })
                                    .collect::<Result<Vec<_>>>()
                            }),
                    )
                })
        })
        .inspect(|fds| debug!(?fds))
}

fn field_ids(schemas: &[FileDescriptor]) -> HashMap<String, i32> {
    debug!(?schemas);

    fn field_ids_with_path(
        path: &[&str],
        schema: &MessageDescriptor,
        id: &mut i32,
    ) -> HashMap<String, i32> {
        debug!(?path, ?schema, ?id);

        let mut ids = HashMap::new();

        for field in schema.fields() {
            debug!(?path, field_name = ?field.name());

            let mut path = Vec::from(path);
            path.push(field.name());
            ids.insert(path.join("."), *id);
            *id += 1;

            match field.runtime_field_type() {
                RuntimeFieldType::Singular(singular) => {
                    debug!(?path, ?singular);

                    if let RuntimeType::Message(message_descriptor) = singular {
                        debug!(?path, ?message_descriptor);

                        if message_descriptor.full_name() == GOOGLE_PROTOBUF_TIMESTAMP {
                            continue;
                        }

                        ids.extend(
                            field_ids_with_path(&path[..], &message_descriptor, id).into_iter(),
                        )
                    }
                }

                RuntimeFieldType::Repeated(repeated) => {
                    debug!(?path, ?repeated);

                    path.push(ARROW_LIST_FIELD_NAME);
                    ids.insert(path.join("."), *id);
                    *id += 1;

                    if let RuntimeType::Message(message_descriptor) = repeated {
                        debug!(?path, ?message_descriptor);

                        ids.extend(
                            field_ids_with_path(&path[..], &message_descriptor, id).into_iter(),
                        )
                    }
                }

                RuntimeFieldType::Map(keys, values) => {
                    debug!(?path, ?keys, ?values);

                    path.push("entries");
                    ids.insert(path.join("."), *id);
                    *id += 1;

                    {
                        let mut path = path.clone();
                        path.push("keys");
                        ids.insert(path.join("."), *id);
                        *id += 1;

                        if let RuntimeType::Message(message_descriptor) = keys {
                            debug!(?path, ?message_descriptor);

                            ids.extend(
                                field_ids_with_path(&path[..], &message_descriptor, id).into_iter(),
                            )
                        }
                    }

                    {
                        let mut path = path.clone();
                        path.push("values");
                        ids.insert(path.join("."), *id);
                        *id += 1;

                        if let RuntimeType::Message(message_descriptor) = values {
                            debug!(?path, ?message_descriptor);

                            ids.extend(
                                field_ids_with_path(&path[..], &message_descriptor, id).into_iter(),
                            )
                        }
                    }
                }
            }
        }

        ids
    }

    let mut id = 1;

    let mut fields_by_package = |relative_name: &str| -> HashMap<String, i32> {
        schemas
            .iter()
            .find_map(|fd| fd.message_by_package_relative_name(relative_name))
            .as_ref()
            .map(|schema| field_ids_with_path(&[relative_name], schema, &mut id))
            .unwrap_or_default()
    };

    let mut ids = HashMap::new();

    ids.extend(fields_by_package(META));
    ids.extend(fields_by_package(KEY));
    ids.extend(fields_by_package(VALUE));

    ids
}

fn append_struct_builder(message: &dyn MessageDyn, builder: &mut StructBuilder) -> Result<()> {
    for (index, ref field) in message.descriptor_dyn().fields().enumerate() {
        debug!(field_name = field.name());

        match field.runtime_field_type() {
            RuntimeFieldType::Singular(_singular) => {
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
                        .ok_or(Error::Downcast)
                        .and_then(|values| {
                            i64::try_from(value)
                                .map_err(Into::into)
                                .map(|value| values.append_value(value))
                        })?,

                    ReflectValueRef::I32(value) | ReflectValueRef::Enum(_, value) => builder
                        .field_builder::<Int32Builder>(index)
                        .ok_or(Error::Downcast)
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::I64(value) => builder
                        .field_builder::<Int64Builder>(index)
                        .ok_or(Error::Downcast)
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::F32(value) => builder
                        .field_builder::<Float32Builder>(index)
                        .ok_or(Error::Downcast)
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::F64(value) => builder
                        .field_builder::<Float64Builder>(index)
                        .ok_or(Error::Downcast)
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::Bool(value) => builder
                        .field_builder::<BooleanBuilder>(index)
                        .ok_or(Error::Downcast)
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::String(value) => builder
                        .field_builder::<StringBuilder>(index)
                        .ok_or(Error::Downcast)
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::Bytes(value) => builder
                        .field_builder::<LargeBinaryBuilder>(index)
                        .ok_or(Error::Downcast)
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::Message(message) => builder
                        .field_builder::<StructBuilder>(index)
                        .ok_or(Error::Downcast)
                        .and_then(|builder| append_struct_builder(message.deref(), builder))?,
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
                            .ok_or(Error::Downcast)
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .and_then(|builder| {
                                i32::try_from(value)
                                    .map_err(Into::into)
                                    .map(|value| builder.append_value(value))
                            })?,

                        ReflectValueRef::U64(value) => values
                            .downcast_mut::<Int64Builder>()
                            .ok_or(Error::Downcast)
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .and_then(|builder| {
                                i64::try_from(value)
                                    .map_err(Into::into)
                                    .map(|value| builder.append_value(value))
                            })?,

                        ReflectValueRef::I32(value) | ReflectValueRef::Enum(_, value) => values
                            .downcast_mut::<Int32Builder>()
                            .ok_or(Error::Downcast)
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::I64(value) => values
                            .downcast_mut::<Int64Builder>()
                            .ok_or(Error::Downcast)
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::F32(value) => values
                            .downcast_mut::<Float32Builder>()
                            .ok_or(Error::Downcast)
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::F64(value) => values
                            .downcast_mut::<Float64Builder>()
                            .ok_or(Error::Downcast)
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::Bool(value) => values
                            .downcast_mut::<BooleanBuilder>()
                            .ok_or(Error::Downcast)
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::String(value) => values
                            .downcast_mut::<StringBuilder>()
                            .ok_or(Error::Downcast)
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::Bytes(value) => values
                            .downcast_mut::<LargeBinaryBuilder>()
                            .ok_or(Error::Downcast)
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::Message(message) => values
                            .downcast_mut::<StructBuilder>()
                            .ok_or(Error::Downcast)
                            .and_then(|builder| append_struct_builder(message.deref(), builder))?,
                    }
                }

                builder.append(true);
            }

            RuntimeFieldType::Map(key, value) => {
                debug!(?key, ?value);

                builder
                    .as_any_mut()
                    .downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
                    .ok_or(Error::Downcast)
                    .and_then(|builder| append_map_builder(message, field, builder))?
            }
        }
    }

    builder.append(true);

    Ok(())
}

fn process_field_descriptor(
    message: &dyn MessageDyn,
    field: &FieldDescriptor,
    builder: &mut dyn ArrayBuilder,
) -> Result<()> {
    debug!(?message);

    match field.runtime_field_type() {
        RuntimeFieldType::Singular(singular) => {
            debug!(?singular);

            match field.get_singular_field_or_default(message) {
                ReflectValueRef::U32(value) => {
                    debug!(?value);
                    builder
                        .as_any_mut()
                        .downcast_mut::<Int32Builder>()
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .and_then(|builder| {
                            i32::try_from(value)
                                .map_err(Into::into)
                                .map(|value| builder.append_value(value))
                        })
                }

                ReflectValueRef::U64(value) => {
                    debug!(?value);
                    builder
                        .as_any_mut()
                        .downcast_mut::<Int64Builder>()
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .and_then(|builder| {
                            i64::try_from(value)
                                .map_err(Into::into)
                                .map(|value| builder.append_value(value))
                        })
                }

                ReflectValueRef::I32(value) | ReflectValueRef::Enum(_, value) => {
                    debug!(?value);
                    builder
                        .as_any_mut()
                        .downcast_mut::<Int32Builder>()
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|builder| builder.append_value(value))
                        .inspect_err(|err| {
                            error!(?err, field_name = field.name(), ?singular, ?value)
                        })
                }

                ReflectValueRef::I64(value) => {
                    debug!(?value);
                    builder
                        .as_any_mut()
                        .downcast_mut::<Int64Builder>()
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|builder| builder.append_value(value))
                }

                ReflectValueRef::F32(value) => {
                    debug!(?value);
                    builder
                        .as_any_mut()
                        .downcast_mut::<Float32Builder>()
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|builder| builder.append_value(value))
                }

                ReflectValueRef::F64(value) => {
                    debug!(?value);
                    builder
                        .as_any_mut()
                        .downcast_mut::<Float64Builder>()
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|builder| builder.append_value(value))
                }

                ReflectValueRef::Bool(value) => {
                    debug!(?value);
                    builder
                        .as_any_mut()
                        .downcast_mut::<BooleanBuilder>()
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|builder| builder.append_value(value))
                }

                ReflectValueRef::String(value) => {
                    debug!(?value);
                    builder
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|builder| builder.append_value(value))
                }

                ReflectValueRef::Bytes(value) => {
                    debug!(?value);
                    builder
                        .as_any_mut()
                        .downcast_mut::<LargeBinaryBuilder>()
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|builder| builder.append_value(value))
                }

                ReflectValueRef::Message(message_ref) => {
                    if message_ref.deref().descriptor_dyn().full_name() == GOOGLE_PROTOBUF_TIMESTAMP
                    {
                        let message = print_to_string(message_ref.deref())?;
                        debug!(message = message.trim_matches('"'));

                        let value = DateTime::parse_from_rfc3339(message.trim_matches('"'))
                            .inspect(|dt| debug!(?dt))
                            .map(|dt| dt.timestamp_micros())?;
                        debug!(?value);

                        builder
                            .as_any_mut()
                            .downcast_mut::<TimestampMicrosecondBuilder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
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

        RuntimeFieldType::Repeated(repeated) => {
            debug!(?repeated);

            let builder = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .ok_or(Error::Downcast)
                .inspect_err(|err| error!(?err, ?repeated))?;

            for value in field.get_repeated(message) {
                match value {
                    ReflectValueRef::U32(value) => {
                        debug!(?value);

                        builder
                            .values()
                            .as_any_mut()
                            .downcast_mut::<Int32Builder>()
                            .ok_or(Error::Downcast)
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .and_then(|builder| {
                                i32::try_from(value)
                                    .map_err(Into::into)
                                    .map(|value| builder.append_value(value))
                            })?
                    }

                    ReflectValueRef::U64(value) => builder
                        .values()
                        .as_any_mut()
                        .downcast_mut::<Int64Builder>()
                        .ok_or(Error::Downcast)
                        .inspect_err(|err| error!(?err, ?value, ?repeated))
                        .and_then(|builder| {
                            i64::try_from(value)
                                .map_err(Into::into)
                                .map(|value| builder.append_value(value))
                        })?,

                    ReflectValueRef::I32(value) | ReflectValueRef::Enum(_, value) => builder
                        .values()
                        .as_any_mut()
                        .downcast_mut::<Int32Builder>()
                        .ok_or(Error::Downcast)
                        .inspect_err(|err| error!(?err, ?value, ?repeated))
                        .map(|builder| builder.append_value(value))?,

                    ReflectValueRef::I64(value) => builder
                        .values()
                        .as_any_mut()
                        .downcast_mut::<Int64Builder>()
                        .ok_or(Error::Downcast)
                        .inspect_err(|err| error!(?err, ?value, ?repeated))
                        .map(|builder| builder.append_value(value))?,

                    ReflectValueRef::F32(value) => builder
                        .values()
                        .as_any_mut()
                        .downcast_mut::<Float32Builder>()
                        .ok_or(Error::Downcast)
                        .inspect_err(|err| error!(?err, ?value, ?repeated))
                        .map(|builder| builder.append_value(value))?,

                    ReflectValueRef::F64(value) => builder
                        .values()
                        .as_any_mut()
                        .downcast_mut::<Float64Builder>()
                        .ok_or(Error::Downcast)
                        .inspect_err(|err| error!(?err, ?value, ?repeated))
                        .map(|builder| builder.append_value(value))?,

                    ReflectValueRef::Bool(value) => builder
                        .values()
                        .as_any_mut()
                        .downcast_mut::<BooleanBuilder>()
                        .ok_or(Error::Downcast)
                        .inspect_err(|err| error!(?err, ?value, ?repeated))
                        .map(|builder| builder.append_value(value))?,

                    ReflectValueRef::String(value) => {
                        debug!(value);
                        builder
                            .values()
                            .as_any_mut()
                            .downcast_mut::<StringBuilder>()
                            .ok_or(Error::Downcast)
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?
                    }

                    ReflectValueRef::Bytes(value) => builder
                        .values()
                        .as_any_mut()
                        .downcast_mut::<LargeBinaryBuilder>()
                        .ok_or(Error::Downcast)
                        .inspect_err(|err| error!(?err, ?value, ?repeated))
                        .map(|builder| builder.append_value(value))?,

                    ReflectValueRef::Message(message_ref) => builder
                        .values()
                        .as_any_mut()
                        .downcast_mut::<StructBuilder>()
                        .ok_or(Error::Downcast)
                        .inspect_err(|err| error!(?err, ?message_ref, ?repeated))
                        .and_then(|builder| append_struct_builder(message_ref.deref(), builder))?,
                }
            }

            builder.append(true);

            Ok(())
        }

        RuntimeFieldType::Map(key, value) => {
            debug!(?key, ?value);

            builder
                .as_any_mut()
                .downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
                .ok_or(Error::Downcast)
                .and_then(|builder| append_map_builder(message, field, builder))
        }
    }
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

fn decode_value(value: ReflectValueRef, builder: &mut dyn ArrayBuilder) -> Result<()> {
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

        ReflectValueRef::Message(message) => builder
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?message))
            .and_then(|builder| append_struct_builder(message.deref(), builder)),
    }
}

fn process_message_descriptor(
    descriptor: Option<MessageDescriptor>,
    encoded: Option<Bytes>,
    builders: &mut Vec<Box<dyn ArrayBuilder>>,
) -> Result<()> {
    let Some(descriptor) = descriptor else {
        return Ok(());
    };

    debug!(
        descriptor = descriptor.name(),
        ?encoded,
        builders = ?builders.iter().map(|rows| rows.len()).collect::<Vec<_>>()
    );

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

    let mut columns = builders.iter_mut();

    for ref field in message.descriptor_dyn().fields() {
        debug!(field_name = field.name());

        columns
            .next()
            .ok_or(Error::BuilderExhausted)
            .and_then(|column| process_field_descriptor(message.as_ref(), field, column))?;
    }

    debug!(
        builders = ?builders.iter().map(|rows| rows.len()).collect::<Vec<_>>()
    );

    Ok(())
}

impl AsArrow for Schema {
    fn as_arrow(&self, partition: i32, batch: &Batch) -> Result<RecordBatch> {
        debug!(?batch);

        let schema = ArrowSchema::from(self);
        debug!(?schema);

        let mut record_builder = RecordBuilder::from(self);

        debug!(
            meta = record_builder.meta.len(),
            keys = record_builder.keys.len(),
            values = record_builder.values.len()
        );

        for record in batch.records.iter() {
            debug!(?record);

            let timestamp =
                DateTime::from_timestamp_millis(batch.base_timestamp + record.timestamp_delta)
                    .map(|dt| dt.to_rfc3339());

            process_message_descriptor(
                self.message_by_package_relative_name(MessageKind::Meta),
                self.encode_from_value(
                    MessageKind::Meta,
                    &timestamp.map_or(
                        json!({"partition": partition}),
                        |timestamp| json!({"partition": partition, "timestamp": timestamp}),
                    ),
                )
                .map(Some)?,
                &mut record_builder.meta,
            )?;

            process_message_descriptor(
                self.message_by_package_relative_name(MessageKind::Key),
                record.key(),
                &mut record_builder.keys,
            )?;

            process_message_descriptor(
                self.message_by_package_relative_name(MessageKind::Value),
                record.value(),
                &mut record_builder.values,
            )?;
        }

        debug!(
            meta_rows = ?record_builder.meta.iter().map(|rows| rows.len()).collect::<Vec<_>>(),
            key_rows = ?record_builder.keys.iter().map(|rows| rows.len()).collect::<Vec<_>>(),
            value_rows = ?record_builder.values.iter().map(|rows| rows.len()).collect::<Vec<_>>()
        );

        let mut columns = vec![];
        columns.append(&mut record_builder.meta);
        columns.append(&mut record_builder.keys);
        columns.append(&mut record_builder.values);

        debug!(columns = columns.len());

        RecordBatch::try_new(
            schema.into(),
            columns.iter_mut().map(|builder| builder.finish()).collect(),
        )
        .map_err(Into::into)
    }
}

impl Schema {
    fn to_json_value(
        &self,
        message_type: MessageKind,
        encoded: Option<Bytes>,
    ) -> Result<(String, Value)> {
        decode(self.message_by_package_relative_name(message_type), encoded)
            .inspect(|decoded| debug!(?decoded))
            .and_then(|decoded| {
                decoded.map_or(
                    Ok((message_type.as_ref().to_lowercase(), Value::Null)),
                    |message| {
                        print_to_string(message.as_ref())
                            .inspect(|s| debug!(s))
                            .map_err(Into::into)
                            .and_then(|s| serde_json::from_str::<Value>(&s).map_err(Into::into))
                            .map(|value| (message_type.as_ref().to_lowercase(), value))
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
    use tansu_kafka_sans_io::record::Record;
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

        let record_batch = schema.as_arrow(0, &batch)?;

        let data_files = iceberg_write(record_batch.clone()).await?;
        assert_eq!(1, data_files.len());
        assert_eq!(2, data_files[0].record_count());

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch.clone())?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-----------+-------------------------+-------+---------+-------------+------------------+--------+",
            "| partition | timestamp               | id    | query   | page_number | results_per_page | corpus |",
            "+-----------+-------------------------+-------+---------+-------------+------------------+--------+",
            "| 0         | 1973-10-17T18:36:57     | 32123 | abc/def | 6           | 13               | 2      |",
            "| 0         | 1973-10-17T18:36:57.001 | 45654 | pqr/stu | 42          | 5                | 6      |",
            "+-----------+-------------------------+-------+---------+-------------+------------------+--------+",
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

        let record_batch = schema.as_arrow(0, &batch)?;

        let data_files = iceberg_write(record_batch.clone()).await?;
        assert_eq!(1, data_files.len());
        assert_eq!(1, data_files[0].record_count());

        let ctx = SessionContext::new();

        _ = ctx.register_batch("ty", record_batch)?;
        let df = ctx.sql("select * from ty").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-----------+---------------------+-------+--------+--------+----+-----+-------+-------+-------+-------+-------+-------+-------+-------+------+--------------+--------------------------------------+",
            "| partition | timestamp           | id    | a      | b      | c  | d   | e     | f     | g     | h     | i     | j     | k     | l     | m    | n            | o                                    |",
            "+-----------+---------------------+-------+--------+--------+----+-----+-------+-------+-------+-------+-------+-------+-------+-------+------+--------------+--------------------------------------+",
            "| 0         | 1973-10-17T18:36:57 | 32123 | 567.65 | 45.654 | -6 | -66 | 23432 | 34543 | 45654 | 67876 | 78987 | 89098 | 90109 | 12321 | true | Hello World! | 616263313233213f242a262829272d3d407e |",
            "+-----------+---------------------+-------+--------+--------+----+-----+-------+-------+-------+-------+-------+-------+-------+-------+------+--------------+--------------------------------------+",
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

        let record_batch = schema.as_arrow(0, &batch)?;

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
            "+-----------+-------------------------+-------+-------+-------------------+",
            "| partition | timestamp               | id    | name  | email             |",
            "+-----------+-------------------------+-------+-------+-------------------+",
            "| 0         | 1973-10-17T18:36:57     | 12321 | alice | alice@example.com |",
            "| 0         | 1973-10-17T18:36:57.001 | 32123 | bob   | bob@example.com   |",
            "+-----------+-------------------------+-------+-------+-------------------+",
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

        let record_batch = schema.as_arrow(0, &batch)?;

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
            "+-----------+---------------------+-----------+---------+---------------+-------------+---------------+",
            "| partition | timestamp           | vendor_id | trip_id | trip_distance | fare_amount | store_and_fwd |",
            "+-----------+---------------------+-----------+---------+---------------+-------------+---------------+",
            "| 0         | 1973-10-17T18:36:57 | 1         | 1000371 | 1.8           | 15.32       | 0             |",
            "+-----------+---------------------+-----------+---------+---------------+-------------+---------------+",
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

        let record_batch = schema.as_arrow(0, &batch)?;

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

        let record_batch = schema.as_arrow(0, &batch)?;

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

        let record_batch = schema.as_arrow(0, &batch)?;

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
            "+-----------+---------------------+-----------------------------+-------+",
            "| partition | timestamp           | project                     | title |",
            "+-----------+---------------------+-----------------------------+-------+",
            "| 0         | 1973-10-17T18:36:57 | {name: xyz, complete: 0.99} | abc   |",
            "+-----------+---------------------+-----------------------------+-------+",
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

        let record_batch = schema.as_arrow(0, &batch)?;
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
            "+-----------+---------------------+-----------------------+-------+-----------+",
            "| partition | timestamp           | url                   | title | snippets  |",
            "+-----------+---------------------+-----------------------+-------+-----------+",
            "| 0         | 1973-10-17T18:36:57 | https://example.com/a | a     | [p, q, r] |",
            "+-----------+---------------------+-----------------------+-------+-----------+",
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

        let record_batch = schema.as_arrow(0, &batch)?;
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
            "+-----------+---------------------+--------------------------------------------------------------------------------------------------------------------------------+",
            "| partition | timestamp           | results                                                                                                                        |",
            "+-----------+---------------------+--------------------------------------------------------------------------------------------------------------------------------+",
            "| 0         | 1973-10-17T18:36:57 | [{url: https://example.com/abc, title: a, snippets: [p, q, r]}, {url: https://example.com/def, title: b, snippets: [x, y, z]}] |",
            "+-----------+---------------------+--------------------------------------------------------------------------------------------------------------------------------+",
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
}
