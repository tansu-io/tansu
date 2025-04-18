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

use std::{collections::HashMap, io::Write, ops::Deref};

use crate::{AsArrow, AsJsonValue, AsKafkaRecord, Error, Result, Validator, arrow::RecordBuilder};
use ::arrow::{
    array::{
        ArrayBuilder, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder, ListBuilder, MapBuilder, StringBuilder, StructBuilder, UInt32Builder,
        UInt64Builder,
    },
    datatypes::{DataType, Field, FieldRef, Fields, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use bytes::{BufMut, Bytes, BytesMut};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use protobuf::{
    CodedInputStream, MessageDyn,
    reflect::{
        FieldDescriptor, FileDescriptor, MessageDescriptor, ReflectValueRef, RuntimeFieldType,
        RuntimeType,
    },
};
use protobuf_json_mapping::{parse_dyn_from_str, print_to_string};
use serde_json::{Map, Value};
use tansu_kafka_sans_io::{ErrorCode, record::inflated::Batch};
use tempfile::{NamedTempFile, tempdir};
use tracing::{debug, error};

const NULLABLE: bool = true;
const SORTED_MAP_KEYS: bool = false;

const KEY: &str = "Key";
const VALUE: &str = "Value";

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Schema {
    file_descriptor: FileDescriptor,
}

impl Schema {
    fn message_value_as_bytes(&self, message_name: &str, json: &Value) -> Result<Option<Bytes>> {
        self.file_descriptor
            .message_by_package_relative_name(message_name)
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
}

impl AsKafkaRecord for Schema {
    fn as_kafka_record(&self, value: &Value) -> Result<tansu_kafka_sans_io::record::Builder> {
        debug!(?value);

        let mut builder = tansu_kafka_sans_io::record::Record::builder();

        if let Some(value) = value.get("key") {
            debug!(?value);

            if let Some(encoded) = self.message_value_as_bytes(KEY, value)? {
                builder = builder.key(encoded.into());
            }
        };

        if let Some(value) = value.get("value") {
            debug!(?value);

            if let Some(encoded) = self.message_value_as_bytes(VALUE, value)? {
                builder = builder.value(encoded.into());
            }
        };

        Ok(builder)
    }
}

fn runtime_type_to_data_type(runtime_type: &RuntimeType) -> DataType {
    match runtime_type {
        RuntimeType::I32 | RuntimeType::Enum(_) => DataType::Int32,
        RuntimeType::I64 => DataType::Int64,
        RuntimeType::U32 => DataType::UInt32,
        RuntimeType::U64 => DataType::UInt64,
        RuntimeType::F32 => DataType::Float32,
        RuntimeType::F64 => DataType::Float64,
        RuntimeType::Bool => DataType::Boolean,
        RuntimeType::String => DataType::Utf8,
        RuntimeType::VecU8 => DataType::Binary,
        RuntimeType::Message(descriptor) => {
            DataType::Struct(Fields::from(message_descriptor_to_fields(descriptor)))
        }
    }
}

fn message_descriptor_to_fields(descriptor: &MessageDescriptor) -> Vec<Field> {
    descriptor
        .fields()
        .map(|field| match field.runtime_field_type() {
            RuntimeFieldType::Singular(ref singular) => {
                debug!(
                    descriptor = descriptor.name(),
                    field_name = field.name(),
                    ?singular
                );

                Field::new(
                    field.name(),
                    runtime_type_to_data_type(singular),
                    !field.is_required(),
                )
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    field.number().to_string(),
                )]))
            }

            RuntimeFieldType::Repeated(ref repeated) => {
                debug!(
                    descriptor = descriptor.name(),
                    field_name = field.name(),
                    ?repeated
                );

                Field::new(
                    field.name(),
                    DataType::new_list(runtime_type_to_data_type(repeated), NULLABLE),
                    !field.is_required(),
                )
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    field.number().to_string(),
                )]))
            }

            RuntimeFieldType::Map(ref key, ref value) => {
                debug!(
                    descriptor = descriptor.name(),
                    field_name = field.name(),
                    ?key,
                    ?value
                );

                let children = FieldRef::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from_iter([
                        Field::new("keys", runtime_type_to_data_type(key), !NULLABLE),
                        Field::new("values", runtime_type_to_data_type(value), NULLABLE),
                    ])),
                    !NULLABLE,
                ));

                Field::new(
                    field.name(),
                    DataType::Map(children, SORTED_MAP_KEYS),
                    NULLABLE,
                )
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    field.number().to_string(),
                )]))
            }
        })
        .collect::<Vec<_>>()
}

fn runtime_type_to_array_builder(runtime_type: &RuntimeType) -> Box<dyn ArrayBuilder> {
    debug!(?runtime_type);

    match runtime_type {
        RuntimeType::I32 | RuntimeType::Enum(_) => Box::new(Int32Builder::new()),
        RuntimeType::I64 => Box::new(Int64Builder::new()),
        RuntimeType::U32 => Box::new(UInt32Builder::new()),
        RuntimeType::U64 => Box::new(UInt64Builder::new()),
        RuntimeType::F32 => Box::new(Float32Builder::new()),
        RuntimeType::F64 => Box::new(Float64Builder::new()),
        RuntimeType::Bool => Box::new(BooleanBuilder::new()),
        RuntimeType::String => Box::new(StringBuilder::new()),
        RuntimeType::VecU8 => Box::new(BinaryBuilder::new()),
        RuntimeType::Message(descriptor) => {
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
                            Field::new(
                                field.name(),
                                runtime_type_to_data_type(singular),
                                !field.is_required(),
                            )
                            .with_metadata(HashMap::from([(
                                PARQUET_FIELD_ID_META_KEY.to_string(),
                                field.number().to_string(),
                            )])),
                            runtime_type_to_array_builder(singular),
                        )
                    }

                    RuntimeFieldType::Repeated(ref repeated) => {
                        debug!(
                            descriptor = descriptor.name(),
                            field_name = field.name(),
                            ?repeated
                        );

                        (
                            Field::new(
                                field.name(),
                                DataType::new_list(runtime_type_to_data_type(repeated), NULLABLE),
                                !field.is_required(),
                            )
                            .with_metadata(HashMap::from([(
                                PARQUET_FIELD_ID_META_KEY.to_string(),
                                field.number().to_string(),
                            )])),
                            Box::new(ListBuilder::new(runtime_type_to_array_builder(repeated)))
                                as Box<dyn ArrayBuilder>,
                        )
                    }

                    RuntimeFieldType::Map(ref key, ref value) => {
                        debug!(
                            descriptor = descriptor.name(),
                            field_name = field.name(),
                            ?key,
                            ?value
                        );

                        let children = FieldRef::new(Field::new(
                            "entries",
                            DataType::Struct(Fields::from_iter([
                                Field::new("keys", runtime_type_to_data_type(key), !NULLABLE),
                                Field::new("values", runtime_type_to_data_type(value), NULLABLE),
                            ])),
                            !NULLABLE,
                        ));

                        (
                            Field::new(
                                field.name(),
                                DataType::Map(children, SORTED_MAP_KEYS),
                                NULLABLE,
                            )
                            .with_metadata(HashMap::from([(
                                PARQUET_FIELD_ID_META_KEY.to_string(),
                                field.number().to_string(),
                            )])),
                            Box::new(MapBuilder::new(
                                None,
                                runtime_type_to_array_builder(key),
                                runtime_type_to_array_builder(value),
                            )) as Box<dyn ArrayBuilder>,
                        )
                    }
                })
                .collect::<(Vec<_>, Vec<_>)>();

            Box::new(StructBuilder::new(fields, builders))
        }
    }
}

fn message_descriptor_array_builders(descriptor: &MessageDescriptor) -> Vec<Box<dyn ArrayBuilder>> {
    descriptor
        .fields()
        .map(|field| match field.runtime_field_type() {
            RuntimeFieldType::Singular(ref singular) => {
                debug!(
                    descriptor = descriptor.name(),
                    field_name = field.name(),
                    ?singular
                );
                runtime_type_to_array_builder(singular)
            }

            RuntimeFieldType::Repeated(ref repeated) => {
                debug!(
                    descriptor = descriptor.name(),
                    field_name = field.name(),
                    ?repeated
                );
                Box::new(ListBuilder::new(runtime_type_to_array_builder(repeated)))
            }

            RuntimeFieldType::Map(ref key, ref value) => {
                debug!(
                    descriptor = descriptor.name(),
                    field_name = field.name(),
                    ?key,
                    ?value
                );

                Box::new(MapBuilder::new(
                    None,
                    runtime_type_to_array_builder(key),
                    runtime_type_to_array_builder(value),
                ))
            }
        })
        .collect::<Vec<_>>()
}

impl From<&Schema> for Vec<Box<dyn ArrayBuilder>> {
    fn from(schema: &Schema) -> Self {
        debug!(?schema);

        let mut builders = vec![];

        if let Some(ref descriptor) = schema.file_descriptor.message_by_package_relative_name(KEY) {
            debug!(?descriptor);
            builders.append(&mut message_descriptor_array_builders(descriptor));
        }

        if let Some(ref descriptor) = schema
            .file_descriptor
            .message_by_package_relative_name(VALUE)
        {
            debug!(?descriptor);
            builders.append(&mut message_descriptor_array_builders(descriptor));
        }

        builders
    }
}

impl From<&Schema> for RecordBuilder {
    fn from(schema: &Schema) -> Self {
        let mut keys = vec![];

        if let Some(ref descriptor) = schema.file_descriptor.message_by_package_relative_name(KEY) {
            debug!(descriptor = descriptor.name());
            keys.append(&mut message_descriptor_array_builders(descriptor));
        }

        let mut values = vec![];

        if let Some(ref descriptor) = schema
            .file_descriptor
            .message_by_package_relative_name(VALUE)
        {
            debug!(descriptor = descriptor.name());
            values.append(&mut message_descriptor_array_builders(descriptor));
        }

        Self { keys, values }
    }
}

impl From<&Schema> for Fields {
    fn from(schema: &Schema) -> Self {
        let mut fields = vec![];

        if let Some(ref descriptor) = schema.file_descriptor.message_by_package_relative_name(KEY) {
            debug!(?descriptor);
            fields.append(&mut message_descriptor_to_fields(descriptor));
        }

        if let Some(ref descriptor) = schema
            .file_descriptor
            .message_by_package_relative_name(VALUE)
        {
            debug!(descriptor = descriptor.name());

            fields.append(&mut message_descriptor_to_fields(descriptor));
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
                self.file_descriptor.message_by_package_relative_name(KEY),
                record.key.clone(),
            )
            .and(validate(
                self.file_descriptor.message_by_package_relative_name(VALUE),
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
        make_fd(proto).map(|file_descriptor| Self { file_descriptor })
    }
}

fn make_fd(proto: Bytes) -> Result<FileDescriptor> {
    tempdir().map_err(Into::into).and_then(|temp_dir| {
        NamedTempFile::new_in(&temp_dir)
            .map_err(Into::into)
            .and_then(|mut temp_file| {
                temp_file.write_all(&proto).map_err(Into::into).and(
                    protobuf_parse::Parser::new()
                        .pure()
                        .input(&temp_file)
                        .include(&temp_dir)
                        .parse_and_typecheck()
                        .map_err(Into::into)
                        .and_then(|mut parsed| {
                            parsed.file_descriptors.pop().map_or(
                                Err(Error::ProtobufFileDescriptorMissing(proto)),
                                |file_descriptor_proto| {
                                    FileDescriptor::new_dynamic(file_descriptor_proto, &[])
                                        .map_err(Into::into)
                                },
                            )
                        }),
                )
            })
    })
}

fn append_struct_builder(message: &dyn MessageDyn, builder: &mut StructBuilder) -> Result<()> {
    for (index, ref field) in message.descriptor_dyn().fields().enumerate() {
        debug!(field_name = field.name());

        match field.runtime_field_type() {
            RuntimeFieldType::Singular(_singular) => {
                match field.get_singular_field_or_default(message) {
                    ReflectValueRef::U32(value) => builder
                        .field_builder::<UInt32Builder>(index)
                        .ok_or(Error::Downcast)
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::U64(value) => builder
                        .field_builder::<UInt64Builder>(index)
                        .ok_or(Error::Downcast)
                        .map(|values| values.append_value(value))?,

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
                        .field_builder::<BinaryBuilder>(index)
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
                            .downcast_mut::<UInt32Builder>()
                            .ok_or(Error::Downcast)
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::U64(value) => values
                            .downcast_mut::<UInt64Builder>()
                            .ok_or(Error::Downcast)
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

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
                            .downcast_mut::<BinaryBuilder>()
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
                        .downcast_mut::<UInt32Builder>()
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|builder| builder.append_value(value))
                }

                ReflectValueRef::U64(value) => {
                    debug!(?value);
                    builder
                        .as_any_mut()
                        .downcast_mut::<UInt64Builder>()
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|builder| builder.append_value(value))
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
                        .downcast_mut::<BinaryBuilder>()
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|builder| builder.append_value(value))
                }

                ReflectValueRef::Message(message_ref) => builder
                    .as_any_mut()
                    .downcast_mut::<StructBuilder>()
                    .ok_or(Error::Downcast)
                    .inspect_err(|err| error!(?err, ?message_ref))
                    .and_then(|builder| append_struct_builder(message_ref.deref(), builder)),
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
                            .downcast_mut::<UInt32Builder>()
                            .ok_or(Error::Downcast)
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?;
                    }

                    ReflectValueRef::U64(value) => builder
                        .values()
                        .as_any_mut()
                        .downcast_mut::<UInt64Builder>()
                        .ok_or(Error::Downcast)
                        .inspect_err(|err| error!(?err, ?value, ?repeated))
                        .map(|builder| builder.append_value(value))?,

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
                        .downcast_mut::<BinaryBuilder>()
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
            .downcast_mut::<UInt32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::U64(value) => builder
            .as_any_mut()
            .downcast_mut::<UInt64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

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
            .downcast_mut::<BinaryBuilder>()
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
    fn as_arrow(&self, batch: &Batch) -> Result<RecordBatch> {
        debug!(?batch);

        let schema = ArrowSchema::from(self);
        debug!(?schema);

        let mut record_builder = RecordBuilder::from(self);

        debug!(
            keys = record_builder.keys.len(),
            values = record_builder.values.len()
        );

        for record in batch.records.iter() {
            debug!(?record);

            process_message_descriptor(
                self.file_descriptor.message_by_package_relative_name(KEY),
                record.key(),
                &mut record_builder.keys,
            )?;

            process_message_descriptor(
                self.file_descriptor.message_by_package_relative_name(VALUE),
                record.value(),
                &mut record_builder.values,
            )?;
        }

        debug!(
            key_rows = ?record_builder.keys.iter().map(|rows| rows.len()).collect::<Vec<_>>(),
            value_rows = ?record_builder.values.iter().map(|rows| rows.len()).collect::<Vec<_>>()
        );

        let mut columns = vec![];
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
        package_relative_name: &str,
        encoded: Option<Bytes>,
    ) -> Result<(String, Value)> {
        decode(
            self.file_descriptor
                .message_by_package_relative_name(package_relative_name),
            encoded,
        )
        .inspect(|decoded| debug!(?decoded))
        .and_then(|decoded| {
            decoded.map_or(
                Ok((package_relative_name.to_lowercase(), Value::Null)),
                |message| {
                    print_to_string(message.as_ref())
                        .inspect(|s| debug!(s))
                        .map_err(Into::into)
                        .and_then(|s| serde_json::from_str::<Value>(&s).map_err(Into::into))
                        .map(|value| (package_relative_name.to_lowercase(), value))
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
                        self.to_json_value(KEY, record.key.clone())
                            .into_iter()
                            .chain(self.to_json_value(VALUE, record.value.clone())),
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
    use ::arrow::util::pretty::pretty_format_batches;
    use datafusion::prelude::*;
    use object_store::{ObjectStore, PutPayload, memory::InMemory, path::Path};
    use serde_json::json;
    use std::{fs::File, sync::Arc, thread};
    use tansu_kafka_sans_io::record::Record;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    fn encode_from_value(fd: &FileDescriptor, message_name: &str, json: &Value) -> Result<Bytes> {
        fd.message_by_package_relative_name(message_name)
            .ok_or(Error::Message(format!("message {message_name} not found")))
            .and_then(|message_descriptor| {
                serde_json::to_string(json)
                    .map_err(Error::from)
                    .and_then(|json| {
                        parse_dyn_from_str(&message_descriptor, json.as_str()).map_err(Into::into)
                    })
                    .and_then(|message| {
                        let mut w = BytesMut::new().writer();
                        message
                            .write_to_writer_dyn(&mut w)
                            .map(|()| Bytes::from(w.into_inner()))
                            .map_err(Into::into)
                    })
            })
    }

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

        let file_descriptor = make_fd(proto)?;

        let key = encode_from_value(&file_descriptor, "Key", &json!({"id": 12321}))?;

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

        let file_descriptor = make_fd(proto)?;

        let value = encode_from_value(
            &file_descriptor,
            "Value",
            &json!({
                "name": "alice",
                "email": "alice@example.com"
            }),
        )?;

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

        let file_descriptor = make_fd(proto.clone())?;

        let key = encode_from_value(&file_descriptor, "Key", &json!({"id": 12321}))?;
        let value = encode_from_value(
            &file_descriptor,
            "Value",
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

        let file_descriptor = make_fd(proto)?;

        let batch = {
            let mut batch = Batch::builder();

            for (key, value) in kv {
                batch = batch.record(
                    Record::builder()
                        .key(encode_from_value(&file_descriptor, KEY, key)?.into())
                        .value(encode_from_value(&file_descriptor, VALUE, value)?.into()),
                );
            }

            batch.build()?
        };

        let schema = Schema { file_descriptor };
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        _ = ctx.register_batch("search", record_batch)?;
        let df = ctx.sql("select * from search").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-------+---------+-------------+------------------+--------+",
            "| id    | query   | page_number | results_per_page | corpus |",
            "+-------+---------+-------------+------------------+--------+",
            "| 32123 | abc/def | 6           | 13               | 2      |",
            "| 45654 | pqr/stu | 42          | 5                | 6      |",
            "+-------+---------+-------------+------------------+--------+",
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

    #[test]
    fn using_other_message_types() -> Result<()> {
        let _guard = init_tracing()?;
        let proto = Bytes::from_static(
            br#"
            syntax = 'proto3';

            message SearchResponse {
              repeated Result results = 1;
            }

            message Result {
              string url = 1;
              string title = 2;
              repeated string snippets = 3;
            }
            "#,
        );

        let file_descriptor = make_fd(proto)?;
        let descriptor = file_descriptor
            .message_by_package_relative_name("SearchResponse")
            .unwrap();

        let fields = message_descriptor_to_fields(&descriptor);
        assert_eq!(1, fields.len());
        assert_eq!("results", fields[0].name());

        Ok(())
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

        let file_descriptor = make_fd(proto)?;

        let batch = {
            let mut batch = Batch::builder();

            for (ref key, ref value) in kv {
                batch = batch.record(
                    Record::builder()
                        .key(encode_from_value(&file_descriptor, KEY, key)?.into())
                        .value(encode_from_value(&file_descriptor, VALUE, value)?.into()),
                );
            }

            batch.build()?
        };

        let schema = Schema { file_descriptor };
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        _ = ctx.register_batch("ty", record_batch)?;
        let df = ctx.sql("select * from ty").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-------+--------+--------+----+-----+-------+-------+-------+-------+-------+-------+-------+-------+------+--------------+--------------------------------------+",
            "| id    | a      | b      | c  | d   | e     | f     | g     | h     | i     | j     | k     | l     | m    | n            | o                                    |",
            "+-------+--------+--------+----+-----+-------+-------+-------+-------+-------+-------+-------+-------+------+--------------+--------------------------------------+",
            "| 32123 | 567.65 | 45.654 | -6 | -66 | 23432 | 34543 | 45654 | 67876 | 78987 | 89098 | 90109 | 12321 | true | Hello World! | 616263313233213f242a262829272d3d407e |",
            "+-------+--------+--------+----+-----+-------+-------+-------+-------+-------+-------+-------+-------+------+--------------+--------------------------------------+",
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

        let file_descriptor = make_fd(proto)?;

        let batch = {
            let mut batch = Batch::builder();

            for (ref key, ref value) in kv {
                batch = batch.record(
                    Record::builder()
                        .key(encode_from_value(&file_descriptor, KEY, key)?.into())
                        .value(encode_from_value(&file_descriptor, VALUE, value)?.into()),
                );
            }

            batch.build()?
        };

        let schema = Schema { file_descriptor };
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        let topic = "abc";

        _ = ctx.register_batch(topic, record_batch)?;
        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-------+-------+-------------------+",
            "| id    | name  | email             |",
            "+-------+-------+-------------------+",
            "| 12321 | alice | alice@example.com |",
            "| 32123 | bob   | bob@example.com   |",
            "+-------+-------+-------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);
        Ok(())
    }

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

        let file_descriptor = make_fd(proto)?;

        let value = encode_from_value(
            &file_descriptor,
            "Value",
            &json!({
                "kv": {"a": 31234, "b": 56765, "c": 12321}
            }),
        )?;

        let batch = Batch::builder()
            .record(Record::builder().value(value.into()))
            .build()?;

        let schema = Schema { file_descriptor };
        let record_batch = schema.as_arrow(&batch)?;

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

        let file_descriptor = make_fd(proto)?;

        let value = encode_from_value(
            &file_descriptor,
            "Value",
            &json!({
                "kv": {"a": {"name": "xyz", "complete": 0.99}}
            }),
        )?;

        let batch = Batch::builder()
            .record(Record::builder().value(value.into()))
            .build()?;

        let schema = Schema { file_descriptor };
        let record_batch = schema.as_arrow(&batch)?;

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

        let file_descriptor = make_fd(proto)?;

        let value = encode_from_value(
            &file_descriptor,
            "Value",
            &json!({
                "project": {"name": "xyz", "complete": 0.99},
                "title": "abc",
            }),
        )?;

        let batch = Batch::builder()
            .record(Record::builder().value(value.into()))
            .build()?;

        let schema = Schema { file_descriptor };
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        let topic = "snippets";
        _ = ctx.register_batch(topic, record_batch)?;
        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-----------------------------+-------+",
            "| project                     | title |",
            "+-----------------------------+-------+",
            "| {name: xyz, complete: 0.99} | abc   |",
            "+-----------------------------+-------+",
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

        let file_descriptor = make_fd(proto)?;

        let value = encode_from_value(
            &file_descriptor,
            "Value",
            &json!({
                "url": "https://example.com/a", "title": "a", "snippets": ["p", "q", "r"]
            }),
        )?;

        let batch = Batch::builder()
            .record(Record::builder().value(value.into()))
            .build()?;

        let schema = Schema { file_descriptor };
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        let topic = "snippets";
        _ = ctx.register_batch(topic, record_batch)?;
        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-----------------------+-------+-----------+",
            "| url                   | title | snippets  |",
            "+-----------------------+-------+-----------+",
            "| https://example.com/a | a     | [p, q, r] |",
            "+-----------------------+-------+-----------+",
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

        let file_descriptor = make_fd(proto)?;

        let value = encode_from_value(
            &file_descriptor,
            "Value",
            &json!({
                "results": [{"url": "https://example.com/abc", "title": "a", "snippets": ["p", "q", "r"]},
                            {"url": "https://example.com/def", "title": "b", "snippets": ["x", "y", "z"]}]
            }),
        )?;

        let batch = Batch::builder()
            .record(Record::builder().value(value.into()))
            .build()?;

        let schema = Schema { file_descriptor };
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        let topic = "snippets";
        _ = ctx.register_batch(topic, record_batch)?;
        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+--------------------------------------------------------------------------------------------------------------------------------+",
            "| results                                                                                                                        |",
            "+--------------------------------------------------------------------------------------------------------------------------------+",
            "| [{url: https://example.com/abc, title: a, snippets: [p, q, r]}, {url: https://example.com/def, title: b, snippets: [x, y, z]}] |",
            "+--------------------------------------------------------------------------------------------------------------------------------+",
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

        let file_descriptor = make_fd(proto.clone())?;

        let key = encode_from_value(&file_descriptor, "Key", &json!({"id": 12321}))?;
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

        let file_descriptor = make_fd(proto.clone())?;

        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");

        let value = encode_from_value(
            &file_descriptor,
            "Value",
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
}
