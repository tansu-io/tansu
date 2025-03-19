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

use std::io::Write;

use crate::{AsArrow, Error, Result, Validator};
use arrow::array::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, ListBuilder, MapBuilder, RecordBatch, StringBuilder, StructBuilder,
    UInt32Builder, UInt64Builder,
};
use arrow_schema::{DataType, Field, FieldRef, Fields};
use bytes::Bytes;
use protobuf::{
    CodedInputStream,
    reflect::{FileDescriptor, MessageDescriptor, ReflectValueRef, RuntimeFieldType, RuntimeType},
};
use tansu_kafka_sans_io::{ErrorCode, record::inflated::Batch};
use tempfile::{NamedTempFile, tempdir};
use tracing::{debug, error};

const KEY: &str = "Key";
const VALUE: &str = "Value";

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct Schema {
    file_descriptor: FileDescriptor,
}

fn runtime_type_to_data_type(runtime_type: &RuntimeType) -> DataType {
    match runtime_type {
        RuntimeType::I32 => DataType::Int32,
        RuntimeType::I64 => DataType::Int64,
        RuntimeType::U32 | RuntimeType::Enum(_) => DataType::UInt32,
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
    let mut fields = vec![];

    for field in descriptor.fields() {
        match field.runtime_field_type() {
            RuntimeFieldType::Singular(ref singular) => {
                fields.push(arrow_schema::Field::new(
                    field.name(),
                    runtime_type_to_data_type(singular),
                    !field.is_required(),
                ));
            }

            RuntimeFieldType::Repeated(ref repeated) => {
                fields.push(arrow_schema::Field::new(
                    field.name(),
                    DataType::new_list(runtime_type_to_data_type(repeated), false),
                    !field.is_required(),
                ));
            }

            RuntimeFieldType::Map(ref key, ref value) => {
                let children = FieldRef::new(arrow_schema::Field::new(
                    "entries",
                    DataType::Struct(Fields::from_iter([
                        arrow_schema::Field::new("key", runtime_type_to_data_type(key), false),
                        arrow_schema::Field::new("value", runtime_type_to_data_type(value), false),
                    ])),
                    false,
                ));

                fields.push(arrow_schema::Field::new(
                    field.name(),
                    DataType::Map(children, false),
                    !field.is_required(),
                ));
            }
        }
    }

    fields
}

fn runtime_type_to_array_builder(runtime_type: &RuntimeType) -> Box<dyn ArrayBuilder> {
    match runtime_type {
        RuntimeType::I32 => Box::new(Int32Builder::new()),
        RuntimeType::I64 => Box::new(Int64Builder::new()),
        RuntimeType::U32 | RuntimeType::Enum(_) => Box::new(UInt32Builder::new()),
        RuntimeType::U64 => Box::new(UInt64Builder::new()),
        RuntimeType::F32 => Box::new(Float32Builder::new()),
        RuntimeType::F64 => Box::new(Float64Builder::new()),
        RuntimeType::Bool => Box::new(BooleanBuilder::new()),
        RuntimeType::String => Box::new(StringBuilder::new()),
        RuntimeType::VecU8 => Box::new(BinaryBuilder::new()),
        RuntimeType::Message(descriptor) => {
            let mut fields = vec![];
            let mut field_builders = vec![];

            for field in descriptor.fields() {
                match field.runtime_field_type() {
                    RuntimeFieldType::Singular(ref singular) => {
                        fields.push(arrow_schema::Field::new(
                            field.name(),
                            runtime_type_to_data_type(singular),
                            !field.is_required(),
                        ));
                        field_builders.push(runtime_type_to_array_builder(singular));
                    }

                    RuntimeFieldType::Repeated(ref repeated) => {
                        fields.push(arrow_schema::Field::new(
                            field.name(),
                            DataType::new_list(runtime_type_to_data_type(repeated), false),
                            !field.is_required(),
                        ));

                        field_builders.push(Box::new(ListBuilder::new(
                            runtime_type_to_array_builder(repeated),
                        )));
                    }

                    RuntimeFieldType::Map(ref key, ref value) => {
                        let children = FieldRef::new(arrow_schema::Field::new(
                            "entries",
                            DataType::Struct(Fields::from_iter([
                                arrow_schema::Field::new(
                                    "key",
                                    runtime_type_to_data_type(key),
                                    false,
                                ),
                                arrow_schema::Field::new(
                                    "value",
                                    runtime_type_to_data_type(value),
                                    false,
                                ),
                            ])),
                            false,
                        ));

                        fields.push(arrow_schema::Field::new(
                            field.name(),
                            DataType::Map(children, false),
                            !field.is_required(),
                        ));

                        field_builders.push(Box::new(MapBuilder::new(
                            None,
                            runtime_type_to_array_builder(key),
                            runtime_type_to_array_builder(value),
                        )));
                    }
                }
            }

            Box::new(StructBuilder::new(fields, field_builders))
        }
    }
}

fn message_descriptor_array_builders(descriptor: &MessageDescriptor) -> Vec<Box<dyn ArrayBuilder>> {
    descriptor.fields().fold(vec![], |mut builders, field| {
        match field.runtime_field_type() {
            RuntimeFieldType::Singular(ref singular) => {
                builders.push(runtime_type_to_array_builder(singular))
            }

            RuntimeFieldType::Repeated(ref repeated) => builders.push(Box::new(ListBuilder::new(
                runtime_type_to_array_builder(repeated),
            ))),

            RuntimeFieldType::Map(ref key, ref value) => {
                builders.push(Box::new(MapBuilder::new(
                    None,
                    runtime_type_to_array_builder(key),
                    runtime_type_to_array_builder(value),
                )));
            }
        }

        builders
    })
}

impl From<&Schema> for Vec<Box<dyn ArrayBuilder>> {
    fn from(schema: &Schema) -> Self {
        let mut builders = vec![];

        if let Some(ref descriptor) = schema.file_descriptor.message_by_package_relative_name(KEY) {
            builders.append(&mut message_descriptor_array_builders(descriptor));
        }

        if let Some(ref descriptor) = schema
            .file_descriptor
            .message_by_package_relative_name(VALUE)
        {
            builders.append(&mut message_descriptor_array_builders(descriptor));
        }

        builders
    }
}

impl From<&Schema> for (Vec<Box<dyn ArrayBuilder>>, Vec<Box<dyn ArrayBuilder>>) {
    fn from(schema: &Schema) -> Self {
        let mut keys = vec![];

        if let Some(ref descriptor) = schema.file_descriptor.message_by_package_relative_name(KEY) {
            keys.append(&mut message_descriptor_array_builders(descriptor));
        }

        let mut values = vec![];

        if let Some(ref descriptor) = schema
            .file_descriptor
            .message_by_package_relative_name(VALUE)
        {
            values.append(&mut message_descriptor_array_builders(descriptor));
        }

        (keys, values)
    }
}

impl From<&Schema> for Fields {
    fn from(schema: &Schema) -> Self {
        let mut fields = vec![];

        if let Some(ref descriptor) = schema.file_descriptor.message_by_package_relative_name(KEY) {
            fields.append(&mut message_descriptor_to_fields(descriptor));
        }

        if let Some(ref descriptor) = schema
            .file_descriptor
            .message_by_package_relative_name(VALUE)
        {
            fields.append(&mut message_descriptor_to_fields(descriptor));
        }

        fields.into()
    }
}

impl From<&Schema> for arrow_schema::Schema {
    fn from(schema: &Schema) -> Self {
        arrow_schema::Schema::new(Fields::from(schema))
    }
}

fn validate(message_descriptor: Option<MessageDescriptor>, encoded: Option<Bytes>) -> Result<()> {
    message_descriptor.map_or(Ok(()), |message_descriptor| {
        encoded.map_or(Err(Error::Api(ErrorCode::InvalidRecord)), |encoded| {
            let mut message = message_descriptor.new_instance();

            message
                .merge_from_dyn(&mut CodedInputStream::from_tokio_bytes(&encoded))
                .inspect_err(|err| error!(?err))
                .map_err(|_err| Error::Api(ErrorCode::InvalidRecord))
        })
    })
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

fn process(
    descriptor: Option<MessageDescriptor>,
    encoded: Option<Bytes>,
    builders: &mut Vec<Box<dyn ArrayBuilder>>,
) -> Result<()> {
    debug!(
        ?descriptor,
        ?encoded,
        builders = ?builders.iter().map(|rows| rows.len()).collect::<Vec<_>>()
    );

    let Some(descriptor) = descriptor else {
        return Ok(());
    };

    let mut message = descriptor.new_instance();

    encoded.map_or(Err(Error::Api(ErrorCode::InvalidRecord)), |encoded| {
        message
            .merge_from_dyn(&mut CodedInputStream::from_tokio_bytes(&encoded))
            .inspect_err(|err| error!(?err))
            .map_err(|_err| Error::Api(ErrorCode::InvalidRecord))
    })?;

    let mut columns = builders.iter_mut();

    for field in message.descriptor_dyn().fields() {
        debug!(field_name = field.name());

        columns
            .next()
            .ok_or(Error::BuilderExhausted)
            .and_then(|column| match field.runtime_field_type() {
                RuntimeFieldType::Singular(singular) => {
                    debug!(?singular);

                    match field.get_singular_field_or_default(message.as_ref()) {
                        ReflectValueRef::U32(value) => {
                            debug!(?value);
                            column
                                .as_any_mut()
                                .downcast_mut::<UInt32Builder>()
                                .ok_or(Error::BadDowncast {
                                    field: field.name().to_owned(),
                                })
                                .map(|builder| builder.append_value(value))
                        }

                        ReflectValueRef::U64(value) => {
                            debug!(?value);
                            column
                                .as_any_mut()
                                .downcast_mut::<UInt64Builder>()
                                .ok_or(Error::BadDowncast {
                                    field: field.name().to_owned(),
                                })
                                .map(|builder| builder.append_value(value))
                        }

                        ReflectValueRef::I32(value) | ReflectValueRef::Enum(_, value) => {
                            debug!(?value);
                            column
                                .as_any_mut()
                                .downcast_mut::<Int32Builder>()
                                .ok_or(Error::BadDowncast {
                                    field: field.name().to_owned(),
                                })
                                .map(|builder| builder.append_value(value))
                        }

                        ReflectValueRef::I64(value) => {
                            debug!(?value);
                            column
                                .as_any_mut()
                                .downcast_mut::<Int64Builder>()
                                .ok_or(Error::BadDowncast {
                                    field: field.name().to_owned(),
                                })
                                .map(|builder| builder.append_value(value))
                        }

                        ReflectValueRef::F32(value) => {
                            debug!(?value);
                            column
                                .as_any_mut()
                                .downcast_mut::<Float32Builder>()
                                .ok_or(Error::BadDowncast {
                                    field: field.name().to_owned(),
                                })
                                .map(|builder| builder.append_value(value))
                        }

                        ReflectValueRef::F64(value) => {
                            debug!(?value);
                            column
                                .as_any_mut()
                                .downcast_mut::<Float64Builder>()
                                .ok_or(Error::BadDowncast {
                                    field: field.name().to_owned(),
                                })
                                .map(|builder| builder.append_value(value))
                        }

                        ReflectValueRef::Bool(value) => {
                            debug!(?value);
                            column
                                .as_any_mut()
                                .downcast_mut::<BooleanBuilder>()
                                .ok_or(Error::BadDowncast {
                                    field: field.name().to_owned(),
                                })
                                .map(|builder| builder.append_value(value))
                        }

                        ReflectValueRef::String(value) => {
                            debug!(?value);
                            column
                                .as_any_mut()
                                .downcast_mut::<StringBuilder>()
                                .ok_or(Error::BadDowncast {
                                    field: field.name().to_owned(),
                                })
                                .map(|builder| builder.append_value(value))
                        }

                        ReflectValueRef::Bytes(value) => {
                            debug!(?value);
                            column
                                .as_any_mut()
                                .downcast_mut::<BinaryBuilder>()
                                .ok_or(Error::BadDowncast {
                                    field: field.name().to_owned(),
                                })
                                .map(|builder| builder.append_value(value))
                        }

                        ReflectValueRef::Message(message_ref) => {
                            debug!(?message_ref);
                            todo!()
                        }
                    }
                }

                RuntimeFieldType::Repeated(repeated) => {
                    debug!(?repeated);

                    let _ = repeated;

                    for _value in field.get_repeated(message.as_ref()) {}

                    todo!()
                }

                RuntimeFieldType::Map(key, value) => {
                    debug!(?key, ?value);
                    todo!()
                }
            })?;
    }

    debug!(
        builders = ?builders.iter().map(|rows| rows.len()).collect::<Vec<_>>()
    );

    Ok(())
}

impl AsArrow for Schema {
    fn as_arrow(&self, batch: &Batch) -> Result<arrow::array::RecordBatch> {
        debug!(?batch);

        let schema = arrow_schema::Schema::from(self);
        debug!(?schema);

        let (mut keys, mut values): (Vec<Box<dyn ArrayBuilder>>, Vec<Box<dyn ArrayBuilder>>) =
            self.into();

        debug!(keys = keys.len(), values = values.len());

        for record in batch.records.iter() {
            debug!(?record);

            process(
                self.file_descriptor.message_by_package_relative_name(KEY),
                record.key(),
                &mut keys,
            )?;

            process(
                self.file_descriptor.message_by_package_relative_name(VALUE),
                record.value(),
                &mut values,
            )?;
        }

        debug!(
            key_rows = ?keys.iter().map(|rows| rows.len()).collect::<Vec<_>>(),
            value_rows = ?values.iter().map(|rows| rows.len()).collect::<Vec<_>>()
        );

        let mut columns = vec![];
        columns.append(&mut keys);
        columns.append(&mut values);

        debug!(columns = columns.len());

        RecordBatch::try_new(
            schema.into(),
            columns.iter_mut().map(|builder| builder.finish()).collect(),
        )
        .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use crate::Registry;

    use super::*;
    use arrow::{array::AsArray, datatypes::Int32Type};
    use bytes::{BufMut, BytesMut};
    use object_store::{ObjectStore, PutPayload, memory::InMemory, path::Path};
    use protobuf_json_mapping::parse_dyn_from_str;
    use serde_json::{Value, json};
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

    #[test]
    fn message_descriptor_enumeration_to_field() -> Result<()> {
        let _guard = init_tracing()?;
        let proto = Bytes::from_static(
            br#"
            syntax = 'proto3';

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

            message SearchRequest {
              string query = 1;
              int32 page_number = 2;
              int32 results_per_page = 3;
              Corpus corpus = 4;
            }
            "#,
        );

        let file_descriptor = make_fd(proto)?;
        let descriptor = file_descriptor
            .message_by_package_relative_name("SearchRequest")
            .unwrap();

        let fields = message_descriptor_to_fields(&descriptor);
        assert_eq!(&DataType::Utf8, fields[0].data_type());
        assert_eq!("query", fields[0].name());
        assert_eq!(&DataType::Int32, fields[1].data_type());
        assert_eq!("page_number", fields[1].name());
        assert_eq!(&DataType::Int32, fields[2].data_type());
        assert_eq!("results_per_page", fields[2].name());
        assert_eq!(&DataType::UInt32, fields[3].data_type());
        assert_eq!("corpus", fields[3].name());

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

    #[test]
    fn message_descriptor_singular_to_field() -> Result<()> {
        let _guard = init_tracing()?;

        let proto = Bytes::from_static(
            br#"
            syntax = 'proto3';

            message A {
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

        let file_descriptor = make_fd(proto)?;
        let descriptor = file_descriptor
            .message_by_package_relative_name("A")
            .unwrap();

        let fields = message_descriptor_to_fields(&descriptor);
        assert_eq!(15, fields.len());
        assert_eq!(&DataType::Float64, fields[0].data_type());
        assert_eq!("a", fields[0].name());
        assert!(fields[0].is_nullable());

        assert_eq!(&DataType::Float32, fields[1].data_type());
        assert_eq!("b", fields[1].name());

        assert_eq!(&DataType::Int32, fields[2].data_type());
        assert_eq!("c", fields[2].name());

        assert_eq!(&DataType::Int64, fields[3].data_type());
        assert_eq!("d", fields[3].name());

        assert_eq!(&DataType::UInt32, fields[4].data_type());
        assert_eq!("e", fields[4].name());

        assert_eq!(&DataType::UInt64, fields[5].data_type());
        assert_eq!("f", fields[5].name());

        assert_eq!(&DataType::Int32, fields[6].data_type());
        assert_eq!("g", fields[6].name());

        assert_eq!(&DataType::Int64, fields[7].data_type());
        assert_eq!("h", fields[7].name());

        assert_eq!(&DataType::UInt32, fields[8].data_type());
        assert_eq!("i", fields[8].name());

        assert_eq!(&DataType::UInt64, fields[9].data_type());
        assert_eq!("j", fields[9].name());

        assert_eq!(&DataType::Int32, fields[10].data_type());
        assert_eq!("k", fields[10].name());

        assert_eq!(&DataType::Int64, fields[11].data_type());
        assert_eq!("l", fields[11].name());

        assert_eq!(&DataType::Boolean, fields[12].data_type());
        assert_eq!("m", fields[12].name());

        assert_eq!(&DataType::Utf8, fields[13].data_type());
        assert_eq!("n", fields[13].name());

        assert_eq!(&DataType::Binary, fields[14].data_type());
        assert_eq!("o", fields[14].name());

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

        let file_descriptor = make_fd(proto)?;

        let alice_key = encode_from_value(&file_descriptor, "Key", &json!({"id": 12321}))?;
        let alice_value = encode_from_value(
            &file_descriptor,
            "Value",
            &json!({
                "name": "alice",
                "email": "alice@example.com"
            }),
        )?;

        let bob_key = encode_from_value(&file_descriptor, "Key", &json!({"id": 32123}))?;
        let bob_value = encode_from_value(
            &file_descriptor,
            "Value",
            &json!({
                "name": "bob",
                "email": "bob@example.com"
            }),
        )?;

        let batch = Batch::builder()
            .record(
                Record::builder()
                    .key(alice_key.into())
                    .value(alice_value.into()),
            )
            .record(
                Record::builder()
                    .key(bob_key.into())
                    .value(bob_value.into()),
            )
            .build()?;

        let schema = Schema { file_descriptor };

        let batches = [schema.as_arrow(&batch)?];

        assert_eq!(&DataType::Int32, batches[0].column(0).data_type());

        let ids = batches
            .iter()
            .flat_map(|batch| batch.column(0).as_primitive::<Int32Type>().values())
            .copied()
            .collect::<Vec<_>>();

        assert_eq!(ids, [12321, 32123]);

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
