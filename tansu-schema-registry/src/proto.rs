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

use crate::{Error, Result, Validator};
use bytes::Bytes;
use protobuf::{
    CodedInputStream,
    reflect::{FileDescriptor, MessageDescriptor},
};
use tansu_kafka_sans_io::{ErrorCode, record::inflated::Batch};
use tempfile::{NamedTempFile, tempdir};
use tracing::{debug, error};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct Schema {
    file_descriptor: FileDescriptor,
}

fn validate(message_descriptor: Option<MessageDescriptor>, encoded: Option<Bytes>) -> Result<()> {
    message_descriptor.map_or(Ok(()), |message_descriptor| {
        let mut message = message_descriptor.new_instance();

        encoded.map_or(Err(Error::Api(ErrorCode::InvalidRecord)), |encoded| {
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
                self.file_descriptor.message_by_package_relative_name("Key"),
                record.key.clone(),
            )
            .and(validate(
                self.file_descriptor
                    .message_by_package_relative_name("Value"),
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

#[cfg(test)]
mod tests {
    use crate::Registry;

    use super::*;
    use bytes::{BufMut, BytesMut};
    use object_store::{ObjectStore, PutPayload, memory::InMemory, path::Path};
    use protobuf_json_mapping::parse_dyn_from_str;
    use serde_json::{Value, json};
    use std::{fs::File, sync::Arc, thread};
    use tansu_kafka_sans_io::record::Record;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    fn encode_from_value(
        fd: &FileDescriptor,
        message_name: &str,
        json: &Value,
    ) -> Result<Option<Bytes>> {
        fd.message_by_package_relative_name(message_name)
            .map(|message_descriptor| {
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
            .transpose()
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

        let key = encode_from_value(&file_descriptor, "Key", &json!({"id": 12321}))?.unwrap();

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
        )?
        .unwrap();

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

        let key = encode_from_value(&file_descriptor, "Key", &json!({"id": 12321}))?.unwrap();
        let value = encode_from_value(
            &file_descriptor,
            "Value",
            &json!({
                "name": "alice",
                "email": "alice@example.com"
            }),
        )?
        .unwrap();

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

        let key = encode_from_value(&file_descriptor, "Key", &json!({"id": 12321}))?.unwrap();
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
        )?
        .unwrap();

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
