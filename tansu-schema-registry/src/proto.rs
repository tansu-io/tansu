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

use std::fs;

use crate::Result;
use bytes::Bytes;
use protobuf::{reflect::FileDescriptor, CodedInputStream, MessageDyn};
use tempfile::tempdir;

fn make_fd(proto: &str) -> Result<Option<FileDescriptor>> {
    let temp_dir = tempdir()?;
    let temp_file = temp_dir.path().join("example.proto");

    fs::write(&temp_file, proto)?;

    protobuf_parse::Parser::new()
        .pure()
        .include(temp_dir.path().to_path_buf())
        .input(&temp_file)
        .parse_and_typecheck()
        .map_err(Into::into)
        .and_then(|mut parsed| {
            parsed
                .file_descriptors
                .pop()
                .map(|file_descriptor_proto| {
                    FileDescriptor::new_dynamic(file_descriptor_proto, &[])
                })
                .transpose()
                .map_err(Into::into)
        })
}

fn decode(
    fd: &FileDescriptor,
    message_name: &str,
    encoded: Bytes,
) -> Result<Option<Box<dyn MessageDyn>>> {
    fd.message_by_package_relative_name(message_name)
        .map(|message_descriptor| {
            let mut message = message_descriptor.new_instance();
            message
                .merge_from_dyn(&mut CodedInputStream::from_tokio_bytes(&encoded))
                .map(|()| message)
                .map_err(Into::into)
        })
        .transpose()
}

#[cfg(test)]
mod tests {

    use crate::Error;

    use super::*;
    use bytes::{BufMut, BytesMut};
    use protobuf::reflect::ReflectFieldRef;
    use protobuf_json_mapping::parse_dyn_from_str;
    use serde_json::{json, Value};

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

    #[test]
    fn make_fd_from_proto3() -> Result<()> {
        let message_name = "Person";

        let proto = r#"
            syntax = 'proto3';

            message Person {
              string name = 1;
              int32 id = 2;
              string email = 3;
            }
            "#;

        let file_descriptor = make_fd(proto)?.unwrap();

        let md = file_descriptor
            .message_by_package_relative_name(message_name)
            .unwrap();
        let fd = md.field_by_name("name").unwrap();
        let _field_type = fd.runtime_field_type();

        let encoded =
            encode_from_value(&file_descriptor, message_name, &json!({"id": 12321}))?.unwrap();

        let decoded = decode(&file_descriptor, message_name, encoded)?.unwrap();
        let field_descriptor = decoded.descriptor_dyn().field_by_name("id").unwrap();

        match field_descriptor.get_reflect(decoded.as_ref()) {
            ReflectFieldRef::Optional(reflect_optional_ref) => {
                assert!(matches!(
                    reflect_optional_ref.value(),
                    Some(protobuf::reflect::ReflectValueRef::I32(12321))
                ));
            }
            ReflectFieldRef::Repeated(_reflect_repeated_ref) => todo!(),
            ReflectFieldRef::Map(_reflect_map_ref) => todo!(),
        }

        Ok(())
    }

    #[test]
    fn make_fd_from_proto2() -> Result<()> {
        let message_name = "Person";

        let proto = r#"
            syntax = 'proto2';

            message Person {
                optional string name = 1;
                optional int32 id = 2;
                optional string email = 3;
            }
            "#;

        let file_descriptor = make_fd(proto)?.unwrap();

        let encoded =
            encode_from_value(&file_descriptor, message_name, &json!({"id": 12321}))?.unwrap();

        let decoded = decode(&file_descriptor, message_name, encoded)?.unwrap();
        let field_descriptor = decoded.descriptor_dyn().field_by_name("id").unwrap();

        match field_descriptor.get_reflect(decoded.as_ref()) {
            ReflectFieldRef::Optional(reflect_optional_ref) => {
                assert!(matches!(
                    reflect_optional_ref.value(),
                    Some(protobuf::reflect::ReflectValueRef::I32(12321))
                ));
            }
            ReflectFieldRef::Repeated(_reflect_repeated_ref) => todo!(),
            ReflectFieldRef::Map(_reflect_map_ref) => todo!(),
        }

        Ok(())
    }
}
