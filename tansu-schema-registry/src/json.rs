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

use crate::{AsArrow, Error, Result, Validator};
use arrow_schema::{DataType, Field, Fields};
use bytes::Bytes;
use serde_json::Value;
use tansu_kafka_sans_io::{ErrorCode, record::inflated::Batch};
use tracing::{debug, warn};

#[derive(Debug)]
pub(crate) struct Schema {
    key: Option<jsonschema::Validator>,
    value: Option<jsonschema::Validator>,
}

fn validate(validator: Option<&jsonschema::Validator>, encoded: Option<Bytes>) -> Result<()> {
    debug!(validator = ?validator, ?encoded);

    validator
        .map_or(Ok(()), |validator| {
            encoded.map_or(Err(Error::Api(ErrorCode::InvalidRecord)), |encoded| {
                serde_json::from_reader(&encoded[..])
                    .map_err(|err| {
                        warn!(?err, ?encoded);
                        Error::Api(ErrorCode::InvalidRecord)
                    })
                    .inspect(|instance| debug!(?instance))
                    .and_then(|instance| {
                        validator
                            .validate(&instance)
                            .inspect_err(|err| warn!(?err, ?validator, %instance))
                            .map_err(|_err| Error::Api(ErrorCode::InvalidRecord))
                    })
            })
        })
        .inspect(|r| debug!(?r))
        .inspect_err(|err| warn!(?err))
}

impl TryFrom<Bytes> for Schema {
    type Error = Error;

    fn try_from(encoded: Bytes) -> Result<Self, Self::Error> {
        serde_json::from_slice::<Value>(&encoded[..])
            .map_err(Into::into)
            .map(|schema| {
                schema.get("properties").map_or(
                    Self {
                        key: None,
                        value: None,
                    },
                    |properties| Self {
                        key: properties
                            .get("key")
                            .and_then(|schema| jsonschema::validator_for(schema).ok()),

                        value: properties
                            .get("value")
                            .and_then(|schema| jsonschema::validator_for(schema).ok()),
                    },
                )
            })
    }
}

impl Validator for Schema {
    fn validate(&self, batch: &Batch) -> Result<()> {
        debug!(?batch);

        for record in &batch.records {
            debug!(?record);

            validate(self.key.as_ref(), record.key.clone())
                .and(validate(self.value.as_ref(), record.value.clone()))?
        }

        Ok(())
    }
}

fn common_numeric_data_type(data_types: &[DataType]) -> Vec<DataType> {
    let mut result = vec![];

    if data_types.contains(&DataType::Float64) {
        result.push(DataType::Float64)
    } else if data_types.contains(&DataType::Int64) {
        result.push(DataType::Int64)
    } else if data_types.contains(&DataType::UInt64) {
        result.push(DataType::UInt64)
    }

    for data_type in data_types {
        if data_type == &DataType::Float64
            || data_type == &DataType::Int64
            || data_type == &DataType::UInt64
        {
            continue;
        }

        result.push(data_type.to_owned());
    }

    result.sort();
    result.dedup();
    result
}

fn common_struct_type(data_types: &[DataType]) -> Vec<DataType> {
    let _ = data_types
        .iter()
        .filter(|data_type| matches!(data_type, DataType::Struct(_)));

    vec![]
}

fn common_data_type(types: &[DataType]) -> Result<DataType> {
    match common_numeric_data_type(types).as_slice() {
        [data_type] => Ok(data_type.to_owned()),

        [DataType::Null, data_type] => Ok(data_type.to_owned()),
        [data_type, DataType::Null] => Ok(data_type.to_owned()),

        [DataType::UInt64, DataType::Int64] => Ok(DataType::Int64),
        [DataType::Int64, DataType::UInt64] => Ok(DataType::Int64),

        [data_type, DataType::Float64]
            if data_type == &DataType::Int64 || data_type == &DataType::UInt64 =>
        {
            Ok(DataType::Float64)
        }
        [DataType::Float64, data_type]
            if data_type == &DataType::Int64 || data_type == &DataType::UInt64 =>
        {
            Ok(DataType::Float64)
        }

        [DataType::Float64, dt1, dt2]
            if (dt1 == &DataType::Int64 || dt1 == &DataType::UInt64)
                && (dt2 == &DataType::Int64 || dt2 == &DataType::UInt64) =>
        {
            Ok(DataType::Float64)
        }

        [dt1, DataType::Float64, dt2]
            if (dt1 == &DataType::Int64 || dt1 == &DataType::UInt64)
                && (dt2 == &DataType::Int64 || dt2 == &DataType::UInt64) =>
        {
            Ok(DataType::Float64)
        }
        [dt1, dt2, DataType::Float64]
            if (dt1 == &DataType::Int64 || dt1 == &DataType::UInt64)
                && (dt2 == &DataType::Int64 || dt2 == &DataType::UInt64) =>
        {
            Ok(DataType::Float64)
        }

        otherwise => Err(Error::NoCommonType(
            otherwise
                .iter()
                .map(|data_type| data_type.to_owned())
                .collect(),
        )),
    }
}

fn sort_dedup(mut input: Vec<DataType>) -> Vec<DataType> {
    input.sort();
    input.dedup();
    input
}

fn value_data_type(value: &Value) -> Result<DataType> {
    match value {
        Value::Null => Ok(DataType::Null),

        Value::Bool(_) => Ok(DataType::Boolean),

        Value::Number(number) => {
            if number.is_u64() {
                Ok(DataType::UInt64)
            } else if number.is_i64() {
                Ok(DataType::Int64)
            } else {
                Ok(DataType::Float64)
            }
        }

        Value::String(_) => Ok(DataType::Utf8),

        Value::Array(values) => values
            .iter()
            .try_fold(Vec::new(), |mut acc, value| {
                value_data_type(value).map(|data_type| {
                    acc.push(data_type);
                    acc
                })
            })
            .map(sort_dedup)
            .and_then(|data_types| common_data_type(data_types.as_slice()))
            .map(|item| DataType::new_list(item, false)),

        Value::Object(map) => map
            .iter()
            .try_fold(Vec::new(), |mut acc, (name, value)| {
                value_data_type(value).map(|data_type| {
                    acc.push(arrow_schema::Field::new(name.to_owned(), data_type, true));
                    acc
                })
            })
            .map(Fields::from_iter)
            .map(DataType::Struct),
    }
}

// fn batch_fields(batch: &Batch) -> Result<Vec<Field>> {
//     for record in &batch.records {
//         if let Some(encoded) = record.key.clone() {
//             match serde_json::from_slice::<Value>(&encoded[..])? {
//                 Value::Null => DataType::Null,
//                 Value::Bool(_) => DataType::Boolean,
//                 Value::Number(number) => {
//                     todo!()
//                 }
//                 Value::String(_) => DataType::Utf8,
//                 Value::Array(values) => todo!(),
//                 Value::Object(map) => todo!(),
//             }
//         }
//     }
//     Ok(vec![])
// }

impl AsArrow for Schema {
    fn as_arrow(&self, batch: &Batch) -> Result<arrow_array::RecordBatch> {
        let key = self.key.as_ref();

        for record in &batch.records {
            if let Some(encoded) = record.key.clone() {
                match serde_json::from_slice::<Value>(&encoded[..])? {
                    Value::Null => todo!(),
                    Value::Bool(_) => todo!(),
                    Value::Number(number) => todo!(),
                    Value::String(_) => todo!(),
                    Value::Array(values) => todo!(),
                    Value::Object(map) => todo!(),
                }
            }
        }

        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::Registry;

    use super::*;
    use object_store::{ObjectStore, PutPayload, memory::InMemory, path::Path};
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

        let payload = serde_json::to_vec(&json!({
            "type": "object",
            "properties": {
                "key": {
                    "type": "number"
                },
                "value": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                        },
                        "email": {
                            "type": "string",
                            "format": "email"
                        }
                    }
                }
            }
        }))
        .map(Bytes::from)
        .map(PutPayload::from)?;

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.json"));
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let key = serde_json::to_vec(&json!(12320)).map(Bytes::from)?;

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

        let payload = serde_json::to_vec(&json!({
            "type": "object",
            "properties": {
                "key": {
                    "type": "number"
                },
                "value": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                        },
                        "email": {
                            "type": "string",
                            "format": "email"
                        }
                    }
                }
            }
        }))
        .map(Bytes::from)
        .map(PutPayload::from)?;

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.json"));
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let value = serde_json::to_vec(&json!({
            "name": "alice",
            "email": "alice@example.com"}))
        .map(Bytes::from)?;

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

        let payload = serde_json::to_vec(&json!({
            "type": "object",
            "properties": {
                "key": {
                    "type": "number"
                },
                "value": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                        },
                        "email": {
                            "type": "string",
                            "format": "email"
                        }
                    }
                }
            }
        }))
        .map(Bytes::from)
        .map(PutPayload::from)?;

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.json"));
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let key = serde_json::to_vec(&json!(12320)).map(Bytes::from)?;

        let value = serde_json::to_vec(&json!({
                "name": "alice",
                "email": "alice@example.com"}))
        .map(Bytes::from)?;

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

        let payload = serde_json::to_vec(&json!({}))
            .map(Bytes::from)
            .map(PutPayload::from)?;

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.json"));
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

        let payload = serde_json::to_vec(&json!({
            "type": "object",
            "properties": {
                "key": {
                    "type": "number"
                },
            }
        }))
        .map(Bytes::from)
        .map(PutPayload::from)?;

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.json"));
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let key = serde_json::to_vec(&json!(12320)).map(Bytes::from)?;

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

        let payload = serde_json::to_vec(&json!({
            "type": "object",
            "properties": {
                "key": {
                    "type": "number"
                },
            }
        }))
        .map(Bytes::from)
        .map(PutPayload::from)?;

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.json"));
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

        let payload = serde_json::to_vec(&json!({
            "type": "object",
            "properties": {
                "value": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                        },
                        "email": {
                            "type": "string",
                            "format": "email"
                        }
                    }
                }
            }
        }))
        .map(Bytes::from)
        .map(PutPayload::from)?;

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.json"));
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");

        let value = serde_json::to_vec(&json!({
                    "name": "alice",
                    "email": "alice@example.com"}))
        .map(Bytes::from)?;

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

        let payload = serde_json::to_vec(&json!({
            "type": "object",
            "properties": {
                "value": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                        },
                        "email": {
                            "type": "string",
                            "format": "email"
                        }
                    }
                }
            }
        }))
        .map(Bytes::from)
        .map(PutPayload::from)?;

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.json"));
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
    fn integer_type_can_be_float_dot_zero() -> Result<()> {
        let schema = json!({"type": "integer"});
        let validator = jsonschema::validator_for(&schema)?;

        assert!(validator.is_valid(&json!(42)));
        assert!(validator.is_valid(&json!(-1)));
        assert!(validator.is_valid(&json!(1.0)));

        Ok(())
    }

    #[test]
    fn common_struct_type() {
        assert_eq!(
            vec![DataType::Struct(Fields::from_iter([Field::new(
                "a",
                DataType::Int64,
                true
            )]))],
            super::common_struct_type(&[
                DataType::Struct(Fields::from_iter([Field::new("a", DataType::Int64, true)])),
                DataType::Struct(Fields::from_iter([Field::new("a", DataType::UInt64, true)])),
            ])
        );
    }

    #[test]
    fn common_data_type() -> Result<()> {
        assert_eq!(
            DataType::Int64,
            super::common_data_type(
                sort_dedup(vec![DataType::UInt64, DataType::Int64]).as_slice()
            )?
        );

        assert_eq!(
            DataType::Float64,
            super::common_data_type(
                sort_dedup(vec![DataType::Float64, DataType::Int64]).as_slice()
            )?
        );

        assert_eq!(
            DataType::Float64,
            super::common_data_type(
                sort_dedup(vec![DataType::Float64, DataType::UInt64, DataType::Int64]).as_slice()
            )?
        );

        assert!(matches!(
            super::common_data_type(
                sort_dedup(vec![DataType::Float64, DataType::Boolean]).as_slice()
            ),
            Err(Error::NoCommonType(_))
        ));

        assert_eq!(
            DataType::Utf8,
            super::common_data_type(sort_dedup(vec![DataType::Utf8, DataType::Null]).as_slice())?
        );

        Ok(())
    }

    #[test]
    fn value_data_type() -> Result<()> {
        assert_eq!(
            DataType::new_list(DataType::Int64, false),
            super::value_data_type(&json!([1, 0, -1]))?
        );

        assert_eq!(
            DataType::new_list(DataType::UInt64, false),
            super::value_data_type(&json!([1, null]))?
        );

        assert_eq!(
            DataType::new_list(DataType::Int64, false),
            super::value_data_type(&json!([1, -1, null]))?
        );

        assert_eq!(
            DataType::new_list(DataType::Float64, false),
            super::value_data_type(&json!([1, 0.0, -1, null]))?
        );

        assert_eq!(
            DataType::new_list(DataType::Float64, false),
            super::value_data_type(&json!([1, 0.0, -1]))?
        );

        assert_eq!(
            DataType::new_list(DataType::Utf8, false),
            super::value_data_type(&json!(["a", "b", "c"]))?
        );

        assert_eq!(
            DataType::new_list(
                DataType::Struct(Fields::from_iter([Field::new("a", DataType::UInt64, true)])),
                false
            ),
            super::value_data_type(&json!([{"a": 10}, {"a": 20}]))?
        );

        assert_eq!(
            DataType::new_list(
                DataType::Struct(Fields::from_iter([Field::new("a", DataType::Int64, true)])),
                false
            ),
            super::value_data_type(&json!([{"a": -1}, {"a": 1}]))?
        );

        Ok(())
    }
}
