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

use bytes::Bytes;
use common::{alphanumeric_string, init_tracing};
use futures::StreamExt;
use lance::dataset::Dataset;
use lance::deps::arrow_array::RecordBatch as LanceRecordBatch;
use object_store::{ObjectStoreExt as _, PutPayload, memory::InMemory, path::Path};
use serde_json::{Value as JsonValue, json};
use tansu_sans_io::{
    ConfigResource, ErrorCode,
    describe_configs_response::DescribeConfigsResult,
    record::{Record, inflated::Batch},
};
use tansu_schema::{
    Registry, Result,
    lake::{House, LakeHouse},
};
use tempfile::tempdir;
use tracing::debug;
use url::Url;

pub mod common;

pub async fn lake_store(
    topic: &str,
    partition: i32,
    schema_registry: Registry,
    config: DescribeConfigsResult,
    inflated: &Batch,
) -> Result<Vec<LanceRecordBatch>> {
    let temp_dir = tempdir()?;
    let location = format!("file://{}", temp_dir.path().to_str().unwrap());
    let database = "test";

    debug!(?location, database, topic, ?inflated);

    let lake_house = House::lance()
        .location(Url::parse(&location)?)
        .database(Some(database.into()))
        .schema_registry(schema_registry)
        .build()?;

    let offset = 543212345;

    lake_house
        .store(topic, partition, offset, inflated, config)
        .await
        .inspect(|result| debug!(?result))
        .inspect_err(|err| debug!(?err))?;

    // Read back from lance
    let dataset = Dataset::open(&format!("{location}/{database}.{topic}")).await?;
    let scanner = dataset.scan();
    let mut batch_stream = scanner.try_into_stream().await?;

    let mut batches = vec![];
    while let Some(batch_result) = batch_stream.next().await {
        batches.push(batch_result?);
    }

    Ok(batches)
}

fn empty_config(topic: &str) -> DescribeConfigsResult {
    DescribeConfigsResult::default()
        .error_code(ErrorCode::None.into())
        .error_message(None)
        .resource_type(ConfigResource::Topic.into())
        .resource_name(topic.into())
        .configs(Some(vec![]))
}

mod json {
    use super::*;

    #[tokio::test]
    async fn key_and_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let schema = json!({
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
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.json"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&schema)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let kv = [
            (
                json!(12321),
                json!({"name": "alice", "email": "alice@example.com"}),
            ),
            (
                json!(32123),
                json!({"name": "bob", "email": "bob@example.com"}),
            ),
        ];

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for (ref key, ref value) in kv {
                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                        .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn grade() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let schema_registry = {
            let schema = Bytes::from_static(include_bytes!("../../etc/schema/grade.json"));

            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.json"));
            _ = object_store
                .put(&location, PutPayload::from(schema))
                .await?;

            Registry::new(object_store)
        };

        let kv = if let JsonValue::Array(values) =
            serde_json::from_slice::<JsonValue>(include_bytes!("../../etc/data/grades.json"))?
        {
            values
                .into_iter()
                .map(|value| {
                    (
                        value.get("key").cloned().unwrap(),
                        value.get("value").cloned().unwrap(),
                    )
                })
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for (ref key, ref value) in kv {
                debug!(?key, ?value);

                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                        .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn key() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let schema_registry = {
            let schema = json!({
                "type": "object",
                "properties": {
                    "key": {
                        "type": "number"
                    }
                }
            });

            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.json"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&schema)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let keys = [json!(12321), json!(23432), json!(34543)];
        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for ref key in keys {
                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn primitive_key_and_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let schema_registry = {
            let schema = json!({
                "type": "object",
                "properties": {
                    "key": {
                        "type": "number"
                    },
                    "value": {
                        "type": "string",
                    }
                }
            });

            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.json"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&schema)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let kv = [
            (json!(12321), json!("alice@example.com")),
            (json!(32123), json!("bob@example.com")),
        ];

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for (ref key, ref value) in kv {
                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                        .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn primitive_key_and_array_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let schema_registry = {
            let schema = json!({
                "type": "object",
                "properties": {
                    "key": {
                        "type": "number"
                    },
                    "value": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    }
                }
            });

            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.json"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&schema)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let kv = [
            (json!(12321), json!(["a", "b", "c"])),
            (json!(32123), json!(["p", "q", "r"])),
        ];

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for (ref key, ref value) in kv {
                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                        .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn primitive_key_and_array_object_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5);

        let schema_registry = {
            let schema = json!({
                "type": "object",
                "properties": {
                    "key": {
                        "type": "number"
                    },
                    "value": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "quantity": {
                                    "type": "integer",
                                },
                                "location": {
                                    "type": "string",
                                }
                            }
                        }
                    }
                }
            });

            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.json"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&schema)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let kv = [
            (
                json!(12321),
                json!([{"quantity": 6, "location": "abc"}, {"quantity": 11, "location": "pqr"}]),
            ),
            (
                json!(32123),
                json!([{"quantity": 3, "location": "abc"},
                       {"quantity": 33, "location": "def"},
                       {"quantity": 21, "location": "xyz"}]),
            ),
        ];

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for (ref key, ref value) in kv {
                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                        .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn primitive_key_and_struct_with_array_field_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let schema_registry = {
            let schema = json!({
                "type": "object",
                "properties": {
                    "key": {
                        "type": "number"
                    },
                    "value": {
                        "type": "object",
                        "properties": {
                            "zone": {
                                "type": "number",
                            },
                            "locations": {
                                "type": "array",
                                "items": {
                                    "type": "string"
                                }
                            }
                        }
                    }
                }
            });

            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.json"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&schema)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let kv = [
            (
                json!(12321),
                json!({"zone": 6, "locations": ["abc", "def"]}),
            ),
            (json!(32123), json!({"zone": 11, "locations": ["pqr"]})),
        ];

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for (ref key, ref value) in kv {
                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                        .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }
}

mod proto {
    use super::*;
    use tansu_schema::proto::{MessageKind, Schema};

    #[tokio::test]
    async fn message_descriptor_singular_to_field() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

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
                    "n": "Hello World!"}),
        )];

        let partition = 32123;

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.proto"));
            _ = object_store
                .put(&location, PutPayload::from(proto.clone()))
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::try_from(proto)?;

        let record_batch = {
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

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn taxi_plain() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let proto = Bytes::from_static(include_bytes!("../../etc/schema/taxi.proto"));

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.proto"));
            _ = object_store
                .put(&location, PutPayload::from(proto.clone()))
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::try_from(proto)?;

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

        let partition = 32123;

        let record_batch = Batch::builder()
            .record(Record::builder().value(value.into()))
            .base_timestamp(119_731_017_000)
            .build()?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn taxi_normalized() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let proto = Bytes::from_static(include_bytes!("../../etc/schema/taxi.proto"));

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.proto"));
            _ = object_store
                .put(&location, PutPayload::from(proto.clone()))
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::try_from(proto)?;

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

        let partition = 32123;

        let record_batch = Batch::builder()
            .record(Record::builder().value(value.into()))
            .base_timestamp(119_731_017_000)
            .build()?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn value_message_ref() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

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

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.proto"));
            _ = object_store
                .put(&location, PutPayload::from(proto.clone()))
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::try_from(proto)?;

        let value = schema.encode_from_value(
            MessageKind::Value,
            &json!({
                "project": {"name": "xyz", "complete": 0.99},
                "title": "abc",
            }),
        )?;

        let partition = 32123;

        let record_batch = Batch::builder()
            .base_timestamp(119_731_017_000)
            .record(Record::builder().value(value.into()))
            .build()?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn simple_repeated() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

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

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.proto"));
            _ = object_store
                .put(&location, PutPayload::from(proto.clone()))
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::try_from(proto)?;

        let value = schema.encode_from_value(
            MessageKind::Value,
            &json!({
                "url": "https://example.com/a", "title": "a", "snippets": ["p", "q", "r"]
            }),
        )?;

        let partition = 32123;

        let record_batch = Batch::builder()
            .base_timestamp(119_731_017_000)
            .record(Record::builder().value(value.into()))
            .build()?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn repeated() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

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

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.proto"));
            _ = object_store
                .put(&location, PutPayload::from(proto.clone()))
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::try_from(proto)?;

        let value = schema.encode_from_value(
            MessageKind::Value,
            &json!({
                "results": [{"url": "https://example.com/abc", "title": "a", "snippets": ["p", "q", "r"]},
                            {"url": "https://example.com/def", "title": "b", "snippets": ["x", "y", "z"]}]
            }),
        )?;

        let partition = 32123;

        let record_batch = Batch::builder()
            .record(Record::builder().value(value.into()))
            .base_timestamp(119_731_017_000)
            .build()?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn customer_schema_migration() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];
        let partition = 32123;

        let proto = Bytes::from_static(
            br#"
            syntax = 'proto3';

            message Value {
                string email_address = 1;
            }
            "#,
        );

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.proto"));
            _ = object_store
                .put(&location, PutPayload::from(proto.clone()))
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::try_from(proto)?;

        let record_batch = Batch::builder()
            .record(
                Record::builder().value(
                    schema
                        .encode_from_value(
                            MessageKind::Value,
                            &json!({"email_address": "test@example.com"}),
                        )?
                        .into(),
                ),
            )
            .base_timestamp(119_731_017_000)
            .build()?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }
}

mod avro {
    use super::*;
    use apache_avro::types::Value as AvroValue;
    use tansu_schema::{
        AsKafkaRecord,
        avro::{Schema, r, schema_write},
    };
    use uuid::Uuid;

    #[tokio::test]
    async fn record_of_primitive_data_types() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "Message",
            "fields": [
                {"name": "value", "type": "record", "fields": [
                {"name": "b", "type": "boolean"},
                {"name": "c", "type": "int"},
                {"name": "d", "type": "long"},
                {"name": "e", "type": "float"},
                {"name": "f", "type": "double"},
                {"name": "g", "type": "bytes"},
                {"name": "h", "type": "string"}
                ]}
            ]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [r(
                schema.value().as_ref().unwrap(),
                [
                    ("b", false.into()),
                    ("c", i32::MAX.into()),
                    ("d", i64::MAX.into()),
                    ("e", f32::MAX.into()),
                    ("f", f64::MAX.into()),
                    ("g", Vec::from(&b"abcdef"[..]).into()),
                    ("h", "pqr".into()),
                ],
            )];

            for value in values {
                batch =
                    batch.record(Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value.into())?.into(),
                    ))
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn record_of_with_list_of_primitive_data_types() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "Message",
            "fields": [
                {"name": "value", "type": "record", "fields": [
                    {"name": "b", "type": "array", "items": "boolean"},
                    {"name": "c", "type": "array", "items": "int"},
                    {"name": "d", "type": "array", "items": "long"},
                    {"name": "e", "type": "array", "items": "float"},
                    {"name": "f", "type": "array", "items": "double"},
                    {"name": "g", "type": "array", "items": "bytes"},
                    {"name": "h", "type": "array", "items": "string"}
                ]}
            ]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [r(
                schema.value().as_ref().unwrap(),
                [
                    ("b", AvroValue::Array(vec![false.into(), true.into()])),
                    (
                        "c",
                        AvroValue::Array(vec![i32::MIN.into(), 0.into(), i32::MAX.into()]),
                    ),
                    (
                        "d",
                        AvroValue::Array(vec![i64::MIN.into(), 0.into(), i64::MAX.into()]),
                    ),
                    (
                        "e",
                        AvroValue::Array(vec![f32::MIN.into(), 0.0f32.into(), f32::MAX.into()]),
                    ),
                    (
                        "f",
                        AvroValue::Array(vec![f64::MIN.into(), 0.0f64.into(), f64::MAX.into()]),
                    ),
                    (
                        "g",
                        AvroValue::Array(vec![Vec::from(&b"abcdef"[..]).into()]),
                    ),
                    (
                        "h",
                        AvroValue::Array(vec!["abc".into(), "pqr".into(), "xyz".into()]),
                    ),
                ],
            )];

            for value in values {
                batch =
                    batch.record(Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value.into())?.into(),
                    ))
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn union() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5);

        let avro = json!({
            "type": "record",
            "name": "union",
            "fields": [{"name": "value", "type": ["null", "float"]}]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [
                AvroValue::Union(1, Box::new(AvroValue::Float(f32::MIN))),
                AvroValue::Union(0, Box::new(AvroValue::Null)),
                AvroValue::Union(1, Box::new(AvroValue::Float(f32::MAX))),
            ];

            for value in values {
                batch = batch.record(
                    Record::builder()
                        .value(schema_write(schema.value().as_ref().unwrap(), value)?.into()),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn enumeration() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "Suit",
            "fields": [
                {
                    "name": "value",
                    "type": "enum",
                    "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
                }
            ]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [
                AvroValue::from(json!("CLUBS")),
                AvroValue::from(json!("HEARTS")),
            ];

            for value in values {
                batch = batch.record(
                    Record::builder()
                        .value(schema_write(schema.value().as_ref().unwrap(), value)?.into()),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn observation_enumeration() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "observation",
            "fields": [
                { "name": "key", "type": "string", "logicalType": "uuid" },
                {
                    "name": "value",
                    "type": "record",
                    "fields": [
                        { "name": "amount", "type": "double" },
                        { "name": "unit", "type": "enum", "symbols": ["CELSIUS", "MILLIBAR"] }
                    ]
                }
            ]
        }
        );

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [json!({
                "key": "1E44D9C2-5E7A-443B-BF10-2B1E5FD72F15",
                "value": {
                    "amount": 23.2,
                    "unit": "CELSIUS"
                }
            })];

            for value in values {
                batch = batch.record(schema.as_kafka_record(&value)?);
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn map() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "Long",
            "fields": [
                {"name": "value", "type": "map", "values": "long", "default": {}},
            ],
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder();

            let values = [AvroValue::from(json!({"a": 1, "b": 3, "c": 5}))];

            for value in values {
                batch = batch.record(
                    Record::builder()
                        .value(schema_write(schema.value().as_ref().unwrap(), value)?.into()),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn simple_integer_key() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "key", "type": "int"}
            ]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let keys = [32123, 45654, 87678, 12321];

            for key in keys {
                batch = batch.record(
                    Record::builder()
                        .key(schema_write(schema.key().as_ref().unwrap(), key.into())?.into()),
                );
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn simple_record_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "Person",
            "fields": [{
                "name": "value",
                "type": "record",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"},
                    {"name": "lucky", "type": "array", "items": "int", "default": []}
                ]}
            ]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [
                r(
                    schema.value().as_ref().unwrap(),
                    [
                        ("id", 32123.into()),
                        ("name", "alice".into()),
                        ("lucky", AvroValue::Array([6.into()].into())),
                    ],
                ),
                r(
                    schema.value().as_ref().unwrap(),
                    [
                        ("id", 45654.into()),
                        ("name", "bob".into()),
                        ("lucky", AvroValue::Array([5.into(), 9.into()].into())),
                    ],
                ),
            ];

            for value in values {
                batch =
                    batch.record(Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value.into())?.into(),
                    ))
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn array_bool_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "array",
                "items": "boolean",
                "default": []
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let values = [[true, true], [false, true], [true, false], [false, false]]
            .into_iter()
            .map(|l| AvroValue::Array(l.into_iter().map(AvroValue::Boolean).collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn array_int_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "array",
                "items": "int",
                "default": []
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [vec![32123, 23432, 12321, 56765], vec![i32::MIN, i32::MAX]]
                .into_iter()
                .map(|l| AvroValue::Array(l.into_iter().map(AvroValue::Int).collect::<Vec<_>>()))
                .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn array_long_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "array",
                "items": "long",
                "default": []
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [vec![32123, 23432, 12321, 56765], vec![i64::MIN, i64::MAX]]
                .into_iter()
                .map(|l| AvroValue::Array(l.into_iter().map(AvroValue::Long).collect::<Vec<_>>()))
                .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn array_float_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "name": "test",
            "type": "record",
            "fields": [{
                "name": "value",
                "type": "array",
                "items": "float",
                "default": []
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [
                vec![3.2123, 23.432, 123.21, 5676.5],
                vec![f32::MIN, f32::MAX],
            ]
            .into_iter()
            .map(|l| AvroValue::Array(l.into_iter().map(AvroValue::Float).collect::<Vec<_>>()))
            .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn array_double_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
               "name": "value",
                "type": "array",
                "items": "double",
                "default": []
            }],
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [
                vec![3.2123, 23.432, 123.21, 5676.5],
                vec![f64::MIN, f64::MAX],
            ]
            .into_iter()
            .map(|l| AvroValue::Array(l.into_iter().map(AvroValue::Double).collect::<Vec<_>>()))
            .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn array_string_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "array",
                "items": "string",
                "default": []
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [
                vec!["abc".to_string(), "def".to_string(), "pqr".to_string()],
                vec!["xyz".to_string()],
            ]
            .into_iter()
            .map(|l| AvroValue::Array(l.into_iter().map(AvroValue::String).collect::<Vec<_>>()))
            .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn array_record_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "xyz",
                    "fields": [{
                        "name": "id",
                        "type": "int"
                    },
                    {
                        "name": "name",
                        "type": "string"
                    }
                ]},
                "default": []
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder();

            let values = [
                AvroValue::Array(vec![
                    AvroValue::Record(vec![
                        ("id".into(), 32123.into()),
                        ("name".into(), "alice".into()),
                    ]),
                    AvroValue::Record(vec![
                        ("id".into(), 45654.into()),
                        ("name".into(), "bob".into()),
                    ]),
                ]),
                AvroValue::Array(vec![AvroValue::Record(vec![
                    ("id".into(), 54345.into()),
                    ("name".into(), "betty".into()),
                ])]),
            ];

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn array_bytes_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "array",
                "items": "bytes",
                "default": []
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [
                vec![b"abc".to_vec(), b"def".to_vec(), b"pqr".to_vec()],
                vec![b"54345".to_vec()],
            ]
            .into_iter()
            .map(|l| AvroValue::Array(l.into_iter().map(AvroValue::Bytes).collect::<Vec<_>>()))
            .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn uuid_logical_type() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "string",
                "logicalType": "uuid"
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [
                "383BB977-7D38-42B5-8BE7-58A1C606DE7A",
                "2C1FDDC8-4EBE-43FD-8F1C-47E18B7A4E21",
                "F9B45334-9AA2-4978-8735-9800D27A551C",
            ]
            .into_iter()
            .map(|uuid| {
                Uuid::parse_str(uuid)
                    .map(AvroValue::Uuid)
                    .map_err(Into::into)
            })
            .collect::<Result<Vec<_>>>()?;

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn time_millis_logical_type() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "int",
                "logicalType": "time-millis"
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder();

            let values = [1, 2, 3]
                .into_iter()
                .map(AvroValue::TimeMillis)
                .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn time_micros_logical_type() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "long",
                "logicalType": "time-micros"
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [1, 2, 3]
                .into_iter()
                .map(AvroValue::TimeMicros)
                .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn timestamp_millis_logical_type() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "long",
                "logicalType": "timestamp-millis"
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder();

            let values = [119_731_017, 1_000_000_000, 1_234_567_890]
                .into_iter()
                .map(|seconds| AvroValue::TimestampMillis(seconds * 1_000))
                .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn timestamp_micros_logical_type() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "long",
                "logicalType": "timestamp-micros"
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [119_731_017, 1_000_000_000, 1_234_567_890]
                .into_iter()
                .map(|seconds| AvroValue::TimestampMicros(seconds * 1_000 * 1_000))
                .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        assert!(!results.is_empty());
        assert!(results[0].num_rows() > 0);

        Ok(())
    }
}
