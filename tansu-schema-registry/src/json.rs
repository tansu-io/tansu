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

use crate::{Error, Result};
use bytes::Bytes;
use serde_json::Value;
use tansu_kafka_sans_io::{record::inflated::Batch, ErrorCode};
use tracing::{debug, warn};

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

impl super::Validator for Schema {
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

#[cfg(test)]
mod tests {
    use crate::Registry;

    use super::*;
    use object_store::{memory::InMemory, path::Path, ObjectStore, PutPayload};
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
}
