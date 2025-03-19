// Copyright ⓒ 2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::io::Cursor;

use bytes::Bytes;
use tansu_kafka_sans_io::{ErrorCode, record::inflated::Batch};
use tracing::debug;

use crate::{Error, Result, Validator};

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Schema {
    key: Option<apache_avro::Schema>,
    value: Option<apache_avro::Schema>,
}

fn parse(encoded: Bytes) -> Result<apache_avro::Schema> {
    apache_avro::Schema::parse_reader(&mut Cursor::new(&encoded[..])).map_err(Into::into)
}

impl Schema {
    pub(crate) fn new(key: Option<Bytes>, value: Option<Bytes>) -> Result<Self> {
        key.map(parse)
            .transpose()
            .and_then(|key| value.map(parse).transpose().map(|value| (key, value)))
            .map(|(key, value)| Self { key, value })
    }
}

fn validate(validator: Option<&apache_avro::Schema>, encoded: Option<Bytes>) -> Result<()> {
    debug!(?validator, ?encoded);
    validator.map_or(Ok(()), |schema| {
        encoded.map_or(Err(Error::Api(ErrorCode::InvalidRecord)), |encoded| {
            apache_avro::Reader::with_schema(schema, &encoded[..])
                .and_then(|reader| reader.into_iter().next().transpose())
                .inspect(|value| debug!(?value))
                .inspect_err(|err| debug!(?err))
                .map_err(|_| Error::Api(ErrorCode::InvalidRecord))
                .and_then(|value| {
                    value
                        .ok_or(Error::Api(ErrorCode::InvalidRecord))
                        .map(|_value| ())
                })
        })
    })
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

#[cfg(test)]
mod tests {
    use std::{fs::File, sync::Arc, thread};

    use crate::Registry;

    use super::*;
    use apache_avro::types::Value;
    use object_store::{ObjectStore, PutPayload, memory::InMemory, path::Path};
    use serde_json::json;
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

        let object_store = InMemory::new();
        {
            let location = Path::from(format!("{topic}/key.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&json!({
                        "type": "int"
                    }))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
                )
                .await?;
        }

        {
            let location = Path::from(format!("{topic}/value.avsc"));
            _ = object_store.put(&location, serde_json::to_vec(&json!({
                "type": "record",
                "name": "Message",
                "fields": [{"name": "name", "type": "string"}, {"name": "email", "type": "string"}]
            }))
            .map(Bytes::from)
            .map(PutPayload::from)?).await?;
        }

        let registry = Registry::new(object_store);

        let key = apache_avro::Schema::parse(&json!({
            "type": "int"
        }))
        .and_then(|schema| {
            let mut writer = apache_avro::Writer::new(&schema, vec![]);
            writer
                .append(Value::Int(32123))
                .and(writer.into_inner())
                .map(Bytes::from)
        })?;

        let batch = Batch::builder()
            .record(Record::builder().key(key.into()))
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

        let object_store = InMemory::new();
        {
            let location = Path::from(format!("{topic}/key.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&json!({
                        "type": "int"
                    }))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
                )
                .await?;
        }

        {
            let location = Path::from(format!("{topic}/value.avsc"));
            _ = object_store.put(&location, serde_json::to_vec(&json!({
                        "type": "record",
                        "name": "Message",
                        "fields": [{"name": "name", "type": "string"}, {"name": "email", "type": "string"}]
                    }))
                    .map(Bytes::from)
                    .map(PutPayload::from)?).await?;
        }

        let registry = Registry::new(object_store);

        let key = apache_avro::Schema::parse(&json!({
            "type": "int"
        }))
        .and_then(|schema| {
            let mut writer = apache_avro::Writer::new(&schema, vec![]);
            writer
                .append(Value::Int(32123))
                .and(writer.into_inner())
                .map(Bytes::from)
        })?;

        let value = apache_avro::Schema::parse(&json!({
            "type": "record",
            "name": "Message",
            "fields": [{"name": "name", "type": "string"}, {"name": "email", "type": "string"}]
        }))
        .and_then(|schema| {
            let mut writer = apache_avro::Writer::new(&schema, vec![]);
            let mut record = apache_avro::types::Record::new(&schema).unwrap();
            record.put("name", "alice");
            record.put("email", "alice@example.com");

            writer
                .append(record)
                .and(writer.into_inner())
                .map(Bytes::from)
        })?;

        let batch = Batch::builder()
            .record(Record::builder().key(key.into()).value(value.into()))
            .build()?;

        registry.validate(topic, &batch).await
    }

    #[tokio::test]
    async fn value_only_invalid_record() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let object_store = InMemory::new();
        {
            let location = Path::from(format!("{topic}/key.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&json!({
                        "type": "int"
                    }))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
                )
                .await?;
        }

        {
            let location = Path::from(format!("{topic}/value.avsc"));
            _ = object_store.put(&location, serde_json::to_vec(&json!({
                        "type": "record",
                        "name": "Message",
                        "fields": [{"name": "name", "type": "string"}, {"name": "email", "type": "string"}]
                    }))
                    .map(Bytes::from)
                    .map(PutPayload::from)?).await?;
        }

        let registry = Registry::new(object_store);

        let value = apache_avro::Schema::parse(&json!({
            "type": "record",
            "name": "Message",
            "fields": [{"name": "name", "type": "string"}, {"name": "email", "type": "string"}]
        }))
        .and_then(|schema| {
            let mut writer = apache_avro::Writer::new(&schema, vec![]);
            let mut record = apache_avro::types::Record::new(&schema).unwrap();
            record.put("name", "alice");
            record.put("email", "alice@example.com");

            writer
                .append(record)
                .and(writer.into_inner())
                .map(Bytes::from)
        })?;

        let batch = Batch::builder()
            .record(Record::builder().value(value.into()))
            .build()?;

        assert!(matches!(
            registry.validate(topic, &batch).await,
            Err(Error::Api(ErrorCode::InvalidRecord))
        ));

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

    #[test]
    fn key() -> Result<()> {
        let _guard = init_tracing()?;

        let input = apache_avro::Schema::parse(&json!({
            "type": "int"
        }))
        .and_then(|schema| {
            let mut writer = apache_avro::Writer::new(&schema, vec![]);
            writer
                .append(Value::Int(32123))
                .and(writer.into_inner())
                .map(Bytes::from)
        })?;

        let batch = Batch::builder()
            .record(Record::builder().key(input.clone().into()))
            .build()?;

        let s = Schema::new(
            serde_json::to_vec(&json!({
                "type": "int"
            }))
            .map(Bytes::from)
            .map(Some)?,
            None,
        )?;

        s.validate(&batch)
    }

    #[test]
    fn invalid_key() -> Result<()> {
        let _guard = init_tracing()?;

        let input = apache_avro::Schema::parse(&json!({
            "type": "long"
        }))
        .and_then(|schema| {
            let mut writer = apache_avro::Writer::new(&schema, vec![]);
            writer
                .append(Value::Long(32123))
                .and(writer.into_inner())
                .map(Bytes::from)
        })?;

        let batch = Batch::builder()
            .record(Record::builder().key(input.clone().into()))
            .build()?;

        let s = Schema::new(
            serde_json::to_vec(&json!({
                "type": "string"
            }))
            .map(Bytes::from)
            .map(Some)?,
            None,
        )?;

        assert!(matches!(
            s.validate(&batch),
            Err(Error::Api(ErrorCode::InvalidRecord))
        ));
        Ok(())
    }

    #[test]
    fn simple_schema() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = json!({
            "type": "record",
            "name": "Message",
            "fields": [{"name": "title", "type": "string"}, {"name": "message", "type": "string"}]
        });

        let schema = apache_avro::Schema::parse(&schema)?;

        let mut record = apache_avro::types::Record::new(&schema).unwrap();
        record.put("title", "Lorem ipsum dolor sit amet");
        record.put("message", "consectetur adipiscing elit");

        let mut writer = apache_avro::Writer::new(&schema, vec![]);
        assert!(writer.append(record)? > 0);

        let input = writer.into_inner()?;
        let reader = apache_avro::Reader::with_schema(&schema, &input[..])?;

        let v = reader.into_iter().next().unwrap()?;

        assert_eq!(
            Value::Record(vec![
                (
                    "title".into(),
                    Value::String("Lorem ipsum dolor sit amet".into()),
                ),
                (
                    "message".into(),
                    Value::String("consectetur adipiscing elit".into()),
                ),
            ]),
            v
        );

        Ok(())
    }
}
