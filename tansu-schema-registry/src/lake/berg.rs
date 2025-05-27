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

use std::{
    collections::HashMap,
    env::{var, vars},
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use crate::{Error, Result, lake::LakeHouse};
use arrow::array::RecordBatch;
use async_trait::async_trait;
use iceberg::{
    Catalog, NamespaceIdent, TableCreation, TableIdent,
    io::{FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY},
    spec::{DataFileFormat, Schema, Transform, UnboundPartitionField, UnboundPartitionSpec},
    table::Table,
    transaction::Transaction,
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
        },
    },
};
use iceberg_catalog_memory::MemoryCatalog;
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use parquet::file::properties::WriterProperties;
use tansu_kafka_sans_io::describe_configs_response::DescribeConfigsResult;
use tracing::{debug, error};
use url::Url;
use uuid::Uuid;

use super::House;

fn env_mapping(k: &str) -> Option<&str> {
    match k {
        "AWS_ACCESS_KEY_ID" => Some(S3_ACCESS_KEY_ID),
        "AWS_SECRET_ACCESS_KEY" => Some(S3_SECRET_ACCESS_KEY),
        "AWS_DEFAULT_REGION" => Some(S3_REGION),
        "AWS_ENDPOINT" => Some(S3_ENDPOINT),
        _ => None,
    }
}

pub fn env_s3_props() -> impl Iterator<Item = (String, String)> {
    vars().filter_map(|(k, v)| env_mapping(k.as_str()).map(|k| (k.to_owned(), v)))
}

#[derive(Clone, Debug, Default)]
pub struct Builder<C = PhantomData<Url>, L = PhantomData<Url>> {
    location: L,
    catalog: C,
    namespace: Option<String>,
}

impl<C, L> Builder<C, L> {
    pub fn location(self, location: Url) -> Builder<C, Url> {
        Builder {
            location,
            catalog: self.catalog,
            namespace: self.namespace,
        }
    }

    pub fn catalog(self, catalog: Url) -> Builder<Url, L> {
        Builder {
            catalog,
            location: self.location,
            namespace: self.namespace,
        }
    }

    pub fn namespace(self, namespace: Option<String>) -> Self {
        Self { namespace, ..self }
    }
}

impl Builder<Url, Url> {
    pub fn build(self) -> Result<House> {
        Iceberg::try_from(self).map(House::Iceberg)
    }
}

#[derive(Clone, Debug)]
pub struct Iceberg {
    catalog: Arc<dyn Catalog>,
    namespace: String,
    tables: Arc<Mutex<HashMap<String, Table>>>,
}

impl TryFrom<Builder<Url, Url>> for Iceberg {
    type Error = Error;

    fn try_from(value: Builder<Url, Url>) -> Result<Self, Self::Error> {
        iceberg_catalog(&value.catalog).map(|catalog| Self {
            catalog,
            namespace: value.namespace.unwrap_or(String::from("tansu")),
            tables: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

fn iceberg_catalog(catalog: &Url) -> Result<Arc<dyn Catalog>> {
    debug!(%catalog);

    match catalog.scheme() {
        "http" | "https" => {
            let catalog_config = RestCatalogConfig::builder()
                .uri(catalog.to_string())
                .warehouse(var("ICEBERG_WAREHOUSE").unwrap_or("lake".into()))
                .props(env_s3_props().collect())
                .build();

            Ok(Arc::new(RestCatalog::new(catalog_config)))
        }

        "memory" => FileIOBuilder::new("memory")
            .build()
            .map(|file_io| MemoryCatalog::new(file_io, None))
            .map(|catalog| Arc::new(catalog) as Arc<dyn Catalog>)
            .map_err(Into::into),

        _otherwise => Err(Error::UnsupportedIcebergCatalogUrl(catalog.to_owned())),
    }
}

impl Iceberg {
    async fn create_namespace(&self) -> Result<NamespaceIdent> {
        let namespace_ident = NamespaceIdent::new(self.namespace.clone());
        debug!(%namespace_ident);

        if !self
            .catalog
            .namespace_exists(&namespace_ident)
            .await
            .inspect(|namespace| debug!(?namespace))
            .inspect_err(|err| debug!(?err))?
        {
            _ = self
                .catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await
                .inspect(|namespace| debug!(?namespace))
                .inspect_err(|err| debug!(?err))?;
        }

        Ok(namespace_ident)
    }

    async fn load_or_create_table(
        &self,
        name: &str,
        schema: Schema,
        config: Config,
    ) -> Result<Table> {
        if let Some(table) = self.tables.lock().map(|guard| guard.get(name).cloned())? {
            return Ok(table);
        }

        let namespace_ident = self.create_namespace().await?;
        let table_ident = TableIdent::new(namespace_ident.clone(), name.into());

        let table = if self.catalog.table_exists(&table_ident).await? {
            self.catalog
                .load_table(&table_ident)
                .await
                .inspect_err(|err| debug!(?err))?
        } else {
            let partition_spec = UnboundPartitionSpec::builder()
                .add_partition_fields(config.partition_fields(&schema))
                .map(|builder| builder.build())
                .and_then(|partition_spec| partition_spec.bind(schema.clone()))
                .inspect(|partition_spec| debug!(?partition_spec))?;

            let table_creation = TableCreation::builder()
                .name(name.into())
                .schema(schema.clone())
                .partition_spec(partition_spec)
                .build();

            self.catalog
                .create_table(&namespace_ident, table_creation)
                .await
                .inspect(|table| debug!(?table))
                .inspect_err(|err| debug!(?err))?
        };

        _ = self
            .tables
            .lock()
            .map(|mut guard| guard.insert(name.to_owned(), table.clone()))?;

        Ok(table)
    }
}

#[async_trait]
impl LakeHouse for Iceberg {
    async fn store(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        record_batch: RecordBatch,
        config: DescribeConfigsResult,
    ) -> Result<()> {
        let _ = config;

        debug!(?record_batch);

        debug!(schema = ?record_batch.schema());

        let schema = Schema::try_from(record_batch.schema().as_ref())
            .inspect(|schema| {
                for field in schema.as_struct().fields() {
                    debug!(?field);
                }
            })
            .inspect_err(|err| debug!(?err))?;

        let table = self
            .load_or_create_table(topic, schema, Config::from(config))
            .await
            .inspect(|table| {
                for field in table.metadata().current_schema().as_struct().fields() {
                    debug!(?field);
                }
            })
            .inspect_err(|err| debug!(?err))?;

        let writer = ParquetWriterBuilder::new(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
            table.file_io().clone(),
            DefaultLocationGenerator::new(table.metadata().clone())?,
            DefaultFileNameGenerator::new(
                topic.to_owned(),
                Some(format!("{partition:0>10}-{offset:0>20}")),
                DataFileFormat::Parquet,
            ),
        );

        let mut data_file_writer = DataFileWriterBuilder::new(writer, None, 0)
            .build()
            .await
            .inspect_err(|err| error!(?err))?;

        data_file_writer
            .write(record_batch)
            .await
            .inspect_err(|err| debug!(?err))?;

        let data_files = data_file_writer
            .close()
            .await
            .inspect(|data_files| debug!(?data_files))
            .inspect_err(|err| debug!(?err))?;

        let commit_uuid = Uuid::now_v7();
        debug!(%commit_uuid);

        let tx = Transaction::new(&table);

        let mut fast_append = tx
            .fast_append(Some(commit_uuid), vec![])
            .inspect_err(|err| debug!(?err))?;

        fast_append
            .add_data_files(data_files)
            .inspect_err(|err| debug!(?err))?;

        let tx = fast_append.apply().await.inspect_err(|err| debug!(?err))?;

        tx.commit(self.catalog.as_ref())
            .await
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
            .and(Ok(()))
    }

    async fn maintain(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
struct Config(Vec<(String, String)>);

impl From<DescribeConfigsResult> for Config {
    fn from(config: DescribeConfigsResult) -> Self {
        Self(config.configs.map_or(vec![], |configs| {
            configs
                .into_iter()
                .filter_map(|config| config.value.map(|value| (config.name, value)))
                .collect::<Vec<(String, String)>>()
        }))
    }
}

impl Config {
    fn as_columns(&self, name: &str) -> Vec<String> {
        self.0
            .iter()
            .flat_map(|(key, value)| {
                if key == name {
                    value
                        .split(",")
                        .map(str::trim)
                        .map(String::from)
                        .collect::<Vec<_>>()
                        .into_iter()
                } else {
                    vec![].into_iter()
                }
            })
            .collect::<Vec<_>>()
    }

    fn partition_fields(&self, schema: &Schema) -> impl Iterator<Item = UnboundPartitionField> {
        self.as_columns("tansu.lake.partition")
            .into_iter()
            .filter_map(|field_name| {
                schema.field_by_name(&field_name).map(|field| {
                    UnboundPartitionField::builder()
                        .source_id(field.id)
                        .name(field_name)
                        .transform(Transform::Identity)
                        .build()
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenv::dotenv;
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use rand::{distr::Alphanumeric, prelude::*, rng};
    use std::{env::var, fs::File, marker::PhantomData, sync::Arc, thread};
    use tansu_kafka_sans_io::{
        ConfigResource, ErrorCode, describe_configs_response::DescribeConfigsResourceResult,
    };
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    pub(crate) fn alphanumeric_string(length: usize) -> String {
        rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
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
    async fn create_namespace() -> Result<()> {
        dotenv().ok();
        let _guard = init_tracing()?;

        let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
        let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
        let namespace = alphanumeric_string(5);

        let lake = Iceberg::try_from(
            Builder::<PhantomData<Url>, PhantomData<Url>>::default()
                .location(Url::parse(location_uri)?)
                .catalog(Url::parse(catalog_uri)?)
                .namespace(Some(namespace.clone())),
        )?;

        let ident = lake.create_namespace().await?;
        assert_eq!(namespace, ident.to_url_string());

        Ok(())
    }

    #[tokio::test]
    async fn create_duplicate_namespace() -> Result<()> {
        dotenv().ok();
        let _guard = init_tracing()?;

        let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
        let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
        let namespace = alphanumeric_string(5);

        {
            let lake = Iceberg::try_from(
                Builder::<PhantomData<Url>, PhantomData<Url>>::default()
                    .location(Url::parse(location_uri)?)
                    .catalog(Url::parse(catalog_uri)?)
                    .namespace(Some(namespace.clone())),
            )?;

            let ident = lake.create_namespace().await?;
            assert_eq!(namespace, ident.to_url_string());
        }

        {
            let lake = Iceberg::try_from(
                Builder::<PhantomData<Url>, PhantomData<Url>>::default()
                    .location(Url::parse(location_uri)?)
                    .catalog(Url::parse(catalog_uri)?)
                    .namespace(Some(namespace.clone())),
            )?;

            let ident = lake.create_namespace().await?;
            assert_eq!(namespace, ident.to_url_string());
        }

        Ok(())
    }

    #[tokio::test]
    async fn create_table() -> Result<()> {
        dotenv().ok();
        let _guard = init_tracing()?;

        let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
        let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
        let namespace = alphanumeric_string(5);

        let lake_house = Iceberg::try_from(
            Builder::<PhantomData<Url>, PhantomData<Url>>::default()
                .location(Url::parse(location_uri)?)
                .catalog(Url::parse(catalog_uri)?)
                .namespace(Some(namespace.clone())),
        )?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .build()?;

        let table_name = alphanumeric_string(5);

        let table = lake_house
            .load_or_create_table(&table_name, schema, Config::default())
            .await?;
        assert_eq!(table_name, table.identifier().name());
        assert_eq!(namespace, table.identifier().namespace().to_url_string());

        assert!(
            table
                .metadata()
                .default_partition_type()
                .fields()
                .is_empty()
        );

        Ok(())
    }

    #[tokio::test]
    async fn create_duplicate_table() -> Result<()> {
        dotenv().ok();
        let _guard = init_tracing()?;

        let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
        let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
        let namespace = alphanumeric_string(5);

        let table_name = alphanumeric_string(5);

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .build()?;

        {
            let lake_house = Iceberg::try_from(
                Builder::<PhantomData<Url>, PhantomData<Url>>::default()
                    .location(Url::parse(location_uri)?)
                    .catalog(Url::parse(catalog_uri)?)
                    .namespace(Some(namespace.clone())),
            )?;

            let table = lake_house
                .load_or_create_table(&table_name, schema.clone(), Config::default())
                .await?;
            assert_eq!(table_name, table.identifier().name());
            assert_eq!(namespace, table.identifier().namespace().to_url_string());
        }

        {
            let lake_house = Iceberg::try_from(
                Builder::<PhantomData<Url>, PhantomData<Url>>::default()
                    .location(Url::parse(location_uri)?)
                    .catalog(Url::parse(catalog_uri)?)
                    .namespace(Some(namespace.clone())),
            )?;

            let table = lake_house
                .load_or_create_table(&table_name, schema, Config::default())
                .await?;
            assert_eq!(table_name, table.identifier().name());
            assert_eq!(namespace, table.identifier().namespace().to_url_string());
        }

        Ok(())
    }

    #[tokio::test]
    async fn create_partitioned_table() -> Result<()> {
        dotenv().ok();
        let _guard = init_tracing()?;

        let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
        let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
        let namespace = alphanumeric_string(5);

        let lake_house = Iceberg::try_from(
            Builder::<PhantomData<Url>, PhantomData<Url>>::default()
                .location(Url::parse(location_uri)?)
                .catalog(Url::parse(catalog_uri)?)
                .namespace(Some(namespace.clone())),
        )?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
                NestedField::optional(4, "pqr", Type::Primitive(PrimitiveType::Timestamp)).into(),
            ])
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .build()?;

        let topic = alphanumeric_string(5);

        let partition = String::from("bar");

        let config = DescribeConfigsResult {
            error_code: ErrorCode::None.into(),
            error_message: None,
            resource_type: ConfigResource::Topic.into(),
            resource_name: topic.clone(),
            configs: Some(vec![DescribeConfigsResourceResult {
                name: String::from("tansu.lake.partition"),
                value: Some(partition.clone()),
                read_only: true,
                is_default: None,
                config_source: None,
                is_sensitive: false,
                synonyms: None,
                config_type: None,
                documentation: None,
            }]),
        };

        let table = lake_house
            .load_or_create_table(&topic, schema, Config::from(config))
            .await?;
        assert_eq!(topic, table.identifier().name());
        assert_eq!(namespace, table.identifier().namespace().to_url_string());

        assert_eq!(
            vec![partition],
            table
                .metadata()
                .default_partition_type()
                .fields()
                .iter()
                .map(|field| field.name.clone())
                .collect::<Vec<_>>()
        );

        Ok(())
    }
}
