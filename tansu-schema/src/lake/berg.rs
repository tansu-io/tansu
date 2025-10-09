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

use std::{
    collections::HashMap,
    env::vars,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use crate::{
    Error, Result,
    lake::{LakeHouse, LakeHouseType},
};
use arrow::array::RecordBatch;
use async_trait::async_trait;
use iceberg::{
    Catalog, MemoryCatalog, NamespaceIdent, TableCreation, TableIdent,
    io::{FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY},
    spec::{DataFileFormat, Schema, TableMetadataBuilder},
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
        },
    },
};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use parquet::file::properties::WriterProperties;
use tansu_sans_io::describe_configs_response::DescribeConfigsResult;
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
    warehouse: Option<String>,
}

impl<C, L> Builder<C, L> {
    pub fn location(self, location: Url) -> Builder<C, Url> {
        Builder {
            location,
            catalog: self.catalog,
            namespace: self.namespace,
            warehouse: self.warehouse,
        }
    }

    pub fn catalog(self, catalog: Url) -> Builder<Url, L> {
        Builder {
            catalog,
            location: self.location,
            namespace: self.namespace,
            warehouse: self.warehouse,
        }
    }

    pub fn namespace(self, namespace: Option<String>) -> Self {
        Self { namespace, ..self }
    }

    pub fn warehouse(self, warehouse: Option<String>) -> Self {
        Self { warehouse, ..self }
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
        iceberg_catalog(&value.catalog, value.warehouse.clone()).map(|catalog| Self {
            catalog,
            namespace: value.namespace.unwrap_or(String::from("tansu")),
            tables: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

fn iceberg_catalog(catalog: &Url, warehouse: Option<String>) -> Result<Arc<dyn Catalog>> {
    debug!(%catalog, ?warehouse);

    match (catalog.scheme(), catalog.path()) {
        ("http" | "https", "/") => {
            let catalog_config = RestCatalogConfig::builder()
                .uri(format!(
                    "{}://{}:{}",
                    catalog.scheme(),
                    catalog.host_str().unwrap_or("localhost"),
                    catalog.port().unwrap_or(80)
                ))
                .warehouse_opt(warehouse)
                .props(env_s3_props().collect())
                .build();

            Ok(Arc::new(RestCatalog::new(catalog_config)))
        }

        ("http" | "https", _) => {
            let catalog_config = RestCatalogConfig::builder()
                .uri(catalog.to_string())
                .warehouse_opt(warehouse)
                .props(env_s3_props().collect())
                .build();

            Ok(Arc::new(RestCatalog::new(catalog_config)))
        }

        ("memory", _) => FileIOBuilder::new("memory")
            .build()
            .map(|file_io| MemoryCatalog::new(file_io, None))
            .map(|catalog| Arc::new(catalog) as Arc<dyn Catalog>)
            .map_err(Into::into),

        (_otherwise, _) => Err(Error::UnsupportedIcebergCatalogUrl(catalog.to_owned())),
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

    async fn load_or_create_table(&self, name: &str, schema: Schema) -> Result<Table> {
        debug!(name, ?schema);
        if let Some(table) = self.tables.lock().map(|guard| guard.get(name).cloned())?
            && table.metadata().current_schema().as_ref() == &schema
        {
            return Ok(table);
        }

        let namespace_ident = self.create_namespace().await?;
        let table_ident = TableIdent::new(namespace_ident.clone(), name.into());

        let table = if self.catalog.table_exists(&table_ident).await? {
            let table = self
                .catalog
                .load_table(&table_ident)
                .await
                .inspect_err(|err| debug!(?err))?;

            if table.metadata().current_schema().as_ref() == &schema {
                table
            } else {
                debug!(current = ?table.metadata(), ?schema);

                let _result = TableMetadataBuilder::new_from_metadata(
                    table.metadata().to_owned(),
                    table
                        .metadata_location()
                        .map(|location| location.to_owned()),
                )
                .add_schema(schema.clone())
                .set_current_schema(TableMetadataBuilder::LAST_ADDED)
                .and_then(|builder| builder.build())
                .inspect(|update| {
                    debug!(?update.metadata);
                    debug!(?update.changes);
                    debug!(?update.expired_metadata_logs);
                })?;

                // let abc = TableCommit::builder()
                //     .ident(table.identifier().to_owned())
                //     .updates(result.changes)
                //     .build();

                // let q = self.catalog.update_table(commit).await?;

                // let commit_uuid = Uuid::now_v7();
                // debug!(%commit_uuid);

                // let tx = Transaction::new(&table);

                self.catalog
                    .load_table(&table_ident)
                    .await
                    .inspect_err(|err| debug!(?err))?
            }
        } else {
            self.catalog
                .create_table(
                    &namespace_ident,
                    TableCreation::builder()
                        .name(name.into())
                        .schema(schema.clone())
                        .build(),
                )
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
            .load_or_create_table(topic, schema.clone())
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

        let tx = tx
            .fast_append()
            .set_commit_uuid(commit_uuid)
            .add_data_files(data_files)
            .apply(tx)
            .inspect_err(|err| debug!(?err))?;

        tx.commit(self.catalog.as_ref())
            .await
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
            .and(Ok(()))
    }

    async fn maintain(&self) -> Result<()> {
        Ok(())
    }

    async fn lake_type(&self) -> Result<LakeHouseType> {
        Ok(LakeHouseType::Iceberg)
    }
}

#[cfg(test)]
mod tests {
    use crate::{AsArrow as _, Generator as _};

    use super::*;
    use arrow::util::pretty::pretty_format_batches;
    use bytes::Bytes;
    use datafusion::prelude::SessionContext;
    use dotenv::dotenv;
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use iceberg_datafusion::IcebergCatalogProvider;
    use rand::{distr::Alphanumeric, prelude::*, rng};
    use std::{env::var, fs::File, marker::PhantomData, sync::Arc, thread};
    use tansu_sans_io::{
        ConfigResource, ErrorCode, describe_configs_response::DescribeConfigsResourceResult,
        record::inflated::Batch,
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
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
        let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
        let warehouse = var("ICEBERG_WAREHOUSE").ok();
        let namespace = alphanumeric_string(5);
        debug!(catalog_uri, location_uri, ?warehouse, namespace);

        let lake = Iceberg::try_from(
            Builder::<PhantomData<Url>, PhantomData<Url>>::default()
                .location(Url::parse(location_uri)?)
                .catalog(Url::parse(catalog_uri)?)
                .warehouse(warehouse.clone())
                .namespace(Some(namespace.clone())),
        )?;

        let ident = lake.create_namespace().await?;
        assert_eq!(namespace, ident.to_url_string());

        Ok(())
    }

    #[tokio::test]
    async fn create_duplicate_namespace() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
        let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
        let warehouse = var("ICEBERG_WAREHOUSE").ok();
        let namespace = alphanumeric_string(5);
        debug!(catalog_uri, location_uri, ?warehouse, namespace);

        {
            let lake = Iceberg::try_from(
                Builder::<PhantomData<Url>, PhantomData<Url>>::default()
                    .location(Url::parse(location_uri)?)
                    .catalog(Url::parse(catalog_uri)?)
                    .warehouse(warehouse.clone())
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
                    .warehouse(warehouse)
                    .namespace(Some(namespace.clone())),
            )?;

            let ident = lake.create_namespace().await?;
            assert_eq!(namespace, ident.to_url_string());
        }

        Ok(())
    }

    #[tokio::test]
    async fn create_table() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
        let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
        let warehouse = var("ICEBERG_WAREHOUSE").ok();
        let namespace = alphanumeric_string(5);

        debug!(catalog_uri, location_uri, ?warehouse, namespace);

        let lake_house = Iceberg::try_from(
            Builder::<PhantomData<Url>, PhantomData<Url>>::default()
                .location(Url::parse(location_uri)?)
                .catalog(Url::parse(catalog_uri)?)
                .namespace(Some(namespace.clone()))
                .warehouse(warehouse.clone()),
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

        let table = lake_house.load_or_create_table(&table_name, schema).await?;
        assert_eq!(table_name, table.identifier().name());
        assert_eq!(namespace, table.identifier().namespace().to_url_string());

        Ok(())
    }

    #[tokio::test]
    async fn create_duplicate_table() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
        let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
        let warehouse = var("ICEBERG_WAREHOUSE").ok();
        let namespace = alphanumeric_string(5);
        let table_name = alphanumeric_string(5);

        debug!(catalog_uri, location_uri, ?warehouse, namespace, table_name);

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
                    .warehouse(warehouse.clone())
                    .namespace(Some(namespace.clone())),
            )?;

            let table = lake_house
                .load_or_create_table(&table_name, schema.clone())
                .await?;
            assert_eq!(table_name, table.identifier().name());
            assert_eq!(namespace, table.identifier().namespace().to_url_string());
        }

        {
            let lake_house = Iceberg::try_from(
                Builder::<PhantomData<Url>, PhantomData<Url>>::default()
                    .location(Url::parse(location_uri)?)
                    .catalog(Url::parse(catalog_uri)?)
                    .namespace(Some(namespace.clone()))
                    .warehouse(warehouse),
            )?;

            let table = lake_house.load_or_create_table(&table_name, schema).await?;
            assert_eq!(table_name, table.identifier().name());
            assert_eq!(namespace, table.identifier().namespace().to_url_string());
        }

        Ok(())
    }

    #[test]
    fn url_parse() -> Result<()> {
        let uri = Url::parse("http://localhost:8181")?;
        assert_eq!("http://localhost:8181/", uri.as_str());
        assert_eq!("http", uri.scheme());
        assert!(uri.has_host());
        assert_eq!(Some("localhost"), uri.host_str());
        assert_eq!(Some(8181), uri.port());
        assert_eq!("/", uri.path());

        let uri = Url::parse("http://localhost:8181/catalog")?;
        assert_eq!("http://localhost:8181/catalog", uri.as_str());
        assert_eq!("http", uri.scheme());
        assert!(uri.has_host());
        assert_eq!(Some("localhost"), uri.host_str());
        assert_eq!(Some(8181), uri.port());
        assert_eq!("/catalog", uri.path());

        Ok(())
    }

    #[tokio::test]
    async fn migrate_schema() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
        let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
        let warehouse = var("ICEBERG_WAREHOUSE").ok();
        let namespace = &alphanumeric_string(5)[..];
        let table_name = &alphanumeric_string(5)[..];

        debug!(catalog_uri, location_uri, ?warehouse, namespace, table_name);

        let lake_house = Iceberg::try_from(
            Builder::<PhantomData<Url>, PhantomData<Url>>::default()
                .location(Url::parse(location_uri)?)
                .catalog(Url::parse(catalog_uri)?)
                .warehouse(warehouse.clone())
                .namespace(Some(namespace.into())),
        )?;

        let catalog = lake_house.catalog.clone();

        let lake_house = House::Iceberg(lake_house);

        let partition = 32123;

        let record_batch_001 = {
            let schema = crate::proto::Schema::try_from(Bytes::from_static(include_bytes!(
                "../../tests/migrate-001.proto"
            )))?;

            Batch::builder()
                .record(schema.generate()?)
                .base_timestamp(119_731_017_000)
                .build()
                .map_err(Into::into)
                .and_then(|batch| schema.as_arrow(partition, &batch, LakeHouseType::Iceberg))?
        };

        let config = DescribeConfigsResult::default()
            .error_code(ErrorCode::None.into())
            .error_message(None)
            .resource_type(ConfigResource::Topic.into())
            .resource_name(table_name.into())
            .configs(Some(vec![
                DescribeConfigsResourceResult::default()
                    .name(String::from("tansu.lake.normalize"))
                    .value(Some(String::from("true")))
                    .read_only(true),
            ]));

        let offset = 543212345;

        lake_house
            .store(
                table_name,
                partition,
                offset,
                record_batch_001,
                config.clone(),
            )
            .await
            .inspect(|result| debug!(?result))
            .inspect_err(|err| debug!(?err))?;

        let ctx = SessionContext::new();
        assert!(
            ctx.register_catalog(
                "iceberg",
                IcebergCatalogProvider::try_new(catalog.clone())
                    .await
                    .map(Arc::new)?,
            )
            .is_none()
        );

        let provider = ctx.catalog("iceberg").expect("provider");

        debug!(schemas = ?provider.schema_names());

        debug!(
            tables = ?provider
                .schema_names()
                .iter()
                .flat_map(|schema_name| {
                    provider
                        .schema(schema_name)
                        .map(|schema| {
                            schema
                                .table_names()
                                .iter()
                                .map(|table_name| format!("{schema_name}.{table_name}"))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
        );

        let df = ctx
            .sql(&format!(r#"select * from iceberg."{namespace}"."{table_name}""#)[..])
            .await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+------------------------------------------------------------------------------------+------------------------+",
            "| meta                                                                               | value                  |",
            "+------------------------------------------------------------------------------------+------------------------+",
            "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {email_address: lorem} |",
            "+------------------------------------------------------------------------------------+------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        let record_batch_002 = {
            let schema = crate::proto::Schema::try_from(Bytes::from_static(include_bytes!(
                "../../tests/migrate-002.proto"
            )))?;

            Batch::builder()
                .record(schema.generate()?)
                .base_timestamp(119_731_017_000)
                .build()
                .map_err(Into::into)
                .and_then(|batch| schema.as_arrow(partition, &batch, LakeHouseType::Iceberg))?
        };

        let offset = 654323456;

        lake_house
            .store(
                table_name,
                partition,
                offset,
                record_batch_002,
                config.clone(),
            )
            .await
            .inspect(|result| debug!(?result))
            .inspect_err(|err| debug!(?err))?;

        let df = ctx
            .sql(&format!(r#"select * from iceberg."{namespace}"."{table_name}""#)[..])
            .await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+------------------------------------------------------------------------------------+------------------------+",
            "| meta                                                                               | value                  |",
            "+------------------------------------------------------------------------------------+------------------------+",
            "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {email_address: lorem} |",
            "+------------------------------------------------------------------------------------+------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }
}
