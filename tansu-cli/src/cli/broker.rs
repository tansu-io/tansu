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

use std::time::Duration;

use crate::{EnvVarExp, Result};

use super::DEFAULT_BROKER;
use clap::{Parser, Subcommand};
use tansu_broker::{NODE_ID, broker::Broker, coordinator::group::administrator::Controller};
use tansu_sans_io::ErrorCode;
use tansu_schema::{
    Registry,
    lake::{self},
};
use tansu_storage::StorageContainer;
use tracing::debug;
use url::Url;
use uuid::Uuid;

#[derive(Clone, Debug, Parser)]
pub(super) struct Arg {
    #[command(subcommand)]
    command: Option<Command>,

    /// All members of the same cluster should use the same id
    #[arg(
        long,
        env = "CLUSTER_ID",
        default_value = "tansu_cluster",
        visible_alias = "kafka-cluster-id"
    )]
    cluster_id: String,

    /// The broker will listen on this address
    #[arg(
        long,
        env = "LISTENER_URL",
        default_value = "tcp://0.0.0.0:9092",
        visible_alias = "kafka-listener-url"
    )]
    listener_url: EnvVarExp<Url>,

    /// This location is advertised to clients in metadata
    #[arg(
        long,
        env = "ADVERTISED_LISTENER_URL",
        default_value = DEFAULT_BROKER,
        visible_alias = "kafka-advertised-listener-url"
    )]
    advertised_listener_url: EnvVarExp<Url>,

    /// Storage engine examples are: postgres://postgres:postgres@localhost, memory://tansu/ or s3://tansu/
    #[arg(long, env = "STORAGE_ENGINE", default_value = "memory://tansu/")]
    storage_engine: EnvVarExp<Url>,

    /// Schema registry examples are: file://./etc/schema or s3://tansu/, containing: topic.json, topic.proto or topic.avsc
    #[arg(long, env = "SCHEMA_REGISTRY")]
    schema_registry: Option<EnvVarExp<Url>>,

    /// Schema registry cache expiry duration
    #[arg(long,value_parser = humantime::parse_duration)]
    schema_registry_cache_expiry: Option<Duration>,

    /// OTEL Exporter OTLP endpoint
    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    otlp_endpoint_url: Option<EnvVarExp<Url>>,
}

#[derive(Clone, Debug, Subcommand)]
pub(super) enum Command {
    /// Schema topics are written as Apache Iceberg tables
    Iceberg {
        /// Apache Parquet files are written to this location, examples are: file://./lake or s3://lake/
        #[arg(long, env = "DATA_LAKE")]
        location: EnvVarExp<Url>,

        /// Apache Iceberg Catalog, examples are: http://localhost:8181/
        #[arg(long, env = "ICEBERG_CATALOG")]
        catalog: EnvVarExp<Url>,

        /// Iceberg namespace
        #[arg(long, env = "ICEBERG_NAMESPACE", default_value = "tansu")]
        namespace: Option<String>,

        /// Iceberg warehouse
        #[arg(long, env = "ICEBERG_WAREHOUSE")]
        warehouse: Option<String>,
    },

    /// Schema topics are written as Delta Lake tables
    Delta {
        /// Apache Parquet files are written to this location, examples are: file://./lake or s3://lake/
        #[arg(long, env = "DATA_LAKE")]
        location: EnvVarExp<Url>,

        /// Delta database
        #[arg(long, env = "DELTA_DATABASE", default_value = "tansu")]
        database: Option<String>,

        /// Throttle the maximum number of records per second
        #[clap(long)]
        records_per_second: Option<u32>,
    },

    /// Schema topics are written in Parquet format
    Parquet {
        /// Apache Parquet files are written to this location, examples are: file://./lake or s3://lake/
        #[arg(long, env = "DATA_LAKE")]
        location: EnvVarExp<Url>,
    },
}

impl Arg {
    pub(super) async fn main(self) -> Result<ErrorCode> {
        self.build()
            .await?
            .main()
            .await
            .inspect(|result| debug!(?result))
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
    }

    async fn build(self) -> Result<Broker<Controller<StorageContainer>, StorageContainer>> {
        let cluster_id = self.cluster_id;
        let incarnation_id = Uuid::now_v7();
        let otlp_endpoint_url = self
            .otlp_endpoint_url
            .map(|env_var_exp| env_var_exp.into_inner());

        let storage_engine = self.storage_engine.into_inner();
        let advertised_listener = self.advertised_listener_url.into_inner();
        let listener = self.listener_url.into_inner();
        let schema_registry = self
            .schema_registry
            .map(|env_var_exp| env_var_exp.into_inner())
            .map(|object_store| {
                Registry::builder_try_from_url(&object_store).map(|registry| {
                    registry
                        .with_cache_expiry_after(self.schema_registry_cache_expiry)
                        .build()
                })
            })
            .transpose()?;

        let lake_house = self
            .command
            .map(|command| match command {
                Command::Iceberg {
                    location,
                    catalog,
                    namespace,
                    warehouse,
                } => lake::House::iceberg()
                    .location(location.into_inner())
                    .catalog(catalog.into_inner())
                    .namespace(namespace)
                    .warehouse(warehouse)
                    .build(),
                Command::Delta {
                    location,
                    database,
                    records_per_second,
                } => lake::House::delta()
                    .location(location.into_inner())
                    .database(database)
                    .records_per_second(records_per_second)
                    .build(),
                Command::Parquet { location } => lake::House::parquet()
                    .location(location.into_inner())
                    .build(),
            })
            .transpose()?;

        Broker::<Controller<StorageContainer>, StorageContainer>::builder()
            .node_id(NODE_ID)
            .cluster_id(cluster_id)
            .incarnation_id(incarnation_id)
            .advertised_listener(advertised_listener)
            .otlp_endpoint_url(otlp_endpoint_url)
            .schema_registry(schema_registry)
            .lake_house(lake_house)
            .storage(storage_engine)
            .listener(listener)
            .build()
            .await
            .map_err(Into::into)
    }
}
