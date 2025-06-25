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

use crate::{EnvVarExp, Error, Result};

use super::DEFAULT_BROKER;
use clap::{Parser, Subcommand};
use tansu_kafka_sans_io::ErrorCode;
use tansu_schema_registry::lake::{self};
use tansu_server::{NODE_ID, broker::Broker, coordinator::group::administrator::Controller};
use tansu_storage::StorageContainer;
use tracing::debug;
use url::Url;
use uuid::Uuid;

#[derive(Clone, Debug, Parser)]
pub(super) struct Arg {
    #[command(subcommand)]
    command: Option<Command>,

    /// All members of the same cluster should use the same id
    #[arg(long, env = "CLUSTER_ID", default_value = "tansu_cluster")]
    kafka_cluster_id: String,

    /// The broker will listen on this address
    #[arg(long, env = "LISTENER_URL", default_value = "tcp://[::]:9092")]
    kafka_listener_url: EnvVarExp<Url>,

    /// This location is advertised to clients in metadata
    #[arg(
        long,
        env = "ADVERTISED_LISTENER_URL",
        default_value = DEFAULT_BROKER,
    )]
    kafka_advertised_listener_url: EnvVarExp<Url>,

    /// Storage engine examples are: postgres://postgres:postgres@localhost, memory://tansu/ or s3://tansu/
    #[arg(long, env = "STORAGE_ENGINE", default_value = "memory://tansu/")]
    storage_engine: EnvVarExp<Url>,

    /// Schema registry examples are: file://./etc/schema or s3://tansu/, containing: topic.json, topic.proto or topic.avsc
    #[arg(long, env = "SCHEMA_REGISTRY")]
    schema_registry: Option<EnvVarExp<Url>>,

    /// OTEL Exporter OTLP endpoint
    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    otlp_endpoint_url: Option<EnvVarExp<Url>>,
}

#[derive(Clone, Debug, Subcommand)]
pub(super) enum Command {
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

    Delta {
        /// Apache Parquet files are written to this location, examples are: file://./lake or s3://lake/
        #[arg(long, env = "DATA_LAKE")]
        location: EnvVarExp<Url>,

        /// Delta database
        #[arg(long, env = "DELTA_DATABASE", default_value = "tansu")]
        database: Option<String>,
    },

    Parquet {
        /// Apache Parquet files are written to this location, examples are: file://./lake or s3://lake/
        #[arg(long, env = "DATA_LAKE")]
        location: EnvVarExp<Url>,
    },
}

impl Arg {
    pub(super) async fn main(self) -> Result<ErrorCode> {
        Broker::<Controller<StorageContainer>, StorageContainer>::try_from(self)?
            .main()
            .await
            .inspect(|result| debug!(?result))
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
    }
}

impl TryFrom<Arg> for tansu_server::broker::Broker<Controller<StorageContainer>, StorageContainer> {
    type Error = Error;

    fn try_from(args: Arg) -> Result<Self, Self::Error> {
        let cluster_id = args.kafka_cluster_id;
        let incarnation_id = Uuid::now_v7();
        let prometheus_listener_url = args
            .otlp_endpoint_url
            .map(|env_var_exp| env_var_exp.into_inner());

        let storage_engine = args.storage_engine.into_inner();
        let advertised_listener = args.kafka_advertised_listener_url.into_inner();
        let listener = args.kafka_listener_url.into_inner();
        let schema = args
            .schema_registry
            .map(|env_var_exp| env_var_exp.into_inner());

        let lake_house = args
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
                Command::Delta { location, database } => lake::House::delta()
                    .location(location.into_inner())
                    .database(database)
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
            .otlp_endpoint_url(prometheus_listener_url)
            .schema_registry(schema)
            .lake_house(lake_house)
            .storage(storage_engine)
            .listener(listener)
            .build()
            .map_err(Into::into)
    }
}
