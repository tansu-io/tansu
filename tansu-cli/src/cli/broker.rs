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
use clap::Args;
use tansu_kafka_sans_io::ErrorCode;
use tansu_server::{NODE_ID, broker::Broker, coordinator::group::administrator::Controller};
use tansu_storage::StorageContainer;
use url::Url;
use uuid::Uuid;

#[derive(Args, Clone, Debug)]
pub(super) struct Arg {
    #[arg(long, env = "CLUSTER_ID")]
    kafka_cluster_id: String,

    #[arg(long, env = "LISTENER_URL", default_value = "tcp://[::]:9092")]
    kafka_listener_url: EnvVarExp<Url>,

    #[arg(
        long,
        env = "ADVERTISED_LISTENER_URL",
        default_value = DEFAULT_BROKER
    )]
    kafka_advertised_listener_url: EnvVarExp<Url>,

    #[arg(
        long,
        env = "STORAGE_ENGINE",
        default_value = "postgres://postgres:postgres@localhost"
    )]
    storage_engine: EnvVarExp<Url>,

    #[arg(long, env = "SCHEMA_REGISTRY")]
    schema_registry: Option<EnvVarExp<Url>>,

    #[arg(long, env = "DATA_LAKE")]
    data_lake: Option<EnvVarExp<Url>>,

    #[arg(
        long,
        env = "PROMETHEUS_LISTENER_URL",
        default_value = "tcp://[::]:9000"
    )]
    prometheus_listener_url: Option<EnvVarExp<Url>>,
}

impl Arg {
    pub(super) async fn main(self) -> Result<ErrorCode> {
        Broker::<Controller<StorageContainer>, StorageContainer>::try_from(self)?
            .main()
            .await
            .map_err(Into::into)
    }
}

impl TryFrom<Arg> for tansu_server::broker::Broker<Controller<StorageContainer>, StorageContainer> {
    type Error = Error;

    fn try_from(args: Arg) -> Result<Self, Self::Error> {
        let cluster_id = args.kafka_cluster_id;
        let incarnation_id = Uuid::now_v7();
        let prometheus_listener_url = args
            .prometheus_listener_url
            .map(|env_var_exp| env_var_exp.into_inner());
        let storage_engine = args.storage_engine.into_inner();
        let advertised_listener = args.kafka_advertised_listener_url.into_inner();
        let listener = args.kafka_listener_url.into_inner();
        let schema = args
            .schema_registry
            .map(|env_var_exp| env_var_exp.into_inner());
        let data_lake = args.data_lake.map(|env_var_exp| env_var_exp.into_inner());

        Broker::<Controller<StorageContainer>, StorageContainer>::builder()
            .node_id(NODE_ID)
            .cluster_id(cluster_id)
            .incarnation_id(incarnation_id)
            .advertised_listener(advertised_listener)
            .prometheus(prometheus_listener_url)
            .schema_registry(schema)
            .data_lake(data_lake)
            .storage(storage_engine)
            .listener(listener)
            .build()
            .map_err(Into::into)
    }
}
