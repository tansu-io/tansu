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

use std::{path::PathBuf, str::FromStr, time::Duration};

use clap::Parser;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use tansu_server::{broker::Broker, coordinator::group::administrator::Controller, Error, Result};
use tansu_storage::{pg::Postgres, s3::S3, StorageContainer};
use tokio::task::JoinSet;
use tracing::debug;
use tracing_subscriber::{fmt::format::FmtSpan, prelude::*, EnvFilter};
use url::Url;

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct ElectionTimeout(Duration);

impl FromStr for ElectionTimeout {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        str::parse::<u64>(s)
            .map(Duration::from_millis)
            .map(Self)
            .map_err(Into::into)
    }
}

#[derive(Clone, Debug)]
struct KeyValue<K, V> {
    #[allow(dead_code)]
    key: K,
    value: V,
}

impl FromStr for KeyValue<String, Url> {
    type Err = Error;

    fn from_str(kv: &str) -> std::result::Result<Self, Self::Err> {
        kv.split_once('=')
            .ok_or(Error::Custom("kv: {kv}".into()))
            .and_then(|(k, v)| {
                Url::try_from(v).map_err(Into::into).map(|value| Self {
                    key: k.to_owned(),
                    value,
                })
            })
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long, default_value = "tcp://0.0.0.0:4567")]
    raft_listener_url: Url,

    #[arg(long, default_value = "10000")]
    raft_election_timeout: ElectionTimeout,

    #[arg(long = "raft-peer-url")]
    raft_peers: Vec<Url>,

    #[arg(long)]
    kafka_cluster_id: String,

    #[arg(long)]
    kafka_rack: Option<String>,

    #[arg(long, default_value = "100")]
    kafka_node_id: i32,

    #[arg(long, default_value = "tcp://0.0.0.0:9092")]
    kafka_listener_url: Url,

    #[arg(long, default_value = "tcp://0.0.0.0:9092")]
    kafka_advertised_listener_url: Url,

    #[arg(long, default_value = "pg=postgres://postgres:postgres@localhost")]
    storage_engine: KeyValue<String, Url>,

    #[arg(long, default_value = ".")]
    work_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_level(true)
                .with_line_number(true)
                .with_thread_ids(true)
                .with_span_events(FmtSpan::ACTIVE),
        )
        .with(EnvFilter::from_default_env())
        .init();

    let args = Cli::parse();

    let mut set = JoinSet::new();

    let storage = match args.storage_engine.value.scheme() {
        "postgres" | "postgresql" => {
            Postgres::builder(args.storage_engine.value.to_string().as_str())
                .map(|builder| builder.cluster(args.kafka_cluster_id.as_str()))
                .map(|builder| builder.node(args.kafka_node_id))
                .map(|builder| builder.build())
                .map(StorageContainer::Postgres)
                .map_err(Into::into)
        }

        "s3" => {
            let bucket_name = args.storage_engine.value.host_str().unwrap_or("tansu");

            let object_store_builder = AmazonS3Builder::from_env()
                .with_bucket_name(bucket_name)
                .with_conditional_put(S3ConditionalPut::ETagMatch);

            S3::new(
                args.kafka_cluster_id.as_str(),
                args.kafka_node_id,
                object_store_builder,
            )
            .map(StorageContainer::S3)
            .map_err(Into::into)
        }

        _unsupported => Err(Error::UnsupportedStorageUrl(args.storage_engine.value)),
    }?;

    {
        let groups = Controller::with_storage(storage.clone())?;

        let mut broker = Broker::new(
            args.kafka_node_id,
            &args.kafka_cluster_id,
            // raft.clone(),
            // applicator.clone(),
            args.kafka_listener_url,
            args.kafka_advertised_listener_url,
            args.kafka_rack,
            storage,
            groups,
        );

        debug!(?broker);

        _ = set.spawn(async move {
            broker.serve().await.unwrap();
        });
    }

    _ = set.join_next().await;

    Ok(())
}
