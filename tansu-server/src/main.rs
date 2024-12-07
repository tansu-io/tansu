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

use std::{str::FromStr, time::Duration};

use clap::Parser;
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    memory::InMemory,
};
use tansu_server::{broker::Broker, coordinator::group::administrator::Controller, Error, Result};
use tansu_storage::{dynostore::DynoStore, pg::Postgres, StorageContainer};
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

const NODE_ID: i32 = 111;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long)]
    kafka_cluster_id: String,

    #[arg(long, default_value = "tcp://0.0.0.0:9092")]
    kafka_listener_url: Url,

    #[arg(long, default_value = "tcp://0.0.0.0:9092")]
    kafka_advertised_listener_url: Url,

    #[arg(long, default_value = "pg=postgres://postgres:postgres@localhost")]
    storage_engine: Url,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_level(true)
                .with_line_number(true)
                .with_thread_ids(false)
                .with_span_events(FmtSpan::NONE),
        )
        .with(EnvFilter::from_default_env())
        .init();

    let args = Cli::parse();

    let mut set = JoinSet::new();

    let storage = match args.storage_engine.scheme() {
        "postgres" | "postgresql" => Postgres::builder(args.storage_engine.to_string().as_str())
            .map(|builder| builder.cluster(args.kafka_cluster_id.as_str()))
            .map(|builder| builder.node(NODE_ID))
            .map(|builder| builder.advertised_listener(args.kafka_advertised_listener_url.clone()))
            .map(|builder| builder.build())
            .map(StorageContainer::Postgres)
            .map_err(Into::into),

        "s3" => {
            let bucket_name = args.storage_engine.host_str().unwrap_or("tansu");

            AmazonS3Builder::from_env()
                .with_bucket_name(bucket_name)
                .with_conditional_put(S3ConditionalPut::ETagMatch)
                .build()
                .map(|object_store| {
                    DynoStore::new(args.kafka_cluster_id.as_str(), NODE_ID, object_store)
                        .advertised_listener(args.kafka_advertised_listener_url.clone())
                })
                .map(StorageContainer::DynoStore)
                .map_err(Into::into)
        }

        "memory" => Ok(StorageContainer::DynoStore(
            DynoStore::new(args.kafka_cluster_id.as_str(), NODE_ID, InMemory::new())
                .advertised_listener(args.kafka_advertised_listener_url.clone()),
        )),

        _unsupported => Err(Error::UnsupportedStorageUrl(args.storage_engine)),
    }?;

    {
        let groups = Controller::with_storage(storage.clone())?;

        let mut broker = Broker::new(
            NODE_ID,
            &args.kafka_cluster_id,
            args.kafka_listener_url,
            args.kafka_advertised_listener_url,
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
