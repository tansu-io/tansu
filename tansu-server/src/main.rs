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

use std::{
    collections::BTreeSet,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

use clap::Parser;
use tansu_raft::{tarpc::RaftTcpTarpcClientFactory, ProvideKvStore, ProvideServer, Raft};
use tansu_server::{
    broker::Broker,
    coordinator::group::{Coordinator, GroupProvider, Manager, ProvideCoordinator},
    raft::{
        Applicator, ApplyStateFactory, Config, ServerFactory, StorageFactory,
        StoragePersistentStateFactory,
    },
    Error, Result,
};
use tansu_storage::{
    segment::{FileSystemSegmentProvider, SegmentProvider},
    Storage,
};
use tokio::task::JoinSet;
use tracing::{debug, Level};
use tracing_subscriber::{filter::Targets, fmt::format::FmtSpan, prelude::*};
use url::Url;

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

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long, default_value = "tcp:://localhost:4567")]
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

    #[arg(long, default_value = "tcp:://localhost:9092")]
    kafka_listener_url: Url,

    #[arg(long, default_value = ".")]
    work_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = Targets::new()
        .with_target("tansu_server", Level::DEBUG)
        .with_target("tansu_storage", Level::DEBUG)
        .with_target("tansu_raft", Level::DEBUG);

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .pretty()
                .with_line_number(true)
                .with_span_events(FmtSpan::ACTIVE)
                .with_thread_ids(true),
        )
        .with(filter)
        .init();

    let args = Cli::parse();

    let peers = args
        .raft_peers
        .iter()
        .fold(BTreeSet::new(), |mut acc, url| {
            acc.insert(url.clone());
            acc
        });

    let applicator = Applicator::new();

    let storage = FileSystemSegmentProvider::new(8_192, args.work_dir.clone())
        .map(|provider| Box::new(provider) as Box<dyn SegmentProvider>)
        .and_then(Storage::with_segment_provider)
        .map(|storage| Arc::new(Mutex::new(storage)))?;

    let server_factory = Box::new(ServerFactory {
        storage: storage.clone(),
    }) as Box<dyn ProvideServer>;

    let raft = Raft::builder()
        .configuration(Box::new(Config::new(
            args.raft_election_timeout.0,
            args.raft_listener_url.clone(),
        )))
        .with_voters(peers)
        .with_kv_store(Box::new(StorageFactory {
            storage: storage.clone(),
        }) as Box<dyn ProvideKvStore>)
        .with_apply_state(Box::new(ApplyStateFactory::new(applicator.clone())))
        .with_persistent_state(Box::new(StoragePersistentStateFactory {
            id: args.raft_listener_url,
            storage: storage.clone(),
        }))
        .with_server(server_factory)
        .with_raft_rpc(Box::<RaftTcpTarpcClientFactory>::default())
        .build()
        .await?;

    let mut set = JoinSet::new();

    {
        let raft = raft.clone();

        set.spawn(async move {
            tansu_raft::main(raft).await.unwrap();
        });
    }

    {
        let group_provider = Box::new(GroupProvider) as Box<dyn ProvideCoordinator>;
        let manager = Box::new(Manager::new(group_provider)) as Box<dyn Coordinator>;

        let mut broker = Broker::new(
            args.kafka_node_id,
            &args.kafka_cluster_id,
            raft.clone(),
            applicator.clone(),
            args.kafka_listener_url,
            args.kafka_rack,
            storage,
            Arc::new(Mutex::new(manager)),
        );

        debug!(?broker);

        set.spawn(async move {
            broker.serve().await.unwrap();
        });
    }

    loop {
        if set.join_next().await.is_none() {
            break;
        }
    }

    Ok(())
}
