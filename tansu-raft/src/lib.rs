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

use std::sync::{Arc, Mutex, MutexGuard};

pub use append_entries::AppendEntries;
pub use apply_state::{ApplyState, ProvideApplyState};
pub use builder::Builder;
pub use clock::Clock;
pub use configuration::{Configuration, ProvideConfiguration};
use context::Context;
use election::Election;
pub use error::Error;
pub use follower::Follower;
pub use kv_store::{log_key_for_index, state_key, KvStore, ProvideKvStore};
pub use log::Log;
pub use log_entry::LogEntry;

pub use outcome::Outcome;
pub use persistent::PersistentEntry;
use replicate::Replicator;
pub use rpc::{ProvideRaftRpc, RaftRpc};
pub use server::{ProvideServer, Server};
use tokio::task::JoinSet;
use tracing::info;

pub mod blocking;
pub mod builder;
pub mod clock;
pub mod configuration;
pub mod election;
pub mod replicate;
pub mod request_vote;
pub mod server;
pub mod tarpc;

mod append_entries;
mod apply_state;
mod candidate;
mod context;
mod error;
mod follower;
mod heartbeat;
mod index;
mod kv_store;
mod leader;
mod log;
mod log_entry;
mod outcome;
mod persistent;
mod rpc;

pub type Term = u64;
pub type Index = u64;

pub type Result<R, E = Error> = std::result::Result<R, E>;

#[derive(Clone, Debug)]
pub struct Raft {
    configuration: Arc<Box<dyn Configuration>>,
    context: Arc<Mutex<Box<dyn Context>>>,
    raft_rpc: Arc<Box<dyn RaftRpc>>,
    apply_state: Arc<Box<dyn ApplyState>>,
    clock: Clock,
}

impl Raft {
    pub fn builder() -> Builder {
        Builder::default()
    }

    pub fn context_lock(&self) -> Result<MutexGuard<'_, Box<dyn Context>>> {
        self.context.lock().map_err(Into::into)
    }
}

pub async fn main(raft: Raft) -> Result<()> {
    let mut set = JoinSet::new();

    {
        let raft = raft.clone();
        set.spawn(async move {
            election_timeout_handler(raft).await.unwrap();
        });
    }

    {
        let raft = raft.clone();
        set.spawn(async move {
            heartbeat_timeout_handler(raft).await.unwrap();
        });
    }

    {
        let raft = raft.clone();
        set.spawn(async move {
            replication_timeout_handler(raft).await.unwrap();
        });
    }

    {
        let raft = raft.clone();
        set.spawn(async move { tarpc::server(raft).await.unwrap() });
    }

    loop {
        if set.join_next().await.is_none() {
            break;
        }
    }

    Ok(())
}

async fn election_timeout_handler(mut raft: Raft) -> Result<()> {
    let mut interval = raft
        .configuration
        .election_timeout()
        .map(tokio::time::interval)?;

    loop {
        interval.tick().await;

        if raft
            .configuration
            .election_timeout()
            .and_then(|timeout| raft.clock.has_expired(timeout))?
        {
            info!("balloting voters");
            raft.ballot_voters().await?
        }
    }
}

async fn heartbeat_timeout_handler(raft: Raft) -> Result<()> {
    let mut interval = raft
        .configuration
        .election_timeout()
        .map(|timeout| timeout.div_f64(2.0))
        .map(tokio::time::interval)?;

    loop {
        interval.tick().await;

        if raft
            .configuration
            .election_timeout()
            .map(|timeout| timeout.div_f64(2.0))
            .and_then(|timeout| raft.clock.has_expired(timeout))?
        {
            raft.heartbeat().await?;
        }
    }
}

async fn replication_timeout_handler(mut raft: Raft) -> Result<()> {
    let mut interval = raft
        .configuration
        .election_timeout()
        .map(|timeout| timeout.div_f64(2.0))
        .map(tokio::time::interval)?;

    loop {
        interval.tick().await;

        if raft
            .configuration
            .election_timeout()
            .map(|timeout| timeout.div_f64(2.0))
            .and_then(|timeout| raft.clock.has_expired(timeout))?
        {
            raft.replicate_log_entries().await?;
        }
    }
}
