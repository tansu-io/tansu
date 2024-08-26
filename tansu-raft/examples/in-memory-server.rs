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

use bytes::{BufMut, Bytes, BytesMut};
use clap::Arg;
use clap::ArgAction;
use clap::Command;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    ops::RangeFrom,
    time::Duration,
};
use tansu_raft::{
    blocking::{persistent::PersistentManager, PersistentState, ProvidePersistentState},
    log_key_for_index,
    tarpc::RaftTcpTarpcClientFactory,
    ApplyState, Configuration, Follower, Index, KvStore, LogEntry, ProvideApplyState,
    ProvideConfiguration, ProvideKvStore, ProvideServer, Raft, Result, Server,
};
use tracing::{debug, info};
use url::Url;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        .with_level(true)
        .with_line_number(true)
        .with_thread_names(true)
        .with_max_level(tracing::Level::DEBUG)
        // .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .init();

    info!("hello world");
    debug!("not so fast");

    let m = Command::new("prog")
        .arg(
            Arg::new("listener_url")
                .long("listener-url")
                .value_parser(Url::parse)
                .default_value("tcp:://localhost:4567"),
        )
        .arg(
            Arg::new("election_timeout")
                .long("election-timeout")
                .default_value("10000")
                .value_parser(|s: &str| str::parse::<u64>(s).map(Duration::from_millis)),
        )
        .arg(
            Arg::new("peers")
                .long("peer-url")
                .value_parser(Url::parse)
                .action(ArgAction::Append),
        )
        .arg(Arg::new("work-dir").long("work-dir"))
        .get_matches();

    tansu_raft::main(
        Raft::builder()
            .configuration(Box::new(Config::new(
                *m.get_one::<Duration>("election_timeout").unwrap(),
                m.get_one::<Url>("listener_url").unwrap().clone(),
            )))
            .with_voters(m.get_many::<Url>("peers").unwrap().fold(
                BTreeSet::new(),
                |mut acc, url| {
                    _ = acc.insert(url.clone());
                    acc
                },
            ))
            .with_kv_store(Box::<BTreeMapKvFactory>::default())
            .with_apply_state(Box::<ApplyStateFactory>::default())
            .with_persistent_state(Box::new(PersistentStateFactory {
                id: m.get_one::<Url>("listener_url").unwrap().clone(),
                initial_persistent_state: Vec::new(),
            }))
            .with_server(Box::<ServerFactory>::default())
            .with_raft_rpc(Box::<RaftTcpTarpcClientFactory>::default())
            .build()
            .await?,
    )
    .await
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct Config {
    election_timeout: Duration,
    listener: Url,
}

impl Config {
    pub(crate) fn new(election_timeout: Duration, listener: Url) -> Self {
        Self {
            election_timeout,
            listener,
        }
    }
}

impl Configuration for Config {
    fn election_timeout(&self) -> Result<Duration> {
        Ok(self.election_timeout)
    }

    fn listener_url(&self) -> Result<Url> {
        Ok(self.listener.clone())
    }
}

impl ProvideConfiguration for Config {
    fn provide_configuration(&self) -> Result<Box<dyn Configuration>> {
        Ok(Box::new(self.clone()))
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct BTreeMapKvFactory {
    pub prev_log_index: Index,
    pub log_entries: Vec<LogEntry>,
}

impl ProvideKvStore for BTreeMapKvFactory {
    fn provide_kv_store(&self) -> Result<Box<dyn KvStore>> {
        let mut index = self.prev_log_index;
        let mut db = BTreeMap::new();
        for entry in &self.log_entries {
            index += 1;
            _ = db.insert(log_key_for_index(index)?, Bytes::from(entry));
        }

        Ok(Box::new(BTreeMapKv { db }))
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
struct BTreeMapKv {
    db: BTreeMap<Bytes, Bytes>,
}

impl KvStore for BTreeMapKv {
    fn get(&self, key: Bytes) -> Result<Option<Bytes>> {
        Ok(self.db.get(&key).cloned())
    }

    fn put(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        _ = self.db.insert(key, value);
        Ok(())
    }

    fn remove_range_from(&mut self, range: RangeFrom<Bytes>) -> Result<()> {
        self.db.retain(|key, _| !range.contains(key));
        Ok(())
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
struct Applicator;

impl ApplyState for Applicator {
    fn apply(&self, _index: Index, state: Option<Bytes>, command: Bytes) -> Result<Bytes> {
        if let Some(state) = state {
            let mut m = BytesMut::new();
            m.put_slice(&state);
            m.put_slice(&b","[..]);
            m.put_slice(&command);

            Ok(Bytes::from(m))
        } else {
            Ok(command)
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct ApplyStateFactory;

impl ProvideApplyState for ApplyStateFactory {
    fn provide_apply_state(&self) -> Result<Box<dyn ApplyState>> {
        Ok(Box::new(Applicator))
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct PersistentStateFactory {
    pub id: Url,
    pub initial_persistent_state: Vec<u8>,
}

impl ProvidePersistentState for PersistentStateFactory {
    fn provide_persistent_state(&self) -> Result<Box<dyn PersistentState>> {
        let inner = Cursor::new(self.initial_persistent_state.clone());
        Ok(Box::new(
            PersistentManager::builder()
                .inner(inner)
                .id(self.id.clone())
                .recover()?,
        ))
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct ServerFactory {
    pub commit_index: Index,
    pub last_applied: Index,
}

impl ProvideServer for ServerFactory {
    fn provide_server(&mut self) -> Result<Box<dyn Server>> {
        Ok(Box::new(
            Follower::builder()
                .commit_index(self.commit_index)
                .last_applied(self.last_applied)
                .build(),
        ) as Box<dyn Server>)
    }
}
