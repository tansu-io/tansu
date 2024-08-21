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

#![allow(dead_code)]

use core::fmt::Debug;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    ops::RangeFrom,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tansu_raft::{
    blocking::{persistent::PersistentManager, PersistentState, ProvidePersistentState},
    log_key_for_index, ApplyState, Configuration, Error, Follower, Index, KvStore, LogEntry,
    Outcome, ProvideApplyState, ProvideConfiguration, ProvideKvStore, ProvideRaftRpc,
    ProvideServer, Raft, RaftRpc, Result, Server, Term,
};
use url::Url;

#[derive(Default)]
pub struct RaftBuilder {
    id: Option<Url>,
    initial_persistent_state: Vec<u8>,
    prev_log_index: Index,
    log_entries: Vec<LogEntry>,
    request_vote_outcome: BTreeMap<Url, Outcome>,
    append_entries_outcome: BTreeMap<Url, ArcDynFnAppendEntryDetail>,
    kv_store: Option<Box<dyn ProvideKvStore>>,
    apply_state: Option<Box<dyn ProvideApplyState>>,
    raft_rpc: Option<Box<dyn ProvideRaftRpc>>,
    voters: BTreeSet<Url>,
}

impl RaftBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn id(self, id: Url) -> Self {
        Self {
            id: Some(id),
            ..self
        }
    }

    pub fn voters(self, voters: BTreeSet<Url>) -> Self {
        Self { voters, ..self }
    }

    pub fn initial_persistent_state(self, initial_persistent_state: Vec<u8>) -> Self {
        Self {
            initial_persistent_state,
            ..self
        }
    }

    pub fn prev_log_index(self, prev_log_index: Index) -> Self {
        Self {
            prev_log_index,
            ..self
        }
    }

    pub fn log_entries(self, log_entries: Vec<LogEntry>) -> Self {
        Self {
            log_entries,
            ..self
        }
    }

    pub fn request_vote_outcome(mut self, recipient: Url, outcome: Outcome) -> Self {
        self.request_vote_outcome.insert(recipient, outcome);
        self
    }

    pub fn append_entries_outcome(
        mut self,
        recipient: Url,
        outcome: ArcDynFnAppendEntryDetail,
    ) -> Self {
        self.append_entries_outcome.insert(recipient, outcome);
        self
    }

    pub fn with_kv_store(self, provider: Box<dyn ProvideKvStore>) -> Self {
        Self {
            kv_store: Some(provider),
            ..self
        }
    }

    pub fn with_apply_state(self, provider: Box<dyn ProvideApplyState>) -> Self {
        Self {
            apply_state: Some(provider),
            ..self
        }
    }

    pub fn with_raft_rpc(self, provider: Box<dyn ProvideRaftRpc>) -> Self {
        Self {
            raft_rpc: Some(provider),
            ..self
        }
    }

    pub async fn build(self) -> Result<Raft> {
        Raft::builder()
            .configuration(Box::new(ConfigurationFactory))
            .with_voters(self.voters)
            .with_kv_store(self.kv_store.unwrap_or_else(|| {
                Box::new(BTreeMapKvFactory {
                    prev_log_index: self.prev_log_index,
                    log_entries: self.log_entries,
                })
            }))
            .with_apply_state(self.apply_state.unwrap())
            .with_persistent_state(Box::new(PersistentStateFactory {
                id: self.id.unwrap(),
                initial_persistent_state: self.initial_persistent_state,
            }))
            .with_raft_rpc(self.raft_rpc.unwrap_or_else(|| {
                Box::new(RaftRpcFactory {
                    append_entries_outcome: self.append_entries_outcome,
                    request_votes_outcome: self.request_vote_outcome,
                    ..Default::default()
                })
            }))
            .with_server(Box::<ServerFactory>::default())
            .build()
            .await
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, Default)]
pub struct ConfigurationFactory;

impl ProvideConfiguration for ConfigurationFactory {
    fn provide_configuration(&self) -> Result<Box<dyn Configuration>> {
        #[derive(Debug)]
        struct Config;

        impl Configuration for Config {
            fn election_timeout(&self) -> Result<std::time::Duration> {
                todo!()
            }

            fn listener_url(&self) -> Result<Url> {
                todo!()
            }
        }

        Ok(Box::new(Config))
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
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

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Applicator;

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

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct ApplyStateFactory;

impl ProvideApplyState for ApplyStateFactory {
    fn provide_apply_state(&self) -> Result<Box<dyn ApplyState>> {
        Ok(Box::new(Applicator))
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AppendEntryDetail {
    pub recipient: Url,
    pub leaders_term: Term,
    pub leader_id: Url,
    pub prev_log_index: Index,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: Index,
}

pub type DynFnAppendEntryDetail = dyn Fn(AppendEntryDetail) -> Outcome + Send + Sync;
pub type ArcDynFnAppendEntryDetail = Arc<DynFnAppendEntryDetail>;

#[derive(Clone, Default)]
struct MockRaftRpc {
    appended_entries: Arc<Mutex<Vec<AppendEntryDetail>>>,
    append_entries_outcome: BTreeMap<Url, ArcDynFnAppendEntryDetail>,
    request_votes_outcome: BTreeMap<Url, Outcome>,
}

impl Debug for MockRaftRpc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(MockRaftRfc))
            .field("appended_entries", &self.appended_entries)
            .field("request_votes_outcomes", &self.request_votes_outcome)
            .finish()
    }
}

#[async_trait]
impl RaftRpc for MockRaftRpc {
    async fn append_entries(
        &self,
        recipient: Url,
        leaders_term: Term,
        leader_id: Url,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: Index,
    ) -> Result<Outcome> {
        let detail = AppendEntryDetail {
            recipient: recipient.clone(),
            leaders_term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        };

        self.appended_entries.lock()?.push(detail.clone());

        self.append_entries_outcome
            .get(&recipient)
            .cloned()
            .map(|f| f(detail))
            .ok_or_else(|| {
                Error::Custom(format!("no append_entries result setup for {}", recipient))
            })
    }

    async fn request_vote(
        &self,
        recipient: Url,
        _candidates_term: Term,
        _candidate_id: Url,
        _last_log_index: Index,
    ) -> Result<Outcome> {
        self.request_votes_outcome
            .get(&recipient)
            .cloned()
            .ok_or_else(|| Error::Custom(format!("no request_vote result setup for {}", recipient)))
    }

    async fn log(&self, _recipient: Url, _entry: Bytes) -> Result<Index> {
        Ok(666)
    }
}

#[derive(Clone, Default)]
pub struct RaftRpcFactory {
    pub appended_entries: Arc<Mutex<Vec<AppendEntryDetail>>>,
    pub append_entries_outcome: BTreeMap<Url, ArcDynFnAppendEntryDetail>,
    pub request_votes_outcome: BTreeMap<Url, Outcome>,
}

#[async_trait]
impl ProvideRaftRpc for RaftRpcFactory {
    async fn provide_raft_rpc(&self) -> Result<Box<dyn RaftRpc>> {
        Ok(Box::new(MockRaftRpc {
            append_entries_outcome: self.append_entries_outcome.clone(),
            request_votes_outcome: self.request_votes_outcome.clone(),
            appended_entries: self.appended_entries.clone(),
        }))
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
            db.insert(log_key_for_index(index)?, Bytes::from(entry));
        }

        Ok(Box::new(BTreeMapKv { db }))
    }
}
