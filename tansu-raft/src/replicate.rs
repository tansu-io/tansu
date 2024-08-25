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

use std::{collections::BTreeMap, sync::MutexGuard};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use url::Url;

use crate::{
    context::Context, log_key_for_index, server::State, Error, Index, LogEntry, Outcome, Raft,
    Result, Term,
};

#[async_trait]
pub trait Replicator {
    async fn replicate_log_entries(&mut self) -> Result<()>;
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct Detail {
    leaders_term: Term,
    leader_id: Url,
    leader_commit: Index,
    log_entries: BTreeMap<Url, ReplicationEntry>,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, Default)]
struct ReplicationEntry {
    prev_log_index: Index,
    prev_log_term: Term,
    entries: Vec<LogEntry>,
}

fn log_entry_for_index(
    context: &mut MutexGuard<'_, Box<dyn Context>>,
    index: Index,
) -> Result<LogEntry> {
    context
        .kv_store()
        .get(log_key_for_index(index)?)
        .and_then(|maybe| {
            maybe
                .ok_or(Error::EmptyLogEntry(index))
                .and_then(LogEntry::try_from)
        })
}

fn replication_entry_for_index(
    context: &mut MutexGuard<'_, Box<dyn Context>>,
    index: Index,
) -> Result<ReplicationEntry> {
    if index == 1 {
        Ok(ReplicationEntry {
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![log_entry_for_index(context, index)?],
        })
    } else {
        log_entry_for_index(context, index - 1).and_then(|entry| {
            Ok(ReplicationEntry {
                prev_log_index: index - 1,
                prev_log_term: entry.term(),
                entries: vec![log_entry_for_index(context, index)?],
            })
        })
    }
}

fn node_log_entries(context: &mut MutexGuard<'_, Box<dyn Context>>) -> Result<Option<Detail>> {
    if context
        .server()
        .state()
        .is_ok_and(|state| state == State::Leader)
    {
        let commit_index = context.server().commit_index()?;

        debug!("commit_index: {}", commit_index);

        Ok(Some(Detail {
            log_entries: context
                .server()
                .replicate_log_entries(context.voters())
                .and_then(|entries| {
                    entries
                        .iter()
                        .try_fold(BTreeMap::new(), |mut acc, (node, index)| {
                            if *index <= commit_index {
                                replication_entry_for_index(context, *index).map(|entry| {
                                    _ = acc.insert(node.clone(), entry);
                                    acc
                                })
                            } else {
                                Ok(acc)
                            }
                        })
                })?,
            leader_id: context.persistent_state().id(),
            leaders_term: context
                .persistent_state_mut()
                .read()
                .map(|state| state.current_term())?,
            leader_commit: context.server().commit_index()?,
        }))
    } else {
        Ok(None)
    }
}

fn adjust_next_index_for_node(
    context: &mut MutexGuard<'_, Box<dyn Context>>,
    outcomes: Vec<Outcome>,
) -> Result<()> {
    outcomes.iter().try_for_each(|outcome| {
        context
            .server_mut()
            .next_index_mut(&outcome.from())
            .and_then(|maybe| {
                maybe
                    .ok_or(Error::UnknownNode(outcome.from()))
                    .and_then(|next_index| {
                        if outcome.result() {
                            *next_index += 1;
                            Ok(())
                        } else if *next_index > 1 {
                            *next_index -= 1;
                            Ok(())
                        } else {
                            Err(Error::BadNextIndex)
                        }
                    })
            })
    })
}

impl Raft {
    fn node_log_entries(&self) -> Result<Option<Detail>> {
        self.context_lock()
            .and_then(|mut context| node_log_entries(&mut context))
    }

    fn adjust_next_index_for_node(&self, outcomes: Vec<Outcome>) -> Result<()> {
        self.context_lock()
            .and_then(|mut context| adjust_next_index_for_node(&mut context, outcomes))
    }
}

#[async_trait]
impl Replicator for Raft {
    async fn replicate_log_entries(&mut self) -> Result<()> {
        if let Some(detail) = self.node_log_entries()? {
            let mut outcomes = Vec::new();

            for (recipient, replication_entry) in detail.log_entries {
                debug!(
                    "recipient: {recipient}, replication_entry: {:?}",
                    replication_entry
                );

                let Ok(outcome) = self
                    .raft_rpc
                    .append_entries(
                        recipient.clone(),
                        detail.leaders_term,
                        detail.leader_id.clone(),
                        replication_entry.prev_log_index,
                        replication_entry.prev_log_term,
                        replication_entry.entries,
                        detail.leader_commit,
                    )
                    .await
                else {
                    warn!("something bad while replicating to recipient: {recipient}",);

                    continue;
                };

                debug!("outcome: {:?} for recipient: {recipient}", outcome,);

                outcomes.push(outcome);
                self.clock.update()?;
            }

            self.adjust_next_index_for_node(outcomes)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(dead_code)]

    use std::{
        collections::{BTreeSet, VecDeque},
        io::Cursor,
        ops::RangeFrom,
        sync::{Arc, Mutex},
    };

    use bytes::{BufMut, Bytes, BytesMut};
    use url::Url;

    use super::*;
    use crate::{
        blocking::{persistent::PersistentManager, PersistentState, ProvidePersistentState},
        election::Election,
        ApplyState, Configuration, Follower, KvStore, PersistentEntry, ProvideApplyState,
        ProvideConfiguration, ProvideKvStore, ProvideRaftRpc, ProvideServer, RaftRpc, Server,
    };

    #[derive(Default)]
    pub(crate) struct RaftBuilder {
        id: Option<Url>,
        initial_persistent_state: Vec<u8>,
        prev_log_index: Index,
        log_entries: Vec<LogEntry>,
        request_vote_outcome: BTreeMap<Url, Outcome>,
        append_entries_outcome: BTreeMap<Url, Outcome>,
        kv_store: Option<Box<dyn ProvideKvStore>>,
        apply_state: Option<Box<dyn ProvideApplyState>>,
        voters: BTreeSet<Url>,
    }

    impl RaftBuilder {
        pub(crate) fn new() -> Self {
            Self {
                ..Default::default()
            }
        }

        pub(crate) fn id(self, id: Url) -> Self {
            Self {
                id: Some(id),
                ..self
            }
        }

        pub(crate) fn voters(self, voters: BTreeSet<Url>) -> Self {
            Self { voters, ..self }
        }

        pub(crate) fn initial_persistent_state(self, initial_persistent_state: Vec<u8>) -> Self {
            Self {
                initial_persistent_state,
                ..self
            }
        }

        pub(crate) fn prev_log_index(self, prev_log_index: Index) -> Self {
            Self {
                prev_log_index,
                ..self
            }
        }

        pub(crate) fn log_entries(self, log_entries: Vec<LogEntry>) -> Self {
            Self {
                log_entries,
                ..self
            }
        }

        pub(crate) fn request_vote_outcome(mut self, recipient: Url, outcome: Outcome) -> Self {
            _ = self.request_vote_outcome.insert(recipient, outcome);
            self
        }

        pub(crate) fn append_entries_outcome(mut self, recipient: Url, outcome: Outcome) -> Self {
            _ = self.append_entries_outcome.insert(recipient, outcome);
            self
        }

        pub(crate) fn with_apply_state(self, provider: Box<dyn ProvideApplyState>) -> Self {
            Self {
                apply_state: Some(provider),
                ..self
            }
        }

        pub(crate) async fn build(self) -> Result<Raft> {
            Raft::builder()
                .configuration(Box::new(ConfigurationFactory))
                .with_voters(self.voters)
                .with_kv_store(self.kv_store.unwrap_or_else(|| {
                    Box::new(BTreeMapKvFactory {
                        prev_log_index: self.prev_log_index,
                        log_entries: self.log_entries,
                    })
                }))
                .with_apply_state(
                    self.apply_state
                        .unwrap_or_else(|| Box::new(ApplyStateFactory::new())),
                )
                .with_persistent_state(Box::new(PersistentStateFactory {
                    id: self.id.unwrap(),
                    initial_persistent_state: self.initial_persistent_state,
                }))
                .with_raft_rpc(Box::new(RaftRpcFactory {
                    append_entries_outcome: self.append_entries_outcome,
                    request_votes_outcome: self.request_vote_outcome,
                }))
                .with_server(Box::<ServerFactory>::default())
                .build()
                .await
        }
    }

    #[derive(
        Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, Default,
    )]
    struct ConfigurationFactory;

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

    #[derive(Default)]
    struct ServerFactory {
        commit_index: Index,
        last_applied: Index,
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
    pub(crate) struct Applicator;

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
    pub(crate) struct ApplyStateFactory;

    impl ApplyStateFactory {
        pub(crate) fn new() -> Self {
            Self
        }
    }

    impl ProvideApplyState for ApplyStateFactory {
        fn provide_apply_state(&self) -> Result<Box<dyn ApplyState>> {
            Ok(Box::new(Applicator))
        }
    }

    #[derive(Debug)]
    #[allow(unused)]
    struct AppendEntryDetail {
        recipient: Url,
        leaders_term: Term,
        leader_id: Url,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: Index,
    }

    #[derive(Debug)]
    struct MockRaftRpc {
        appended_entries: Arc<Mutex<Vec<AppendEntryDetail>>>,
        append_entries_outcome: BTreeMap<Url, Outcome>,
        request_votes_outcome: BTreeMap<Url, Outcome>,
        logged: VecDeque<(Url, Bytes)>,
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
            self.appended_entries.lock()?.push(AppendEntryDetail {
                recipient: recipient.clone(),
                leaders_term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            });

            self.append_entries_outcome
                .get(&recipient)
                .cloned()
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
                .ok_or_else(|| {
                    Error::Custom(format!("no request_vote result setup for {}", recipient))
                })
        }

        async fn log(&self, _recipient: Url, _entry: Bytes) -> Result<Index> {
            Ok(666)
        }
    }

    struct RaftRpcFactory {
        append_entries_outcome: BTreeMap<Url, Outcome>,
        request_votes_outcome: BTreeMap<Url, Outcome>,
    }

    #[async_trait]
    impl ProvideRaftRpc for RaftRpcFactory {
        async fn provide_raft_rpc(&self) -> Result<Box<dyn RaftRpc>> {
            Ok(Box::new(MockRaftRpc {
                append_entries_outcome: self.append_entries_outcome.clone(),
                request_votes_outcome: self.request_votes_outcome.clone(),
                appended_entries: Arc::new(Mutex::new(Vec::new())),
                logged: VecDeque::new(),
            }))
        }
    }

    struct PersistentStateFactory {
        id: Url,
        initial_persistent_state: Vec<u8>,
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

    #[derive(Debug)]
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

    struct BTreeMapKvFactory {
        prev_log_index: Index,
        log_entries: Vec<LogEntry>,
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

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn log_entry_for_index() -> Result<()> {
        let id = Url::parse("tcp://localhost:1234/")?;
        let current_term = 1;

        let mut pm = PersistentManager::builder()
            .inner(Cursor::new(vec![]))
            .id(id.clone())
            .recover()?;

        pm.write(
            PersistentEntry::builder()
                .current_term(current_term)
                .voted_for(None)
                .build(),
        )?;

        let abc = LogEntry::builder().term(current_term).value("abc").build();
        let def = LogEntry::builder().term(current_term).value("def").build();
        let ghi = LogEntry::builder().term(current_term).value("ghi").build();
        let jkl = LogEntry::builder().term(current_term).value("jkl").build();

        let r = RaftBuilder::new()
            .id(id.clone())
            .initial_persistent_state(pm.into_inner().into_inner())
            .prev_log_index(0)
            .log_entries(vec![abc.clone(), def.clone(), ghi.clone(), jkl.clone()])
            .with_apply_state(Box::new(ApplyStateFactory::new()))
            .build()
            .await?;

        assert_eq!(
            abc,
            r.context_lock()
                .and_then(|mut context| super::log_entry_for_index(&mut context, 1))?
        );
        assert_eq!(
            def,
            r.context_lock()
                .and_then(|mut context| super::log_entry_for_index(&mut context, 2))?
        );
        assert_eq!(
            ghi,
            r.context_lock()
                .and_then(|mut context| super::log_entry_for_index(&mut context, 3))?
        );
        assert_eq!(
            jkl,
            r.context_lock()
                .and_then(|mut context| super::log_entry_for_index(&mut context, 4))?
        );

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn replication_entry_for_index() -> Result<()> {
        let id = Url::parse("tcp://localhost:1234/")?;

        let mut pm = PersistentManager::builder()
            .inner(Cursor::new(vec![]))
            .id(id.clone())
            .recover()?;

        pm.write(
            PersistentEntry::builder()
                .current_term(5)
                .voted_for(None)
                .build(),
        )?;

        let abc = LogEntry::builder().term(11).value("abc").build();
        let def = LogEntry::builder().term(12).value("def").build();
        let ghi = LogEntry::builder().term(13).value("ghi").build();
        let jkl = LogEntry::builder().term(14).value("jkl").build();

        let r = RaftBuilder::new()
            .id(id.clone())
            .initial_persistent_state(pm.into_inner().into_inner())
            .prev_log_index(0)
            .log_entries(vec![abc.clone(), def.clone(), ghi.clone(), jkl.clone()])
            .with_apply_state(Box::new(ApplyStateFactory))
            .build()
            .await?;

        assert_eq!(
            ReplicationEntry {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![abc.clone()]
            },
            r.context_lock()
                .and_then(|mut context| super::replication_entry_for_index(&mut context, 1))?
        );

        assert_eq!(
            ReplicationEntry {
                prev_log_index: 1,
                prev_log_term: 11,
                entries: vec![def.clone()]
            },
            r.context_lock()
                .and_then(|mut context| super::replication_entry_for_index(&mut context, 2))?
        );

        assert_eq!(
            ReplicationEntry {
                prev_log_index: 2,
                prev_log_term: 12,
                entries: vec![ghi.clone()]
            },
            r.context_lock()
                .and_then(|mut context| super::replication_entry_for_index(&mut context, 3))?
        );

        assert_eq!(
            ReplicationEntry {
                prev_log_index: 3,
                prev_log_term: 13,
                entries: vec![jkl.clone()]
            },
            r.context_lock()
                .and_then(|mut context| super::replication_entry_for_index(&mut context, 4))?
        );

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn node_log_entries() -> Result<()> {
        let id = Url::parse("tcp://localhost:1234/")?;
        let current_term = 1;

        let mut pm = PersistentManager::builder()
            .inner(Cursor::new(vec![]))
            .id(id.clone())
            .recover()?;

        pm.write(
            PersistentEntry::builder()
                .current_term(current_term)
                .voted_for(None)
                .build(),
        )?;

        let a = Url::parse("tcp://localhost/32123")?;
        let b = Url::parse("tcp://localhost/78987")?;

        let abc = LogEntry::builder().term(current_term).value("abc").build();
        let def = LogEntry::builder().term(current_term).value("def").build();
        let ghi = LogEntry::builder().term(current_term).value("ghi").build();
        let jkl = LogEntry::builder().term(current_term).value("jkl").build();

        let mut r = RaftBuilder::new()
            .id(id.clone())
            .initial_persistent_state(pm.into_inner().into_inner())
            .prev_log_index(0)
            .voters([a.clone(), b.clone()].into())
            .log_entries(vec![abc.clone(), def.clone(), ghi.clone(), jkl.clone()])
            .with_apply_state(Box::new(ApplyStateFactory))
            .request_vote_outcome(
                a.clone(),
                Outcome::builder()
                    .result(true)
                    .term(current_term + 1)
                    .from(a.clone())
                    .build(),
            )
            .request_vote_outcome(
                b.clone(),
                Outcome::builder()
                    .result(false)
                    .term(current_term + 1)
                    .from(b.clone())
                    .build(),
            )
            .append_entries_outcome(
                a.clone(),
                Outcome::builder()
                    .result(true)
                    .term(current_term + 1)
                    .from(a.clone())
                    .build(),
            )
            .append_entries_outcome(
                b.clone(),
                Outcome::builder()
                    .result(true)
                    .term(current_term + 1)
                    .from(b.clone())
                    .build(),
            )
            .build()
            .await?;

        r.ballot_voters().await?;

        r.context_lock().and_then(|mut context| {
            *context.server_mut().commit_index_mut()? = 4;
            Ok(())
        })?;

        r.context_lock().and_then(|mut context| {
            *context.server_mut().next_index_mut(&b)?.unwrap() = 2;
            Ok(())
        })?;

        assert_eq!(
            Some(Detail {
                leaders_term: 2,
                leader_id: id,
                leader_commit: 4,
                log_entries: BTreeMap::from([
                    (
                        a,
                        ReplicationEntry {
                            prev_log_index: 0,
                            prev_log_term: 0,
                            entries: vec![abc.clone()],
                        },
                    ),
                    (
                        b,
                        ReplicationEntry {
                            prev_log_index: 1,
                            prev_log_term: 1,
                            entries: vec![def.clone()],
                        },
                    ),
                ]),
            }),
            r.node_log_entries()?
        );

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn adjust_next_index_for_node() -> Result<()> {
        let id = Url::parse("tcp://localhost:1234/")?;
        let current_term = 1;

        let mut pm = PersistentManager::builder()
            .inner(Cursor::new(vec![]))
            .id(id.clone())
            .recover()?;

        pm.write(
            PersistentEntry::builder()
                .current_term(current_term)
                .voted_for(None)
                .build(),
        )?;

        let abc = LogEntry::builder().term(current_term).value("abc").build();
        let def = LogEntry::builder().term(current_term).value("def").build();
        let ghi = LogEntry::builder().term(current_term).value("ghi").build();
        let jkl = LogEntry::builder().term(current_term).value("jkl").build();

        let a = Url::parse("tcp://localhost/32123")?;
        let b = Url::parse("tcp://localhost/78987")?;

        let mut r = RaftBuilder::new()
            .id(id.clone())
            .initial_persistent_state(pm.into_inner().into_inner())
            .prev_log_index(0)
            .voters([a.clone(), b.clone()].into())
            .log_entries(vec![abc.clone(), def.clone(), ghi.clone(), jkl.clone()])
            .with_apply_state(Box::new(ApplyStateFactory))
            .request_vote_outcome(
                a.clone(),
                Outcome::builder()
                    .result(true)
                    .term(current_term + 1)
                    .from(a.clone())
                    .build(),
            )
            .request_vote_outcome(
                b.clone(),
                Outcome::builder()
                    .result(false)
                    .term(current_term + 1)
                    .from(b.clone())
                    .build(),
            )
            .append_entries_outcome(
                a.clone(),
                Outcome::builder()
                    .result(true)
                    .term(current_term + 1)
                    .from(a.clone())
                    .build(),
            )
            .append_entries_outcome(
                b.clone(),
                Outcome::builder()
                    .result(true)
                    .term(current_term + 1)
                    .from(b.clone())
                    .build(),
            )
            .build()
            .await?;

        r.ballot_voters().await?;

        r.context_lock().and_then(|mut context| {
            *context.server_mut().next_index_mut(&a)?.unwrap() = 42;
            *context.server_mut().next_index_mut(&b)?.unwrap() = 3;
            Ok(())
        })?;

        r.context_lock().and_then(|mut context| {
            super::adjust_next_index_for_node(
                &mut context,
                vec![
                    Outcome::builder()
                        .result(true)
                        .term(current_term + 1)
                        .from(a.clone())
                        .build(),
                    Outcome::builder()
                        .result(false)
                        .term(current_term + 1)
                        .from(b.clone())
                        .build(),
                ],
            )
        })?;

        assert_eq!(
            43,
            r.context_lock().and_then(|mut context| context
                .server_mut()
                .next_index_mut(&a)
                .and_then(|maybe| {
                    maybe
                        .ok_or(Error::UnknownNode(a.clone()))
                        .map(|next_index| *next_index)
                }))?
        );

        assert_eq!(
            2,
            r.context_lock().and_then(|mut context| context
                .server_mut()
                .next_index_mut(&b)
                .and_then(|maybe| {
                    maybe
                        .ok_or(Error::UnknownNode(b.clone()))
                        .map(|next_index| *next_index)
                }))?
        );

        r.context_lock().and_then(|mut context| {
            super::adjust_next_index_for_node(
                &mut context,
                vec![
                    Outcome::builder()
                        .result(true)
                        .term(current_term + 1)
                        .from(a.clone())
                        .build(),
                    Outcome::builder()
                        .result(false)
                        .term(current_term + 1)
                        .from(b.clone())
                        .build(),
                ],
            )
        })?;

        assert_eq!(
            44,
            r.context_lock().and_then(|mut context| context
                .server_mut()
                .next_index_mut(&a)
                .and_then(|maybe| {
                    maybe
                        .ok_or(Error::UnknownNode(a.clone()))
                        .map(|next_index| *next_index)
                }))?
        );

        assert_eq!(
            1,
            r.context_lock().and_then(|mut context| context
                .server_mut()
                .next_index_mut(&b)
                .and_then(|maybe| {
                    maybe
                        .ok_or(Error::UnknownNode(b.clone()))
                        .map(|next_index| *next_index)
                }))?
        );

        assert!(matches!(
            r.context_lock().and_then(|mut context| {
                super::adjust_next_index_for_node(
                    &mut context,
                    vec![
                        Outcome::builder()
                            .result(true)
                            .term(current_term + 1)
                            .from(a.clone())
                            .build(),
                        Outcome::builder()
                            .result(false)
                            .term(current_term + 1)
                            .from(b.clone())
                            .build(),
                    ],
                )
            }),
            Err(Error::BadNextIndex)
        ));

        Ok(())
    }
}
