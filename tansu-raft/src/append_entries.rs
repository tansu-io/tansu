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

use core::fmt::Debug;
use std::sync::MutexGuard;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::debug;
use url::Url;

use crate::{
    context::Context, log_key_for_index, state_key, Error, Index, LogEntry, Outcome, Raft, Result,
    Term,
};

#[async_trait]
pub trait AppendEntries: Debug {
    async fn append_entries(
        &mut self,
        leaders_term: Term,
        leader: Url,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: Index,
    ) -> Result<Outcome>;
}

#[async_trait]
impl AppendEntries for Raft {
    async fn append_entries(
        &mut self,
        leaders_term: Term,
        leader: Url,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: Index,
    ) -> Result<Outcome> {
        self.clock.update()?;

        self.context_lock().and_then(|mut context| {
            context.persistent_state_mut().read().and_then(|entry| {
                match entry.current_term() {
                    current_term if leaders_term >= current_term => {
                        if leaders_term > current_term {
                            // If RPC request or response contains term T > currentTerm: set
                            // currentTerm = T, convert to follower
                            // (§5.1)
                            context.transition_to_follower(leaders_term)?;
                        }

                        *context.server_mut().leader_mut()? = Some(leader);
                        context.wake_pending_leader_election();

                        if prev_log_index == 0 && prev_log_term == 0 {
                            let mut index = prev_log_index;

                            for entry in entries {
                                index += 1;

                                log_key_for_index(index).and_then(|log_key| {
                                    if let Some(encoded) =
                                        context.kv_store().get(log_key.clone())?
                                    {
                                        let existing = LogEntry::try_from(encoded)?;

                                        if existing.term() != entry.term() {
                                            // If an existing entry conflicts with a new one (same
                                            // index but
                                            // different terms), delete the existing entry and all
                                            // that
                                            // follow it (§5.3)

                                            context
                                                .kv_store_mut()
                                                .remove_range_from(log_key.clone()..)?;
                                            context
                                                .kv_store_mut()
                                                .put(log_key.clone(), Bytes::from(entry))
                                        } else {
                                            Ok(())
                                        }
                                    } else {
                                        // Append any new entries not already in the log

                                        context
                                            .kv_store_mut()
                                            .put(log_key.clone(), Bytes::from(entry))
                                    }
                                })?;
                            }

                            if context
                                .server()
                                .commit_index()
                                .is_ok_and(|commit_index| leader_commit > commit_index)
                            {
                                context
                                    .server_mut()
                                    .commit_index_mut()
                                    .map(|commit_index| {
                                        *commit_index = std::cmp::min(index, leader_commit)
                                    })?;
                            }

                            self.apply_log_entries(&mut context)?;

                            Ok(Outcome {
                                from: context.persistent_state().id(),
                                term: leaders_term,
                                result: true,
                            })
                        } else if let Some(encoded) =
                            context.kv_store().get(log_key_for_index(prev_log_index)?)?
                        {
                            let log_entry = LogEntry::try_from(encoded)?;

                            if log_entry.term() == prev_log_term {
                                let mut index = prev_log_index;

                                for entry in entries {
                                    index += 1;
                                    let log_key = log_key_for_index(index)?;

                                    if let Some(encoded) =
                                        context.kv_store().get(log_key.clone())?
                                    {
                                        if LogEntry::try_from(encoded)?.term() != entry.term() {
                                            // If an existing entry conflicts with a new one (same
                                            // index but
                                            // different terms), delete the existing entry and all
                                            // that
                                            // follow it (§5.3)

                                            context
                                                .kv_store_mut()
                                                .remove_range_from(log_key.clone()..)?;
                                            context
                                                .kv_store_mut()
                                                .put(log_key.clone(), Bytes::from(entry))?;
                                        }
                                    } else {
                                        // Append any new entries not already in the log

                                        context
                                            .kv_store_mut()
                                            .put(log_key.clone(), Bytes::from(entry))?;
                                    }
                                }

                                // If leaderCommit > commitIndex, set commitIndex =
                                // min(leaderCommit, index of last new entry)
                                //

                                if context
                                    .server()
                                    .commit_index()
                                    .is_ok_and(|commit_index| leader_commit > commit_index)
                                {
                                    context.server_mut().commit_index_mut().map(
                                        |commit_index| {
                                            *commit_index = std::cmp::min(index, leader_commit)
                                        },
                                    )?;
                                }

                                self.apply_log_entries(&mut context)?;

                                Ok(Outcome {
                                    from: context.persistent_state().id(),
                                    term: leaders_term,
                                    result: true,
                                })
                            } else {
                                // Reply false if log doesn’t contain an entry at prevLogIndex
                                // whose term matches prevLogTerm (§5.3)
                                Ok(Outcome {
                                    from: context.persistent_state().id(),
                                    term: leaders_term,
                                    result: false,
                                })
                            }
                        } else {
                            // Reply false if log doesn’t contain an entry at prevLogIndex
                            Ok(Outcome {
                                from: context.persistent_state().id(),
                                term: leaders_term,
                                result: false,
                            })
                        }
                    }

                    // Reply false if term < currentTerm (§5.1)
                    current_term => Ok(Outcome {
                        from: context.persistent_state().id(),
                        term: current_term,
                        result: false,
                    }),
                }
            })
        })
    }
}

impl Raft {
    pub fn apply_log_entries(&self, context: &mut MutexGuard<'_, Box<dyn Context>>) -> Result<()> {
        while context.server().commit_index()? > context.server().last_applied()? {
            context
                .server_mut()
                .last_applied_mut()
                .map(|last_applied| *last_applied += 1)?;

            let initial = context.kv_store().get(state_key())?;

            debug!(
                "applying log entries, initial state: {:?}, last applied: {}",
                initial,
                context.server().last_applied()?
            );

            context
                .kv_store()
                .get(
                    context
                        .server()
                        .last_applied()
                        .and_then(log_key_for_index)?,
                )
                .and_then(|entry| {
                    entry.ok_or(context.server().last_applied().map(Error::EmptyLogEntry)?)
                })
                .and_then(LogEntry::try_from)
                .inspect(|entry| debug!(?entry))
                .and_then(|entry| {
                    self.apply_state
                        .apply(context.server().last_applied()?, initial, entry.value())
                })
                .inspect(|updated| debug!(?updated))
                .and_then(|updated| context.kv_store_mut().put(state_key(), updated))?;
        }

        Ok(())
    }
}
