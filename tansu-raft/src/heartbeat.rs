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

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use url::Url;

use crate::{log_key_for_index, server::State, Index, LogEntry, Raft, Result, Term};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
struct Detail {
    leader_id: Url,
    current_term: Term,
    commit_index: Index,
    prev_log_index: Index,
    prev_log_term: Term,
}

impl Raft {
    fn heartbeat_detail(&self) -> Result<BTreeMap<Url, Detail>> {
        let mut context = self.context_lock()?;

        let leader_id = context.persistent_state().id();
        let current_term = context.persistent_state_mut().read()?.current_term();
        let commit_index = context.server().commit_index()?;

        let mut details = BTreeMap::new();

        if context
            .server()
            .state()
            .is_ok_and(|state| state == State::Leader)
        {
            self.clock.update()?;

            for voter in context.voters().iter().cloned() {
                let next_index = context
                    .server()
                    .next_index(&voter)
                    .map(|option| option.unwrap_or(1))?;

                let prev_log_index = next_index - 1;

                info!("voter={} prev_log_index={}", voter, prev_log_index);

                if prev_log_index == 0 {
                    _ = details.insert(
                        voter,
                        Detail {
                            leader_id: leader_id.clone(),
                            current_term,
                            commit_index,
                            prev_log_index: 0,
                            prev_log_term: 0,
                        },
                    );
                } else {
                    let Some(log_entry) = context
                        .kv_store()
                        .get(log_key_for_index(prev_log_index)?)
                        .and_then(|option| {
                            option.map_or_else(
                                || Ok(None),
                                |bytes| LogEntry::try_from(bytes).map(Some),
                            )
                        })?
                    else {
                        continue;
                    };

                    info!(
                        "voter={} prev_log_index={} log_entry={:?}",
                        voter, prev_log_index, log_entry
                    );

                    _ = details.insert(
                        voter,
                        Detail {
                            leader_id: leader_id.clone(),
                            current_term,
                            commit_index,
                            prev_log_index,
                            prev_log_term: log_entry.term(),
                        },
                    );
                }
            }

            info!(
                "leader={}, current_term={}, commit_index={}",
                leader_id, current_term, commit_index
            );
        }

        Ok(details)
    }

    pub async fn heartbeat(&self) -> Result<()> {
        let mut outcomes = Vec::new();
        for (peer, detail) in self.heartbeat_detail()? {
            info!("sending heartbeat to peer: {}", peer);

            let Ok(outcome) = self
                .raft_rpc
                .append_entries(
                    peer.clone(),
                    detail.current_term,
                    detail.leader_id,
                    detail.prev_log_index,
                    detail.prev_log_term,
                    vec![],
                    detail.commit_index,
                )
                .await
            else {
                continue;
            };

            outcomes.push(outcome);
            self.clock.update()?;
        }

        _ = self
            .context_lock()
            .and_then(|mut context| context.transition_to_follower_with_newer_term(&outcomes))?;

        self.context_lock().and_then(|mut context| {
            if context
                .server()
                .state()
                .is_ok_and(|state| state == State::Leader)
            {
                debug!("as leader about to apply log entries");
                self.apply_log_entries(&mut context)
            } else {
                Ok(())
            }
        })
    }
}
