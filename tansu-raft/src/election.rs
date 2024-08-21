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

use std::collections::BTreeSet;

use async_trait::async_trait;
use tracing::debug;
use url::Url;

use crate::{log_key_for_index, Error, Index, LogEntry, Outcome, Raft, Result, Term};

#[async_trait]
pub trait Election {
    async fn ballot_voters(&mut self) -> Result<()>;
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct Detail {
    candidate_id: Url,
    election_term: Term,
    last_log_index: Index,
    last_log_term: Term,
    voters: BTreeSet<Url>,
    votes: Vec<Outcome>,
}

impl Raft {
    fn call_election(&self) -> Result<Detail> {
        let mut context = self.context.lock()?;

        context.transition_to_candidate()?;

        let candidate_id = context.persistent_state().id();
        let election_term = context.persistent_state_mut().read()?.current_term();
        let last_log_index = context.server().commit_index()?;

        let last_log_term = if last_log_index == 0 {
            0
        } else {
            context
                .kv_store()
                .get(log_key_for_index(last_log_index)?)
                .and_then(|option| option.ok_or(Error::EmptyLogEntry(last_log_index)))
                .and_then(LogEntry::try_from)
                .map(|log_entry| log_entry.term())?
        };

        Ok(Detail {
            candidate_id: candidate_id.clone(),
            election_term,
            last_log_index,
            last_log_term,
            voters: context.voters(),

            // we vote for ourselves
            votes: vec![Outcome::builder()
                .term(election_term)
                .result(true)
                .from(candidate_id)
                .build()],
        })
    }

    fn election_result(&mut self, election_detail: Detail) -> Result<bool> {
        let mut context = self.context.lock()?;

        if context.transition_to_follower_with_newer_term(&election_detail.votes)? {
            // If RPC request or response contains term
            // T > currentTerm: set currentTerm = T,
            //  convert to follower (§5.1)
            Ok(false)
        } else {
            let current_term = election_detail.election_term;
            let majority = (election_detail.votes.len() / 2) + 1;
            let ayes = election_detail
                .votes
                .iter()
                .filter(|o| o.term() == current_term && o.result())
                .count();

            debug!(name: "election", ?majority, ?ayes, ?current_term);

            let id = context.persistent_state().id();

            if ayes >= majority {
                debug!("majority have voted for {}, in term: {}", id, current_term);

                context.transition_to_leader(
                    &election_detail
                        .votes
                        .iter()
                        .filter(|outcome| outcome.from() != id)
                        .map(|outcome| outcome.from())
                        .collect::<Vec<Url>>(),
                )?;

                Ok(true)
            } else {
                debug!("no majority for {}, in term: {}", id, current_term);
                context.transition_to_follower(current_term)?;
                Ok(false)
            }
        }
    }
}

#[async_trait]
impl Election for Raft {
    async fn ballot_voters(&mut self) -> Result<()> {
        let mut election = self.call_election()?;

        for voter in election.voters.iter().cloned() {
            let Ok(vote) = self
                .raft_rpc
                .request_vote(
                    voter,
                    election.election_term,
                    election.candidate_id.clone(),
                    election.last_log_index,
                )
                .await
            else {
                continue;
            };

            self.clock.update()?;
            election.votes.push(vote);
        }

        if self.election_result(election.clone())? {
            // Leaders:
            // Upon election: send initial empty AppendEntries RPCs (heartbeat) to each
            // server; repeat during idle periods to prevent election timeouts
            // (§5.2)
            self.heartbeat().await?;
        }

        Ok(())
    }
}
