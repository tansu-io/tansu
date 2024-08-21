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

use async_trait::async_trait;
use url::Url;

use crate::{Index, Outcome, PersistentEntry, Raft, Result, Term};

#[async_trait]
pub trait RequestVote {
    async fn request_vote(
        &mut self,
        candidates_term: Term,
        candidate_id: Url,
        last_log_index: Index,
    ) -> Result<Outcome>;
}

#[async_trait]
pub trait ProvideRequestVote {
    async fn provide_request_vote(&self) -> Result<Box<dyn RequestVote>>;
}

#[async_trait]
impl RequestVote for Raft {
    async fn request_vote(
        &mut self,
        candidates_term: Term,
        candidate_id: Url,
        last_log_index: Index,
    ) -> Result<Outcome> {
        self.clock.update()?;

        self.context_lock().and_then(|mut context| {
            context.persistent_state_mut().read().and_then(|entry| {
                match entry.current_term() {
                    current_term if candidates_term > current_term => {
                        // transition to follower
                        context.transition_to_follower(candidates_term)?;

                        context
                            .server()
                            .request_vote(
                                &PersistentEntry::builder()
                                    .current_term(candidates_term)
                                    .voted_for(None)
                                    .build(),
                                context.persistent_state().id(),
                                candidates_term,
                                candidate_id.clone(),
                                last_log_index,
                            )
                            .and_then(|outcome @ Outcome { term, result, .. }| {
                                context
                                    .persistent_state_mut()
                                    .write(
                                        PersistentEntry::builder()
                                            .current_term(term)
                                            .voted_for(if result {
                                                Some(candidate_id)
                                            } else {
                                                None
                                            })
                                            .build(),
                                    )
                                    .map(|()| outcome)
                            })
                    }

                    current_term if candidates_term < current_term => Ok(Outcome {
                        from: context.persistent_state().id(),
                        term: current_term,
                        result: false,
                    }),

                    _ => context
                        .server()
                        .request_vote(
                            &entry,
                            context.persistent_state().id(),
                            candidates_term,
                            candidate_id.clone(),
                            last_log_index,
                        )
                        .and_then(|outcome @ Outcome { term, result, .. }| {
                            context
                                .persistent_state_mut()
                                .write(
                                    PersistentEntry::builder()
                                        .current_term(term)
                                        .voted_for(if result { Some(candidate_id) } else { None })
                                        .build(),
                                )
                                .map(|()| outcome)
                        }),
                }
            })
        })
    }
}
