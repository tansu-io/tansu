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

use std::marker::PhantomData;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::{server::State, Error, Index, Outcome, PersistentEntry, Result, Server, Term};

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Follower {
    commit_index: Index,
    last_applied: Index,
    leader: Option<Url>,
}

impl From<Follower> for Box<dyn Server> {
    fn from(value: Follower) -> Self {
        Box::new(value) as Box<dyn Server>
    }
}

impl TryFrom<&Box<dyn Server>> for Follower {
    type Error = Error;

    fn try_from(value: &Box<dyn Server>) -> Result<Self, Self::Error> {
        Ok(Self {
            commit_index: value.commit_index()?,
            last_applied: value.last_applied()?,
            leader: None,
        })
    }
}

impl Follower {
    pub fn builder() -> FollowerBuilder {
        FollowerBuilder::default()
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct FollowerBuilder<T = PhantomData<Index>, U = PhantomData<Index>> {
    commit_index: T,
    last_applied: U,
}

impl FollowerBuilder<Index, Index> {
    pub fn build(self) -> Follower {
        Follower {
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            ..Default::default()
        }
    }
}

impl<T, U> FollowerBuilder<T, U> {
    pub fn commit_index(self, commit_index: Index) -> FollowerBuilder<Index, U> {
        FollowerBuilder {
            commit_index,
            last_applied: self.last_applied,
        }
    }

    pub fn last_applied(self, last_applied: Index) -> FollowerBuilder<T, Index> {
        FollowerBuilder {
            commit_index: self.commit_index,
            last_applied,
        }
    }
}

impl Server for Follower {
    fn request_vote(
        &self,
        entry: &PersistentEntry,
        id: Url,
        candidates_term: Term,
        candidate_id: Url,
        last_log_index: Index,
    ) -> Result<Outcome> {
        // If votedFor is null or candidateId, and candidate’s log is at
        // least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
        entry.voted_for().map_or_else(
            || {
                Ok(Outcome::builder()
                    .term(candidates_term)
                    .result(last_log_index >= self.commit_index)
                    .from(id.clone())
                    .build())
            },
            |voted_for_id| {
                Ok(Outcome::builder()
                    .term(candidates_term)
                    .result(voted_for_id == candidate_id)
                    .from(id.clone())
                    .build())
            },
        )
    }

    fn commit_index(&self) -> Result<Index> {
        Ok(self.commit_index)
    }

    fn last_applied(&self) -> Result<Index> {
        Ok(self.last_applied)
    }

    fn commit_index_mut(&mut self) -> Result<&mut Index> {
        Ok(&mut self.commit_index)
    }

    fn last_applied_mut(&mut self) -> Result<&mut Index> {
        Ok(&mut self.last_applied)
    }

    fn state(&self) -> Result<State> {
        Ok(State::Follower)
    }

    fn leader(&self) -> Result<Option<Url>> {
        Ok(self.leader.clone())
    }

    fn leader_mut(&mut self) -> Result<&mut Option<Url>> {
        Ok(&mut self.leader)
    }
}
