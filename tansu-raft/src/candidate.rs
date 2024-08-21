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

use serde::{Deserialize, Serialize};
use url::Url;

use crate::{server::State, Error, Index, Outcome, PersistentEntry, Result, Server, Term};

#[derive(
    Copy, Clone, Debug, Deserialize, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct Candidate {
    commit_index: Index,
    last_applied: Index,
}

impl From<Candidate> for Box<dyn Server> {
    fn from(value: Candidate) -> Self {
        Box::new(value) as Box<dyn Server>
    }
}

impl TryFrom<&Box<dyn Server>> for Candidate {
    type Error = Error;

    fn try_from(value: &Box<dyn Server>) -> Result<Self, Self::Error> {
        Ok(Self {
            commit_index: value.commit_index()?,
            last_applied: value.last_applied()?,
        })
    }
}

impl Server for Candidate {
    fn request_vote(
        &self,
        entry: &PersistentEntry,
        id: Url,
        candidates_term: Term,
        candidate: Url,
        _last_log_index: Index,
    ) -> Result<Outcome> {
        Ok(Outcome::builder()
            .term(candidates_term)
            .result(
                entry
                    .voted_for()
                    .expect("candidate that hasn't voted for self!")
                    == candidate,
            )
            .from(id)
            .build())
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
        Ok(State::Candidate)
    }

    fn leader(&self) -> Result<Option<Url>> {
        Ok(None)
    }
}
