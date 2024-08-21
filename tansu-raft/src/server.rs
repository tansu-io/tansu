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
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use url::Url;

use crate::{Error, Index, Outcome, PersistentEntry, Result, Term};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum State {
    Follower,
    Candidate,
    Leader,
}

pub trait Server: Debug + Send + Sync {
    fn request_vote(
        &self,
        entry: &PersistentEntry,
        id: Url,
        candidates_term: Term,
        candidate_id: Url,
        last_log_index: Index,
    ) -> Result<Outcome>;

    fn commit_index(&self) -> Result<Index>;
    fn last_applied(&self) -> Result<Index>;

    fn commit_index_mut(&mut self) -> Result<&mut Index>;
    fn last_applied_mut(&mut self) -> Result<&mut Index>;

    #[allow(unused_variables)]
    fn next_index(&self, node: &Url) -> Result<Option<Index>> {
        Err(Error::NotLeader)
    }

    #[allow(unused_variables)]
    fn next_index_mut(&mut self, node: &Url) -> Result<Option<&mut Index>> {
        Err(Error::NotLeader)
    }

    #[allow(unused_variables)]
    fn match_index(&self, node: &Url) -> Result<Option<Index>> {
        Err(Error::NotLeader)
    }

    #[allow(unused_variables)]
    fn match_index_mut(&mut self, node: &Url) -> Result<Option<&mut Index>> {
        Err(Error::NotLeader)
    }

    #[allow(unused_variables)]
    fn replicate_log_entries(&self, nodes: BTreeSet<Url>) -> Result<BTreeMap<Url, Index>> {
        Err(Error::NotLeader)
    }

    fn state(&self) -> Result<State>;

    fn leader(&self) -> Result<Option<Url>>;

    fn leader_mut(&mut self) -> Result<&mut Option<Url>> {
        Err(Error::NotFollower)
    }
}

pub trait ProvideServer {
    fn provide_server(&mut self) -> Result<Box<dyn Server>>;
}
