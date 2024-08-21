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

use std::{
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
};

use serde::{Deserialize, Serialize};
use url::Url;

use crate::{server::State, Error, Index, Outcome, PersistentEntry, Result, Server, Term};

#[derive(Clone, Default, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Leader {
    commit_index: Index,
    last_applied: Index,
    next_index: BTreeMap<Url, Index>,
    match_index: BTreeMap<Url, Index>,
}

#[derive(Clone, Default, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct LeaderBuilder<T = PhantomData<BTreeMap<Url, Index>>> {
    commit_index: Index,
    last_applied: Index,
    next_index: T,
    match_index: T,
}

impl LeaderBuilder<BTreeMap<Url, Index>> {
    pub fn build(self) -> Leader {
        Leader {
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            next_index: self.next_index,
            match_index: self.match_index,
        }
    }
}

impl<T> LeaderBuilder<T> {
    pub fn with_voters(self, voters: &[Url]) -> LeaderBuilder<BTreeMap<Url, Index>> {
        LeaderBuilder {
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            next_index: voters
                .iter()
                .cloned()
                .map(|url| (url, self.commit_index + 1))
                .collect(),
            match_index: voters.iter().cloned().map(|url| (url, 0)).collect(),
        }
    }
}

impl From<Leader> for Box<dyn Server> {
    fn from(value: Leader) -> Self {
        Box::new(value) as Box<dyn Server>
    }
}

impl TryFrom<&Box<dyn Server>> for LeaderBuilder {
    type Error = Error;

    fn try_from(value: &Box<dyn Server>) -> Result<Self, Self::Error> {
        Ok(Self {
            commit_index: value.commit_index()?,
            last_applied: value.last_applied()?,
            ..Default::default()
        })
    }
}

impl Server for Leader {
    fn request_vote(
        &self,
        entry: &PersistentEntry,
        id: Url,
        candidates_term: Term,
        candidate_id: Url,
        last_log_index: Index,
    ) -> Result<Outcome> {
        entry.voted_for().map_or_else(
            || {
                // candidate’s log is at least as up-to-date as receiver’s log, grant vote
                // (§5.2, §5.4)
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

    fn next_index(&self, node: &Url) -> Result<Option<Index>> {
        Ok(self.next_index.get(node).copied())
    }

    fn next_index_mut(&mut self, node: &Url) -> Result<Option<&mut Index>> {
        Ok(self.next_index.get_mut(node))
    }

    fn match_index(&self, node: &Url) -> Result<Option<Index>> {
        Ok(self.match_index.get(node).copied())
    }

    fn match_index_mut(&mut self, node: &Url) -> Result<Option<&mut Index>> {
        Ok(self.match_index.get_mut(node))
    }

    fn state(&self) -> Result<State> {
        Ok(State::Leader)
    }

    fn replicate_log_entries(&self, nodes: BTreeSet<Url>) -> Result<BTreeMap<Url, Index>> {
        Ok(nodes
            .iter()
            .cloned()
            .fold(BTreeMap::new(), |mut acc, node| {
                if let Some(index) = self.next_index.get(&node) {
                    acc.insert(node, *index);
                    acc
                } else {
                    acc
                }
            }))
    }

    fn leader(&self) -> Result<Option<Url>> {
        Ok(Some(Url::parse("tag:tansu.io,2024:self")?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replicate_log_entries_with_empty_nodes() -> Result<()> {
        assert_eq!(
            BTreeMap::new(),
            Leader::default().replicate_log_entries(BTreeSet::new())?
        );

        Ok(())
    }

    #[test]
    fn replicate_log_entries_with_unknown_node() -> Result<()> {
        assert_eq!(
            BTreeMap::new(),
            Leader::default()
                .replicate_log_entries(BTreeSet::from([Url::parse("tcp://localhost:1234")?]))?
        );

        Ok(())
    }

    #[test]
    fn replicate_log_entries() -> Result<()> {
        let ni = [
            (Url::parse("tag:tansu.io,2024:32123")?, 123),
            (Url::parse("tag:tansu.io,2024:98789")?, 789),
        ];
        let mut leader = Leader::default();

        ni.iter().cloned().for_each(|(node, index)| {
            leader.next_index.insert(node, index);
        });

        assert_eq!(
            ni.iter().cloned().collect::<BTreeMap<Url, Index>>(),
            leader.replicate_log_entries(ni.iter().map(|(node, _)| node).cloned().collect())?
        );

        Ok(())
    }
}
