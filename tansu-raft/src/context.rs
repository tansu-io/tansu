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
use std::{collections::BTreeSet, marker::PhantomData, task::Waker};

use tracing::debug;
use url::Url;

use crate::{
    blocking::PersistentState, candidate::Candidate, follower::Follower, leader::LeaderBuilder,
    KvStore, Outcome, Result, Server, Term,
};

#[derive(Debug)]
pub(crate) struct Situation {
    persistent_state: Box<dyn PersistentState>,
    server: Box<dyn Server>,
    kv_store: Box<dyn KvStore>,
    voters: BTreeSet<Url>,
    pending_leader_election: Vec<Waker>,
}

impl Situation {
    pub(crate) fn builder() -> SituationBuilder {
        SituationBuilder {
            ..Default::default()
        }
    }
}

#[derive(Default)]
pub(crate) struct SituationBuilder<
    R = PhantomData<Box<dyn PersistentState>>,
    S = PhantomData<Box<dyn Server>>,
    T = PhantomData<Box<dyn KvStore>>,
    U = PhantomData<BTreeSet<Url>>,
> {
    persistent_state: R,
    server: S,
    kv_store: T,
    voters: U,
}

impl SituationBuilder<Box<dyn PersistentState>, Box<dyn Server>, Box<dyn KvStore>, BTreeSet<Url>> {
    pub(crate) fn build(self) -> Result<Situation> {
        Ok(Situation {
            persistent_state: self.persistent_state,
            server: self.server,
            kv_store: self.kv_store,
            voters: self.voters,
            pending_leader_election: Vec::new(),
        })
    }
}

impl<R, S, T, U> SituationBuilder<R, S, T, U> {
    pub(crate) fn persistent_state(
        self,
        persistent_state: Box<dyn PersistentState>,
    ) -> SituationBuilder<Box<dyn PersistentState>, S, T, U> {
        SituationBuilder {
            persistent_state,
            server: self.server,
            kv_store: self.kv_store,
            voters: self.voters,
        }
    }

    pub(crate) fn server(
        self,
        server: Box<dyn Server>,
    ) -> SituationBuilder<R, Box<dyn Server>, T, U> {
        SituationBuilder {
            persistent_state: self.persistent_state,
            server,
            kv_store: self.kv_store,
            voters: self.voters,
        }
    }

    pub(crate) fn kv_store(
        self,
        kv_store: Box<dyn KvStore>,
    ) -> SituationBuilder<R, S, Box<dyn KvStore>, U> {
        SituationBuilder {
            persistent_state: self.persistent_state,
            server: self.server,
            kv_store,
            voters: self.voters,
        }
    }

    pub(crate) fn voters(self, voters: BTreeSet<Url>) -> SituationBuilder<R, S, T, BTreeSet<Url>> {
        SituationBuilder {
            persistent_state: self.persistent_state,
            server: self.server,
            kv_store: self.kv_store,
            voters,
        }
    }
}

pub trait Context: Debug + Send + Sync {
    fn persistent_state(&self) -> &dyn PersistentState;
    fn persistent_state_mut(&mut self) -> &mut dyn PersistentState;

    fn server(&self) -> &dyn Server;
    fn server_mut(&mut self) -> &mut dyn Server;

    fn kv_store(&self) -> &dyn KvStore;
    fn kv_store_mut(&mut self) -> &mut dyn KvStore;

    fn transition_to_follower(&mut self, current_term: Term) -> Result<()>;
    fn transition_to_candidate(&mut self) -> Result<()>;
    fn transition_to_leader(&mut self, voters: &[Url]) -> Result<()>;

    fn transition_to_follower_with_newer_term(&mut self, results: &[Outcome]) -> Result<bool>;

    fn voters(&self) -> BTreeSet<Url>;

    fn register_pending_leader_election(&mut self, waker: Waker);
    fn wake_pending_leader_election(&mut self);
}

impl Context for Situation {
    fn persistent_state(&self) -> &dyn PersistentState {
        &*self.persistent_state
    }

    fn persistent_state_mut(&mut self) -> &mut dyn PersistentState {
        &mut *self.persistent_state
    }

    fn server(&self) -> &dyn Server {
        &*self.server
    }

    fn kv_store(&self) -> &dyn KvStore {
        &*self.kv_store
    }

    fn kv_store_mut(&mut self) -> &mut dyn KvStore {
        &mut *self.kv_store
    }

    fn server_mut(&mut self) -> &mut dyn Server {
        &mut *self.server
    }

    fn transition_to_follower(&mut self, current_term: Term) -> Result<()> {
        self.server = Follower::try_from(&self.server)?.into();
        self.persistent_state.transition_to_follower(current_term)?;
        Ok(())
    }

    fn transition_to_candidate(&mut self) -> Result<()> {
        self.server = Candidate::try_from(&self.server)?.into();
        self.persistent_state.transition_to_candidate()?;
        Ok(())
    }

    fn transition_to_leader(&mut self, voters: &[Url]) -> Result<()> {
        self.server = LeaderBuilder::try_from(&self.server)?
            .with_voters(voters)
            .build()
            .into();
        self.persistent_state.transition_to_leader()?;
        self.wake_pending_leader_election();
        Ok(())
    }

    // If RPC request or response contains term
    // T > currentTerm: set currentTerm = T,
    // convert to follower (§5.1)
    fn transition_to_follower_with_newer_term(&mut self, results: &[Outcome]) -> Result<bool> {
        results
            .iter()
            .map(|o| o.term())
            .max()
            .map_or(Ok(false), |max_poll_term| {
                self.persistent_state_mut().read().and_then(|pe| {
                    if max_poll_term > pe.current_term() {
                        self.persistent_state_mut()
                            .write(pe.transition_to_follower(max_poll_term))
                            .map(|()| true)
                    } else {
                        Ok(false)
                    }
                })
            })
    }

    fn voters(&self) -> BTreeSet<Url> {
        self.voters.clone()
    }

    fn register_pending_leader_election(&mut self, waker: Waker) {
        self.pending_leader_election.push(waker)
    }

    fn wake_pending_leader_election(&mut self) {
        for waker in self.pending_leader_election.drain(..) {
            debug!(?waker);
            waker.wake()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, io::Cursor, ops::RangeFrom};

    use bytes::Bytes;

    use super::*;
    use crate::{blocking::persistent::PersistentManager, PersistentEntry};

    #[derive(Debug)]
    struct BTreeMapKv {
        db: BTreeMap<Bytes, Bytes>,
    }

    impl BTreeMapKv {
        fn new() -> Self {
            Self {
                db: BTreeMap::new(),
            }
        }
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

    #[test]
    fn transition_to_follower() -> Result<()> {
        let id = Url::parse("tcp://localhost:1234/")?;
        let commit_index = 0;
        let last_applied = 0;
        let current_term = 32123;
        let voted_for = Some(Url::parse("tcp://localhost:4321/")?);

        let mut pm = PersistentManager::builder()
            .inner(Cursor::new(vec![]))
            .id(id)
            .recover()
            .map(Box::new)?;

        pm.write(
            PersistentEntry::builder()
                .current_term(current_term)
                .voted_for(voted_for)
                .build(),
        )?;

        let mut situation = Situation::builder()
            .persistent_state(pm)
            .server(Box::new(
                Follower::builder()
                    .commit_index(commit_index)
                    .last_applied(last_applied)
                    .build(),
            ))
            .kv_store(Box::new(BTreeMapKv::new()))
            .voters(BTreeSet::new())
            .build()?;

        situation.transition_to_follower(98789)?;

        let post = situation.persistent_state_mut().read()?;
        assert_eq!(98789, post.current_term());
        assert_eq!(None, post.voted_for());

        Ok(())
    }

    #[test]
    fn transition_to_candidate() -> Result<()> {
        let id = Url::parse("tcp://localhost:1234/")?;
        let commit_index = 0;
        let last_applied = 0;
        let current_term = 32123;
        let voted_for = Some(Url::parse("tcp://localhost:4321/")?);

        let mut pm = PersistentManager::builder()
            .inner(Cursor::new(vec![]))
            .id(id.clone())
            .recover()
            .map(Box::new)?;

        pm.write(
            PersistentEntry::builder()
                .current_term(current_term)
                .voted_for(voted_for)
                .build(),
        )?;

        let mut situation = Situation::builder()
            .persistent_state(pm)
            .server(Box::new(
                Follower::builder()
                    .commit_index(commit_index)
                    .last_applied(last_applied)
                    .build(),
            ))
            .kv_store(Box::new(BTreeMapKv::new()))
            .voters(BTreeSet::new())
            .build()?;

        situation.transition_to_candidate()?;

        let post = situation.persistent_state_mut().read()?;
        assert_eq!(current_term + 1, post.current_term());
        assert_eq!(Some(id), post.voted_for());

        Ok(())
    }

    #[test]
    fn transition_to_leader() -> Result<()> {
        let id = Url::parse("tcp://localhost:1234/")?;
        let commit_index = 0;
        let last_applied = 0;
        let current_term = 32123;
        let voted_for = Some(Url::parse("tcp://localhost:4321/")?);

        let mut pm = PersistentManager::builder()
            .inner(Cursor::new(vec![]))
            .id(id)
            .recover()
            .map(Box::new)?;

        pm.write(
            PersistentEntry::builder()
                .current_term(current_term)
                .voted_for(voted_for.clone())
                .build(),
        )?;

        let mut situation = Situation::builder()
            .persistent_state(pm)
            .server(Box::new(
                Follower::builder()
                    .commit_index(commit_index)
                    .last_applied(last_applied)
                    .build(),
            ))
            .kv_store(Box::new(BTreeMapKv::new()))
            .voters(BTreeSet::new())
            .build()?;

        situation.transition_to_leader(&[])?;

        let post = situation.persistent_state_mut().read()?;
        assert_eq!(current_term, post.current_term());
        assert_eq!(voted_for, post.voted_for());

        Ok(())
    }
    #[test]
    fn transition_to_follower_with_newer_term() -> Result<()> {
        let id = Url::parse("tcp://localhost:1234/")?;
        let commit_index = 0;
        let last_applied = 0;
        let current_term = 32123;
        let voted_for = Some(Url::parse("tcp://localhost:4321/")?);

        let mut pm = PersistentManager::builder()
            .inner(Cursor::new(vec![]))
            .id(id.clone())
            .recover()
            .map(Box::new)?;

        pm.write(
            PersistentEntry::builder()
                .current_term(current_term)
                .voted_for(voted_for.clone())
                .build(),
        )?;

        let mut situation = Situation::builder()
            .persistent_state(pm)
            .server(Box::new(
                Follower::builder()
                    .commit_index(commit_index)
                    .last_applied(last_applied)
                    .build(),
            ))
            .kv_store(Box::new(BTreeMapKv::new()))
            .voters(BTreeSet::new())
            .build()?;

        _ = situation.transition_to_follower_with_newer_term(&[])?;

        let post = situation.persistent_state_mut().read()?;
        assert_eq!(current_term, post.current_term());
        assert_eq!(voted_for, post.voted_for());

        _ = situation.transition_to_follower_with_newer_term(&[
            Outcome::builder()
                .term(current_term - 1)
                .result(false)
                .from(id.clone())
                .build(),
            Outcome::builder()
                .term(current_term - 2)
                .result(false)
                .from(id.clone())
                .build(),
        ])?;

        let post = situation.persistent_state_mut().read()?;
        assert_eq!(current_term, post.current_term());
        assert_eq!(voted_for, post.voted_for());

        _ = situation.transition_to_follower_with_newer_term(&[
            Outcome::builder()
                .term(current_term - 1)
                .result(false)
                .from(id.clone())
                .build(),
            Outcome::builder()
                .term(current_term + 5)
                .result(false)
                .from(id)
                .build(),
        ])?;

        let post = situation.persistent_state_mut().read()?;
        assert_eq!(current_term + 5, post.current_term());
        assert_eq!(None, post.voted_for());

        Ok(())
    }
}
