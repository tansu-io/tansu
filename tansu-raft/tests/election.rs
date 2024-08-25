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

mod common;

use std::{collections::BTreeMap, io::Cursor, sync::Arc};

use bytes::{BufMut, Bytes, BytesMut};
use common::RaftBuilder;
use tansu_raft::{
    blocking::{persistent::PersistentManager, PersistentState},
    election::Election,
    request_vote::RequestVote,
    server::State,
    ApplyState, Index, Outcome, PersistentEntry, ProvideApplyState, Result,
};
use url::Url;

use crate::common::AppendEntryDetail;

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Applicator;

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

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct ApplyStateFactory;

impl ProvideApplyState for ApplyStateFactory {
    fn provide_apply_state(&self) -> Result<Box<dyn ApplyState>> {
        Ok(Box::new(Applicator))
    }
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn vote_for_candidate_with_newer_term() -> Result<()> {
    let id = Url::parse("tcp://localhost:1234/")?;

    let mut r = RaftBuilder::new()
        .id(id.clone())
        .initial_persistent_state(vec![])
        .prev_log_index(0)
        .log_entries(vec![])
        .with_apply_state(Box::new(ApplyStateFactory))
        .build()
        .await?;

    let candidates_term = 1;
    let candidate_id = Url::parse("tcp://localhost:6/")?;
    let last_log_index = 54321;

    let outcome = r
        .request_vote(candidates_term, candidate_id, last_log_index)
        .await?;

    assert_eq!(id, outcome.from());
    assert_eq!(1, outcome.term());
    assert!(outcome.result());

    Ok(())
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn vote_against_candidate_with_older_term() -> Result<()> {
    let id = Url::parse("tcp://localhost:1234/")?;
    let current_term = 456;
    let voted_for = None;

    let mut pm = PersistentManager::builder()
        .inner(Cursor::new(vec![]))
        .id(id.clone())
        .recover()?;

    pm.write(
        PersistentEntry::builder()
            .current_term(current_term)
            .voted_for(voted_for)
            .build(),
    )?;

    let mut r = RaftBuilder::new()
        .id(id.clone())
        .initial_persistent_state(pm.into_inner().into_inner())
        .prev_log_index(0)
        .log_entries(vec![])
        .with_apply_state(Box::new(ApplyStateFactory))
        .build()
        .await?;

    let candidates_term = 1;
    let candidate_id = Url::parse("tcp://localhost:6/")?;
    let last_log_index = 54321;

    let outcome = r
        .request_vote(candidates_term, candidate_id, last_log_index)
        .await?;

    assert_eq!(id, outcome.from());
    assert_eq!(current_term, outcome.term());
    assert!(!outcome.result());

    Ok(())
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn vote_for_first_candidate_with_current_term() -> Result<()> {
    let id = Url::parse("tcp://localhost:1234/")?;
    let current_term = 456;
    let voted_for = None;

    let mut pm = PersistentManager::builder()
        .inner(Cursor::new(vec![]))
        .id(id.clone())
        .recover()?;

    pm.write(
        PersistentEntry::builder()
            .current_term(current_term)
            .voted_for(voted_for)
            .build(),
    )?;

    let mut r = RaftBuilder::new()
        .id(id.clone())
        .initial_persistent_state(pm.into_inner().into_inner())
        .prev_log_index(0)
        .log_entries(vec![])
        .with_apply_state(Box::new(ApplyStateFactory))
        .build()
        .await?;

    let candidates_term = current_term;
    let candidate_id = Url::parse("tcp://localhost:6/")?;
    let last_log_index = 54321;

    let outcome = r
        .request_vote(candidates_term, candidate_id, last_log_index)
        .await?;

    assert_eq!(id, outcome.from());
    assert_eq!(current_term, outcome.term());
    assert!(outcome.result());

    Ok(())
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn deny_vote_having_already_voted_for_different_candidate() -> Result<()> {
    let id = Url::parse("tcp://localhost:1234/")?;
    let current_term = 456;
    let voted_for = Some(id.clone());

    let candidates_term = current_term;
    let candidate_id = Url::parse("tcp://localhost:6/")?;
    let last_log_index = 54321;

    let mut pm = PersistentManager::builder()
        .inner(Cursor::new(vec![]))
        .id(id.clone())
        .recover()?;

    pm.write(
        PersistentEntry::builder()
            .current_term(current_term)
            .voted_for(voted_for)
            .build(),
    )?;

    let mut r = RaftBuilder::new()
        .id(id.clone())
        .initial_persistent_state(pm.into_inner().into_inner())
        .prev_log_index(0)
        .log_entries(vec![])
        .with_apply_state(Box::new(ApplyStateFactory))
        .build()
        .await?;

    let outcome = r
        .request_vote(candidates_term, candidate_id, last_log_index)
        .await?;

    assert_eq!(id, outcome.from());
    assert_eq!(current_term, outcome.term());
    assert!(!outcome.result());

    Ok(())
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn confirm_vote_having_already_voted_for_same_candidate() -> Result<()> {
    let id = Url::parse("tcp://localhost:1234/")?;
    let current_term = 456;
    let candidates_term = current_term;
    let candidate_id = Url::parse("tcp://localhost:6/")?;
    let last_log_index = 54321;

    let voted_for = Some(candidate_id.clone());

    let mut pm = PersistentManager::builder()
        .inner(Cursor::new(vec![]))
        .id(id.clone())
        .recover()?;

    pm.write(
        PersistentEntry::builder()
            .current_term(current_term)
            .voted_for(voted_for)
            .build(),
    )?;

    let mut r = RaftBuilder::new()
        .id(id.clone())
        .initial_persistent_state(pm.into_inner().into_inner())
        .prev_log_index(0)
        .log_entries(vec![])
        .with_apply_state(Box::new(ApplyStateFactory))
        .build()
        .await?;

    let outcome = r
        .request_vote(candidates_term, candidate_id, last_log_index)
        .await?;

    assert_eq!(id, outcome.from());
    assert_eq!(current_term, outcome.term());
    assert!(outcome.result());

    Ok(())
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn call_election_only_voter() -> Result<()> {
    let id = Url::parse("tcp://localhost:1234/")?;
    let current_term = 456;
    let candidate_id = Url::parse("tcp://localhost:6/")?;
    let prev_log_index = 0;
    let voted_for = Some(candidate_id);

    let mut pm = PersistentManager::builder()
        .inner(Cursor::new(vec![]))
        .id(id.clone())
        .recover()?;

    pm.write(
        PersistentEntry::builder()
            .current_term(current_term)
            .voted_for(voted_for)
            .build(),
    )?;

    let mut r = RaftBuilder::new()
        .id(id.clone())
        .initial_persistent_state(pm.into_inner().into_inner())
        .prev_log_index(prev_log_index)
        .log_entries(vec![])
        .with_apply_state(Box::new(ApplyStateFactory))
        .build()
        .await?;

    r.ballot_voters().await?;

    let mut context = r.context_lock()?;
    let pe = context.persistent_state_mut().read()?;

    assert_eq!(current_term + 1, pe.current_term());
    assert_eq!(Some(id), pe.voted_for());
    assert_eq!(State::Leader, context.server().state()?);

    Ok(())
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn call_election_split_vote() -> Result<()> {
    let id = Url::parse("tcp://localhost:1234/")?;
    let current_term = 456;
    let candidate_id = Url::parse("tcp://localhost:6/")?;
    let prev_log_index = 0;
    let voted_for = Some(candidate_id);

    let mut pm = PersistentManager::builder()
        .inner(Cursor::new(vec![]))
        .id(id.clone())
        .recover()?;

    pm.write(
        PersistentEntry::builder()
            .current_term(current_term)
            .voted_for(voted_for)
            .build(),
    )?;

    let a = Url::parse("tcp://localhost/32123")?;

    let mut r = RaftBuilder::new()
        .id(id.clone())
        .voters([a.clone()].into())
        .initial_persistent_state(pm.into_inner().into_inner())
        .prev_log_index(prev_log_index)
        .log_entries(vec![])
        .request_vote_outcome(
            a.clone(),
            Outcome::builder()
                .result(false)
                .term(current_term + 1)
                .from(a.clone())
                .build(),
        )
        .with_apply_state(Box::new(ApplyStateFactory))
        .build()
        .await?;

    r.ballot_voters().await?;

    let mut context = r.context_lock()?;
    let pe = context.persistent_state_mut().read()?;

    assert_eq!(current_term + 1, pe.current_term());
    assert_eq!(None, pe.voted_for());
    assert_eq!(State::Follower, context.server().state()?);

    Ok(())
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn call_election_follower_by_simple_majority() -> Result<()> {
    let id = Url::parse("tcp://localhost:1234/")?;
    let current_term = 456;
    let candidate_id = Url::parse("tcp://localhost:6/")?;
    let prev_log_index = 0;
    let voted_for = Some(candidate_id);

    let mut pm = PersistentManager::builder()
        .inner(Cursor::new(vec![]))
        .id(id.clone())
        .recover()?;

    pm.write(
        PersistentEntry::builder()
            .current_term(current_term)
            .voted_for(voted_for)
            .build(),
    )?;

    let a = Url::parse("tcp://localhost/32123")?;
    let b = Url::parse("tcp://localhost/78987")?;

    let mut r = RaftBuilder::new()
        .id(id.clone())
        .voters([a.clone(), b.clone()].into())
        .initial_persistent_state(pm.into_inner().into_inner())
        .prev_log_index(prev_log_index)
        .log_entries(vec![])
        .request_vote_outcome(
            a.clone(),
            Outcome::builder()
                .result(false)
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
        .with_apply_state(Box::new(ApplyStateFactory))
        .build()
        .await?;

    r.ballot_voters().await?;

    let mut context = r.context_lock()?;
    let pe = context.persistent_state_mut().read()?;

    assert_eq!(current_term + 1, pe.current_term());
    assert_eq!(None, pe.voted_for());
    assert_eq!(State::Follower, context.server().state()?);

    Ok(())
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn call_election_leader_by_simple_majority() -> Result<()> {
    let id = Url::parse("tcp://localhost:1234/")?;
    let current_term = 456;
    let candidate_id = Url::parse("tcp://localhost:6/")?;
    let prev_log_index = 0;
    let voted_for = Some(candidate_id);

    let mut pm = PersistentManager::builder()
        .inner(Cursor::new(vec![]))
        .id(id.clone())
        .recover()?;

    pm.write(
        PersistentEntry::builder()
            .current_term(current_term)
            .voted_for(voted_for)
            .build(),
    )?;

    let a = Url::parse("tcp://localhost/32123")?;
    let b = Url::parse("tcp://localhost/78987")?;

    let mut append_entries_outcome = BTreeMap::new();
    _ = append_entries_outcome.insert(
        a.clone(),
        Arc::new(|detail: AppendEntryDetail| {
            Outcome::builder()
                .result(true)
                .term(detail.leaders_term)
                .from(a.clone())
                .build()
        }) as Arc<dyn Fn(AppendEntryDetail) -> Outcome + Send + Sync>,
    );

    _ = append_entries_outcome.insert(
        b.clone(),
        Arc::new(|detail: AppendEntryDetail| {
            Outcome::builder()
                .result(true)
                .term(detail.leaders_term)
                .from(b.clone())
                .build()
        }) as Arc<dyn Fn(AppendEntryDetail) -> Outcome + Send + Sync>,
    );

    let mut r = RaftBuilder::new()
        .id(id.clone())
        .voters([a.clone(), b.clone()].into())
        .initial_persistent_state(pm.into_inner().into_inner())
        .prev_log_index(prev_log_index)
        .log_entries(vec![])
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
                .from(a.clone())
                .build(),
        )
        .with_apply_state(Box::new(ApplyStateFactory))
        .build()
        .await?;

    r.ballot_voters().await?;

    let mut context = r.context_lock()?;
    let pe = context.persistent_state_mut().read()?;

    assert_eq!(current_term + 1, pe.current_term());
    assert_eq!(Some(id), pe.voted_for());
    assert_eq!(State::Leader, context.server().state()?);

    Ok(())
}
