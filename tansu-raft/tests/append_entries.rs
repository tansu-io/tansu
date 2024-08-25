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

use std::io::Cursor;

use bytes::{BufMut, Bytes, BytesMut};
use common::RaftBuilder;
use tansu_raft::{
    blocking::{persistent::PersistentManager, PersistentState},
    log_key_for_index, AppendEntries, ApplyState, Index, LogEntry, PersistentEntry,
    ProvideApplyState, Result,
};
use url::Url;

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
async fn deny_append_entries_with_old_leader_term() -> Result<()> {
    let id = Url::parse("tcp://localhost:1234/")?;
    let current_term = 456;
    let candidate_id = Url::parse("tcp://localhost:4334/")?;

    let leaders_term = 0;
    let leader_id = Url::parse("tcp://localhost:54345/")?;
    let prev_log_index = 123;
    let prev_log_term = 567;
    let entries = vec![];
    let leader_commit = 5;

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
        .prev_log_index(0)
        .log_entries(vec![])
        .with_apply_state(Box::new(ApplyStateFactory))
        .build()
        .await?;

    let outcome = r
        .append_entries(
            leaders_term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        )
        .await?;

    assert_eq!(id, outcome.from());
    assert_eq!(current_term, outcome.term());
    assert!(!outcome.result());

    Ok(())
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn append_entries_mismatch_prev_log_term() -> Result<()> {
    let id = Url::parse("tcp://localhost:1234/")?;
    let current_term = 456;
    let candidate_id = Url::parse("tcp://localhost:6/")?;

    let leaders_term = current_term;
    let leader_id = Url::parse("tcp://localhost:4321/")?;
    let prev_log_index = 123;
    let prev_log_term = 567;
    let leader_commit = 5;

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

    let prev_log_entry = LogEntry::builder()
        .term(current_term - 1)
        .value("hello world!")
        .build();

    let entries = vec![prev_log_entry.clone()];

    let mut r = RaftBuilder::new()
        .id(id.clone())
        .initial_persistent_state(pm.into_inner().into_inner())
        .prev_log_index(prev_log_index - 1)
        .log_entries(entries)
        .with_apply_state(Box::new(ApplyStateFactory))
        .build()
        .await?;

    let outcome = r
        .append_entries(
            leaders_term,
            leader_id,
            prev_log_index,
            prev_log_term,
            vec![],
            leader_commit,
        )
        .await?;

    assert_eq!(id, outcome.from());
    assert_eq!(current_term, outcome.term());
    assert!(!outcome.result());

    let context = r.context_lock()?;

    assert_eq!(
        Some(prev_log_entry),
        context
            .kv_store()
            .get(log_key_for_index(prev_log_index)?)
            .map(|option| option.map(|bytes| LogEntry::try_from(bytes).unwrap()))?
    );

    Ok(())
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn append_entries_delete_conflicts() -> Result<()> {
    let id = Url::parse("tcp://localhost:1234/")?;
    let current_term = 456;
    let candidate_id = Url::parse("tcp://localhost:6/")?;

    let leaders_term = current_term;
    let leader_id = Url::parse("tcp://localhost:4321/")?;
    let prev_log_index = 123;
    let prev_log_term = current_term - 1;
    let leader_commit = 0;

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

    let prev_log_entry = LogEntry::builder()
        .term(current_term - 1)
        .value("hello world!")
        .build();

    let entries = vec![
        prev_log_entry.clone(),
        LogEntry::builder()
            .term(current_term + 1)
            .value("conflicting log entry term #1")
            .build(),
        LogEntry::builder()
            .term(current_term + 1)
            .value("conflicting log entry term #2")
            .build(),
    ];

    let mut r = RaftBuilder::new()
        .id(id.clone())
        .initial_persistent_state(pm.into_inner().into_inner())
        .prev_log_index(prev_log_index - 1)
        .log_entries(entries)
        .with_apply_state(Box::new(ApplyStateFactory))
        .build()
        .await?;

    let a_new_log_entry = LogEntry::builder()
        .term(current_term)
        .value("a new entry")
        .build();

    let outcome = r
        .append_entries(
            leaders_term,
            leader_id,
            prev_log_index,
            prev_log_term,
            vec![a_new_log_entry.clone()],
            leader_commit,
        )
        .await?;

    assert_eq!(id, outcome.from());
    assert_eq!(current_term, outcome.term());
    assert!(outcome.result());

    let context = r.context_lock()?;

    assert_eq!(
        Some(prev_log_entry),
        context
            .kv_store()
            .get(log_key_for_index(prev_log_index)?)
            .map(|option| option.map(|bytes| LogEntry::try_from(bytes).unwrap()))?
    );

    assert_eq!(
        Some(a_new_log_entry),
        context
            .kv_store()
            .get(log_key_for_index(prev_log_index + 1)?)
            .map(|option| option.map(|bytes| LogEntry::try_from(bytes).unwrap()))?
    );

    assert_eq!(
        None,
        context
            .kv_store()
            .get(log_key_for_index(prev_log_index + 2)?)?
    );

    Ok(())
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn append_entries_mismatch_no_prev_log_entry() -> Result<()> {
    let id = Url::parse("tcp://localhost:1234/")?;
    let current_term = 456;
    let candidate_id = Url::parse("tcp://localhost:6/")?;

    let leaders_term = current_term;
    let leader_id = Url::parse("tcp://localhost:4321/")?;
    let prev_log_index = 123;
    let prev_log_term = 567;
    let leader_commit = 5;

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

    let entries = vec![];

    let mut r = RaftBuilder::new()
        .id(id.clone())
        .initial_persistent_state(pm.into_inner().into_inner())
        .prev_log_index(prev_log_index)
        .log_entries(entries)
        .with_apply_state(Box::new(ApplyStateFactory))
        .build()
        .await?;

    let outcome = r
        .append_entries(
            leaders_term,
            leader_id,
            prev_log_index,
            prev_log_term,
            vec![],
            leader_commit,
        )
        .await?;

    assert_eq!(id, outcome.from());
    assert_eq!(current_term, outcome.term());
    assert!(!outcome.result());
    Ok(())
}
