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

use std::{
    collections::BTreeMap,
    io::Cursor,
    sync::{Arc, Mutex},
};

use tansu_raft::{
    blocking::{persistent::PersistentManager, PersistentState},
    election::Election,
    replicate::Replicator,
    LogEntry, Outcome, PersistentEntry, Raft, Result,
};
use url::Url;

use crate::common::{
    AppendEntryDetail, ApplyStateFactory, BTreeMapKvFactory, ConfigurationFactory,
    PersistentStateFactory, RaftRpcFactory, ServerFactory,
};

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn check_replication() -> Result<()> {
    let id = Url::parse("tcp://localhost:54345/")?;
    let current_term = 1;

    let appended_entries = Arc::new(Mutex::new(Vec::new()));

    let mut pm = PersistentManager::builder()
        .inner(Cursor::new(vec![]))
        .id(id.clone())
        .recover()?;

    pm.write(
        PersistentEntry::builder()
            .current_term(current_term)
            .build(),
    )?;

    let abc = LogEntry::builder().term(current_term).value("abc").build();
    let def = LogEntry::builder().term(current_term).value("def").build();
    let ghi = LogEntry::builder().term(current_term).value("ghi").build();
    let jkl = LogEntry::builder().term(current_term).value("jkl").build();
    let mno = LogEntry::builder().term(current_term).value("mno").build();
    let pqr = LogEntry::builder().term(current_term).value("pqr").build();
    let stu = LogEntry::builder().term(current_term).value("stu").build();
    let vwx = LogEntry::builder().term(current_term).value("vwx").build();

    let a = Url::parse("tcp://localhost/32123")?;
    let b = Url::parse("tcp://localhost/78987")?;

    let mut request_votes_outcome = BTreeMap::new();
    request_votes_outcome.insert(
        a.clone(),
        Outcome::builder()
            .result(true)
            .term(current_term + 1)
            .from(a.clone())
            .build(),
    );
    request_votes_outcome.insert(
        b.clone(),
        Outcome::builder()
            .result(false)
            .term(current_term + 1)
            .from(b.clone())
            .build(),
    );

    let mut append_entries_outcome = BTreeMap::new();
    append_entries_outcome.insert(
        a.clone(),
        Arc::new(move |detail: AppendEntryDetail| {
            Outcome::builder()
                .result(true)
                .term(detail.leaders_term)
                .from(detail.recipient)
                .build()
        }) as Arc<dyn Fn(AppendEntryDetail) -> Outcome + Send + Sync>,
    );

    append_entries_outcome.insert(
        b.clone(),
        Arc::new(|detail: AppendEntryDetail| {
            Outcome::builder()
                .result(true)
                .term(detail.leaders_term)
                .from(detail.recipient)
                .build()
        }) as Arc<dyn Fn(AppendEntryDetail) -> Outcome + Send + Sync>,
    );

    let prev_log_index = 0;

    let mut r = Raft::builder()
        .configuration(Box::new(ConfigurationFactory))
        .with_voters([a.clone(), b.clone()].iter().cloned().collect())
        .with_persistent_state(Box::new(PersistentStateFactory {
            id: id.clone(),
            initial_persistent_state: pm.into_inner().into_inner(),
        }))
        .with_apply_state(Box::new(ApplyStateFactory))
        .with_kv_store(Box::new(BTreeMapKvFactory {
            prev_log_index,
            log_entries: vec![
                abc.clone(),
                def.clone(),
                ghi.clone(),
                jkl.clone(),
                mno.clone(),
                pqr.clone(),
                stu.clone(),
                vwx.clone(),
            ],
        }))
        .with_raft_rpc(Box::new(RaftRpcFactory {
            appended_entries: appended_entries.clone(),
            request_votes_outcome,
            append_entries_outcome,
        }))
        .with_server(Box::<ServerFactory>::default())
        .build()
        .await?;

    r.context_lock().and_then(|mut context| {
        *context.server_mut().commit_index_mut()? = 2;
        Ok(())
    })?;

    r.ballot_voters().await?;

    assert_eq!(
        vec![
            AppendEntryDetail {
                recipient: a.clone(),
                leaders_term: 2,
                leader_id: id.clone(),
                prev_log_index: 2,
                prev_log_term: 1,
                entries: vec![],
                leader_commit: 2,
            },
            AppendEntryDetail {
                recipient: b.clone(),
                leaders_term: 2,
                leader_id: id.clone(),
                prev_log_index: 2,
                prev_log_term: 1,
                entries: vec![],
                leader_commit: 2,
            }
        ],
        appended_entries.lock()?.split_off(0)
    );

    // manually adjust next index...
    r.context_lock().and_then(|mut context| {
        *context.server_mut().next_index_mut(&a)?.unwrap() = 1;
        *context.server_mut().next_index_mut(&b)?.unwrap() = 1;
        Ok(())
    })?;

    r.replicate_log_entries().await?;

    assert_eq!(
        vec![
            AppendEntryDetail {
                recipient: a.clone(),
                leaders_term: 2,
                leader_id: id.clone(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![LogEntry::builder().term(1).value("abc").build()],
                leader_commit: 2
            },
            AppendEntryDetail {
                recipient: b.clone(),
                leaders_term: 2,
                leader_id: id.clone(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![LogEntry::builder().term(1).value("abc").build()],
                leader_commit: 2
            }
        ],
        appended_entries.lock()?.split_off(0)
    );

    r.replicate_log_entries().await?;

    assert_eq!(
        vec![
            AppendEntryDetail {
                recipient: a.clone(),
                leaders_term: 2,
                leader_id: id.clone(),
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![LogEntry::builder().term(1).value("def").build()],
                leader_commit: 2
            },
            AppendEntryDetail {
                recipient: b.clone(),
                leaders_term: 2,
                leader_id: id,
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![LogEntry::builder().term(1).value("def").build()],
                leader_commit: 2
            }
        ],
        appended_entries.lock()?.split_off(0)
    );

    r.replicate_log_entries().await?;
    assert_eq!(0, appended_entries.lock()?.len());

    Ok(())
}
