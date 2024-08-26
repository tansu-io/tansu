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

use core::fmt::Debug;
use std::{
    collections::BTreeMap,
    io::Cursor,
    sync::{Arc, Mutex},
};

use bytes::{Buf, Bytes};
use common::RaftBuilder;
use tansu_raft::{
    blocking::{persistent::PersistentManager, PersistentState},
    election::Election,
    server::State,
    ApplyState, Index, Log, PersistentEntry, ProvideApplyState, Result,
};
use tracing::debug;
use url::Url;

type BoxedCallback = Box<dyn Fn(&u32) + Send + Sync>;

#[derive(Clone, Default)]
struct Applicator {
    when_applied: Arc<Mutex<BTreeMap<Index, BoxedCallback>>>,
}

impl Debug for Applicator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(Applicator)).finish()
    }
}

impl Applicator {
    fn new() -> Self {
        Self {
            when_applied: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub(crate) fn when_applied(
        &self,
        index: Index,
        callback: impl Fn(&u32) + Send + Sync + 'static,
    ) -> Result<()> {
        _ = self
            .when_applied
            .lock()
            .map(|mut when_applied| when_applied.insert(index, Box::new(callback)))?;
        Ok(())
    }
}

impl ApplyState for Applicator {
    fn apply(&self, index: Index, state: Option<Bytes>, mut command: Bytes) -> Result<Bytes> {
        debug!("state: {:?}, command: {:?}", state, command);

        let state = state
            .or(Some(Bytes::copy_from_slice(&0u32.to_be_bytes())))
            .inspect(|state| _ = dbg!(state))
            .map(|mut state| state.get_u32())
            .inspect(|state| _ = dbg!(state))
            .map(|state| command.get_u32() + state)
            .inspect(|state| _ = dbg!(state))
            .unwrap();

        _ = self
            .when_applied
            .lock()
            .map(|mut context| context.remove(&index).map(|callback| callback(&state)))?;

        Ok(Bytes::copy_from_slice(&state.to_be_bytes()))
    }
}

#[derive(Clone, Debug, Default)]
pub struct ApplyStateFactory {
    applicator: Applicator,
}

impl ApplyStateFactory {
    fn new(applicator: Applicator) -> Self {
        Self { applicator }
    }
}

impl ProvideApplyState for ApplyStateFactory {
    fn provide_apply_state(&self) -> Result<Box<dyn ApplyState>> {
        Ok(Box::new(self.applicator.clone()))
    }
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn side_effect_while_applying_log() -> Result<()> {
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

    let applicator = Applicator::new();

    let mut r = RaftBuilder::new()
        .id(id.clone())
        .initial_persistent_state(pm.into_inner().into_inner())
        .prev_log_index(prev_log_index)
        .log_entries(vec![])
        .with_apply_state(Box::new(ApplyStateFactory::new(applicator.clone())))
        .build()
        .await?;

    r.ballot_voters().await?;

    r.context_lock().and_then(|context| {
        assert_eq!(State::Leader, context.server().state()?);
        assert_eq!(0, context.server().commit_index()?);
        assert_eq!(0, context.server().last_applied()?);
        Ok(())
    })?;

    let initial_w = 123u32;

    let w = Arc::new(Mutex::new(initial_w));

    let value_of_w = {
        let w = w.clone();

        move || {
            let lock = w.lock().unwrap();
            *lock
        }
    };

    assert_eq!(initial_w, value_of_w());

    let j = 321u32;

    let q = {
        let w = w.clone();

        move |state: &u32| {
            let mut lock = w.lock().unwrap();
            *lock += j + state;
        }
    };

    let command = 7u32;

    assert_eq!(
        1,
        r.log(Bytes::copy_from_slice(&command.to_be_bytes()))
            .await?
    );
    applicator.when_applied(1, q)?;

    r.context_lock().and_then(|context| {
        assert_eq!(State::Leader, context.server().state()?);
        assert_eq!(1, context.server().commit_index()?);
        assert_eq!(0, context.server().last_applied()?);
        Ok(())
    })?;

    assert_eq!(123u32, value_of_w());

    r.context_lock()
        .and_then(|mut context| r.apply_log_entries(&mut context))?;

    assert_eq!(initial_w + j + command, value_of_w());

    r.context_lock().and_then(|context| {
        assert_eq!(State::Leader, context.server().state()?);
        assert_eq!(1, context.server().commit_index()?);
        assert_eq!(1, context.server().last_applied()?);
        Ok(())
    })?;

    Ok(())
}
