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

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tansu_raft::{
    blocking::{persistent::PersistentManager, PersistentState},
    state_key, AppendEntries, ApplyState, Error, Index, LogEntry, PersistentEntry,
    ProvideApplyState, Result,
};
use url::Url;

use crate::common::RaftBuilder;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Serialize, Deserialize)]
struct State {
    value: i32,
}

#[typetag::serde(tag = "type")]
trait Command {
    fn apply(&self, state: State) -> Result<State>;
}

type BoxDynCommand = Box<dyn Command>;

#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Serialize, Deserialize,
)]
struct Product {
    factor: i32,
}

#[typetag::serde]
impl Command for Product {
    fn apply(&self, state: State) -> Result<State> {
        Ok(State {
            value: state.value * self.factor,
        })
    }
}

#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Serialize, Deserialize,
)]
struct Sum {
    value: i32,
}

#[typetag::serde]
impl Command for Sum {
    fn apply(&self, state: State) -> Result<State> {
        Ok(State {
            value: state.value + self.value,
        })
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Applicator;

impl ApplyState for Applicator {
    fn apply(&self, _index: Index, state: Option<Bytes>, command: Bytes) -> Result<Bytes> {
        state
            .map_or_else(
                || Ok(State::default()),
                |encoded| serde_json::from_slice(&encoded).map_err(|error| error.into()),
            )
            .and_then(|state| BoxDynCommand::try_from(command)?.apply(state))
            .and_then(Bytes::try_from)
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct ApplyStateFactory;

impl ProvideApplyState for ApplyStateFactory {
    fn provide_apply_state(&self) -> Result<Box<dyn ApplyState>> {
        Ok(Box::new(Applicator))
    }
}

impl TryFrom<State> for Bytes {
    type Error = Error;

    fn try_from(value: State) -> Result<Self, Self::Error> {
        serde_json::to_vec(&value)
            .map(Bytes::from)
            .map_err(|error| error.into())
    }
}

impl TryFrom<Bytes> for BoxDynCommand {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        serde_json::from_slice::<BoxDynCommand>(&value).map_err(|error| error.into())
    }
}

impl TryFrom<&dyn Command> for Bytes {
    type Error = Error;

    fn try_from(value: &dyn Command) -> Result<Self, Self::Error> {
        serde_json::to_vec(value)
            .map(Bytes::from)
            .map_err(|error| error.into())
    }
}

impl TryFrom<Bytes> for State {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        serde_json::from_slice::<State>(&value).map_err(|error| error.into())
    }
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn append_entries_applies_command_to_state() -> Result<()> {
    let id = Url::parse("tcp://localhost:1234/")?;
    let current_term = 1;
    let leaders_term = current_term;
    let leader_id = Url::parse("tcp://localhost:4321/")?;
    let prev_log_index = 0;
    let prev_log_term = current_term - 1;
    let leader_commit = 3;

    let mut pm = PersistentManager::builder()
        .inner(Cursor::new(vec![]))
        .id(id.clone())
        .recover()?;

    pm.write(
        PersistentEntry::builder()
            .current_term(current_term)
            .voted_for(None)
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

    let entries = vec![
        LogEntry::builder()
            .term(leaders_term)
            .value(Bytes::try_from(&Sum { value: 5 } as &dyn Command)?)
            .build(),
        LogEntry::builder()
            .term(leaders_term)
            .value(Bytes::try_from(&Product { factor: 8 } as &dyn Command)?)
            .build(),
        LogEntry::builder()
            .term(leaders_term)
            .value(Bytes::try_from(&Sum { value: 2 } as &dyn Command)?)
            .build(),
    ];

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
    assert_eq!(leaders_term, outcome.term());
    assert!(outcome.result());

    let context = r.context_lock()?;

    assert_eq!(
        State { value: 42 },
        context
            .kv_store()
            .get(state_key())
            .and_then(|option| option
                .ok_or(Error::Custom(String::from("state not found")))
                .and_then(State::try_from))?
    );

    Ok(())
}
