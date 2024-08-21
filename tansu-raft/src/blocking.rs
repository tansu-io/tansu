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

pub mod persistent;

use core::fmt::{self, Debug, Formatter};

use url::Url;

use crate::{persistent::PersistentEntry, Result, Term};

pub trait PersistentState: Send + Sync {
    fn read(&mut self) -> Result<PersistentEntry>;
    fn write(&mut self, entry: PersistentEntry) -> Result<()>;
    fn id(&self) -> Url;

    fn transition_to_follower(&mut self, current_term: Term) -> Result<()> {
        let entry = self.read()?.transition_to_follower(current_term);
        self.write(entry)
    }

    fn transition_to_candidate(&mut self) -> Result<()> {
        let entry = self.read()?.transition_to_candidate(self.id());
        self.write(entry)
    }

    fn transition_to_leader(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Debug for dyn PersistentState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(PersistentState))
            .field("id", &self.id())
            .finish()
    }
}

pub trait ProvidePersistentState {
    fn provide_persistent_state(&self) -> Result<Box<dyn PersistentState>>;
}
