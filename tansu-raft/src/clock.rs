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
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::Result;

#[derive(Clone, Debug)]
pub struct Clock(Arc<Mutex<Instant>>);

impl Default for Clock {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(Instant::now())))
    }
}

impl Clock {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn update(&self) -> Result<()> {
        *self.0.lock()? = Instant::now();
        Ok(())
    }

    pub fn has_expired(&self, timeout: Duration) -> Result<bool> {
        Ok(Instant::now().duration_since(*self.0.lock()?) > timeout)
    }
}
