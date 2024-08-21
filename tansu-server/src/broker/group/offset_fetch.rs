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

use std::sync::{Arc, Mutex, MutexGuard};

use tansu_kafka_sans_io::{
    offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopic},
    Body,
};

use crate::{coordinator::group::Coordinator, Result};

#[derive(Debug)]
pub struct OffsetFetchRequest {
    pub groups: Arc<Mutex<Box<dyn Coordinator>>>,
}

impl OffsetFetchRequest {
    pub fn groups_lock(&self) -> Result<MutexGuard<'_, Box<dyn Coordinator>>> {
        self.groups.lock().map_err(|error| error.into())
    }
}

impl OffsetFetchRequest {
    pub fn response(
        &self,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> Result<Body> {
        self.groups_lock().and_then(|mut coordinator| {
            coordinator.offset_fetch(group_id, topics, groups, require_stable)
        })
    }
}
