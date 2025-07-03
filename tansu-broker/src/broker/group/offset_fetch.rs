// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use tansu_sans_io::{
    Body,
    offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopic},
};

use crate::{Result, coordinator::group::Coordinator};

#[derive(Debug)]
pub struct OffsetFetchRequest<C> {
    coordinator: C,
}

impl<C> OffsetFetchRequest<C>
where
    C: Coordinator,
{
    pub fn with_coordinator(coordinator: C) -> Self {
        Self { coordinator }
    }

    pub async fn response(
        &mut self,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> Result<Body> {
        self.coordinator
            .offset_fetch(group_id, topics, groups, require_stable)
            .await
    }
}
