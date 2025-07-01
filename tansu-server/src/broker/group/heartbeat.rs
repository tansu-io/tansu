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

use tansu_sans_io::Body;

use crate::{Result, coordinator::group::Coordinator};

#[derive(Debug)]
pub struct HeartbeatRequest<C> {
    coordinator: C,
}

impl<C> HeartbeatRequest<C> {
    pub fn with_coordinator(coordinator: C) -> Self
    where
        C: Coordinator,
    {
        Self { coordinator }
    }
}

impl<C> HeartbeatRequest<C>
where
    C: Coordinator,
{
    pub async fn response(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<Body> {
        self.coordinator
            .heartbeat(group_id, generation_id, member_id, group_instance_id)
            .await
    }
}
