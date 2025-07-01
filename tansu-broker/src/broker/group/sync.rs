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

use tansu_sans_io::{Body, sync_group_request::SyncGroupRequestAssignment};

use crate::{Result, coordinator::group::Coordinator};

#[derive(Debug)]
pub struct SyncGroupRequest<C> {
    coordinator: C,
}

impl<C> SyncGroupRequest<C>
where
    C: Coordinator,
{
    pub fn with_coordinator(coordinator: C) -> Self {
        Self { coordinator }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn response(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> Result<Body> {
        self.coordinator
            .sync(
                group_id,
                generation_id,
                member_id,
                group_instance_id,
                protocol_type,
                protocol_name,
                assignments,
            )
            .await
    }
}
