// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
