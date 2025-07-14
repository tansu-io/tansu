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
