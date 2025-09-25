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

use tansu_sans_io::JoinGroupRequest;

use crate::Error;

#[derive(Clone, Default, Eq, Hash, Debug, Ord, PartialEq, PartialOrd)]
pub struct Group {
    group_id: String,
    group_instance_id: Option<String>,
    rebalance_timeout: Option<i32>,
    session_timeout: i32,
}

impl Group {
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            ..Default::default()
        }
    }

    pub fn group_instance_id(self, group_instance_id: Option<String>) -> Self {
        Self {
            group_instance_id,
            ..self
        }
    }

    pub fn rebalance_timeout(self, rebalance_timeout: Option<i32>) -> Self {
        Self {
            rebalance_timeout,
            ..self
        }
    }

    pub fn session_timeout(self, session_timeout: i32) -> Self {
        Self {
            session_timeout,
            ..self
        }
    }
}

fn group(group_id: &str, topics: &[&str]) -> Result<(), Error> {
    let q = JoinGroupRequest::default().group_id(group_id.into());

    Ok(())
}
