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

use std::collections::HashMap;

use bytes::Bytes;

#[allow(dead_code)]
struct Group {
    id: String,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    members: HashMap<String, Member>,
}

#[allow(dead_code)]
struct Member {
    metadata: Bytes,
}

#[allow(dead_code)]
struct MemberMetadata {
    version: i16,
    topics: Vec<String>,
    user_data: Option<Bytes>,
    owned_partitions: Vec<TopicPartition>,
    generation_id: Option<i32>,
    rack_id: Option<String>,
}

#[allow(dead_code)]
struct TopicPartition {
    topic: String,
    partitions: Vec<i32>,
}
