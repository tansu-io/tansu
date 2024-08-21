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
