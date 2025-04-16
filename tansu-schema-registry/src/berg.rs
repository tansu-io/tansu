// Copyright ⓒ 2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::fmt::Display;

use iceberg::writer::file_writer::location_generator::{FileNameGenerator, LocationGenerator};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Topition {
    topic: String,
    partition: i32,
}

impl Display for Topition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{:0>10}", self.topic, self.partition)
    }
}

impl Topition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Offset(i64);

impl From<i64> for Offset {
    fn from(offset: i64) -> Self {
        Self(offset)
    }
}

impl Display for Offset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:0>20}", self.0)
    }
}

impl LocationGenerator for Topition {
    fn generate_location(&self, file_name: &str) -> String {
        format!("{self}/{file_name}")
    }
}

impl FileNameGenerator for Offset {
    fn generate_file_name(&self) -> String {
        format!("{self}.parquet")
    }
}
