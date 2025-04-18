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

use std::{env::vars, fmt::Display};

use iceberg::{
    io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY},
    writer::file_writer::location_generator::{FileNameGenerator, LocationGenerator},
};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Topition {
    topic: String,
    partition: i32,
}

impl Display for Topition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "s3://lake/topic/{}/{:0>10}", self.topic, self.partition)
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

pub fn env_mapping(k: &str) -> &str {
    match k {
        "AWS_ACCESS_KEY_ID" => S3_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY" => S3_SECRET_ACCESS_KEY,
        "AWS_DEFAULT_REGION" => S3_REGION,
        "AWS_ENDPOINT" => S3_ENDPOINT,
        _ => unreachable!("{k}"),
    }
}

pub fn env_s3_props() -> impl Iterator<Item = (String, String)> {
    vars()
        .filter(|&(ref k, _)| {
            k == "AWS_ACCESS_KEY_ID"
                || k == "AWS_SECRET_ACCESS_KEY"
                || k == "AWS_DEFAULT_REGION"
                || k == "AWS_ENDPOINT"
        })
        .map(|(k, v)| (env_mapping(k.as_str()).to_owned(), v))
}
