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

use std::time::Duration;

use super::DEFAULT_BROKER;
use crate::Result;
use clap::Args;
use tansu_generator::Generate;
use tansu_kafka_sans_io::ErrorCode;
use url::Url;

#[derive(Args, Clone, Debug)]
pub(super) struct Arg {
    /// The URL of the broker to produce messages into
    #[arg(long, default_value = DEFAULT_BROKER, env = "ADVERTISED_LISTENER_URL")]
    broker: Url,

    /// The topic to generate messages into
    #[clap(value_parser)]
    topic: String,

    /// The partition to produce messages into
    #[arg(long, default_value = "0")]
    partition: i32,

    /// Schema registry examples are: file://./etc/schema or s3://tansu/, containing: topic.json, topic.proto or topic.avsc
    #[arg(long, env = "SCHEMA_REGISTRY")]
    schema_registry: Url,

    /// Message batch size used by every producer
    #[arg(long, default_value = "1")]
    batch_size: u32,

    /// The maximum number of messages per second
    #[clap(long)]
    per_second: Option<u32>,

    /// The number of producers generating messages
    #[arg(long, default_value = "1")]
    producers: u32,

    /// Stop sending messages after this time
    #[arg(long)]
    duration_seconds: Option<u64>,
}

impl Arg {
    pub(super) async fn main(self) -> Result<ErrorCode> {
        let generate = Generate::builder()
            .broker(self.broker)
            .topic(self.topic)
            .partition(self.partition)
            .schema_registry(self.schema_registry)
            .batch_size(self.batch_size)
            .per_second(self.per_second)
            .producers(self.producers)
            .duration(self.duration_seconds.map(Duration::from_secs))
            .build()?;

        generate.main().await.map_err(Into::into)
    }
}
