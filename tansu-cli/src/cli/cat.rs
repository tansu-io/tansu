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

use super::DEFAULT_BROKER;
use crate::Result;
use clap::Subcommand;
use tansu_cat::Cat;
use tansu_sans_io::ErrorCode;
use url::Url;

#[derive(Clone, Debug, Subcommand)]
pub(super) enum Command {
    /// Produce Avro/JSON/Protobuf messages to a topic
    Produce {
        /// The URL of the broker to produce messages into
        #[arg(long, default_value = DEFAULT_BROKER, env = "ADVERTISED_LISTENER_URL")]
        broker: Url,

        /// The topic to produce messages into
        #[clap(value_parser)]
        topic: String,

        /// Input filename or '-' for stdin
        #[clap(value_parser, default_value = "-")]
        file: String,

        /// The partition to produce messages into
        #[arg(long, default_value = "0")]
        partition: i32,

        /// Schema registry examples are: file://./etc/schema or s3://tansu/, containing: topic.json, topic.proto or topic.avsc
        #[arg(long, env = "SCHEMA_REGISTRY")]
        schema_registry: Option<Url>,
    },

    /// Consume Avro/JSON/Protobuf messages from a topic
    Consume {
        /// The URL of the broker to consume messages from
        #[arg(long, default_value = DEFAULT_BROKER, env = "ADVERTISED_LISTENER_URL")]
        broker: Url,

        /// The topic to consume messages from
        #[clap(value_parser)]
        topic: String,

        /// The partition to consume messages from
        #[arg(long, default_value = "0")]
        partition: i32,

        /// Schema registry examples are: file://./etc/schema or s3://tansu/, containing: topic.json, topic.proto or topic.avsc
        #[arg(long, env = "SCHEMA_REGISTRY")]
        schema_registry: Option<Url>,

        /// The maximum time in milliseconds to wait for a message
        #[arg(long, default_value = "5000")]
        max_wait_time_ms: i32,

        /// The minimum number of bytes to wait for
        #[arg(long, default_value = "1")]
        min_bytes: i32,

        /// The maximum bytes to wait for
        #[arg(long, default_value = "52428800")]
        max_bytes: Option<i32>,

        /// The fetch offset to start from
        #[arg(long, default_value = "0")]
        fetch_offset: i64,

        /// The partition to consume from
        #[arg(long, default_value = "1048576")]
        partition_max_bytes: i32,
    },
}

impl From<Command> for tansu_cat::Cat {
    fn from(value: Command) -> Self {
        match value {
            Command::Produce {
                broker,
                topic,
                file,
                partition,
                schema_registry,
            } => Cat::produce()
                .broker(broker)
                .topic(topic)
                .partition(partition)
                .schema_registry(schema_registry)
                .file_name(file)
                .build(),

            Command::Consume {
                broker,
                topic,
                partition,
                schema_registry,
                max_wait_time_ms,
                min_bytes,
                max_bytes,
                fetch_offset,
                partition_max_bytes,
            } => Cat::consume()
                .broker(broker)
                .topic(topic)
                .partition(partition)
                .schema_registry(schema_registry)
                .max_wait_time_ms(max_wait_time_ms)
                .min_bytes(min_bytes)
                .max_bytes(max_bytes)
                .fetch_offset(fetch_offset)
                .partition_max_bytes(partition_max_bytes)
                .build(),
        }
    }
}

impl Command {
    pub(super) async fn main(self) -> Result<ErrorCode> {
        tansu_cat::Cat::from(self).main().await.map_err(Into::into)
    }
}
