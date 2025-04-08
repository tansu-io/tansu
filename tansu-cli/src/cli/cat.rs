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
use tansu_kafka_sans_io::ErrorCode;
use url::Url;

#[derive(Clone, Debug, Subcommand)]
pub(super) enum Command {
    #[command(about = "Produce Avro/JSON/Protobuf messages to a topic")]
    Produce {
        #[arg(long, default_value = DEFAULT_BROKER, env = "ADVERTISED_LISTENER_URL", help = "The URL of the broker to produce messages into")]
        broker: Url,

        #[clap(value_parser, help = "The topic to produce messages into")]
        topic: String,

        #[clap(
            value_parser,
            default_value = "-",
            help = "Input filename or '-' for stdin"
        )]
        file: String,

        #[arg(
            long,
            default_value = "0",
            help = "The partition to produce messages into"
        )]
        partition: i32,

        #[arg(
            long,
            env = "SCHEMA_REGISTRY",
            help = "Schema registry examples are: file://./etc/schema or s3://tansu/, containing: topic.json, topic.proto or topic.avsc"
        )]
        schema_registry: Option<Url>,
    },

    #[command(about = "Consume Avro/JSON/Protobuf messages from a topic")]
    Consume {
        #[arg(long, default_value = DEFAULT_BROKER, env = "ADVERTISED_LISTENER_URL", help = "The URL of the broker to consume messages from")]
        broker: Url,

        #[clap(value_parser, help = "The topic to consume messages from")]
        topic: String,

        #[arg(
            long,
            default_value = "0",
            help = "The partition to consume messages from"
        )]
        partition: i32,

        #[arg(
            long,
            env = "SCHEMA_REGISTRY",
            help = "Schema registry examples are: file://./etc/schema or s3://tansu/, containing: topic.json, topic.proto or topic.avsc"
        )]
        schema_registry: Option<Url>,

        #[arg(
            long,
            default_value = "5000",
            help = "The maximum time in milliseconds to wait for a message"
        )]
        max_wait_time_ms: i32,

        #[arg(
            long,
            default_value = "1",
            help = "The minimum number of bytes to wait for"
        )]
        min_bytes: i32,

        #[arg(
            long,
            default_value = "52428800",
            help = "The maximum bytes to wait for"
        )]
        max_bytes: Option<i32>,

        #[arg(long, default_value = "0", help = "The fetch offset to start from")]
        fetch_offset: i64,

        #[arg(
            long,
            default_value = "1048576",
            help = "The partition to consume from"
        )]
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
