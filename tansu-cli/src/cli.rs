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

use std::process;

use crate::Result;
use clap::{Parser, Subcommand};
use tansu_kafka_sans_io::ErrorCode;
use tracing::debug;

mod broker;
mod cat;
mod generator;
mod proxy;
mod topic;

const DEFAULT_BROKER: &str = "tcp://localhost:9092";

#[derive(Clone, Debug, Parser)]
#[command(name = "tansu", version, about, long_about = None, args_conflicts_with_subcommands = true)]
pub struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    #[clap(flatten)]
    broker: broker::Arg,
}

#[derive(Clone, Debug, Subcommand)]
enum Command {
    /// Apache Kafka compatible broker with Avro, JSON, Protobuf schema validation [default if no command supplied]
    Broker(Box<broker::Arg>),

    /// Easily consume or produce Avro, JSON or Protobuf messages to a topic
    Cat {
        #[command(subcommand)]
        command: cat::Command,
    },

    /// Traffic Generator for schema backed topics
    Generator(Box<generator::Arg>),

    /// Apache Kafka compatible proxy
    Proxy(Box<proxy::Arg>),

    /// Create or delete topics managed by the broker
    Topic {
        #[command(subcommand)]
        command: topic::Command,
    },
}

impl Cli {
    pub async fn main() -> Result<ErrorCode> {
        debug!(pid = process::id());

        let cli = Cli::parse();

        match cli.command.unwrap_or(Command::Broker(Box::new(cli.broker))) {
            Command::Broker(arg) => arg
                .main()
                .await
                .inspect(|result| debug!(?result))
                .inspect_err(|err| debug!(?err)),

            Command::Cat { command } => command.main().await,

            Command::Generator(arg) => arg
                .main()
                .await
                .inspect(|result| debug!(?result))
                .inspect_err(|err| debug!(?err)),

            Command::Proxy(arg) => {
                tansu_proxy::Proxy::main(arg.listener_url.into_inner(), arg.origin_url.into_inner())
                    .await
                    .map_err(Into::into)
            }

            Command::Topic { command } => command.main().await,
        }
    }
}
