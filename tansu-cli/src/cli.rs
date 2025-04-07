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

use crate::Result;
use clap::{Parser, Subcommand};
use tansu_kafka_sans_io::ErrorCode;

mod broker;
mod cat;
mod proxy;
mod topic;

const DEFAULT_BROKER: &str = "tcp://localhost:9092";

#[derive(Clone, Debug, Parser)]
#[command(version, about, long_about = None, args_conflicts_with_subcommands = true)]
pub struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    #[clap(flatten)]
    broker: broker::Arg,
}

#[derive(Clone, Debug, Subcommand)]
enum Command {
    Broker(Box<broker::Arg>),

    #[command(arg_required_else_help = true)]
    Cat {
        #[command(subcommand)]
        command: cat::Command,
    },

    #[command(arg_required_else_help = true)]
    Topic {
        #[command(subcommand)]
        command: topic::Command,
    },

    #[command(arg_required_else_help = true)]
    Proxy(Box<proxy::Arg>),
}

impl Cli {
    pub async fn main() -> Result<ErrorCode> {
        let cli = Cli::parse();

        match cli.command {
            None => cli.broker.main().await,

            Some(Command::Broker(arg)) => arg.main().await,

            Some(Command::Cat { command }) => command.main().await,

            Some(Command::Proxy(arg)) => {
                tansu_proxy::Proxy::main(arg.listener_url.into_inner(), arg.origin_url.into_inner())
                    .await
                    .map_err(Into::into)
            }

            Some(Command::Topic { command }) => command.main().await,
        }
    }
}
