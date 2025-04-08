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
use clap::Subcommand;
use tansu_kafka_sans_io::ErrorCode;
use tansu_topic::Topic;
use url::Url;

use super::DEFAULT_BROKER;

#[derive(Clone, Debug, Subcommand)]
pub(super) enum Command {
    #[command(about = "Create a topic")]
    Create {
        #[arg(long, default_value = DEFAULT_BROKER)]
        broker: Url,

        #[clap(value_parser)]
        name: String,

        #[arg(long, default_value = "3")]
        partitions: i32,
    },

    #[command(about = "Delete an existing topic")]
    Delete {
        #[arg(long, default_value = DEFAULT_BROKER)]
        broker: Url,

        #[clap(value_parser)]
        name: String,
    },
}

impl From<Command> for Topic {
    fn from(value: Command) -> Self {
        match value {
            Command::Create {
                broker,
                name,
                partitions,
            } => Topic::create()
                .broker(broker)
                .name(name)
                .partitions(partitions)
                .build(),

            Command::Delete { broker, name } => Topic::delete().broker(broker).name(name).build(),
        }
    }
}

impl Command {
    pub(super) async fn main(self) -> Result<ErrorCode> {
        Topic::from(self).main().await.map_err(Into::into)
    }
}
