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

use clap::Parser;
use tansu_proxy::Result;
use tokio::task::JoinSet;
use tracing::{debug, Level};
use tracing_subscriber::{filter::Targets, fmt::format::FmtSpan, prelude::*};
use url::Url;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long, default_value = "tcp://localhost:9092")]
    listener_url: Url,

    #[arg(long, default_value = "tcp://localhost:19092")]
    origin_url: Url,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = Targets::new().with_target("tansu_proxy", Level::DEBUG);

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .pretty()
                .with_line_number(true)
                .with_span_events(FmtSpan::ACTIVE)
                .with_thread_ids(true),
        )
        .with(filter)
        .init();

    let args = Cli::parse();

    let mut set = JoinSet::new();

    {
        let proxy = tansu_proxy::Proxy::new(args.listener_url, args.origin_url);
        debug!(?proxy);

        set.spawn(async move { proxy.listen().await.unwrap() });
    }

    loop {
        if set.join_next().await.is_none() {
            break;
        }
    }

    Ok(())
}
