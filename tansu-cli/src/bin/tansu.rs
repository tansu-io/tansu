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

use dotenv::dotenv;
use tansu_cli::{Cli, Result};
use tansu_kafka_sans_io::ErrorCode;
use tracing::{debug, error};
use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan, prelude::*};

#[tokio::main]
async fn main() -> Result<ErrorCode> {
    dotenv().ok();

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(
            tracing_subscriber::fmt::layer()
                .with_level(true)
                .with_line_number(true)
                .with_thread_ids(false)
                .with_span_events(FmtSpan::NONE),
        )
        .init();

    Cli::main()
        .await
        .inspect(|error_code| debug!(%error_code))
        .inspect_err(|err| error!(%err))
}
