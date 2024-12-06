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

use common::{alphanumeric_string, init_tracing, register_broker, storage_container, StorageType};
use rand::{prelude::*, thread_rng};
use tansu_server::Result;
use tansu_storage::Storage;
use tracing::debug;
use url::Url;
use uuid::Uuid;

mod common;

#[tokio::test]
async fn with_txn() -> Result<()> {
    let _guard = init_tracing()?;

    let mut rng = thread_rng();

    let cluster_id = Uuid::now_v7();
    let broker_id = rng.gen_range(0..i32::MAX);
    let mut sc = Url::parse("tcp://127.0.0.1/")
        .map_err(Into::into)
        .and_then(|advertised_listener| {
            storage_container(
                StorageType::Postgres,
                cluster_id,
                broker_id,
                advertised_listener,
            )
        })?;

    register_broker(&cluster_id, broker_id, &mut sc).await?;

    let transaction_id: String = alphanumeric_string(10);
    debug!(?transaction_id);

    let transaction_timeout_ms = 10_000;

    let first = sc
        .init_producer(
            Some(transaction_id.as_str()),
            transaction_timeout_ms,
            Some(-1),
            Some(-1),
        )
        .await?;
    debug!(?first);

    assert_eq!(0, first.epoch);

    let second = sc
        .init_producer(
            Some(transaction_id.as_str()),
            transaction_timeout_ms,
            Some(-1),
            Some(-1),
        )
        .await?;
    debug!(?second);

    assert_eq!(first.id, second.id);
    assert_eq!(1, second.epoch);

    Ok(())
}
