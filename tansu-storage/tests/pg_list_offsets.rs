// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(feature = "postgres")]
use rand::{distr::Alphanumeric, prelude::*, rng};

#[cfg(feature = "postgres")]
use tansu_sans_io::{ErrorCode, IsolationLevel};

#[cfg(feature = "postgres")]
use tansu_storage::{
    BrokerRegistrationRequest, Error, ListOffsetRequest, Result, Storage, StorageContainer,
    Topition,
};

#[cfg(feature = "postgres")]
use tracing::subscriber::DefaultGuard;

#[cfg(feature = "postgres")]
use uuid::Uuid;

#[cfg(feature = "postgres")]
use tansu_storage::pg::Postgres;

#[cfg(feature = "postgres")]
fn init_tracing() -> Result<DefaultGuard> {
    use std::{fs::File, sync::Arc, thread};

    use tracing::Level;
    use tracing_subscriber::fmt::format::FmtSpan;

    Ok(tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_max_level(Level::DEBUG)
            .with_span_events(FmtSpan::ACTIVE)
            .with_writer(
                thread::current()
                    .name()
                    .ok_or(Error::Message(String::from("unnamed thread")))
                    .and_then(|name| {
                        File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME")))
                            .map_err(Into::into)
                    })
                    .map(Arc::new)?,
            )
            .finish(),
    ))
}

#[cfg(feature = "postgres")]
fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
    Postgres::builder("postgres://postgres:postgres@localhost")
        .map(|builder| builder.cluster(cluster))
        .map(|builder| builder.node(node))
        .map(|builder| builder.build())
        .map(StorageContainer::Postgres)
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn list_offset() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster_id = Uuid::now_v7();
    let broker_id = rng().random_range(0..i32::MAX);
    let incarnation_id = Uuid::now_v7();

    let mut storage_container = storage_container(cluster_id, broker_id)?;

    let broker_registration = BrokerRegistrationRequest {
        broker_id,
        cluster_id: cluster_id.into(),
        incarnation_id,
        rack: None,
    };

    storage_container
        .register_broker(broker_registration)
        .await?;

    let name: String = rng()
        .sample_iter(&Alphanumeric)
        .take(15)
        .map(char::from)
        .collect();

    let num_partitions = rng().random_range(1..64);

    let partition = rng().random_range(0..num_partitions);

    let topition = Topition::new(name, partition);

    let earliest = storage_container
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition, ListOffsetRequest::Earliest)],
        )
        .await?;

    assert_eq!(ErrorCode::None, earliest[0].1.error_code);

    Ok(())
}
