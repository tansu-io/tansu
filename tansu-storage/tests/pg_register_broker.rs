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

use rand::{prelude::*, rng};
use tansu_storage::{
    BrokerRegistrationRequest, Error, Result, Storage, StorageContainer, pg::Postgres,
};
use tracing::subscriber::DefaultGuard;
use uuid::Uuid;

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

fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
    Postgres::builder("postgres://postgres:postgres@localhost")
        .map(|builder| builder.cluster(cluster))
        .map(|builder| builder.node(node))
        .map(|builder| builder.build())
        .map(StorageContainer::Postgres)
}

#[ignore]
#[tokio::test]
async fn register_broker() -> Result<()> {
    let _guard = init_tracing()?;

    let mut rng = rng();

    let cluster_id = Uuid::now_v7();
    let broker_id = rng.random_range(0..i32::MAX);
    let incarnation_id = Uuid::now_v7();

    let storage_container = storage_container(cluster_id, broker_id)?;
    let port = rng.random_range(1024..u16::MAX);

    let broker_registration = BrokerRegistrationRequest {
        broker_id,
        cluster_id: cluster_id.into(),
        incarnation_id,
        rack: None,
    };

    storage_container
        .register_broker(broker_registration)
        .await?;

    let brokers = storage_container.brokers().await?;
    assert_eq!(1, brokers.len());
    assert_eq!(broker_id, brokers[0].broker_id);
    assert_eq!(port, u16::try_from(brokers[0].port)?);
    assert_eq!("test.local", brokers[0].host);

    Ok(())
}
