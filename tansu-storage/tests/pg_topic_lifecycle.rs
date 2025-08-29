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

use rand::{distr::Alphanumeric, prelude::*, rng};
use tansu_sans_io::{ErrorCode, create_topics_request::CreatableTopic};
use tansu_storage::{
    BrokerRegistrationRequest, Error, Result, Storage, StorageContainer, TopicId, pg::Postgres,
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

#[tokio::test]
async fn topic_lifecycle() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster_id = Uuid::now_v7();
    let broker_id = rng().random_range(0..i32::MAX);
    let incarnation_id = Uuid::now_v7();

    let storage_container = storage_container(cluster_id, broker_id)?;

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
    let replication_factor = rng().random_range(0..64);
    let assignments = Some([].into());
    let configs = Some([].into());

    let creatable = CreatableTopic::default()
        .name(name.clone())
        .num_partitions(num_partitions)
        .replication_factor(replication_factor)
        .assignments(assignments)
        .configs(configs);

    let id = storage_container
        .create_topic(creatable.clone(), false)
        .await?;

    // metadata via topic uuid
    //
    let metadata = storage_container
        .metadata(Some(&[TopicId::from(id)]))
        .await?;

    assert_eq!(1, metadata.topics().len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(metadata.topics()[0].error_code)?
    );
    assert_eq!(Some(name.clone()), metadata.topics()[0].name);
    assert_eq!(
        id,
        metadata.topics()[0].topic_id.map(Uuid::from_bytes).unwrap()
    );

    // metadata via topic name
    //
    let metadata = storage_container
        .metadata(Some(&[TopicId::from(name.clone())]))
        .await?;

    assert_eq!(1, metadata.topics().len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(metadata.topics()[0].error_code)?
    );
    assert_eq!(Some(name.clone()), metadata.topics()[0].name);
    assert_eq!(
        id,
        metadata.topics()[0].topic_id.map(Uuid::from_bytes).unwrap()
    );

    // creating a topic with the same name causes an API error: topic already exists
    //
    assert!(matches!(
        storage_container.create_topic(creatable, false).await,
        Err(Error::Api(ErrorCode::TopicAlreadyExists))
    ));

    assert!(matches!(
        storage_container
            .delete_topic(&TopicId::from(name.clone()))
            .await,
        Ok(ErrorCode::None)
    ));

    assert!(matches!(
        storage_container.delete_topic(&TopicId::from(name)).await,
        Ok(ErrorCode::UnknownTopicOrPartition)
    ));

    Ok(())
}
