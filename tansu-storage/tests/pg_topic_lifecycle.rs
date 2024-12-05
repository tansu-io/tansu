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

use rand::{distributions::Alphanumeric, prelude::*, thread_rng};
use tansu_kafka_sans_io::{
    broker_registration_request::Listener, create_topics_request::CreatableTopic, ErrorCode,
};
use tansu_storage::{
    pg::Postgres, BrokerRegistationRequest, Error, Result, Storage, StorageContainer, TopicId,
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

    let mut rng = thread_rng();

    let cluster_id = Uuid::now_v7();
    let broker_id = rng.gen_range(0..i32::MAX);
    let incarnation_id = Uuid::now_v7();

    let mut storage_container = storage_container(cluster_id, broker_id)?;
    let port = rng.gen_range(1024..u16::MAX);
    let security_protocol = rng.gen_range(0..i16::MAX);

    let broker_registration = BrokerRegistationRequest {
        broker_id,
        cluster_id: cluster_id.into(),
        incarnation_id,
        listeners: vec![Listener {
            name: "broker".into(),
            host: "test.local".into(),
            port,
            security_protocol,
        }],
        features: vec![],
        rack: None,
    };

    storage_container
        .register_broker(broker_registration)
        .await?;

    let name: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(15)
        .map(char::from)
        .collect();

    let num_partitions = rng.gen_range(1..64);
    let replication_factor = rng.gen_range(0..64);
    let assignments = Some([].into());
    let configs = Some([].into());

    let creatable = CreatableTopic {
        name: name.clone(),
        num_partitions,
        replication_factor,
        assignments,
        configs,
    };

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
