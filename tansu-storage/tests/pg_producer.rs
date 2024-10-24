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

use bytes::Bytes;
use rand::{distributions::Alphanumeric, prelude::*, thread_rng};
use tansu_kafka_sans_io::{
    broker_registration_request::Listener,
    create_topics_request::CreatableTopic,
    record::{inflated, Record},
    ErrorCode,
};
use tansu_storage::{
    pg::Postgres, BrokerRegistationRequest, Error, ListOffsetRequest, Result, Storage,
    StorageContainer, Topition,
};
use tracing::{debug, subscriber::DefaultGuard};
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
async fn produce() -> Result<()> {
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

    let _id = storage_container
        .create_topic(creatable.clone(), false)
        .await?;

    let partition = rng.gen_range(0..num_partitions);

    let topition = Topition::new(name, partition);

    let before_produce_earliest = storage_container
        .list_offsets(&[(topition.clone(), ListOffsetRequest::Earliest)])
        .await
        .inspect(|offset| debug!(?offset))?;

    assert_eq!(1, before_produce_earliest.len());
    assert_eq!(ErrorCode::None, before_produce_earliest[0].1.error_code);

    let before_produce_latest = storage_container
        .list_offsets(&[(topition.clone(), ListOffsetRequest::Latest)])
        .await
        .inspect(|offset| debug!(?offset))?;

    assert_eq!(1, before_produce_latest.len());
    assert_eq!(ErrorCode::None, before_produce_latest[0].1.error_code);

    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into()))
        .build()
        .and_then(TryInto::try_into)
        .inspect(|deflated| debug!(?deflated))?;

    let offset = storage_container
        .produce(&topition, batch)
        .await
        .inspect(|offset| debug!(?offset))?;

    assert_eq!(before_produce_latest[0].1.offset, Some(offset));

    let after_produce_earliest = storage_container
        .list_offsets(&[(topition.clone(), ListOffsetRequest::Earliest)])
        .await
        .inspect(|offset| debug!(?offset))?;

    assert_eq!(1, after_produce_earliest.len());
    assert_eq!(ErrorCode::None, after_produce_earliest[0].1.error_code);
    assert_eq!(
        before_produce_earliest[0].1.offset,
        after_produce_earliest[0].1.offset
    );

    let after_produce_latest = storage_container
        .list_offsets(&[(topition.clone(), ListOffsetRequest::Latest)])
        .await
        .inspect(|offset| debug!(?offset))?;

    assert_eq!(1, after_produce_latest.len());
    assert_eq!(ErrorCode::None, after_produce_latest[0].1.error_code);
    assert_eq!(Some(offset), after_produce_latest[0].1.offset);

    // assert_eq!(
    //     ErrorCode::None,
    //     storage_container.delete_topic(&TopicId::from(id)).await?
    // );

    Ok(())
}
