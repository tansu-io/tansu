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

use std::{fmt, io, sync::Arc};

use dotenv::dotenv;
use tansu_sans_io::create_topics_request::CreatableTopic;
use tansu_storage::{BrokerRegistrationRequest, Storage, StorageContainer};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::{EnvFilter, filter::ParseError};
use url::Url;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[allow(dead_code)]
    Io(Arc<io::Error>),

    #[allow(dead_code)]
    Message(String),

    #[allow(dead_code)]
    ParseFilter(Arc<ParseError>),

    Parse(#[from] url::ParseError),

    Protocol(#[from] tansu_sans_io::Error),
    Schema(#[from] tansu_schema::Error),
    Storage(#[from] tansu_storage::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        Self::ParseFilter(Arc::new(value))
    }
}

pub(crate) fn init_tracing() -> Result<DefaultGuard, Error> {
    use std::{fs::File, sync::Arc, thread};

    _ = dotenv().ok();

    Ok(tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
            )
            .with_writer(
                thread::current()
                    .name()
                    .ok_or(Error::Message(String::from("unnamed thread")))
                    .and_then(|name| {
                        File::create(format!(
                            "../logs/{}/{}::{name}.log",
                            env!("CARGO_PKG_NAME"),
                            env!("CARGO_CRATE_NAME")
                        ))
                        .map_err(Into::into)
                    })
                    .map(Arc::new)?,
            )
            .finish(),
    ))
}

#[allow(dead_code)]
pub(crate) async fn build_storage(
    cluster_id: &str,
    node_id: i32,
    storage: Url,
) -> Result<Arc<Box<dyn Storage>>, Error> {
    StorageContainer::builder()
        .cluster_id(cluster_id)
        .node_id(node_id)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
        .storage(storage)
        .build()
        .await
        .map_err(Into::into)
}

#[allow(dead_code)]
pub(crate) async fn register_broker<S>(
    storage: &S,
    cluster_id: &str,
    node_id: i32,
) -> Result<(), Error>
where
    S: Storage + ?Sized,
{
    storage
        .register_broker(BrokerRegistrationRequest {
            broker_id: node_id,
            cluster_id: cluster_id.to_owned(),
            incarnation_id: Uuid::nil(),
            rack: None,
        })
        .await
        .map_err(Into::into)
}

#[allow(dead_code)]
pub(crate) async fn create_topic<S>(
    storage: &S,
    topic: &str,
    partitions: i32,
) -> Result<Uuid, Error>
where
    S: Storage + ?Sized,
{
    storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.to_owned())
                .num_partitions(partitions)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await
        .map_err(Into::into)
}
