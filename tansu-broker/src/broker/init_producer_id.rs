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

use crate::Result;
use tansu_storage::{ProducerIdResponse, Storage};
use tracing::debug;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InitProducerIdRequest<S> {
    storage: S,
}

impl<S> InitProducerIdRequest<S>
where
    S: Storage,
{
    pub fn with_storage(storage: S) -> Self {
        Self { storage }
    }

    pub async fn response(
        &mut self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        debug!(
            ?transaction_id,
            ?transaction_timeout_ms,
            ?producer_id,
            ?producer_epoch
        );

        self.storage
            .init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            )
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use tansu_sans_io::ErrorCode;
    use tansu_storage::StorageContainer;
    use tracing::subscriber::DefaultGuard;
    use url::Url;

    #[cfg(miri)]
    fn init_tracing() -> Result<()> {
        Ok(())
    }

    #[cfg(not(miri))]
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
                        .ok_or(Error::Custom(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME")))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    #[tokio::test]
    async fn no_txn_init_producer_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = "abc";
        let node = 12321;

        let transaction_id = None;
        let transaction_timeout_ms = 0;
        let producer_id = Some(-1);
        let producer_epoch = Some(-1);

        let storage = StorageContainer::builder()
            .cluster_id(cluster)
            .node_id(node)
            .advertised_listener(Url::parse("tcp://localhost:9092")?)
            .schema_registry(None)
            .storage(Url::parse("memory://")?)
            .build()
            .await?;

        let mut request = InitProducerIdRequest::with_storage(storage);

        assert_eq!(
            ProducerIdResponse {
                error: ErrorCode::None,
                id: 1,
                epoch: 0
            },
            request
                .response(
                    transaction_id,
                    transaction_timeout_ms,
                    producer_id,
                    producer_epoch,
                )
                .await?
        );

        assert_eq!(
            ProducerIdResponse {
                error: ErrorCode::None,
                id: 2,
                epoch: 0
            },
            request
                .response(
                    transaction_id,
                    transaction_timeout_ms,
                    producer_id,
                    producer_epoch,
                )
                .await?
        );

        Ok(())
    }
}
