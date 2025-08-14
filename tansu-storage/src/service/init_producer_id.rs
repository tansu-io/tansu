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

use rama::{Context, Service};
use tansu_sans_io::{ApiKey, Body, InitProducerIdRequest, InitProducerIdResponse};

use crate::{Error, Result, Storage};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InitProducerIdService<S> {
    storage: S,
}

impl<S> ApiKey for InitProducerIdService<S> {
    const KEY: i16 = InitProducerIdRequest::KEY;
}

impl<S> InitProducerIdService<S>
where
    S: Storage,
{
    pub fn new(storage: S) -> Self {
        Self { storage }
    }
}

impl<S, State, Q> Service<State, Q> for InitProducerIdService<S>
where
    S: Storage,
    State: Clone + Send + Sync + 'static,
    Q: Into<Body> + Send + Sync + 'static,
{
    type Response = Body;
    type Error = Error;

    async fn serve(&self, _ctx: Context<State>, request: Q) -> Result<Self::Response, Self::Error> {
        let init_producer_id = InitProducerIdRequest::try_from(request.into())?;

        self.storage
            .init_producer(
                init_producer_id.transactional_id.as_deref(),
                init_producer_id.transaction_timeout_ms,
                init_producer_id.producer_id,
                init_producer_id.producer_epoch,
            )
            .await
            .map(|response| {
                InitProducerIdResponse::default()
                    .throttle_time_ms(0)
                    .error_code(response.error.into())
                    .producer_id(response.id)
                    .producer_epoch(response.epoch)
                    .into()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Error, dynostore::DynoStore};
    use object_store::memory::InMemory;
    use rama::Context;
    use tansu_sans_io::ErrorCode;
    use tracing::subscriber::DefaultGuard;

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

    #[tokio::test]
    async fn no_txn_init_producer_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = "abc";
        let node = 12321;

        let transactional_id = None;
        let transaction_timeout_ms = 0;
        let producer_id = Some(-1);
        let producer_epoch = Some(-1);

        let service = InitProducerIdService::new(DynoStore::new(cluster, node, InMemory::new()));

        assert_eq!(
            service
                .serve(
                    Context::default(),
                    InitProducerIdRequest::default()
                        .transactional_id(transactional_id.clone())
                        .transaction_timeout_ms(transaction_timeout_ms)
                        .producer_id(producer_id)
                        .producer_epoch(producer_epoch)
                )
                .await?,
            InitProducerIdResponse::default()
                .error_code(ErrorCode::None.into())
                .producer_id(1)
                .producer_epoch(0)
                .into()
        );

        assert_eq!(
            service
                .serve(
                    Context::default(),
                    InitProducerIdRequest::default()
                        .transactional_id(transactional_id)
                        .transaction_timeout_ms(transaction_timeout_ms)
                        .producer_id(producer_id)
                        .producer_epoch(producer_epoch)
                )
                .await?,
            InitProducerIdResponse::default()
                .error_code(ErrorCode::None.into())
                .producer_id(2)
                .producer_epoch(0)
                .into()
        );

        Ok(())
    }
}
