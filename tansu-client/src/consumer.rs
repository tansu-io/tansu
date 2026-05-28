// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::{
    fmt,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use rama::{Context, Service};
use tansu_sans_io::{
    ApiKey, Frame, Header, HeartbeatResponse, MetadataResponse, RootMessageMeta,
    consumer::{ConsumerAssignor, GroupConsumer},
};
use tokio::time::sleep;
use tracing::debug;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConsumerGroupLayer<A> {
    group_id: String,
    heartbeat_interval: Duration,
    topics: Vec<String>,
    metadata: MetadataResponse,
    assignor: A,
}

impl<A> ConsumerGroupLayer<A>
where
    A: ConsumerAssignor,
{
    pub fn new(
        group_id: impl Into<String>,
        topics: impl IntoIterator<Item = String>,
        metadata: MetadataResponse,
        assignor: A,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            topics: topics.into_iter().collect(),
            metadata,
            assignor,
            heartbeat_interval: Duration::from_secs(3),
        }
    }
}

#[derive(Clone)]
pub struct ConsumerGroupService<S, A> {
    inner: S,
    consumer: Arc<Mutex<GroupConsumer<A>>>,
    heartbeat_interval: Duration,
}

impl<S, A> fmt::Debug for ConsumerGroupService<S, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(ConsumerGroupService))
            .field("heartbeat", &self.heartbeat_interval)
            .finish_non_exhaustive()
    }
}

impl<State, S, A> Service<State, ()> for ConsumerGroupService<S, A>
where
    S: Service<State, Frame, Response = Frame>,
    A: ConsumerAssignor + Send + Sync + 'static,
    S::Error:
        From<tansu_sans_io::Error> + for<'a> From<PoisonError<MutexGuard<'a, GroupConsumer<A>>>>,
    State: Clone + Send + Sync + 'static,
{
    type Response = S::Response;

    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, _: ()) -> Result<Self::Response, Self::Error> {
        let mut input = None;

        loop {
            debug!(?input);

            let next_action = self
                .consumer
                .lock()
                .map_err(Self::Error::from)
                .and_then(|mut consumer| {
                    consumer
                        .next_action(input)
                        .inspect(|next_action| debug!(?next_action))
                        .map_err(Into::into)
                })
                .map(|next_action| {
                    let api_key = next_action.api_key();
                    let api_version = RootMessageMeta::messages()
                        .requests()
                        .get(&api_key)
                        .map(|message_meta| message_meta.version.valid().end)
                        .unwrap_or_default();

                    Frame {
                        size: 0,
                        header: Header::Request {
                            api_key,
                            api_version,
                            correlation_id: 0,
                            client_id: Some(env!("CARGO_PKG_NAME").into()),
                        },
                        body: next_action,
                    }
                })?;

            input = self
                .inner
                .serve(ctx.clone(), next_action)
                .await
                .map(|frame| frame.body)
                .inspect(|input| debug!(input.api_name = input.api_name()))
                .map(Some)?;

            if input
                .as_ref()
                .is_some_and(|input| input.api_key() == HeartbeatResponse::KEY)
            {
                sleep(self.heartbeat_interval).await
            }
        }
    }
}
