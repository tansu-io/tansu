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
use tansu_sans_io::{
    ApiKey, Body, DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse,
};

use crate::{Error, Result, Storage, TopicId};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DescribeTopicPartitionsService<S> {
    storage: S,
}

impl<S> ApiKey for DescribeTopicPartitionsService<S> {
    const KEY: i16 = DescribeTopicPartitionsRequest::KEY;
}

impl<S> DescribeTopicPartitionsService<S>
where
    S: Storage,
{
    pub fn new(storage: S) -> Self {
        Self { storage }
    }
}

impl<S, State, Q> Service<State, Q> for DescribeTopicPartitionsService<S>
where
    S: Storage,
    State: Clone + Send + Sync + 'static,
    Q: Into<Body> + Send + Sync + 'static,
{
    type Response = Body;
    type Error = Error;

    async fn serve(&self, _ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        let describe_topitions = DescribeTopicPartitionsRequest::try_from(req.into())?;
        self.storage
            .describe_topic_partitions(
                describe_topitions
                    .topics
                    .as_ref()
                    .map(|topics| topics.iter().map(TopicId::from).collect::<Vec<_>>())
                    .as_deref(),
                describe_topitions.response_partition_limit,
                describe_topitions.cursor.map(Into::into),
            )
            .await
            .map(|topics| {
                DescribeTopicPartitionsResponse::default()
                    .throttle_time_ms(0)
                    .topics(Some(topics))
                    .next_cursor(None)
                    .into()
            })
    }
}
