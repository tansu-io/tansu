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
use tansu_sans_io::{ApiKey, DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse};

use crate::{Error, Result, Storage, TopicId};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DescribeTopicPartitionsService;

impl ApiKey for DescribeTopicPartitionsService {
    const KEY: i16 = DescribeTopicPartitionsRequest::KEY;
}

impl<G> Service<G, DescribeTopicPartitionsRequest> for DescribeTopicPartitionsService
where
    G: Storage,
{
    type Response = DescribeTopicPartitionsResponse;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<G>,
        req: DescribeTopicPartitionsRequest,
    ) -> Result<Self::Response, Self::Error> {
        ctx.state()
            .describe_topic_partitions(
                req.topics
                    .as_ref()
                    .map(|topics| topics.iter().map(TopicId::from).collect::<Vec<_>>())
                    .as_deref(),
                req.response_partition_limit,
                req.cursor.map(Into::into),
            )
            .await
            .map(|topics| {
                DescribeTopicPartitionsResponse::default()
                    .throttle_time_ms(0)
                    .topics(Some(topics))
                    .next_cursor(None)
            })
    }
}
