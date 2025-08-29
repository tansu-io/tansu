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
    ApiKey, CreateTopicsRequest, CreateTopicsResponse, ErrorCode, NULL_TOPIC_ID,
    create_topics_response::CreatableTopicResult,
};
use tracing::debug;

use crate::{Error, Result, Storage};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CreateTopicsService;

impl ApiKey for CreateTopicsService {
    const KEY: i16 = CreateTopicsRequest::KEY;
}

impl<G> Service<G, CreateTopicsRequest> for CreateTopicsService
where
    G: Storage,
{
    type Response = CreateTopicsResponse;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<G>,
        req: CreateTopicsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let mut topics = vec![];

        for topic in req.topics.unwrap_or_default() {
            let name = topic.name.clone();

            let num_partitions = Some(match topic.num_partitions {
                -1 => 1,
                otherwise => otherwise,
            });

            let replication_factor = Some(match topic.replication_factor {
                -1 => 3,
                otherwise => otherwise,
            });

            match ctx
                .state()
                .create_topic(topic, req.validate_only.unwrap_or_default())
                .await
            {
                Ok(topic_id) => {
                    debug!(?topic_id);

                    topics.push(
                        CreatableTopicResult::default()
                            .name(name)
                            .topic_id(Some(topic_id.into_bytes()))
                            .error_code(ErrorCode::None.into())
                            .error_message(None)
                            .topic_config_error_code(Some(ErrorCode::None.into()))
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .configs(Some([].into())),
                    );
                }

                Err(Error::Api(error_code)) => topics.push(
                    CreatableTopicResult::default()
                        .name(name)
                        .topic_id(Some(NULL_TOPIC_ID))
                        .error_code(error_code.into())
                        .error_message(Some(error_code.to_string()))
                        .topic_config_error_code(None)
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .configs(Some([].into())),
                ),

                Err(error) => {
                    debug!(?error);

                    topics.push(
                        CreatableTopicResult::default()
                            .name(name)
                            .topic_id(Some(NULL_TOPIC_ID))
                            .error_code(ErrorCode::UnknownServerError.into())
                            .error_message(None)
                            .topic_config_error_code(None)
                            .num_partitions(None)
                            .replication_factor(None)
                            .configs(Some([].into())),
                    )
                }
            }
        }

        Ok(CreateTopicsResponse::default()
            .topics(Some(topics))
            .throttle_time_ms(Some(0)))
    }
}

#[cfg(test)]
mod tests {
    use crate::dynostore::DynoStore;
    use object_store::memory::InMemory;
    use rama::Context;
    use tansu_sans_io::{NULL_TOPIC_ID, create_topics_request::CreatableTopic};

    use super::*;

    #[tokio::test]
    async fn create() -> Result<()> {
        let cluster = "abc";
        let node = 12321;

        let storage = DynoStore::new(cluster, node, InMemory::new());
        let service = CreateTopicsService;
        let ctx = Context::with_state(storage);

        let name = "pqr";
        let num_partitions = 5;
        let replication_factor = 3;
        let assignments = Some([].into());
        let configs = Some([].into());

        let response = service
            .serve(
                ctx,
                CreateTopicsRequest::default()
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name(name.into())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(assignments)
                            .configs(configs),
                    ]))
                    .validate_only(Some(false)),
            )
            .await?;

        let topics = response.topics.unwrap_or_default();

        assert_eq!(1, topics.len());
        assert_eq!(name, topics[0].name.as_str());
        assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
        assert_eq!(Some(5), topics[0].num_partitions);
        assert_eq!(Some(3), topics[0].replication_factor);
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

        Ok(())
    }

    #[tokio::test]
    async fn create_with_default() -> Result<()> {
        let cluster = "abc";
        let node = 12321;

        let storage = DynoStore::new(cluster, node, InMemory::new());
        let service = CreateTopicsService;
        let ctx = Context::with_state(storage);

        let name = "pqr";
        let num_partitions = -1;
        let replication_factor = -1;
        let assignments = Some([].into());
        let configs = Some([].into());

        let response = service
            .serve(
                ctx,
                CreateTopicsRequest::default()
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name(name.into())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(assignments)
                            .configs(configs),
                    ]))
                    .validate_only(Some(false)),
            )
            .await?;

        let topics = response.topics.unwrap_or_default();

        assert_eq!(1, topics.len());
        assert_eq!(name, topics[0].name.as_str());
        assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
        assert_eq!(Some(1), topics[0].num_partitions);
        assert_eq!(Some(3), topics[0].replication_factor);
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

        Ok(())
    }

    #[tokio::test]
    async fn duplicate() -> Result<()> {
        let cluster = "abc";
        let node = 12321;

        let storage = DynoStore::new(cluster, node, InMemory::new());
        let service = CreateTopicsService;
        let ctx = Context::with_state(storage);

        let name = "pqr";
        let num_partitions = 5;
        let replication_factor = 3;
        let assignments = Some([].into());
        let configs = Some([].into());

        let response = service
            .serve(
                ctx.clone(),
                CreateTopicsRequest::default()
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name(name.into())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(assignments.clone())
                            .configs(configs.clone()),
                    ]))
                    .validate_only(Some(false)),
            )
            .await?;

        let topics = response.topics.unwrap_or_default();

        assert_eq!(1, topics.len());
        assert_eq!(name, topics[0].name.as_str());
        assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
        assert_eq!(Some(5), topics[0].num_partitions);
        assert_eq!(Some(3), topics[0].replication_factor);
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

        let response = service
            .serve(
                ctx,
                CreateTopicsRequest::default()
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name(name.into())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(assignments)
                            .configs(configs),
                    ]))
                    .validate_only(Some(false)),
            )
            .await?;

        let topics = response.topics.unwrap_or_default();

        assert_eq!(1, topics.len());
        assert_eq!(name, topics[0].name.as_str());
        assert_eq!(Some(NULL_TOPIC_ID), topics[0].topic_id);
        assert_eq!(Some(5), topics[0].num_partitions);
        assert_eq!(Some(3), topics[0].replication_factor);
        assert_eq!(
            ErrorCode::TopicAlreadyExists,
            ErrorCode::try_from(topics[0].error_code)?
        );
        Ok(())
    }
}
