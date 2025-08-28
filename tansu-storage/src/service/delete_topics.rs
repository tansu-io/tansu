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
    ApiKey, DeleteTopicsRequest, DeleteTopicsResponse, delete_topics_response::DeletableTopicResult,
};

use crate::{Error, Result, Storage};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DeleteTopicsService;

impl ApiKey for DeleteTopicsService {
    const KEY: i16 = DeleteTopicsRequest::KEY;
}

impl<G> Service<G, DeleteTopicsRequest> for DeleteTopicsService
where
    G: Storage,
{
    type Response = DeleteTopicsResponse;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<G>,
        req: DeleteTopicsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let mut responses = vec![];

        for topic in req.topics.unwrap_or_default() {
            let error_code = ctx.state().delete_topic(&topic.clone().into()).await?;
            responses.push(
                DeletableTopicResult::default()
                    .name(topic.name.clone())
                    .topic_id(Some(topic.topic_id))
                    .error_code(i16::from(error_code))
                    .error_message(Some(error_code.to_string())),
            );
        }

        for topic in req.topic_names.unwrap_or_default() {
            let error_code = ctx.state().delete_topic(&topic.clone().into()).await?;

            responses.push(
                DeletableTopicResult::default()
                    .name(Some(topic))
                    .topic_id(None)
                    .error_code(i16::from(error_code))
                    .error_message(Some(error_code.to_string())),
            );
        }

        Ok(DeleteTopicsResponse::default()
            .throttle_time_ms(Some(0))
            .responses(Some(responses)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{dynostore::DynoStore, service::create_topics::CreateTopicsService};
    use assert_matches::assert_matches;
    use object_store::memory::InMemory;
    use rama::Context;
    use tansu_sans_io::{
        CreateTopicsRequest, CreateTopicsResponse, ErrorCode, NULL_TOPIC_ID,
        create_topics_request::CreatableTopic, delete_topics_request::DeleteTopicState,
    };
    use uuid::Uuid;

    #[tokio::test]
    async fn delete_unknown_by_name() -> Result<()> {
        let cluster = "abc";
        let node = 12321;

        let storage = DynoStore::new(cluster, node, InMemory::new());
        let service = DeleteTopicsService;
        let ctx = Context::with_state(storage);

        let topic = "pqr";

        let error_code = ErrorCode::UnknownTopicOrPartition;

        assert_eq!(
            DeleteTopicsResponse::default()
                .throttle_time_ms(Some(0))
                .responses(Some(vec![
                    DeletableTopicResult::default()
                        .error_code(error_code.into())
                        .error_message(Some(error_code.to_string()))
                        .name(Some(topic.into())),
                ])),
            service
                .serve(
                    ctx,
                    DeleteTopicsRequest::default().topic_names(Some(vec![topic.into()]))
                )
                .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn delete_unknown_by_uuid() -> Result<()> {
        let cluster = "abc";
        let node = 12321;

        let storage = DynoStore::new(cluster, node, InMemory::new());
        let service = DeleteTopicsService;
        let ctx = Context::with_state(storage);

        let topic = Uuid::new_v4();

        let error_code = ErrorCode::UnknownTopicOrPartition;

        assert_eq!(
            DeleteTopicsResponse::default()
                .throttle_time_ms(Some(0))
                .responses(Some(vec![
                    DeletableTopicResult::default()
                        .error_code(error_code.into())
                        .error_message(Some(error_code.to_string()))
                        .topic_id(Some(topic.into_bytes()))
                ])),
            service
                .serve(
                    ctx,
                    DeleteTopicsRequest::default().topics(Some(vec![
                        DeleteTopicState::default().topic_id(topic.into_bytes())
                    ]))
                )
                .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn create_delete_create_by_name() -> Result<()> {
        let cluster = "abc";
        let node = 12321;

        let storage = DynoStore::new(cluster, node, InMemory::new());
        let create_topics = CreateTopicsService;
        let ctx = Context::with_state(storage);

        let name = "pqr";
        let num_partitions = 5;
        let replication_factor = 3;
        let assignments = Some([].into());
        let configs = Some([].into());

        let topic = "pqr";

        let error_code = ErrorCode::None;

        assert_matches!(
            create_topics
                .serve(
                    ctx.clone(),
                    CreateTopicsRequest::default()
                        .topics(Some(
                            [CreatableTopic::default()
                                .name(name.into())
                                .num_partitions(num_partitions)
                                .replication_factor(replication_factor)
                                .assignments(assignments.clone())
                                .configs(configs.clone()),]
                            .into()
                        ))
                        .validate_only(Some(false))
                )
                .await?,
            CreateTopicsResponse { topics: Some(topics), ..} => {
                assert_eq!(topics.len(), 1);
                assert_eq!(topic, topics[0].name.as_str());
                assert_matches!(topics[0].configs.as_ref(), Some(configs) if configs.len() == 0);
                assert_eq!(topics[0].topic_config_error_code, Some(0));
                assert_eq!(topics[0].num_partitions, Some(num_partitions));
                assert_eq!(topics[0].replication_factor, Some(replication_factor));
                assert_eq!(topics[0].error_code, i16::from(error_code));
            }
        );

        let delete_topics = DeleteTopicsService;

        let error_code = ErrorCode::None;

        assert_eq!(
            DeleteTopicsResponse::default()
                .throttle_time_ms(Some(0))
                .responses(Some(vec![
                    DeletableTopicResult::default()
                        .error_code(error_code.into())
                        .error_message(Some(error_code.to_string()))
                        .name(Some(name.into()))
                        .topic_id(Some(NULL_TOPIC_ID)),
                ])),
            delete_topics
                .serve(
                    ctx.clone(),
                    DeleteTopicsRequest::default().topics(Some(vec![
                        DeleteTopicState::default()
                            .name(Some(name.into()))
                            .topic_id(NULL_TOPIC_ID),
                    ]))
                )
                .await?
        );

        assert_matches!(
            create_topics
                .serve(
                    ctx.clone(),
                    CreateTopicsRequest::default()
                        .topics(Some(
                            [CreatableTopic::default()
                                .name(name.into())
                                .num_partitions(num_partitions)
                                .replication_factor(replication_factor)
                                .assignments(assignments.clone())
                                .configs(configs.clone()),]
                            .into()
                        ))
                        .validate_only(Some(false))
                )
                .await?,
            CreateTopicsResponse { topics: Some(topics), ..} => {
                assert_eq!(topics.len(), 1);
                assert_eq!(topic, topics[0].name.as_str());
                assert_matches!(topics[0].configs.as_ref(), Some(configs) if configs.len() == 0);
                assert_eq!(topics[0].topic_config_error_code, Some(0));
                assert_eq!(topics[0].num_partitions, Some(num_partitions));
                assert_eq!(topics[0].replication_factor, Some(replication_factor));
                assert_eq!(topics[0].error_code, i16::from(error_code));
            }
        );

        Ok(())
    }
}
