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

use crate::Result;
use tansu_kafka_sans_io::{
    delete_topics_request::DeleteTopicState, delete_topics_response::DeletableTopicResult,
};
use tansu_storage::Storage;
use tracing::debug;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DeleteTopicsRequest<S> {
    storage: S,
}

impl<S> DeleteTopicsRequest<S>
where
    S: Storage,
{
    pub fn with_storage(storage: S) -> Self {
        Self { storage }
    }

    pub async fn response(
        &mut self,
        topics: Option<Vec<DeleteTopicState>>,
        topic_names: Option<Vec<String>>,
    ) -> Result<Vec<DeletableTopicResult>> {
        debug!(?topics, ?topic_names);

        let mut responses = vec![];

        if let Some(topics) = topics {
            for topic in topics {
                let error_code = self.storage.delete_topic(&topic.clone().into()).await?;

                responses.push(DeletableTopicResult {
                    name: topic.name.clone(),
                    topic_id: Some(topic.topic_id),
                    error_code: i16::from(error_code),
                    error_message: Some(error_code.to_string()),
                });
            }
        }

        if let Some(topic_names) = topic_names {
            for name in topic_names {
                let topic_id = name.clone().into();
                let error_code = self.storage.delete_topic(&topic_id).await?;

                responses.push(DeletableTopicResult {
                    name: Some(name),
                    topic_id: None,
                    error_code: i16::from(error_code),
                    error_message: Some(error_code.to_string()),
                });
            }
        }

        Ok(responses)
    }
}

#[cfg(test)]
mod tests {
    use crate::broker::create_topic::CreateTopic;

    use super::*;
    use object_store::memory::InMemory;
    use tansu_kafka_sans_io::{create_topics_request::CreatableTopic, ErrorCode};
    use tansu_storage::{dynostore::DynoStore, NULL_TOPIC_ID};
    use uuid::Uuid;

    #[tokio::test]
    async fn delete_unknown_by_name() -> Result<()> {
        let cluster = "abc";
        let node = 12321;

        let mut storage = DynoStore::new(cluster, node, InMemory::new());

        let topic = "pqr";

        assert_eq!(
            ErrorCode::UnknownTopicOrPartition,
            storage.delete_topic(&topic.into()).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn delete_unknown_by_uuid() -> Result<()> {
        let cluster = "abc";
        let node = 12321;

        let mut storage = DynoStore::new(cluster, node, InMemory::new());

        let topic = Uuid::new_v4();

        assert_eq!(
            ErrorCode::UnknownTopicOrPartition,
            storage.delete_topic(&topic.into()).await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn create_delete_create_by_name() -> Result<()> {
        let cluster = "abc";
        let node = 12321;

        let storage = DynoStore::new(cluster, node, InMemory::new());

        let name = "pqr";
        let num_partitions = 5;
        let replication_factor = 3;
        let assignments = Some([].into());
        let configs = Some([].into());
        let validate_only = false;

        let created = CreateTopic::with_storage(storage.clone())
            .response(
                Some(vec![CreatableTopic {
                    name: name.into(),
                    num_partitions,
                    replication_factor,
                    assignments: assignments.clone(),
                    configs: configs.clone(),
                }]),
                validate_only,
            )
            .await?;

        assert_eq!(1, created.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(created[0].error_code)?);

        let deleted = DeleteTopicsRequest::with_storage(storage.clone())
            .response(
                Some(vec![DeleteTopicState {
                    name: Some(name.into()),
                    topic_id: NULL_TOPIC_ID,
                }]),
                Some(vec![]),
            )
            .await?;

        assert_eq!(1, deleted.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(deleted[0].error_code)?);
        assert_eq!(Some(NULL_TOPIC_ID), deleted[0].topic_id);

        let created = CreateTopic::with_storage(storage.clone())
            .response(
                Some(vec![CreatableTopic {
                    name: name.into(),
                    num_partitions,
                    replication_factor,
                    assignments,
                    configs,
                }]),
                validate_only,
            )
            .await?;

        assert_eq!(1, created.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(created[0].error_code)?);

        Ok(())
    }
}
