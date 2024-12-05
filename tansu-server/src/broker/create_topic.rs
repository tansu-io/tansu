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
    create_topics_request::CreatableTopic, create_topics_response::CreatableTopicResult, ErrorCode,
};
use tansu_storage::Storage;
use tracing::debug;

#[derive(Clone, Debug)]
pub struct CreateTopic<S> {
    storage: S,
}

impl<S> CreateTopic<S>
where
    S: Storage,
{
    pub fn with_storage(storage: S) -> Self {
        Self { storage }
    }

    async fn create_topic(
        &mut self,
        mut topic: CreatableTopic,
        validate_only: bool,
    ) -> CreatableTopicResult {
        let _ = validate_only;

        if topic.num_partitions == -1 {
            topic.num_partitions = 1;
        }

        if topic.replication_factor == -1 {
            topic.replication_factor = 3
        }

        let name = topic.name.clone();
        let num_partitions = Some(topic.num_partitions);
        let replication_factor = Some(topic.replication_factor);

        match self.storage.create_topic(topic, validate_only).await {
            Ok(topic_id) => {
                debug!(?topic_id);

                CreatableTopicResult {
                    name,
                    topic_id: Some(topic_id.into_bytes()),
                    error_code: ErrorCode::None.into(),
                    error_message: None,
                    topic_config_error_code: Some(ErrorCode::None.into()),
                    num_partitions,
                    replication_factor,
                    configs: Some([].into()),
                }
            }

            Err(tansu_storage::Error::Api(error_code)) => CreatableTopicResult {
                name,
                topic_id: Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
                error_code: error_code.into(),
                error_message: Some(error_code.to_string()),
                topic_config_error_code: None,
                num_partitions,
                replication_factor,
                configs: Some([].into()),
            },

            Err(error) => {
                debug!(?error);

                CreatableTopicResult {
                    name,
                    topic_id: Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
                    error_code: ErrorCode::UnknownServerError.into(),
                    error_message: None,
                    topic_config_error_code: None,
                    num_partitions: None,
                    replication_factor: None,
                    configs: Some([].into()),
                }
            }
        }
    }

    pub async fn response(
        &mut self,
        creatable: Option<Vec<CreatableTopic>>,
        validate_only: bool,
    ) -> Result<Vec<CreatableTopicResult>> {
        debug!(?creatable, ?validate_only);

        let mut topics =
            Vec::with_capacity(creatable.as_ref().map_or(0, |creatable| creatable.len()));

        if let Some(creatable) = creatable {
            for topic in creatable {
                topics.push(self.create_topic(topic, validate_only).await)
            }
        }

        Ok(topics)
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use tansu_storage::{dynostore::DynoStore, NULL_TOPIC_ID};

    use super::*;

    #[tokio::test]
    async fn create() -> Result<()> {
        let cluster = "abc";
        let node = 12321;

        let storage = DynoStore::new(cluster, node, InMemory::new());

        let mut create_topic = CreateTopic::with_storage(storage);

        let name = "pqr";
        let num_partitions = 5;
        let replication_factor = 3;
        let assignments = Some([].into());
        let configs = Some([].into());
        let validate_only = false;

        let r = create_topic
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

        assert_eq!(1, r.len());
        assert_eq!(name, r[0].name.as_str());
        assert_ne!(Some(NULL_TOPIC_ID), r[0].topic_id);
        assert_eq!(Some(5), r[0].num_partitions);
        assert_eq!(Some(3), r[0].replication_factor);
        assert_eq!(ErrorCode::None, ErrorCode::try_from(r[0].error_code)?);

        Ok(())
    }

    #[tokio::test]
    async fn create_with_default() -> Result<()> {
        let cluster = "abc";
        let node = 12321;

        let storage = DynoStore::new(cluster, node, InMemory::new());

        let mut create_topic = CreateTopic::with_storage(storage);

        let name = "pqr";
        let num_partitions = -1;
        let replication_factor = -1;
        let assignments = Some([].into());
        let configs = Some([].into());
        let validate_only = false;

        let r = create_topic
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

        assert_eq!(1, r.len());
        assert_eq!(name, r[0].name.as_str());
        assert_ne!(Some(NULL_TOPIC_ID), r[0].topic_id);
        assert_eq!(Some(1), r[0].num_partitions);
        assert_eq!(Some(3), r[0].replication_factor);
        assert_eq!(ErrorCode::None, ErrorCode::try_from(r[0].error_code)?);

        Ok(())
    }

    #[tokio::test]
    async fn duplicate() -> Result<()> {
        let cluster = "abc";
        let node = 12321;

        let storage = DynoStore::new(cluster, node, InMemory::new());

        let mut create_topic = CreateTopic::with_storage(storage);

        let name = "pqr";
        let num_partitions = 5;
        let replication_factor = 3;
        let assignments = Some([].into());
        let configs = Some([].into());
        let validate_only = false;

        let r = create_topic
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

        assert_eq!(1, r.len());
        assert_eq!(name, r[0].name.as_str());
        assert_ne!(Some(NULL_TOPIC_ID), r[0].topic_id);
        assert_eq!(Some(5), r[0].num_partitions);
        assert_eq!(Some(3), r[0].replication_factor);
        assert_eq!(ErrorCode::None, ErrorCode::try_from(r[0].error_code)?);

        let r = create_topic
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

        assert_eq!(1, r.len());
        assert_eq!(name, r[0].name.as_str());
        assert_eq!(Some(NULL_TOPIC_ID), r[0].topic_id);
        assert_eq!(Some(5), r[0].num_partitions);
        assert_eq!(Some(3), r[0].replication_factor);
        assert_eq!(
            ErrorCode::TopicAlreadyExists,
            ErrorCode::try_from(r[0].error_code)?
        );
        Ok(())
    }
}
