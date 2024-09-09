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

use tansu_kafka_sans_io::{
    create_topics_request::CreatableTopic, create_topics_response::CreatableTopicResult, Body,
    ErrorCode,
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
        &self,
        topic: CreatableTopic,
        validate_only: bool,
    ) -> CreatableTopicResult {
        let _ = validate_only;

        let config = topic
            .configs
            .as_ref()
            .map(|configs| {
                configs
                    .iter()
                    .map(|config| (config.name.as_str(), config.value.as_deref()))
                    .collect()
            })
            .unwrap_or(vec![]);

        if let Ok(topic_id) = self
            .storage
            .create_topic(topic.name.as_str(), topic.num_partitions, config.as_slice())
            .await
        {
            CreatableTopicResult {
                name: topic.name,
                topic_id: Some(topic_id.into_bytes()),
                error_code: ErrorCode::None.into(),
                error_message: None,
                topic_config_error_code: Some(ErrorCode::None.into()),
                num_partitions: Some(topic.num_partitions),
                replication_factor: Some(3),
                configs: Some([].into()),
            }
        } else {
            CreatableTopicResult {
                name: topic.name,
                topic_id: None,
                error_code: ErrorCode::UnknownServerError.into(),
                error_message: None,
                topic_config_error_code: None,
                num_partitions: None,
                replication_factor: None,
                configs: Some([].into()),
            }
        }
    }

    pub async fn request(
        &self,
        creatable: Option<Vec<CreatableTopic>>,
        validate_only: bool,
    ) -> Body {
        debug!(?creatable, ?validate_only);

        let mut topics =
            Vec::with_capacity(creatable.as_ref().map_or(0, |creatable| creatable.len()));

        if let Some(creatable) = creatable {
            for topic in creatable {
                topics.push(self.create_topic(topic, validate_only).await)
            }
        }

        Body::CreateTopicsResponse {
            throttle_time_ms: Some(0),
            topics: Some(topics),
        }
    }
}
