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

use tansu_sans_io::{
    Body, metadata_request::MetadataRequestTopic, metadata_response::MetadataResponse,
};
use tansu_storage::{Storage, TopicId};
use tracing::error;

use crate::Result;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MetadataRequest<S> {
    storage: S,
}

impl<S> MetadataRequest<S>
where
    S: Storage,
{
    pub fn with_storage(storage: S) -> Self {
        Self { storage }
    }

    pub async fn response(&mut self, topics: Option<Vec<MetadataRequestTopic>>) -> Result<Body> {
        let throttle_time_ms = Some(0);

        let topics = topics.map(|topics| topics.iter().map(TopicId::from).collect::<Vec<_>>());

        let response = self
            .storage
            .metadata(topics.as_deref())
            .await
            .inspect_err(|err| error!(?err))?;
        let brokers = Some(response.brokers().to_owned());
        let cluster_id = response.cluster().map(|s| s.into());
        let controller_id = response.controller();
        let topics = Some(response.topics().to_owned());
        let cluster_authorized_operations = None;

        Ok(MetadataResponse::default()
            .throttle_time_ms(throttle_time_ms)
            .brokers(brokers)
            .cluster_id(cluster_id)
            .controller_id(controller_id)
            .topics(topics)
            .cluster_authorized_operations(cluster_authorized_operations)
            .into())
    }
}
