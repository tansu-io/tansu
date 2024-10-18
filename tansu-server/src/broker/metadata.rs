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

use tansu_kafka_sans_io::{metadata_request::MetadataRequestTopic, Body};
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

        Ok(Body::MetadataResponse {
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
            cluster_authorized_operations,
        })
    }
}
