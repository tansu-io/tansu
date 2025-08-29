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
use tansu_sans_io::{ApiKey, MetadataRequest, MetadataResponse};
use tracing::{debug, error};

use crate::{Error, Result, Storage, TopicId};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MetadataService;

impl ApiKey for MetadataService {
    const KEY: i16 = MetadataRequest::KEY;
}

impl<G> Service<G, MetadataRequest> for MetadataService
where
    G: Storage,
{
    type Response = MetadataResponse;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<G>,
        req: MetadataRequest,
    ) -> Result<Self::Response, Self::Error> {
        debug!(?req);

        let topics = req
            .topics
            .map(|topics| topics.iter().map(TopicId::from).collect::<Vec<_>>());

        let response = ctx
            .state()
            .metadata(topics.as_deref())
            .await
            .inspect_err(|err| error!(?err))?;
        let brokers = Some(response.brokers().to_owned());
        let cluster_id = response.cluster().map(|s| s.into());
        let controller_id = response.controller();
        let topics = Some(response.topics().to_owned());
        let cluster_authorized_operations = Some(-1);

        let throttle_time_ms = Some(0);

        Ok(MetadataResponse::default()
            .throttle_time_ms(throttle_time_ms)
            .brokers(brokers)
            .cluster_id(cluster_id)
            .controller_id(controller_id)
            .topics(topics)
            .cluster_authorized_operations(cluster_authorized_operations))
    }
}
