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

use tansu_client::{Client, Manager};
use tansu_sans_io::{
    DeleteTopicsRequest, DeleteTopicsResponse, ErrorCode, NULL_TOPIC_ID,
    delete_topics_request::DeleteTopicState, delete_topics_response::DeletableTopicResult,
};
use tracing::debug;
use url::Url;

use crate::{Error, Result};

use super::Topic;

#[derive(Clone, Debug, Default)]
pub struct Builder<B, N> {
    broker: B,
    name: N,
}

impl<B, N> Builder<B, N> {
    pub fn broker(self, broker: Url) -> Builder<Url, N> {
        Builder {
            broker,
            name: self.name,
        }
    }

    pub fn name(self, name: impl Into<String>) -> Builder<B, String> {
        Builder {
            broker: self.broker,
            name: name.into(),
        }
    }
}

impl Builder<Url, String> {
    pub fn build(self) -> Topic {
        Topic::Delete(Configuration {
            broker: self.broker,
            name: self.name,
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Configuration {
    broker: Url,
    name: String,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct Delete {
    configuration: Configuration,
}

impl TryFrom<Configuration> for Delete {
    type Error = Error;

    fn try_from(configuration: Configuration) -> Result<Self, Self::Error> {
        Ok(Delete { configuration })
    }
}

impl Delete {
    pub(crate) async fn main(self) -> Result<ErrorCode> {
        let client = Manager::builder(self.configuration.broker.clone())
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .build()
            .await
            .inspect(|pool| debug!(?pool))
            .map(Client::new)?;

        let timeout_ms = 30_000;

        let req = DeleteTopicsRequest::default()
            .topics(Some(
                [DeleteTopicState::default()
                    .name(Some(self.configuration.name))
                    .topic_id(NULL_TOPIC_ID)]
                .into(),
            ))
            .topic_names(None)
            .timeout_ms(timeout_ms);

        let DeleteTopicsResponse { responses, .. } = client
            .call(req)
            .await
            .inspect(|response| debug!(?response))?;

        let responses = responses.unwrap_or_default();
        assert_eq!(1, responses.len());

        let DeletableTopicResult { error_code, .. } =
            responses.first().expect("responses: {responses:?}");

        ErrorCode::try_from(error_code).map_err(Into::into)
    }
}
