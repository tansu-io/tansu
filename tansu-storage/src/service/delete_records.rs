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
use tansu_sans_io::{ApiKey, Body, DeleteRecordsRequest, DeleteRecordsResponse};

use crate::{Error, Result, Storage};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DeleteRecordsService<S> {
    storage: S,
}

impl<S> ApiKey for DeleteRecordsService<S> {
    const KEY: i16 = DeleteRecordsRequest::KEY;
}

impl<S> DeleteRecordsService<S>
where
    S: Storage,
{
    pub fn new(storage: S) -> Self {
        Self { storage }
    }
}

impl<S, State, Q> Service<State, Q> for DeleteRecordsService<S>
where
    S: Storage,
    State: Clone + Send + Sync + 'static,
    Q: Into<Body> + Send + Sync + 'static,
{
    type Response = Body;
    type Error = Error;

    async fn serve(&self, _ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        let delete_records = DeleteRecordsRequest::try_from(req.into())?;

        self.storage
            .delete_records(delete_records.topics.as_deref().unwrap_or(&[]))
            .await
            .map(Some)
            .map(|topics| {
                DeleteRecordsResponse::default()
                    .throttle_time_ms(0)
                    .topics(topics)
                    .into()
            })
    }
}
