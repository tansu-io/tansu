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

use crate::Result;
use tansu_sans_io::{
    Body, delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsResponse,
};
use tansu_storage::Storage;
use tracing::debug;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DeleteRecordsRequest<S> {
    storage: S,
}

impl<S> DeleteRecordsRequest<S>
where
    S: Storage,
{
    pub fn with_storage(storage: S) -> Self {
        Self { storage }
    }

    pub async fn request(&mut self, topics: &[DeleteRecordsTopic]) -> Result<Body> {
        let topics = self
            .storage
            .delete_records(topics)
            .await
            .inspect_err(|err| debug!(?err, ?topics))
            .map(Some)?;

        Ok(DeleteRecordsResponse::default()
            .throttle_time_ms(0)
            .topics(topics)
            .into())
    }
}
