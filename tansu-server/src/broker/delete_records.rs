// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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
use tansu_kafka_sans_io::{Body, delete_records_request::DeleteRecordsTopic};
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

        Ok(Body::DeleteRecordsResponse {
            throttle_time_ms: 0,
            topics,
        })
    }
}
