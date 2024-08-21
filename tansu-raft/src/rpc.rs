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

use core::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;
use url::Url;

use crate::{Index, LogEntry, Outcome, Result, Term};

#[async_trait]
pub trait RaftRpc: Debug + Send + Sync {
    #[allow(clippy::too_many_arguments)]
    async fn append_entries(
        &self,
        recipient: Url,
        leaders_term: Term,
        leader_id: Url,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: Index,
    ) -> Result<Outcome>;

    async fn request_vote(
        &self,
        recipient: Url,
        candidates_term: Term,
        candidate_id: Url,
        last_log_index: Index,
    ) -> Result<Outcome>;

    async fn log(&self, recipient: Url, entry: Bytes) -> Result<Index>;
}

#[async_trait]
pub trait ProvideRaftRpc: Send {
    async fn provide_raft_rpc(&self) -> Result<Box<dyn RaftRpc>>;
}
