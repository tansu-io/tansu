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

use crate::{
    log::Log, request_vote::RequestVote, AppendEntries, Error, Index, LogEntry, Outcome,
    ProvideRaftRpc, Raft, RaftRpc, Result, Term,
};
use bytes::Bytes;
use futures::{future, prelude::*};
use tarpc::{
    client, context,
    server::{incoming::Incoming, Channel},
    tokio_serde::formats::Json,
};
use tracing::debug;
use url::Url;

#[tarpc::service]
pub trait TarpcRaft {
    #[allow(clippy::too_many_arguments)]
    async fn append_entries(
        leaders_term: Term,
        leader_id: Url,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: Index,
    ) -> Result<Outcome>;

    async fn request_vote(
        candidates_term: Term,
        candidate_id: Url,
        last_log_index: Index,
    ) -> Result<Outcome>;

    async fn log(entry: Bytes) -> Result<Index>;
}

#[derive(Clone, Debug)]
struct TcpTarpcServer {
    inner: Raft,
}

impl TarpcRaft for TcpTarpcServer {
    #[allow(clippy::too_many_arguments)]
    async fn append_entries(
        mut self,
        _context: ::tarpc::context::Context,
        leaders_term: Term,
        leader_id: Url,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: Index,
    ) -> Result<Outcome> {
        self.inner
            .append_entries(
                leaders_term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            )
            .await
    }

    async fn request_vote(
        mut self,
        _context: ::tarpc::context::Context,
        candidates_term: Term,
        candidate_id: Url,
        last_log_index: Index,
    ) -> Result<Outcome> {
        self.inner
            .request_vote(candidates_term, candidate_id, last_log_index)
            .await
    }

    async fn log(mut self, _context: ::tarpc::context::Context, entry: Bytes) -> Result<Index> {
        self.inner.log(entry).await
    }
}

pub async fn server(raft: Raft) -> Result<()> {
    let listener = raft.configuration.listener_url()?;
    debug!("listener: {}", listener.as_str());

    let server_address = (
        listener.host_str().unwrap_or("localhost"),
        listener.port().unwrap_or(4567),
    );
    debug!(?server_address);

    let mut listener = tarpc::serde_transport::tcp::listen(&server_address, Json::default).await?;

    _ = listener.config_mut().max_frame_length(usize::MAX);
    listener
        .filter_map(|r| future::ready(r.ok()))
        .map(tarpc::server::BaseChannel::with_defaults)
        .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
        .map(|channel| {
            let server = TcpTarpcServer {
                inner: raft.clone(),
            };

            channel.execute(server.serve()).for_each(spawn)
        })
        .buffer_unordered(10)
        .for_each(|_| async {})
        .await;
    Ok(())
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    _ = tokio::spawn(fut);
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct RaftTcpTarpcClientFactory;

#[async_trait::async_trait]
impl ProvideRaftRpc for RaftTcpTarpcClientFactory {
    async fn provide_raft_rpc(&self) -> Result<Box<dyn RaftRpc>> {
        Ok(Box::<RaftTcpTarpcClient>::default())
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
struct RaftTcpTarpcClient;

#[async_trait::async_trait]
impl RaftRpc for RaftTcpTarpcClient {
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
    ) -> Result<Outcome> {
        self.client_for_url(&recipient)
            .await?
            .append_entries(
                context::current(),
                leaders_term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            )
            .await
            .map_err(|error| error.into())
            .and_then(|flatten| flatten)
    }

    async fn request_vote(
        &self,
        recipient: Url,
        candidates_term: Term,
        candidate_id: Url,
        last_log_index: Index,
    ) -> Result<Outcome> {
        self.client_for_url(&recipient)
            .await?
            .request_vote(
                context::current(),
                candidates_term,
                candidate_id,
                last_log_index,
            )
            .await
            .map_err(|error| error.into())
            .and_then(|flatten| flatten)
    }

    async fn log(&self, recipient: Url, entry: Bytes) -> Result<Index> {
        self.client_for_url(&recipient)
            .await?
            .log(context::current(), entry)
            .await
            .map_err(|error| error.into())
            .and_then(|flatten| flatten)
    }
}

impl RaftTcpTarpcClient {
    async fn client_for_url(&self, url: &Url) -> Result<TarpcRaftClient> {
        if let (Some(hostname), Some(port)) = (url.host_str(), url.port()) {
            Self::client(hostname, port).await
        } else {
            Err(Error::BadUrlForNode(url.clone()))
        }
    }

    async fn client(hostname: &str, port: u16) -> Result<TarpcRaftClient> {
        let mut transport = tarpc::serde_transport::tcp::connect((hostname, port), Json::default);
        _ = transport.config_mut().max_frame_length(usize::MAX);

        transport
            .await
            .map(|transport| TarpcRaftClient::new(client::Config::default(), transport).spawn())
            .map_err(|error| error.into())
    }
}
