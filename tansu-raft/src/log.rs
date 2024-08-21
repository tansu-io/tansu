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
use std::{
    pin::Pin,
    sync::MutexGuard,
    task::{self, Poll},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::Future;
use tracing::debug;
use url::Url;

use crate::{context::Context, log_key_for_index, server::State, Index, LogEntry, Raft, Result};

#[async_trait]
pub trait Log: Debug + Send + Sync {
    async fn log(&mut self, entry: Bytes) -> Result<Index>;
}

#[derive(Clone, Debug, Hash, Eq, Ord, PartialEq, PartialOrd)]
enum LogResult {
    Store(Index),
    Forward(Url),
}

#[async_trait]
impl Log for Raft {
    async fn log(&mut self, entry: Bytes) -> Result<Index> {
        debug!(?entry);

        match LeaderElectedFuture::new(entry.clone(), self).await {
            LogResult::Forward(leader) => {
                debug!(?entry, ?leader);
                self.raft_rpc.log(leader, entry).await
            }

            LogResult::Store(index) => {
                debug!(?index);
                Ok(index)
            }
        }
    }
}

struct LeaderElectedFuture<'a> {
    entry: Bytes,
    raft: &'a Raft,
}

impl<'a> LeaderElectedFuture<'a> {
    fn new(entry: Bytes, raft: &'a Raft) -> Self {
        Self { entry, raft }
    }

    fn next_index(context: &mut MutexGuard<'_, Box<dyn Context>>) -> Result<Index> {
        let index = context.server_mut().commit_index_mut()?;
        *index += 1;
        Ok(*index)
    }

    fn previous_index(context: &mut MutexGuard<'_, Box<dyn Context>>) -> Result<Index> {
        let index = context.server_mut().commit_index_mut()?;
        *index -= 1;
        Ok(*index)
    }
}

impl<'a> Future for LeaderElectedFuture<'a> {
    type Output = LogResult;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        let Ok(mut context) = self.raft.context_lock() else {
            return Poll::Pending;
        };

        let Ok(state) = context.server().state() else {
            context.register_pending_leader_election(cx.waker().clone());
            return Poll::Pending;
        };

        if state == State::Leader {
            let Ok(current_term) = context
                .persistent_state_mut()
                .read()
                .map(|pe| pe.current_term())
            else {
                context.register_pending_leader_election(cx.waker().clone());
                return Poll::Pending;
            };

            let Ok(index) = Self::next_index(&mut context) else {
                context.register_pending_leader_election(cx.waker().clone());
                return Poll::Pending;
            };

            let Ok(log_key) = log_key_for_index(index) else {
                context.register_pending_leader_election(cx.waker().clone());
                return Poll::Pending;
            };

            debug!(?current_term, ?index, ?log_key, ?self.entry);

            match context.kv_store_mut().put(
                log_key,
                LogEntry::builder()
                    .value(self.entry.clone())
                    .term(current_term)
                    .build()
                    .into(),
            ) {
                Ok(()) => Poll::Ready(LogResult::Store(index)),
                _ => {
                    context.register_pending_leader_election(cx.waker().clone());
                    _ = Self::previous_index(&mut context);
                    Poll::Pending
                }
            }
        } else if let Ok(Some(leader)) = context.server().leader() {
            debug!("leader={}", leader.as_str());
            Poll::Ready(LogResult::Forward(leader))
        } else {
            context.register_pending_leader_election(cx.waker().clone());
            Poll::Pending
        }
    }
}
