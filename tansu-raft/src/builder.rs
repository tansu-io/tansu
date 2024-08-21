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

use std::{
    collections::BTreeSet,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use url::Url;

use crate::{
    blocking::ProvidePersistentState,
    context::{Context, Situation},
    Clock, ProvideApplyState, ProvideConfiguration, ProvideKvStore, ProvideRaftRpc, ProvideServer,
    Raft, Result,
};

#[derive(Default)]
pub struct Builder<
    N = PhantomData<Box<dyn ProvideConfiguration>>,
    O = PhantomData<Box<dyn ProvideServer>>,
    P = PhantomData<Box<dyn ProvideKvStore>>,
    Q = PhantomData<Box<dyn ProvideApplyState>>,
    R = PhantomData<Box<dyn ProvideRaftRpc>>,
    S = PhantomData<Box<dyn ProvidePersistentState>>,
    T = PhantomData<BTreeSet<Url>>,
> {
    configuration: N,
    server: O,
    kv_store: P,
    apply_state: Q,
    raft_rpc: R,
    persistent_state: S,
    voters: T,
}

impl
    Builder<
        Box<dyn ProvideConfiguration>,
        Box<dyn ProvideServer>,
        Box<dyn ProvideKvStore>,
        Box<dyn ProvideApplyState>,
        Box<dyn ProvideRaftRpc>,
        Box<dyn ProvidePersistentState>,
        BTreeSet<Url>,
    >
{
    pub async fn build(mut self) -> Result<Raft> {
        Ok(Raft {
            configuration: self.configuration.provide_configuration().map(Arc::new)?,
            context: Situation::builder()
                .voters(self.voters)
                .kv_store(self.kv_store.provide_kv_store()?)
                .persistent_state(self.persistent_state.provide_persistent_state()?)
                .server(self.server.provide_server()?)
                .build()
                .map(|situation| Arc::new(Mutex::new(Box::new(situation) as Box<dyn Context>)))?,
            raft_rpc: self.raft_rpc.provide_raft_rpc().await.map(Arc::new)?,
            apply_state: self.apply_state.provide_apply_state().map(Arc::new)?,
            clock: Clock::new(),
        })
    }
}

impl<N, O, P, Q, R, S, T> Builder<N, O, P, Q, R, S, T> {
    pub fn configuration(
        self,
        configuration: Box<dyn ProvideConfiguration>,
    ) -> Builder<Box<dyn ProvideConfiguration>, O, P, Q, R, S, T> {
        Builder {
            configuration,
            server: self.server,
            kv_store: self.kv_store,
            apply_state: self.apply_state,
            raft_rpc: self.raft_rpc,
            persistent_state: self.persistent_state,
            voters: self.voters,
        }
    }
    pub fn with_server(
        self,
        provider: Box<dyn ProvideServer>,
    ) -> Builder<N, Box<dyn ProvideServer>, P, Q, R, S, T> {
        Builder {
            configuration: self.configuration,
            server: provider,
            kv_store: self.kv_store,
            apply_state: self.apply_state,
            raft_rpc: self.raft_rpc,
            persistent_state: self.persistent_state,
            voters: self.voters,
        }
    }

    pub fn with_kv_store(
        self,
        provider: Box<dyn ProvideKvStore>,
    ) -> Builder<N, O, Box<dyn ProvideKvStore>, Q, R, S, T> {
        Builder {
            configuration: self.configuration,
            server: self.server,
            kv_store: provider,
            apply_state: self.apply_state,
            raft_rpc: self.raft_rpc,
            persistent_state: self.persistent_state,
            voters: self.voters,
        }
    }

    pub fn with_apply_state(
        self,
        provider: Box<dyn ProvideApplyState>,
    ) -> Builder<N, O, P, Box<dyn ProvideApplyState>, R, S, T> {
        Builder {
            configuration: self.configuration,
            server: self.server,
            kv_store: self.kv_store,
            apply_state: provider,
            raft_rpc: self.raft_rpc,
            persistent_state: self.persistent_state,
            voters: self.voters,
        }
    }

    // callback provider for the Raft RPCs to the nodes
    pub fn with_raft_rpc(
        self,
        provider: Box<dyn ProvideRaftRpc>,
    ) -> Builder<N, O, P, Q, Box<dyn ProvideRaftRpc>, S, T> {
        Builder {
            configuration: self.configuration,
            server: self.server,
            kv_store: self.kv_store,
            apply_state: self.apply_state,
            raft_rpc: provider,
            persistent_state: self.persistent_state,
            voters: self.voters,
        }
    }

    pub fn with_persistent_state(
        self,
        provider: Box<dyn ProvidePersistentState>,
    ) -> Builder<N, O, P, Q, R, Box<dyn ProvidePersistentState>, T> {
        Builder {
            configuration: self.configuration,
            server: self.server,
            kv_store: self.kv_store,
            apply_state: self.apply_state,
            raft_rpc: self.raft_rpc,
            persistent_state: provider,
            voters: self.voters,
        }
    }

    pub fn with_voters(self, voters: BTreeSet<Url>) -> Builder<N, O, P, Q, R, S, BTreeSet<Url>> {
        Builder {
            configuration: self.configuration,
            server: self.server,
            kv_store: self.kv_store,
            apply_state: self.apply_state,
            raft_rpc: self.raft_rpc,
            persistent_state: self.persistent_state,
            voters,
        }
    }
}
