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

use rama::Layer;
use tansu_service::{
    BytesFrameLayer, BytesFrameService, FrameRouteService, TcpBytesLayer, TcpBytesService,
    TcpContext, TcpContextLayer, TcpContextService,
};
use tansu_storage::Storage;
use tracing::debug;

use crate::{Error, Result, coordinator::group::Coordinator};

pub mod coordinator;
pub mod storage;

type TcpRouteFrame =
    TcpContextService<TcpBytesService<BytesFrameService<FrameRouteService<(), Error>>, ()>>;

pub fn services<C, S>(cluster_id: &str, coordinator: C, storage: S) -> Result<TcpRouteFrame, Error>
where
    S: Storage,
    C: Coordinator,
{
    storage::services(FrameRouteService::<(), Error>::builder(), storage)
        .inspect(|builder| debug!(?builder))
        .and_then(|builder| {
            coordinator::services(builder, coordinator).inspect(|builder| debug!(?builder))
        })
        .and_then(|builder| builder.build().map_err(Into::into))
        .map(|route| {
            (
                TcpContextLayer::new(TcpContext::default().cluster_id(Some(cluster_id.into()))),
                TcpBytesLayer::<()>::default(),
                BytesFrameLayer,
            )
                .into_layer(route)
        })
}
