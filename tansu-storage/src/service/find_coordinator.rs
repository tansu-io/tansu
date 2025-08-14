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
use tansu_sans_io::{
    ApiKey, Body, ErrorCode, FindCoordinatorRequest, FindCoordinatorResponse,
    find_coordinator_response::Coordinator,
};

use crate::{Error, Result, Storage};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FindCoordinatorService<S> {
    storage: S,
}

impl<S> ApiKey for FindCoordinatorService<S> {
    const KEY: i16 = FindCoordinatorRequest::KEY;
}

impl<S> FindCoordinatorService<S>
where
    S: Storage,
{
    pub fn new(storage: S) -> Self {
        Self { storage }
    }
}

impl<S, State, Q> Service<State, Q> for FindCoordinatorService<S>
where
    S: Storage,
    State: Clone + Send + Sync + 'static,
    Q: Into<Body> + Send + Sync + 'static,
{
    type Response = Body;
    type Error = Error;

    async fn serve(&self, _ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        let find_coordinator = FindCoordinatorRequest::try_from(req.into())?;

        let node_id = self.storage.node()?;

        let listener = self.storage.advertised_listener()?;
        let host = listener.host_str().unwrap_or("localhost");
        let port = i32::from(listener.port().unwrap_or(9092));

        Ok(FindCoordinatorResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(Some(ErrorCode::None.into()))
            .error_message(Some("NONE".into()))
            .node_id(Some(node_id))
            .host(Some(host.into()))
            .port(Some(port))
            .coordinators(find_coordinator.coordinator_keys.map(|keys| {
                keys.iter()
                    .map(|key| {
                        Coordinator::default()
                            .key(key.to_string())
                            .node_id(node_id)
                            .host(host.into())
                            .port(port)
                            .error_code(ErrorCode::None.into())
                            .error_message(None)
                    })
                    .collect()
            }))
            .into())
    }
}
