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

use rama::Service;
use tansu_sans_io::{ApiKey, Body, Frame};

use crate::{Error, coordinator::group::Coordinator};

pub mod heartbeat;
pub mod join;
pub mod leave;
pub mod offset_commit;
pub mod offset_fetch;
pub mod sync;

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct Request<C> {
    coordinator: C,
    frame: Frame,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CoordinatorService<C, S> {
    coordinator: C,
    service: S,
}

impl<C, S> ApiKey for CoordinatorService<C, S>
where
    S: ApiKey,
{
    const KEY: i16 = S::KEY;
}

impl<C, S> CoordinatorService<C, S>
where
    C: Coordinator,
    S: ApiKey,
    S: Service<(), Request<C>, Response = Body, Error = Error>,
{
    pub fn new(coordinator: C, service: S) -> Self {
        Self {
            coordinator,
            service,
        }
    }
}

impl<C, S> Service<(), Frame> for CoordinatorService<C, S>
where
    C: Coordinator,
    S: Service<(), Request<C>, Response = Body, Error = Error>,
{
    type Response = Body;
    type Error = Error;

    async fn serve(
        &self,
        ctx: rama::Context<()>,
        frame: Frame,
    ) -> Result<Self::Response, Self::Error> {
        self.service
            .serve(
                ctx,
                Request {
                    coordinator: self.coordinator.clone(),
                    frame,
                },
            )
            .await
    }
}
