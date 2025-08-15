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
use tansu_sans_io::{ApiKey, Body, Frame};

use crate::Error;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StorageService<S> {
    inner: S,
}

impl<S> ApiKey for StorageService<S>
where
    S: ApiKey,
{
    const KEY: i16 = S::KEY;
}

impl<S> StorageService<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, State> Service<State, Frame> for StorageService<S>
where
    S: Service<State, Body>,
    S::Error: Into<Error>,
    State: Clone + Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        request: Frame,
    ) -> Result<Self::Response, Self::Error> {
        self.inner
            .serve(ctx, request.body)
            .await
            .map_err(Into::into)
    }
}
