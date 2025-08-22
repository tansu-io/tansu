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

use rama::{Context, Layer, Service};
use tansu_sans_io::{Frame, Request};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameService<S> {
    inner: S,
}

impl<S, State> Service<State, Frame> for FrameService<S>
where
    S: Service<State, Frame>,
    State: Send + Sync + 'static,
{
    type Response = S::Response;

    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        self.inner.serve(ctx, req).await
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameLayer;

impl<S> Layer<S> for FrameLayer {
    type Service = FrameService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestService<S> {
    inner: S,
}

impl<S, Q, State> Service<State, Q> for RequestService<S>
where
    S: Service<State, Q>,
    Q: Request,
    State: Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        self.inner.serve(ctx, req).await
    }
}
