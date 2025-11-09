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

use std::sync::{Arc, Mutex};

use crate::Error;
use rama::{
    Layer as _, Service as _,
    layer::{MapErrLayer, MapStateLayer},
};
use tansu_auth::{Authentication, SaslAuthenticateService, SaslHandshakeService};
use tansu_sans_io::{ApiKey as _, SaslAuthenticateRequest, SaslHandshakeRequest};
use tansu_service::{FrameRequestLayer, FrameRouteBuilder};

pub fn services(
    builder: FrameRouteBuilder<(), Error>,
    authentication: Arc<Mutex<Option<Authentication>>>,
) -> Result<FrameRouteBuilder<(), Error>, Error> {
    [authenticate, handshake]
        .iter()
        .try_fold(builder, |builder, service| {
            service(builder, authentication.clone())
        })
}

pub fn authenticate(
    builder: FrameRouteBuilder<(), Error>,
    authentication: Arc<Mutex<Option<Authentication>>>,
) -> Result<FrameRouteBuilder<(), Error>, Error> {
    builder
        .with_route(
            SaslAuthenticateRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| authentication),
                FrameRequestLayer::<SaslAuthenticateRequest>::new(),
            )
                .into_layer(SaslAuthenticateService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn handshake(
    builder: FrameRouteBuilder<(), Error>,
    authentication: Arc<Mutex<Option<Authentication>>>,
) -> Result<FrameRouteBuilder<(), Error>, Error> {
    builder
        .with_route(
            SaslHandshakeRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| authentication),
                FrameRequestLayer::<SaslHandshakeRequest>::new(),
            )
                .into_layer(SaslHandshakeService)
                .boxed(),
        )
        .map_err(Into::into)
}
