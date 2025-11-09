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

use std::{
    io::Cursor,
    sync::{Arc, Mutex},
};

use crate::{Authentication, Error};
use bytes::Bytes;
use rama::{Context, Service};
use tansu_sans_io::{ApiKey, ErrorCode, SaslAuthenticateRequest, SaslAuthenticateResponse};
use tracing::debug;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SaslAuthenticateService;

impl ApiKey for SaslAuthenticateService {
    const KEY: i16 = SaslAuthenticateRequest::KEY;
}

impl Service<Arc<Mutex<Option<Authentication>>>, SaslAuthenticateRequest>
    for SaslAuthenticateService
{
    type Response = SaslAuthenticateResponse;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<Arc<Mutex<Option<Authentication>>>>,
        req: SaslAuthenticateRequest,
    ) -> Result<Self::Response, Self::Error> {
        ctx.state()
            .lock()
            .map_err(Into::into)
            .and_then(|mut guard| {
                if let Some(Authentication::Session(session)) = guard.as_mut() {
                    let mut outcome = Cursor::new(Vec::new());
                    session
                        .step(Some(&req.auth_bytes), &mut outcome)
                        .map_err(Into::into)
                        .inspect(|state| debug!(?state))
                        .and(Ok(SaslAuthenticateResponse::default()
                            .error_code(ErrorCode::None.into())
                            .error_message(Some("NONE".into()))
                            .auth_bytes(Bytes::from(outcome.into_inner()))
                            .session_lifetime_ms(Some(60_000))))
                } else {
                    _ = guard.take();

                    Ok(SaslAuthenticateResponse::default()
                        .error_code(ErrorCode::IllegalSaslState.into())
                        .error_message(None)
                        .auth_bytes(Bytes::from_static(b""))
                        .session_lifetime_ms(None))
                }
            })
    }
}
