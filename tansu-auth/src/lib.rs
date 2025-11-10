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

use rsasl::{
    callback::{Context, Request, SessionCallback, SessionData},
    config::SASLConfig,
    prelude::{SASLError, SASLServer, Session, SessionError, Validation},
    property::{AuthId, AuthzId, Password},
    validate::{Validate, ValidationError},
};
use std::{
    fmt::{self, Debug, Formatter},
    sync::{Arc, Mutex, PoisonError},
};
use tansu_storage::Storage;
use thiserror::Error;
use tokio::runtime::Handle;
use tracing::debug;

mod authenticate;
mod handshake;

pub use authenticate::SaslAuthenticateService;
pub use handshake::SaslHandshakeService;

#[derive(Clone, Debug, Error)]
pub enum Error {
    Poison,
    SansIo(#[from] tansu_sans_io::Error),
    Sasl(Arc<SASLError>),
    SaslSession(Arc<SessionError>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl From<SASLError> for Error {
    fn from(value: SASLError) -> Self {
        Self::Sasl(Arc::new(value))
    }
}

impl From<SessionError> for Error {
    fn from(value: SessionError) -> Self {
        Self::SaslSession(Arc::new(value))
    }
}

#[derive(Clone, Default)]
pub struct Authentication {
    stage: Arc<Mutex<Option<Stage>>>,
}

pub enum Stage {
    Server(SASLServer<Justification>),
    Session(Session<Justification>),
    Finished,
}

impl Debug for Stage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Stage)).finish()
    }
}

impl Authentication {
    pub fn server(config: Arc<SASLConfig>) -> Self {
        Self {
            stage: Arc::new(Mutex::new(Some(Stage::Server(
                SASLServer::<Justification>::new(config),
            )))),
        }
    }

    pub fn is_authenticated(&self) -> bool {
        self.stage
            .lock()
            .map(|guard| {
                if let Some(Stage::Finished) = guard.as_ref() {
                    true
                } else {
                    false
                }
            })
            .ok()
            .unwrap_or_default()
    }
}

impl Debug for Authentication {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Authentication)).finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, Hash, Ord, PartialEq, PartialOrd)]
pub enum AuthError {
    NoSuchUser,
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Success;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Justification;

impl Validation for Justification {
    type Value = Result<Success, AuthError>;
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct Callback<S> {
    handle: Handle,
    storage: S,
}

impl<S> Callback<S>
where
    S: Storage,
{
    pub fn new(handle: Handle, storage: S) -> Self
    where
        S: Storage,
    {
        Self { handle, storage }
    }

    fn test_validate(
        &self,
        _session_data: &SessionData,
        context: &Context<'_>,
    ) -> Result<Result<Success, AuthError>, Error> {
        let _authzid = context
            .get_ref::<AuthzId>()
            .inspect(|authz_id| debug!(?authz_id));
        let _authid = context
            .get_ref::<AuthId>()
            .inspect(|auth_id| debug!(?auth_id))
            .expect("SIMPLE validation requested but AuthId prop is missing!");
        let _password = context
            .get_ref::<Password>()
            .inspect(|password| debug!(?password))
            .expect("SIMPLE validation requested but Password prop is missing!");

        Ok(Ok(Success))
    }
}

impl<S> SessionCallback for Callback<S>
where
    S: Storage,
{
    fn callback(
        &self,
        session_data: &SessionData,
        context: &Context<'_>,
        request: &mut Request<'_>,
    ) -> Result<(), SessionError> {
        let _ = (session_data, context, request);
        debug!(?session_data);

        if session_data.mechanism().mechanism.starts_with("SCRAM-") {
            let mechanism = session_data.mechanism().mechanism;
            let auth_id = context.get_ref::<AuthId>().expect("auth_id");
            debug!(?auth_id, ?mechanism);

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            let storage = self.storage.clone();

            _ = rt
                .block_on(async {
                    storage
                        .user_scram_credential(auth_id, tansu_sans_io::ScramMechanism::Scram256)
                        .await
                })
                .inspect(|credential| debug!(?credential))
                .expect("credentials");
        }

        Ok(())
    }

    fn validate(
        &self,
        session_data: &SessionData,
        context: &Context<'_>,
        validate: &mut Validate<'_>,
    ) -> Result<(), ValidationError> {
        debug!(?session_data);

        if session_data.mechanism().mechanism == "PLAIN" {
            _ = validate.with::<Justification, _>(|| {
                self.test_validate(session_data, context)
                    .map_err(|e| ValidationError::Boxed(Box::new(e)))
            })?;
        }

        Ok(())
    }
}

pub fn configuration<S>(handle: Handle, storage: S) -> Result<Arc<SASLConfig>, Error>
where
    S: Storage,
{
    SASLConfig::builder()
        .with_defaults()
        .with_callback(Callback::new(handle, storage))
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_send<T: Send>() {}
    fn is_sync<T: Sync>() {}

    #[test]
    fn authentication() {
        is_send::<Authentication>();
        is_sync::<Authentication>();
    }
}
