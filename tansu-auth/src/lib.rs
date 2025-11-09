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
    property::AuthId,
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

pub enum Authentication {
    Server(SASLServer<Justification>),
    Session(Session<Justification>),
}

impl Authentication {
    pub fn server(config: Arc<SASLConfig>) -> Arc<Mutex<Option<Authentication>>> {
        Arc::new(Mutex::new(Some(Authentication::Server(SASLServer::<
            Justification,
        >::new(
            config
        )))))
    }
}

impl Debug for Authentication {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Authentication)).finish()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Success;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Justification;

impl Validation for Justification {
    type Value = Result<Success, Error>;
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
            let auth_id = context.get_ref::<AuthId>();
            debug!(?auth_id, ?mechanism);
        }

        Ok(())
    }

    fn validate(
        &self,
        session_data: &SessionData,
        context: &Context<'_>,
        validate: &mut Validate<'_>,
    ) -> Result<(), ValidationError> {
        let _ = (session_data, context, validate);
        debug!(?session_data);

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
