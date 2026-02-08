// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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
    mechanisms::scram::properties::ScramStoredPassword,
    prelude::{SASLError, SASLServer, Session, SessionError, Validation},
    property::{AuthId, AuthzId, Password},
    validate::{Validate, ValidationError},
};
use std::{
    fmt::{self, Debug, Formatter},
    str::FromStr,
    sync::{Arc, Mutex, PoisonError},
};
use tansu_sans_io::ScramMechanism;
use tansu_storage::Storage;
use thiserror::Error;
use tokio::task::JoinError;
use tracing::debug;

mod authenticate;
mod handshake;

pub use authenticate::SaslAuthenticateService;
pub use handshake::SaslHandshakeService;

#[derive(Clone, Debug, Error)]
pub enum Error {
    Join(Arc<JoinError>),
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

impl From<JoinError> for Error {
    fn from(value: JoinError) -> Self {
        Self::Join(Arc::new(value))
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
    Finished(Option<Success>),
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
            .map(|guard| matches!(guard.as_ref(), Some(Stage::Finished(_))))
            .ok()
            .unwrap_or_default()
    }
}

impl Debug for Authentication {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Authentication)).finish()
    }
}

#[derive(Debug, Error)]
pub enum AuthError {
    Bad,
    Io(tansu_sans_io::Error),
    MissingProperty { mechanism: String, property: String },
    NoSuchUser,
    UnknownMechanism(String),
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Success {
    auth_id: String,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Justification;

impl Validation for Justification {
    type Value = Result<Success, AuthError>;
}

#[derive(Clone, Debug)]
pub struct Callback<S> {
    storage: S,
}

impl<S> Callback<S>
where
    S: Storage,
{
    pub fn new(storage: S) -> Self
    where
        S: Storage,
    {
        Self { storage }
    }

    fn check(
        &self,
        session_data: &SessionData,
        context: &Context<'_>,
    ) -> Result<Result<Success, AuthError>, Error> {
        if session_data.mechanism().mechanism == "PLAIN" {
            Ok(context
                .get_ref::<Password>()
                .ok_or(AuthError::MissingProperty {
                    mechanism: session_data.mechanism().mechanism.to_string(),
                    property: "Password".into(),
                })
                .and(
                    context
                        .get_ref::<AuthId>()
                        .inspect(|auth_id| {
                            debug!(mechanism = %session_data.mechanism().mechanism, auth_id)
                        })
                        .ok_or(AuthError::MissingProperty {
                            mechanism: session_data.mechanism().mechanism.to_string(),
                            property: "AuthId".into(),
                        }).map(ToString::to_string).map(|auth_id| {
                            Success { auth_id }
                        })
                ))
        } else if session_data.mechanism().mechanism.starts_with("SCRAM-") {
            Ok(context
                .get_ref::<AuthId>()
                .inspect(|auth_id| debug!(mechanism = %session_data.mechanism().mechanism, auth_id))
                .ok_or(AuthError::MissingProperty {
                    mechanism: session_data.mechanism().mechanism.to_string(),
                    property: "AuthId".into(),
                })
                .and_then(|auth_id| {
                    context
                        .get_ref::<AuthzId>()
                        .inspect(|authz_id| {
                            debug!(mechanism = %session_data.mechanism().mechanism, authz_id)
                        })
                        .map_or(Ok(Success{
                            auth_id:auth_id.to_string()
                        }), |authz_id| {
                            if authz_id == auth_id {
                                Ok(Success{
                                    auth_id:auth_id.to_string()
                                })
                            } else {
                                Err(AuthError::Bad)
                            }
                        })
                }))
        } else {
            Ok(Err(AuthError::UnknownMechanism(
                session_data.mechanism().mechanism.to_string(),
            )))
        }
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
        debug!(?session_data);

        if session_data.mechanism().mechanism.starts_with("SCRAM-") {
            let mechanism = ScramMechanism::from_str(session_data.mechanism().mechanism)
                .map_err(|error| SessionError::Boxed(Box::new(error)))?;

            let auth_id = context
                .get_ref::<AuthId>()
                .ok_or(SessionError::ValidationError(
                    ValidationError::MissingRequiredProperty,
                ))?;

            debug!(?auth_id, ?mechanism);

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            let storage = self.storage.clone();

            if let Some(credential) = rt
                .block_on(async { storage.user_scram_credential(auth_id, mechanism).await })
                .inspect(|credential| debug!(?credential))
                .expect("credentials")
            {
                _ = request
                    .satisfy::<ScramStoredPassword<'_>>(&ScramStoredPassword::new(
                        credential.iterations() as u32,
                        &credential.salt()[..],
                        &credential.stored_key()[..],
                        &credential.server_key()[..],
                    ))
                    .inspect_err(|err| debug!(auth_id, ?mechanism, ?err))?;
            }
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

        _ = validate.with::<Justification, _>(|| {
            self.check(session_data, context)
                .map_err(|e| ValidationError::Boxed(Box::new(e)))
        })?;

        Ok(())
    }
}

pub fn configuration<S>(storage: S) -> Result<Arc<SASLConfig>, Error>
where
    S: Storage,
{
    SASLConfig::builder()
        .with_defaults()
        .with_callback(Callback::new(storage))
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
