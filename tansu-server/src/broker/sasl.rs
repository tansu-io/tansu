// Copyright ⓒ 2024 Peter Morgan <peter.james.morgan@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use crate::Error;
use rsasl::{
    callback::{Context, Request, SessionCallback, SessionData},
    prelude::{SASLServer, Session, Validation},
    validate::Validate,
};
use tansu_storage::Storage;
use tracing::debug;

pub enum Authentication {
    Server(SASLServer<Justification>),
    Session(Session<Justification>),
    None,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Success;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Justification;

impl Validation for Justification {
    type Value = Result<Success, Error>;
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Callback<S> {
    storage: S,
}

impl<S> Callback<S>
where
    S: Storage,
{
    pub fn with_storage(storage: S) -> Self {
        Self { storage }
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
    ) -> Result<(), rsasl::prelude::SessionError> {
        let _ = (session_data, context, request);
        debug!(?session_data);
        Ok(())
    }

    fn validate(
        &self,
        session_data: &SessionData,
        context: &Context<'_>,
        validate: &mut Validate<'_>,
    ) -> Result<(), rsasl::validate::ValidationError> {
        // silence the 'arg X not used' errors without having to prefix the parameter names with _
        let _ = (session_data, context, validate);
        debug!(?session_data);
        Ok(())
    }
}
