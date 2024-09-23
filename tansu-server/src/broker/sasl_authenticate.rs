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

use std::io::Cursor;

use crate::Result;
use bytes::Bytes;
use tansu_kafka_sans_io::{Body, ErrorCode};
use tracing::debug;

use super::sasl::Authentication;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SaslAuthenticate;

impl SaslAuthenticate {
    pub async fn response(
        &self,
        auth_bytes: Bytes,
        authentication: Authentication,
    ) -> Result<(Authentication, Body)> {
        let Authentication::Session(mut session) = authentication else {
            return Ok((
                Authentication::None,
                Body::SaslAuthenticateResponse {
                    error_code: ErrorCode::IllegalSaslState.into(),
                    error_message: None,
                    auth_bytes: Bytes::from_static(b""),
                    session_lifetime_ms: None,
                },
            ));
        };

        let mut outcome = Cursor::new(Vec::new());
        let state = session.step(Some(&auth_bytes), &mut outcome)?;
        debug!(?state);

        Ok((
            Authentication::Session(session),
            Body::SaslAuthenticateResponse {
                error_code: ErrorCode::None.into(),
                error_message: Some("NONE".into()),
                auth_bytes: Bytes::from(outcome.into_inner()),
                session_lifetime_ms: Some(60_000),
            },
        ))
    }
}
