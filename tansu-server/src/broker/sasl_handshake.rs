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

use crate::Result;
use rsasl::prelude::Mechname;
use tansu_kafka_sans_io::{Body, ErrorCode};
use tracing::debug;

use super::sasl::Authentication;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SaslHandshake;

impl SaslHandshake {
    pub async fn response(
        &self,
        mechanism: &str,
        authentication: Authentication,
    ) -> Result<(Authentication, Body)> {
        let Authentication::Server(server) = authentication else {
            return Ok((
                Authentication::None,
                Body::SaslHandshakeResponse {
                    error_code: ErrorCode::UnsupportedSaslMechanism.into(),
                    mechanisms: Some(vec![mechanism.into()]),
                },
            ));
        };

        if let Ok(mechanism) = Mechname::parse(mechanism.as_bytes()) {
            debug!(?mechanism);

            let mechanisms = server
                .get_available()
                .into_iter()
                .map(|mechanism| mechanism.to_string())
                .collect();

            server
                .start_suggested(mechanism)
                .map_err(Into::into)
                .map(Authentication::Session)
                .map(|session| {
                    (
                        session,
                        Body::SaslHandshakeResponse {
                            error_code: ErrorCode::None.into(),
                            mechanisms: Some(mechanisms),
                        },
                    )
                })
        } else {
            Ok((
                Authentication::None,
                Body::SaslHandshakeResponse {
                    error_code: ErrorCode::UnsupportedSaslMechanism.into(),
                    mechanisms: Some(vec![mechanism.into()]),
                },
            ))
        }
    }
}
