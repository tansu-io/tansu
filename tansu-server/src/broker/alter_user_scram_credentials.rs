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

use bytes::Bytes;
use rsasl::mechanisms::scram::tools::derive_keys;
use sha2::{Digest, Sha256, Sha512};
use tansu_kafka_sans_io::{
    alter_user_scram_credentials_request::{ScramCredentialDeletion, ScramCredentialUpsertion},
    alter_user_scram_credentials_response::AlterUserScramCredentialsResult,
    Body, ErrorCode, ScramMechanism,
};
use tansu_storage::{ScramCredential, Storage};

use crate::Result;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AlterUserScramCredentials<S> {
    storage: S,
}

impl<S> AlterUserScramCredentials<S>
where
    S: Storage,
{
    pub fn with_storage(storage: S) -> Self {
        Self { storage }
    }

    pub async fn response(
        &self,
        deletions: Option<Vec<ScramCredentialDeletion>>,
        upsertions: Option<Vec<ScramCredentialUpsertion>>,
    ) -> Result<Body> {
        let mut results = vec![];

        if let Some(_deletions) = deletions {}

        if let Some(upsertions) = upsertions {
            for upsertion in upsertions {
                let (mechanism, stored_key, server_key) =
                    ScramMechanism::try_from(upsertion.mechanism).map(|mechanism| {
                        if mechanism == ScramMechanism::Scram256 {
                            let (client_key, server_key) =
                                derive_keys::<Sha256>(&upsertion.salted_password);

                            (
                                mechanism,
                                Bytes::copy_from_slice(Sha256::digest(client_key).as_slice()),
                                Bytes::copy_from_slice(server_key.as_slice()),
                            )
                        } else {
                            let (client_key, server_key) =
                                derive_keys::<Sha512>(&upsertion.salted_password);

                            (
                                mechanism,
                                Bytes::copy_from_slice(Sha256::digest(client_key).as_slice()),
                                Bytes::copy_from_slice(server_key.as_slice()),
                            )
                        }
                    })?;

                let credential = ScramCredential::new(
                    upsertion.salt,
                    upsertion.iterations,
                    stored_key,
                    server_key,
                );

                results.push(
                    self.storage
                        .upsert_user_scram_credential(
                            upsertion.name.as_str(),
                            mechanism,
                            credential,
                        )
                        .await
                        .map_or(
                            AlterUserScramCredentialsResult {
                                user: upsertion.name.clone(),
                                error_code: ErrorCode::UnsupportedSaslMechanism.into(),
                                error_message: Some("".into()),
                            },
                            |()| AlterUserScramCredentialsResult {
                                user: upsertion.name.clone(),
                                error_code: ErrorCode::None.into(),
                                error_message: Some("".into()),
                            },
                        ),
                );
            }
        }

        Ok(Body::AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: Some(results),
        })
    }
}
