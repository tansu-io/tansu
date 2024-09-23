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

use bytes::{Buf, Bytes};
use digest::{
    consts::U256,
    core_api::{BufferKindUser, CoreWrapper},
    typenum::{IsLess, Le, NonZero},
    FixedOutputReset, HashMarker,
};
use rsasl::mechanisms::scram::tools::derive_keys;
use sha2::{Digest, Sha256, Sha512};
use tansu_kafka_sans_io::{
    alter_user_scram_credentials_request::{ScramCredentialDeletion, ScramCredentialUpsertion},
    ScramMechanism,
};
use tansu_storage::Storage;

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
    ) -> Result<()> {
        if let Some(_deletions) = deletions {
            todo!()
        }

        if let Some(upsertions) = upsertions {
            for upsertion in upsertions {
                match ScramMechanism::try_from(upsertion.mechanism) {
                    Ok(mechanism) => {
                        let (stored_key, server_key) = if mechanism == ScramMechanism::Scram256 {
                            let (client_key, server_key) =
                                derive_keys::<Sha256>(&upsertion.salted_password);

                            (
                                Bytes::copy_from_slice(Sha256::digest(client_key).as_slice()),
                                Bytes::copy_from_slice(server_key.as_slice()),
                            )
                        } else {
                            let (client_key, server_key) =
                                derive_keys::<Sha512>(&upsertion.salted_password);

                            (
                                Bytes::copy_from_slice(Sha256::digest(client_key).as_slice()),
                                Bytes::copy_from_slice(server_key.as_slice()),
                            )
                        };

                        let q = self
                            .storage
                            .upsert_user_scram_credential(
                                upsertion.name.as_str(),
                                mechanism,
                                upsertion.salted_password,
                                upsertion.iterations,
                                stored_key,
                                server_key,
                            )
                            .await;
                    }

                    _ => todo!(),
                }
            }
        }

        Ok(())
    }
}
