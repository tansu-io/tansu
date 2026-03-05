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

use crate::Result;
use bytes::Bytes;
use clap::{Subcommand, ValueEnum};
use rand::{RngCore as _, rng};
use rsasl::mechanisms::scram::tools::hash_password;
use sha2::{Sha256, Sha512, digest::generic_array::GenericArray};
use tansu_client::{Client, ConnectionManager};
use tansu_sans_io::{
    AlterUserScramCredentialsRequest, ErrorCode, ScramMechanism,
    alter_user_scram_credentials_request::{ScramCredentialDeletion, ScramCredentialUpsertion},
};
use tracing::debug;
use url::Url;

use super::DEFAULT_BROKER;

#[derive(Clone, Debug, Subcommand)]
pub(super) enum Command {
    /// Create a user
    Create {
        /// Broker URL
        #[arg(long, default_value = DEFAULT_BROKER)]
        broker: Url,

        /// The name of the user
        name: String,

        /// Password
        password: String,

        // Iterations
        #[arg(long, default_value = "8192")]
        iterations: Option<u32>,

        // Mode
        #[arg(long, value_enum, default_value = "scram512")]
        mechanism: Mechanism,
    },

    /// Delete a user
    Delete {
        /// Broker URL
        #[arg(long, default_value = DEFAULT_BROKER)]
        broker: Url,

        /// The name of the user
        name: String,

        // Mode
        #[arg(long, value_enum, default_value = "scram512")]
        mechanism: Mechanism,
    },
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, PartialOrd, Ord, ValueEnum)]
pub(super) enum Mechanism {
    /// Scram 256
    Scram256,

    /// Scram 512
    #[default]
    Scram512,
}

impl Mechanism {
    fn salted_password(&self, password: &[u8], iterations: u32, salt: &[u8]) -> Bytes {
        match self {
            Self::Scram256 => {
                let mut salted_password = GenericArray::default();
                hash_password::<Sha256>(password, iterations, salt, &mut salted_password);

                Bytes::copy_from_slice(salted_password.as_slice())
            }

            Self::Scram512 => {
                let mut salted_password = GenericArray::default();
                hash_password::<Sha512>(password, iterations, salt, &mut salted_password);

                Bytes::copy_from_slice(salted_password.as_slice())
            }
        }
    }
}

impl From<&Mechanism> for i8 {
    fn from(value: &Mechanism) -> Self {
        match value {
            Mechanism::Scram256 => ScramMechanism::Scram256.into(),
            Mechanism::Scram512 => ScramMechanism::Scram512.into(),
        }
    }
}

impl Command {
    fn broker(&self) -> Url {
        match self {
            Command::Create { broker, .. } => broker.to_owned(),
            Command::Delete { broker, .. } => broker.to_owned(),
        }
    }

    fn upsertions(&self) -> Option<Vec<ScramCredentialUpsertion>> {
        match self {
            Command::Create {
                name,
                password,
                iterations,
                mechanism,
                ..
            } => {
                const DEFAULT_SALT_LEN: usize = 32;
                const DEFAULT_ITERATIONS: u32 = 2u32.pow(14);

                let mut salt = [0u8; DEFAULT_SALT_LEN];
                rng().fill_bytes(&mut salt);

                let iterations = iterations.unwrap_or(DEFAULT_ITERATIONS);

                let salted_password =
                    mechanism.salted_password(password.as_bytes(), iterations, &salt[..]);

                Some(
                    [ScramCredentialUpsertion::default()
                        .name(name.into())
                        .mechanism(mechanism.into())
                        .iterations(iterations as i32)
                        .salt(Bytes::copy_from_slice(&salt[..]))
                        .salted_password(salted_password)]
                    .into(),
                )
            }

            _ => Some([].into()),
        }
    }

    fn deletions(&self) -> Option<Vec<ScramCredentialDeletion>> {
        match self {
            Command::Create { .. } => Some([].into()),
            Command::Delete {
                name,
                mechanism: mode,
                ..
            } => Some(
                [ScramCredentialDeletion::default()
                    .name(name.into())
                    .mechanism(mode.into())]
                .into(),
            ),
        }
    }

    pub(super) async fn main(self) -> Result<ErrorCode> {
        let client = ConnectionManager::builder(self.broker())
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .build()
            .await
            .inspect(|pool| debug!(?pool))
            .map(Client::new)?;

        let req = AlterUserScramCredentialsRequest::default()
            .deletions(self.deletions())
            .upsertions(self.upsertions());

        _ = client
            .call(req)
            .await
            .inspect(|response| debug!(?response))?;

        Ok(ErrorCode::None)
    }
}
