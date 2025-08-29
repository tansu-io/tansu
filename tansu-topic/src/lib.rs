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

use std::{fmt, io, result};

use create::Create;
use delete::Delete;
use std::{marker::PhantomData, sync::Arc};
use tansu_sans_io::ErrorCode;
use url::Url;

mod create;
mod delete;

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    Api(ErrorCode),
    Client(#[from] tansu_client::Error),
    Io(Arc<io::Error>),
    Protocol(#[from] tansu_sans_io::Error),
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Topic {
    Create(create::Configuration),
    Delete(delete::Configuration),
}

impl Topic {
    pub fn create() -> create::Builder<PhantomData<Url>, PhantomData<String>, PhantomData<i32>> {
        create::Builder::default()
    }

    pub fn delete() -> delete::Builder<PhantomData<Url>, PhantomData<String>> {
        delete::Builder::default()
    }

    pub async fn main(self) -> Result<ErrorCode> {
        match self {
            Self::Create(configuration) => Create::try_from(configuration)?.main().await,
            Self::Delete(configuration) => Delete::try_from(configuration)?.main().await,
        }
    }
}
