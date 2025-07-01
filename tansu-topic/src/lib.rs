// Copyright ⓒ 2025 Peter Morgan <peter.james.morgan@gmail.com>
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
