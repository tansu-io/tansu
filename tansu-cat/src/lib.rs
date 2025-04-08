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

use std::{fmt, io, marker::PhantomData, result, sync::Arc};

use consume::Consume;
use produce::Produce;
use tansu_kafka_sans_io::ErrorCode;
use tokio_util::codec::LinesCodecError;
use url::Url;

mod consume;
mod produce;

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    Api(ErrorCode),
    Io(Arc<io::Error>),
    LinesCodec(#[from] LinesCodecError),
    Protocol(#[from] tansu_kafka_sans_io::Error),
    Schema(#[from] tansu_schema_registry::Error),
    SerdeJson(#[from] serde_json::Error),
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Cat {
    Consume(Box<consume::Configuration>),
    Produce(Box<produce::Configuration>),
}

impl Cat {
    pub fn produce() -> produce::Builder<
        PhantomData<Url>,
        PhantomData<String>,
        PhantomData<i32>,
        PhantomData<Option<Url>>,
        PhantomData<String>,
    > {
        Builder::produce()
    }

    pub fn consume() -> consume::Builder<
        PhantomData<Url>,
        PhantomData<String>,
        PhantomData<i32>,
        PhantomData<Option<Url>>,
    > {
        Builder::consume()
    }

    pub async fn main(self) -> Result<ErrorCode> {
        match self {
            Self::Produce(configuration) => Produce::try_from(*configuration)?.main().await,
            Self::Consume(configuration) => Consume::try_from(*configuration)?.main().await,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Builder;

impl Builder {
    pub fn produce() -> produce::Builder<
        PhantomData<Url>,
        PhantomData<String>,
        PhantomData<i32>,
        PhantomData<Option<Url>>,
        PhantomData<String>,
    > {
        produce::Builder::default()
    }

    pub fn consume() -> consume::Builder<
        PhantomData<Url>,
        PhantomData<String>,
        PhantomData<i32>,
        PhantomData<Option<Url>>,
    > {
        consume::Builder::default()
    }
}
