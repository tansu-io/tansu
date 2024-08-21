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

use std::{
    num::ParseIntError,
    sync::{PoisonError, TryLockError},
};

use serde::{Deserialize, Serialize};
use url::Url;

use crate::Index;

#[derive(thiserror::Error, Debug, Serialize, Deserialize)]
pub enum Error {
    #[serde(skip)]
    #[error("io")]
    Io(#[from] std::io::Error),

    #[error("bad ident")]
    BadIdent,

    #[error("mismatch id")]
    IdMismatch,

    #[serde(skip)]
    #[error("varint")]
    VarInt(#[from] tansu_varint::Error),

    #[error("poison")]
    Poison,

    #[error("try lock")]
    TryLock,

    #[error("crc")]
    Crc,

    #[serde(skip)]
    #[error("utf8")]
    StringUtf8(#[from] std::string::FromUtf8Error),

    #[serde(skip)]
    #[error("utf8")]
    StrUtf8(#[from] std::str::Utf8Error),

    #[error("not leader")]
    NotLeader,

    #[error("custom")]
    Custom(String),

    #[serde(skip)]
    #[error("json")]
    Json(#[from] serde_json::Error),

    #[error("log entry is empty")]
    EmptyLogEntry(Index),

    #[error("unknown node")]
    UnknownNode(Url),

    #[serde(skip)]
    #[error("bad URL for node")]
    BadUrlForNode(Url),

    #[serde(skip)]
    #[error("tarpc")]
    Tarpc(#[from] tarpc::client::RpcError),

    #[error("broken")]
    Broken,

    #[error("next index cannot be less than 1")]
    BadNextIndex,

    #[error("log not found at index")]
    LogNotFound(Index),

    #[serde(skip)]
    #[error("parse url")]
    ParseUrl(#[from] url::ParseError),

    #[serde(skip)]
    #[error("parse int")]
    ParseInt(#[from] ParseIntError),

    #[error("not follower")]
    NotFollower,

    #[error("no leader elected")]
    NoLeaderElected,
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl<T> From<&mut PoisonError<T>> for Error {
    fn from(_value: &mut PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl<T> From<TryLockError<T>> for Error {
    fn from(_value: TryLockError<T>) -> Self {
        Self::TryLock
    }
}
