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

use crate::{primitive::ByteSize, record::codec::Octets, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Header {
    #[serde(serialize_with = "Octets::serialize")]
    #[serde(deserialize_with = "Octets::deserialize")]
    pub key: Option<Bytes>,

    #[serde(serialize_with = "Octets::serialize")]
    #[serde(deserialize_with = "Octets::deserialize")]
    pub value: Option<Bytes>,
}

impl From<Builder> for Header {
    fn from(value: Builder) -> Self {
        Self {
            key: value.key.0,
            value: value.value.0,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Builder {
    key: Octets,
    value: Octets,
}

impl Builder {
    #[must_use]
    pub fn key(mut self, key: Vec<u8>) -> Self {
        self.key = Octets::from(Some(key));
        self
    }

    #[must_use]
    pub fn value(mut self, value: Vec<u8>) -> Self {
        self.value = Octets::from(Some(value));
        self
    }

    #[must_use]
    pub fn build(self) -> Header {
        Header {
            key: self.key.0,
            value: self.value.0,
        }
    }
}

impl From<Header> for Builder {
    fn from(value: Header) -> Self {
        Self {
            key: value.key.into(),
            value: value.value.into(),
        }
    }
}

impl ByteSize for Builder {
    fn size_in_bytes(&self) -> Result<usize> {
        self.key
            .size_in_bytes()
            .and_then(|ksz| self.value.size_in_bytes().map(|vsz| ksz + vsz))
    }
}
