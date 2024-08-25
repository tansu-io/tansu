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

use std::marker::PhantomData;

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::{Error, Term};

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct LogEntry {
    term: Term,
    value: Bytes,
}

impl LogEntry {
    pub fn term(&self) -> Term {
        self.term
    }

    pub fn value(&self) -> Bytes {
        self.value.clone()
    }
}

impl LogEntry {
    pub fn builder() -> LogEntryBuilder {
        LogEntryBuilder {
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LogEntryBuilder<T = PhantomData<Term>, V = PhantomData<Bytes>> {
    term: T,
    value: V,
}

impl LogEntryBuilder<Term, Bytes> {
    pub fn build(self) -> LogEntry {
        LogEntry {
            term: self.term,
            value: self.value,
        }
    }
}

impl<T, V> LogEntryBuilder<T, V> {
    pub fn term(self, term: Term) -> LogEntryBuilder<Term, V> {
        LogEntryBuilder {
            term,
            value: self.value,
        }
    }

    pub fn value(self, value: impl Into<Bytes>) -> LogEntryBuilder<T, Bytes> {
        LogEntryBuilder {
            term: self.term,
            value: value.into(),
        }
    }
}

impl From<LogEntry> for Bytes {
    fn from(log_entry: LogEntry) -> Self {
        let mut encoded = BytesMut::new();
        _ = tansu_varint::put_u64_into(log_entry.term, &mut encoded);
        encoded.put(log_entry.value);
        Bytes::from(encoded)
    }
}

impl From<&LogEntry> for Bytes {
    fn from(log_entry: &LogEntry) -> Self {
        let mut encoded = BytesMut::new();
        _ = tansu_varint::put_u64_into(log_entry.term, &mut encoded);
        encoded.put(log_entry.value.clone());
        Bytes::from(encoded)
    }
}

impl TryFrom<Bytes> for LogEntry {
    type Error = Error;

    fn try_from(mut encoded: Bytes) -> Result<Self, Self::Error> {
        let term = tansu_varint::get_u64(&mut encoded)?;

        Ok(LogEntry {
            term,
            value: encoded,
        })
    }
}

impl TryFrom<Vec<u8>> for LogEntry {
    type Error = Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(Bytes::from(value))
    }
}
