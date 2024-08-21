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

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc::Crc;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{Error, Result, Term};

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PersistentEntry {
    current_term: Term,
    voted_for: Option<Url>,
}

impl PersistentEntry {
    pub fn current_term(&self) -> Term {
        self.current_term
    }

    pub fn voted_for(&self) -> Option<Url> {
        self.voted_for.clone()
    }

    pub fn transition_to_follower(self, current_term: Term) -> Self {
        Self {
            voted_for: None,
            current_term,
        }
    }

    pub fn transition_to_candidate(self, voted_for: Url) -> Self {
        Self {
            current_term: self.current_term + 1,
            voted_for: Some(voted_for),
        }
    }

    pub fn transition_to_leader(self) -> Self {
        Self { ..self }
    }

    pub fn builder() -> EntryBuilder {
        EntryBuilder::default()
    }
}

#[derive(Clone, Default, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EntryBuilder<T = PhantomData<Term>> {
    current_term: T,
    voted_for: Option<Url>,
}

impl<T> EntryBuilder<T> {
    pub fn current_term(self, current_term: Term) -> EntryBuilder<Term> {
        EntryBuilder {
            current_term,
            voted_for: self.voted_for,
        }
    }

    pub fn voted_for(self, voted_for: Option<Url>) -> Self {
        Self { voted_for, ..self }
    }
}

impl EntryBuilder<Term> {
    pub fn build(self) -> PersistentEntry {
        PersistentEntry {
            current_term: self.current_term,
            voted_for: self.voted_for,
        }
    }
}

impl From<PersistentEntry> for Bytes {
    fn from(value: PersistentEntry) -> Self {
        let mut encoded = BytesMut::new();
        let mut size = 0;
        size += tansu_varint::put_u64_into(value.current_term, &mut encoded);

        if let Some(voted_for) = value.voted_for {
            let s = String::from(voted_for);
            size += s.len();
            encoded.put(s.as_bytes())
        }

        let size_varint_length = tansu_varint::put_usize_into(size, &mut encoded);
        encoded.rotate_right(size_varint_length);

        encoded.put_u32_le(Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(&encoded));
        encoded.rotate_right(size_of::<u32>());

        Bytes::from(encoded)
    }
}

impl TryFrom<BytesMut> for PersistentEntry {
    type Error = Error;

    fn try_from(value: BytesMut) -> Result<Self, Self::Error> {
        PersistentEntry::try_from(Bytes::from(value))
    }
}

impl TryFrom<Bytes> for PersistentEntry {
    type Error = Error;

    fn try_from(mut encoded: Bytes) -> Result<Self, Self::Error> {
        let crc = encoded.get_u32_le();

        if Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(&encoded) == crc {
            let _length = tansu_varint::get_usize(&mut encoded);

            Ok(PersistentEntry {
                current_term: tansu_varint::get_u64(&mut encoded)?,
                voted_for: if encoded.has_remaining() {
                    Some(Url::parse(&String::from_utf8(encoded.into())?)?)
                } else {
                    None
                },
            })
        } else {
            Err(Error::Crc)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Result;

    #[test]
    fn none_from_try_from() -> Result<()> {
        let expected = PersistentEntry {
            current_term: 12321,
            voted_for: None,
        };

        let encoded = Bytes::from(expected.clone());
        assert_eq!(expected, PersistentEntry::try_from(encoded)?);

        Ok(())
    }

    #[test]
    fn some_from_try_from() -> Result<()> {
        let expected = PersistentEntry {
            current_term: 12321,
            voted_for: Some(Url::parse("tcp://localhost:1234")?),
        };

        let encoded = Bytes::from(expected.clone());
        assert_eq!(expected, PersistentEntry::try_from(encoded)?);

        Ok(())
    }

    #[test]
    fn transition_to_follower() -> Result<()> {
        let current_term = 456;

        assert_eq!(
            PersistentEntry::builder()
                .current_term(current_term + 5)
                .voted_for(None)
                .build(),
            PersistentEntry::builder()
                .current_term(current_term)
                .voted_for(Some(Url::parse("tcp://localhost:1234")?))
                .build()
                .transition_to_follower(current_term + 5)
        );

        Ok(())
    }

    #[test]
    fn transition_to_candidate() -> Result<()> {
        let current_term = 456;
        let voted_for = Url::parse("tcp://localhost:1234")?;
        let other = Url::parse("tcp://localhost:32123")?;

        assert_eq!(
            PersistentEntry::builder()
                .current_term(current_term + 1)
                .voted_for(Some(voted_for.clone()))
                .build(),
            PersistentEntry::builder()
                .current_term(current_term)
                .voted_for(Some(other))
                .build()
                .transition_to_candidate(voted_for)
        );

        Ok(())
    }

    #[test]
    fn transition_to_leader() -> Result<()> {
        let current_term = 456;
        let voted_for = Some(Url::parse("tcp://localhost:1234")?);

        assert_eq!(
            PersistentEntry::builder()
                .current_term(current_term)
                .voted_for(voted_for.clone())
                .build(),
            PersistentEntry::builder()
                .current_term(current_term)
                .voted_for(voted_for)
                .build()
                .transition_to_leader()
        );

        Ok(())
    }
}
