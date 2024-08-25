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
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    mem::size_of,
    path::PathBuf,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use url::Url;

use crate::{blocking::PersistentState, persistent::PersistentEntry, Error, Result};

const IDENT: &[u8] = b"tag:tansu.io,2024-03-01/persistent-state";

#[derive(Debug)]
pub struct PersistentManager<T> {
    id: Url,
    id_size: u64,
    inner: T,
    entry: Option<PersistentEntry>,
}

impl<T> PersistentManager<T> {
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T> PersistentManager<T>
where
    T: Seek,
{
    fn seek_from(&self) -> SeekFrom {
        SeekFrom::Start(IDENT.len() as u64 + self.id_size)
    }

    fn seek(&mut self) -> Result<()> {
        _ = self.inner.seek(self.seek_from())?;
        Ok(())
    }
}

impl<T> PersistentState for PersistentManager<T>
where
    T: Read + Write + Seek + Send + Sync,
{
    fn write(&mut self, entry: PersistentEntry) -> Result<()> {
        self.seek()?;

        self.inner.write_all(&Bytes::from(entry.clone()))?;
        self.inner.flush()?;
        self.entry = Some(entry);
        Ok(())
    }

    fn read(&mut self) -> Result<PersistentEntry> {
        self.entry.clone().map_or_else(
            || {
                self.seek()?;

                let mut buf = BytesMut::zeroed(size_of::<u32>());
                self.inner.read_exact(&mut buf)?;
                let crc = Bytes::from(buf).get_u32_le();

                let (length, encoded_varint_length) =
                    tansu_varint::blocking::read_usize(&mut self.inner)?;
                let mut packet = BytesMut::zeroed(length);
                self.inner.read_exact(&mut packet)?;
                packet.put_u32_le(crc);
                packet.put(encoded_varint_length.clone());
                packet.rotate_right(size_of::<u32>() + encoded_varint_length.len());

                let entry = PersistentEntry::try_from(packet)?;
                self.entry = Some(entry.clone());
                Ok(entry)
            },
            Ok,
        )
    }

    fn id(&self) -> Url {
        self.id.clone()
    }
}

impl<T> PersistentManager<T>
where
    T: Read + Write + Seek + Send + Sync,
{
    pub fn builder(
    ) -> PersistentManagerBuilder<PhantomData<T>, PhantomData<Url>, PhantomData<PathBuf>> {
        PersistentManagerBuilder::default()
    }

    fn new(id: Url, id_size: u64, inner: T) -> Self {
        Self {
            id,
            id_size,
            inner,
            entry: None,
        }
    }

    fn recover(id: Url, mut inner: T) -> Result<Self> {
        _ = inner.seek(SeekFrom::Start(0))?;

        let mut ident = BytesMut::zeroed(IDENT.len());

        match inner.read(&mut ident)? {
            0 => {
                inner.write_all(IDENT)?;
                let s = String::from(id.clone());
                _ = tansu_varint::blocking::write_usize(&mut inner, s.len())?;
                inner.write_all(s.as_bytes())?;

                let mut pm = Self::new(id, inner.stream_position()?, inner);
                pm.write(PersistentEntry::default())?;
                Ok(pm)
            }

            _ if ident == IDENT => {
                dbg!(&ident);
                let id_size = tansu_varint::blocking::read_usize(&mut inner)?;
                let mut found_id = BytesMut::zeroed(id_size.0);

                inner.read_exact(&mut found_id)?;
                dbg!(&found_id);

                if Url::parse(&String::from_utf8(found_id.to_vec())?)? == id {
                    let mut pm = Self::new(id, inner.stream_position()?, inner);
                    _ = pm.read()?;
                    Ok(pm)
                } else {
                    Err(Error::IdMismatch)
                }
            }

            _ => Err(Error::BadIdent),
        }
    }
}

pub struct PersistentManagerBuilder<T, U, V> {
    inner: T,
    id: U,
    path: V,
}

impl<T, U, V> Default for PersistentManagerBuilder<PhantomData<T>, PhantomData<U>, PhantomData<V>> {
    fn default() -> Self {
        Self {
            inner: PhantomData,
            id: PhantomData,
            path: PhantomData,
        }
    }
}

impl<T, U, V> PersistentManagerBuilder<PhantomData<T>, PhantomData<U>, PhantomData<V>> {
    pub fn new() -> Self {
        PersistentManagerBuilder::default()
    }
}

impl<T> PersistentManagerBuilder<T, Url, PhantomData<PathBuf>>
where
    T: Read + Write + Seek + Send + Sync,
{
    pub fn recover(self) -> Result<PersistentManager<T>> {
        PersistentManager::recover(self.id, self.inner)
    }
}

impl<T> PersistentManagerBuilder<PhantomData<T>, Url, PathBuf>
where
    T: Read + Write + Seek + Send,
{
    pub fn recover(self) -> Result<PersistentManager<File>> {
        let inner = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.path.join("raft-persistent.state"))?;
        PersistentManager::recover(self.id, inner)
    }
}

impl<T, U, V> PersistentManagerBuilder<T, U, V> {
    pub fn id(self, id: impl Into<Url>) -> PersistentManagerBuilder<T, Url, V> {
        PersistentManagerBuilder {
            inner: self.inner,
            id: id.into(),
            path: self.path,
        }
    }
}

impl<T, U, V> PersistentManagerBuilder<T, U, V> {
    pub fn path(self, path: impl Into<PathBuf>) -> PersistentManagerBuilder<T, U, PathBuf> {
        PersistentManagerBuilder {
            inner: self.inner,
            id: self.id,
            path: path.into(),
        }
    }
}

impl<T, U, V> PersistentManagerBuilder<PhantomData<T>, U, V>
where
    T: Read + Write + Seek + Send,
{
    pub fn inner(self, inner: T) -> PersistentManagerBuilder<T, U, V> {
        PersistentManagerBuilder {
            inner,
            id: self.id,
            path: self.path,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use url::Url;

    use super::*;

    #[test]
    fn bad_ident() -> Result<()> {
        let mut inner = Vec::new();
        for i in b"mary had a little lamb" {
            inner.push(*i);
        }

        assert!(matches!(
            PersistentManager::builder()
                .inner(Cursor::new(inner))
                .id(Url::parse("tcp://localhost:1234/")?)
                .recover(),
            Err(Error::BadIdent)
        ));
        Ok(())
    }

    #[test]
    fn read_empty() -> Result<()> {
        let inner = Cursor::new(vec![]);
        let id = Url::parse("tcp://localhost:1234/")?;

        let mut pm = PersistentManager::builder()
            .inner(inner)
            .id(id.clone())
            .recover()?;

        let expected = PersistentEntry::builder()
            .current_term(0)
            .voted_for(None)
            .build();

        assert_eq!(id, pm.id());
        assert_eq!(expected, pm.read()?);
        Ok(())
    }

    #[test]
    fn write_vote_some_into_empty() -> Result<()> {
        let inner = Cursor::new(vec![]);
        let id = Url::parse("tcp://localhost:1234/")?;
        let term = 456;
        let mut pm = PersistentManager::builder()
            .inner(inner)
            .id(id.clone())
            .recover()?;
        let expected = PersistentEntry::builder()
            .current_term(term)
            .voted_for(Some(Url::parse("tcp://localhost:1234")?))
            .build();

        pm.write(expected.clone())?;
        dbg!(&pm.inner);
        assert_eq!(expected, pm.read()?);
        assert_eq!(id, pm.id());
        Ok(())
    }

    #[test]
    fn recovery() -> Result<()> {
        let inner = Cursor::new(vec![]);
        let id = Url::parse("tag:tansu.io,2024:self")?;
        let voted_for = Url::parse("tag:tansu.io,2024:candidate")?;
        let term = 456;
        let mut pm = PersistentManager::builder()
            .inner(inner)
            .id(id.clone())
            .recover()?;

        let expected = PersistentEntry::builder()
            .current_term(term)
            .voted_for(Some(voted_for))
            .build();

        pm.write(expected.clone())?;

        dbg!(&pm.inner);

        let mut recovered = PersistentManager::builder()
            .inner(pm.into_inner())
            .id(id.clone())
            .recover()?;

        assert_eq!(id, recovered.id());
        assert_eq!(expected, recovered.read()?);
        Ok(())
    }

    #[test]
    fn write_vote_none_into_empty() -> Result<()> {
        let inner = Cursor::new(vec![]);
        let id = Url::parse("tcp://localhost:1234/")?;
        let mut pm = PersistentManager::builder()
            .inner(inner)
            .id(id.clone())
            .recover()?;
        let expected = PersistentEntry::builder().current_term(456).build();

        pm.write(expected.clone())?;
        assert_eq!(expected, pm.read()?);
        assert_eq!(id, pm.id());
        Ok(())
    }

    #[test]
    fn overwrite_into_empty() -> Result<()> {
        let inner = Cursor::new(vec![]);
        let id = Url::parse("tcp://localhost:1234/")?;
        let mut pm = PersistentManager::builder()
            .inner(inner)
            .id(id.clone())
            .recover()?;

        let e1 = PersistentEntry::builder()
            .current_term(456)
            .voted_for(Some(Url::parse("tcp://localhost:1234")?))
            .build();

        pm.write(e1)?;

        let e2 = PersistentEntry::builder()
            .current_term(457)
            .voted_for(None)
            .build();

        pm.write(e2.clone())?;

        dbg!(&pm.inner);
        assert_eq!(e2, pm.read()?);
        assert_eq!(id, pm.id());

        Ok(())
    }

    #[test]
    fn mismatch_id() -> Result<()> {
        let inner = Cursor::new(vec![]);
        let id1 = Url::parse("tcp://localhost:1234/")?;

        let original = PersistentManager::builder()
            .inner(inner)
            .id(id1)
            .recover()?;

        let data = original.into_inner();
        dbg!(&data);

        let id2 = Url::parse("tcp://localhost:4321/")?;

        assert!(matches!(
            PersistentManager::builder().inner(data).id(id2).recover(),
            Err(Error::IdMismatch)
        ));

        Ok(())
    }

    #[test]
    fn transition_to_follower() -> Result<()> {
        let inner = Cursor::new(vec![]);
        let id = Url::parse("tcp://localhost:1234/")?;
        let current_term = 456;

        let mut pm = PersistentManager::builder().inner(inner).id(id).recover()?;

        pm.write(
            PersistentEntry::builder()
                .current_term(current_term)
                .voted_for(Some(Url::parse("tcp://localhost:1234")?))
                .build(),
        )?;

        pm.transition_to_follower(current_term + 5)?;

        assert_eq!(
            PersistentEntry::builder()
                .current_term(current_term + 5)
                .voted_for(None)
                .build(),
            pm.read()?
        );

        pm.transition_to_follower(current_term + 15)?;

        assert_eq!(
            PersistentEntry::builder()
                .current_term(current_term + 15)
                .voted_for(None)
                .build(),
            pm.read()?
        );

        Ok(())
    }

    #[test]
    fn transition_to_candidate() -> Result<()> {
        let inner = Cursor::new(vec![]);
        let id = Url::parse("tcp://localhost:1234")?;
        let current_term = 456;

        let mut pm = PersistentManager::builder()
            .inner(inner)
            .id(id.clone())
            .recover()?;

        pm.write(
            PersistentEntry::builder()
                .current_term(current_term)
                .voted_for(Some(Url::parse("tcp://localhost:32123")?))
                .build(),
        )?;

        pm.transition_to_candidate()?;

        assert_eq!(
            PersistentEntry::builder()
                .current_term(current_term + 1)
                .voted_for(Some(id.clone()))
                .build(),
            pm.read()?
        );

        pm.transition_to_candidate()?;

        assert_eq!(
            PersistentEntry::builder()
                .current_term(current_term + 2)
                .voted_for(Some(id))
                .build(),
            pm.read()?
        );

        Ok(())
    }

    #[test]
    fn transition_to_leader() -> Result<()> {
        let inner = Cursor::new(vec![]);
        let id = Url::parse("tcp://localhost:1234/")?;
        let current_term = 456;
        let voted_for = Some(Url::parse("tcp://localhost:1234")?);

        let mut pm = PersistentManager::builder()
            .inner(inner)
            .id(id.clone())
            .recover()?;

        pm.write(
            PersistentEntry::builder()
                .current_term(current_term)
                .voted_for(voted_for.clone())
                .build(),
        )?;

        pm.transition_to_leader()?;

        assert_eq!(
            PersistentEntry::builder()
                .current_term(current_term)
                .voted_for(voted_for.clone())
                .build(),
            pm.read()?
        );

        pm.transition_to_leader()?;

        assert_eq!(
            PersistentEntry::builder()
                .current_term(current_term)
                .voted_for(voted_for)
                .build(),
            pm.read()?
        );

        Ok(())
    }
}
