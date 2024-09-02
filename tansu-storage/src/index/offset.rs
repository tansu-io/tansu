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

use super::{Offset, OffsetProvider};
use crate::{Error, Result, TopitionOffset};
use std::{
    cmp::Ordering,
    fmt::{self, Debug, Formatter},
    fs::{create_dir_all, File, OpenOptions},
    io::{Cursor, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
};
use tracing::debug;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MemoryOffsetProvider<'data> {
    data: &'data [u8],
}

impl<'data> MemoryOffsetProvider<'data> {
    pub fn new(data: &'data [u8]) -> Self {
        Self { data }
    }
}

impl<'data> OffsetProvider for MemoryOffsetProvider<'data> {
    fn provide_offset(&self, tpo: &TopitionOffset) -> Result<Box<dyn Offset>> {
        debug!(?tpo);

        let index = OffsetIndex::builder()
            .base_offset(tpo.offset())
            .in_memory(vec![])
            .build();

        Ok(Box::new(index))
    }
}

#[derive(Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FileSystemOffsetProvider<P> {
    dir: P,
}

impl<P> FileSystemOffsetProvider<P>
where
    P: AsRef<Path> + Debug + Send + Sync,
{
    pub fn new(dir: P) -> Self {
        Self { dir }
    }

    fn filename(&self, tpo: &TopitionOffset) -> PathBuf {
        self.dir
            .as_ref()
            .join(PathBuf::from(tpo))
            .with_extension("index")
    }
}

impl<P> OffsetProvider for FileSystemOffsetProvider<P>
where
    P: AsRef<Path> + Debug + Send + Sync,
{
    fn provide_offset(&self, tpo: &TopitionOffset) -> Result<Box<dyn Offset>> {
        debug!(?tpo);

        create_dir_all(self.dir.as_ref().join(PathBuf::from(tpo.topition())))?;

        let index_name = self.filename(tpo);
        debug!(?index_name);

        let index = OffsetIndex::builder()
            .base_offset(tpo.offset())
            .file_system(index_name)?
            .build();

        Ok(Box::new(index))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Builder;

pub struct OffsetIndex<S> {
    storage: S,
    base_offset: i64,
    entries: u32,
    last_offset: Option<i64>,
}

impl<S> Debug for OffsetIndex<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Offset))
            .field("base_offset", &self.base_offset)
            .field("entries", &self.entries)
            .field("last_offset", &self.last_offset)
            .finish()
    }
}

pub struct OffsetIndexBuilder<S> {
    storage: S,
    base_offset: i64,
}

impl<S> Debug for OffsetIndexBuilder<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(OffsetIndexBuilder))
            .field("base_offset", &self.base_offset)
            .finish()
    }
}

impl OffsetIndex<PhantomData<Builder>> {
    pub fn builder() -> OffsetIndexBuilder<Builder> {
        OffsetIndexBuilder {
            storage: Builder,
            base_offset: 0,
        }
    }
}

impl<S> OffsetIndexBuilder<S> {
    pub fn base_offset(self, base_offset: i64) -> Self {
        Self {
            base_offset,
            ..self
        }
    }
}

impl OffsetIndexBuilder<Builder> {
    pub fn in_memory<T>(self, data: T) -> OffsetIndexBuilder<Cursor<T>>
    where
        T: AsRef<[u8]>,
    {
        OffsetIndexBuilder {
            storage: Cursor::new(data),
            base_offset: self.base_offset,
        }
    }

    pub fn file_system<P>(self, path: P) -> Result<OffsetIndexBuilder<File>>
    where
        P: AsRef<Path>,
    {
        OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .map(|storage| OffsetIndexBuilder {
                storage,
                base_offset: self.base_offset,
            })
            .map_err(Into::into)
    }
}

impl<S> OffsetIndexBuilder<S>
where
    S: Read + Seek + Write,
{
    pub fn build(self) -> OffsetIndex<S> {
        OffsetIndex {
            storage: self.storage,
            base_offset: self.base_offset,
            entries: 0,
            last_offset: None,
        }
    }
}

impl<S> Offset for OffsetIndex<S>
where
    S: Read + Seek + Send + Write,
{
    fn append(&mut self, offset: i64, position: u64) -> Result<()> {
        debug!(?offset, ?position);

        if self.last_offset.is_none()
            || self
                .last_offset
                .is_some_and(|last_offset| offset > last_offset)
        {
            u32::try_from(position)
                .map_err(Into::into)
                .and_then(|position| {
                    self.storage.seek(SeekFrom::End(0)).map_err(Into::into).and(
                        self.relative_offset(offset)
                            .and_then(|relative| self.write(relative, position)),
                    )
                })
                .map(|()| {
                    self.entries += 1;
                    _ = self.last_offset.replace(offset);
                })
        } else {
            Err(Error::LessThanLastOffset {
                offset,
                last_offset: self.last_offset,
            })
        }
    }

    fn position_for_offset(&mut self, offset: i64) -> Result<u64> {
        if offset < self.base_offset {
            Err(Error::LessThanBaseOffset {
                offset,
                base_offset: self.base_offset,
            })
        } else if self.entries == 0 {
            Ok(0)
        } else {
            self.relative_offset(offset)
                .and_then(|offset| self.search(offset, 0, self.entries - 1))
        }
    }
}

impl<S> OffsetIndex<S>
where
    S: Read + Seek + Write,
{
    fn search(&mut self, relative_offset: u32, begin: u32, end: u32) -> Result<u64> {
        let midpoint = (begin + end + 1) >> 1;
        debug!(?relative_offset, ?begin, ?end);

        self.entry_at(midpoint)
            .inspect(|entry| debug!(?midpoint, ?entry))
            .and_then(|entry| match relative_offset.cmp(&entry.relative_offset) {
                ordering @ Ordering::Less => {
                    debug!(?ordering, ?begin, ?midpoint, ?end, ?self.entries);

                    if end - begin > 1 {
                        self.search(relative_offset, begin, midpoint)
                    } else if midpoint >= 1 {
                        self.entry_at(midpoint - 1).map(|entry| match entry {
                            entry @ Entry { .. }
                                if entry.relative_offset > relative_offset && midpoint == 1 =>
                            {
                                0
                            }

                            Entry { position, .. } => u64::from(position),
                        })
                    } else {
                        Ok(u64::from(entry.position))
                    }
                }

                ordering @ Ordering::Equal => {
                    debug!(?ordering);

                    Ok(u64::from(entry.position))
                }

                ordering @ Ordering::Greater => {
                    debug!(?ordering, ?begin, ?midpoint, ?end, ?self.entries);

                    if end - begin > 1 {
                        self.search(relative_offset, midpoint, end)
                    } else if midpoint + 1 < self.entries {
                        self.entry_at(midpoint + 1)
                            .inspect(|entry| debug!(?entry))
                            .map(|entry| u64::from(entry.position))
                    } else {
                        Ok(u64::from(entry.position))
                    }
                }
            })
    }

    fn relative_offset(&self, offset: i64) -> Result<u32> {
        if offset >= self.base_offset {
            u32::try_from(offset - self.base_offset).map_err(Into::into)
        } else {
            Err(Error::LessThanBaseOffset {
                offset,
                base_offset: self.base_offset,
            })
        }
    }

    fn entry_at(&mut self, nth: u32) -> Result<Entry> {
        if nth < self.entries {
            self.seek_to_nth(nth).and(self.read())
        } else {
            Err(Error::NoSuchEntry { nth })
        }
    }

    fn seek_to_nth(&mut self, nth: u32) -> Result<u64> {
        let position = 8 * u64::from(nth);
        self.storage
            .seek(SeekFrom::Start(position))
            .map_err(Into::into)
    }

    fn read(&mut self) -> Result<Entry> {
        let mut relative_offset = [0u8; 4];
        let mut position = [0u8; 4];
        self.storage
            .read_exact(&mut relative_offset)
            .and(self.storage.read_exact(&mut position))
            .and(Ok(Entry {
                relative_offset: u32::from_be_bytes(relative_offset),
                position: u32::from_be_bytes(position),
            }))
            .map_err(Into::into)
    }

    fn write(&mut self, relative_offset: u32, position: u32) -> Result<()> {
        self.storage
            .write_all(&relative_offset.to_be_bytes())
            .and(self.storage.write_all(&position.to_be_bytes()))
            .map_err(Into::into)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Entry {
    relative_offset: u32,
    position: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs::File, sync::Arc, thread};
    use tracing::subscriber::DefaultGuard;

    #[cfg(miri)]
    fn init_tracing() -> Result<()> {
        Ok(())
    }

    #[cfg(not(miri))]
    fn init_tracing() -> Result<DefaultGuard> {
        let subscriber = tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(
                thread::current()
                    .name()
                    .ok_or(Error::Message(String::from("unnamed thread")))
                    .and_then(|name| {
                        File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME")))
                            .map_err(Into::into)
                    })
                    .map(Arc::new)?,
            )
            .finish();

        Ok(tracing::subscriber::set_default(subscriber))
    }

    #[test]
    fn monotonically_increasing_offset() -> Result<()> {
        let _guard = init_tracing()?;

        let mut index = OffsetIndex::builder()
            .base_offset(0)
            .in_memory(vec![])
            .build();
        index.append(1, 1)?;
        index.append(2, 2)?;
        index.append(5, 5)?;

        assert!(matches!(
            index.append(3, 3),
            Err(Error::LessThanLastOffset {
                offset: 3,
                last_offset: Some(5)
            })
        ));
        Ok(())
    }

    #[test]
    fn entry_at() -> Result<()> {
        let _guard = init_tracing()?;

        let base_offset = 54_345;
        let mut index = OffsetIndex::builder()
            .base_offset(base_offset)
            .in_memory(vec![])
            .build();

        index.append(base_offset + 5, 0)?;
        index.append(base_offset + 11, 1)?;
        index.append(base_offset + 172, 2)?;
        index.append(base_offset + 245, 5)?;

        assert_eq!(0, index.entry_at(0).map(|entry| entry.position)?);
        assert_eq!(1, index.entry_at(1).map(|entry| entry.position)?);
        assert_eq!(2, index.entry_at(2).map(|entry| entry.position)?);
        assert_eq!(5, index.entry_at(3).map(|entry| entry.position)?);

        assert!(matches!(
            index.entry_at(4),
            Err(Error::NoSuchEntry { nth: 4 })
        ));

        Ok(())
    }

    #[test]
    fn reject_append_position_too_large() -> Result<()> {
        let base_offset = 32_123;
        let mut index = OffsetIndex::builder()
            .base_offset(base_offset)
            .in_memory(vec![])
            .build();

        index.append(base_offset, u32::MAX as u64)?;

        assert!(matches!(
            index.append(base_offset + 1, (u32::MAX as u64) + 1),
            Err(Error::TryFromInt(_))
        ));

        Ok(())
    }

    #[test]
    fn append_must_be_bigger_than_base_offset() -> Result<()> {
        let _guard = init_tracing()?;

        let mut index = OffsetIndex::builder()
            .base_offset(32123)
            .in_memory(vec![])
            .build();

        assert!(matches!(
            index.append(12321, 12321),
            Err(Error::LessThanBaseOffset {
                base_offset: 32123,
                offset: 12321
            })
        ));
        Ok(())
    }

    #[test]
    fn position_for_offset_when_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let mut index = OffsetIndex::builder()
            .base_offset(32123)
            .in_memory(vec![])
            .build();

        assert_eq!(0, index.position_for_offset(32123)?);
        Ok(())
    }

    #[test]
    fn position_for_offset_with_indexed_position() -> Result<()> {
        let _guard = init_tracing()?;

        let mut index = OffsetIndex::builder()
            .base_offset(0)
            .in_memory(vec![])
            .build();
        index.append(55, 1)?;
        index.append(66, 2)?;
        index.append(77, 5)?;
        index.append(88, 7)?;

        assert_eq!(1, index.position_for_offset(55)?);
        assert_eq!(2, index.position_for_offset(66)?);
        assert_eq!(5, index.position_for_offset(77)?);
        assert_eq!(7, index.position_for_offset(88)?);

        assert_eq!(2, index.position_for_offset(70)?);

        Ok(())
    }

    #[test]
    fn position_for_offset_with_unindexed_mid_position() -> Result<()> {
        let _guard = init_tracing()?;

        let mut index = OffsetIndex::builder()
            .base_offset(0)
            .in_memory(vec![])
            .build();
        index.append(55, 1)?;
        index.append(66, 2)?;
        index.append(77, 5)?;
        index.append(88, 7)?;

        assert_eq!(2, index.position_for_offset(70)?);

        Ok(())
    }

    #[test]
    fn position_for_offset_bigger_than_indexed() -> Result<()> {
        let _guard = init_tracing()?;

        let mut index = OffsetIndex::builder()
            .base_offset(0)
            .in_memory(vec![])
            .build();
        index.append(55, 1)?;
        index.append(66, 2)?;
        index.append(77, 5)?;
        index.append(88, 7)?;

        assert_eq!(7, index.position_for_offset(90)?);
        Ok(())
    }

    #[test]
    fn position_for_offset_smaller_than_indexed() -> Result<()> {
        let _guard = init_tracing()?;

        let mut index = OffsetIndex::builder()
            .base_offset(23)
            .in_memory(vec![])
            .build();
        index.append(55, 1)?;
        index.append(66, 2)?;
        index.append(77, 5)?;
        index.append(88, 7)?;

        assert_eq!(0, index.position_for_offset(50)?);
        Ok(())
    }
}
