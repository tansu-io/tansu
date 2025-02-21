// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use tracing::debug;

use super::{Time, TimeProvider};
use crate::{Error, Result, TopitionOffset};
use std::{
    cmp::Ordering,
    fmt::{self, Debug, Formatter},
    fs::{File, OpenOptions, create_dir_all},
    io::{Cursor, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MemoryTimeProvider<'data> {
    data: &'data [u8],
}

impl<'data> MemoryTimeProvider<'data> {
    pub fn new(data: &'data [u8]) -> Self {
        Self { data }
    }
}

impl TimeProvider for MemoryTimeProvider<'_> {
    fn provide_time(&self, tpo: &TopitionOffset) -> Result<Box<dyn Time>> {
        debug!(?tpo);

        let index = TimeIndex::builder()
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
            .with_extension("timeindex")
    }
}

impl<P> TimeProvider for FileSystemOffsetProvider<P>
where
    P: AsRef<Path> + Debug + Send + Sync,
{
    fn provide_time(&self, tpo: &TopitionOffset) -> Result<Box<dyn Time>> {
        debug!(?tpo);

        create_dir_all(self.dir.as_ref().join(PathBuf::from(tpo.topition())))?;

        let index_name = self.filename(tpo);
        debug!(?index_name);

        let index = TimeIndex::builder()
            .base_offset(tpo.offset())
            .file_system(index_name)?
            .build();

        Ok(Box::new(index))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Builder;

pub struct TimeIndex<S> {
    storage: S,
    base_offset: i64,
    entries: u32,
    min_time: Option<i64>,
    max_time: Option<i64>,
}

impl<S> Debug for TimeIndex<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Offset))
            .field("base_offset", &self.base_offset)
            .field("entries", &self.entries)
            .field("min_time", &self.min_time)
            .field("max_time", &self.max_time)
            .finish()
    }
}

pub struct TimeIndexBuilder<S> {
    storage: S,
    base_offset: i64,
}

impl<S> Debug for TimeIndexBuilder<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(TimeIndexBuilder))
            .field("base_offset", &self.base_offset)
            .finish()
    }
}

impl TimeIndex<PhantomData<Builder>> {
    pub fn builder() -> TimeIndexBuilder<Builder> {
        TimeIndexBuilder {
            storage: Builder,
            base_offset: 0,
        }
    }
}

impl<S> TimeIndexBuilder<S> {
    pub fn base_offset(self, base_offset: i64) -> Self {
        Self {
            base_offset,
            ..self
        }
    }
}

impl TimeIndexBuilder<Builder> {
    pub fn in_memory<T>(self, data: T) -> TimeIndexBuilder<Cursor<T>>
    where
        T: AsRef<[u8]>,
    {
        TimeIndexBuilder {
            storage: Cursor::new(data),
            base_offset: self.base_offset,
        }
    }

    pub fn file_system<P>(self, path: P) -> Result<TimeIndexBuilder<File>>
    where
        P: AsRef<Path>,
    {
        OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .map(|storage| TimeIndexBuilder {
                storage,
                base_offset: self.base_offset,
            })
            .map_err(Into::into)
    }
}

impl<S> TimeIndexBuilder<S>
where
    S: Read + Seek + Write,
{
    pub fn build(self) -> TimeIndex<S> {
        TimeIndex {
            storage: self.storage,
            base_offset: self.base_offset,
            entries: 0,
            min_time: None,
            max_time: None,
        }
    }
}

impl<S> Time for TimeIndex<S>
where
    S: Read + Seek + Send + Write,
{
    fn append(&mut self, time: i64, offset: i64) -> Result<()> {
        debug!(?time, ?offset);

        if self.max_time.is_none() || self.max_time.is_some_and(|max_time| time > max_time) {
            self.storage
                .seek(SeekFrom::End(0))
                .map_err(Into::into)
                .and(
                    self.relative_offset(offset)
                        .and_then(|relative| self.write(time, relative)),
                )
                .map(|_| {
                    self.entries += 1;
                    _ = self.max_time.replace(time);
                })
        } else {
            Err(Error::LessThanMaxTime {
                time,
                max_time: self.max_time,
            })
        }
    }

    fn offset_for_time(&mut self, time: i64) -> Result<i64> {
        if self.min_time.is_some_and(|min_time| time < min_time) {
            Err(Error::LessThanMinTime {
                time,
                min_time: self.min_time,
            })
        } else if self.entries == 0 {
            Ok(self.base_offset)
        } else {
            self.search(time, 0, self.entries - 1)
        }
    }
}

impl<S> TimeIndex<S>
where
    S: Read + Seek + Write,
{
    fn search(&mut self, time: i64, begin: u32, end: u32) -> Result<i64> {
        let midpoint = (begin + end + 1) >> 1;
        debug!(?time, ?begin, ?end);

        self.entry_at(midpoint)
            .inspect(|entry| debug!(?midpoint, ?entry))
            .and_then(|entry| match time.cmp(&entry.time) {
                ordering @ Ordering::Less => {
                    debug!(?ordering, ?begin, ?midpoint, ?end, ?self.entries);

                    if end - begin > 1 {
                        self.search(time, begin, midpoint)
                    } else if midpoint >= 1 {
                        self.entry_at(midpoint - 1).map(|entry| match entry {
                            entry @ Entry { .. } if entry.time > time && midpoint == 1 => {
                                self.offset(0)
                            }

                            Entry {
                                relative_offset, ..
                            } => self.offset(relative_offset),
                        })
                    } else {
                        Ok(self.offset(entry.relative_offset))
                    }
                }

                ordering @ Ordering::Equal => {
                    debug!(?ordering);

                    Ok(self.offset(entry.relative_offset))
                }

                ordering @ Ordering::Greater => {
                    debug!(?ordering, ?begin, ?midpoint, ?end, ?self.entries);

                    if end - begin > 1 {
                        self.search(time, midpoint, end)
                    } else if midpoint + 1 < self.entries {
                        self.entry_at(midpoint + 1)
                            .inspect(|entry| debug!(?entry))
                            .map(|entry| self.offset(entry.relative_offset))
                    } else {
                        Ok(self.offset(entry.relative_offset))
                    }
                }
            })
    }

    fn offset(&self, relative_offset: u32) -> i64 {
        self.base_offset + i64::from(relative_offset)
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
        debug!(?nth);
        if nth < self.entries {
            self.seek_to_nth(nth).and(self.read())
        } else {
            Err(Error::NoSuchEntry { nth })
        }
    }

    fn seek_to_nth(&mut self, nth: u32) -> Result<u64> {
        let position = 12 * nth as u64;
        self.storage
            .seek(SeekFrom::Start(position))
            .map_err(Into::into)
    }

    fn read(&mut self) -> Result<Entry> {
        let mut time = [0u8; 8];
        let mut relative_offset = [0u8; 4];
        self.storage
            .read_exact(&mut time)
            .and(self.storage.read_exact(&mut relative_offset))
            .map(|()| {
                debug!(?time, ?relative_offset);
                Entry {
                    time: i64::from_be_bytes(time),
                    relative_offset: u32::from_be_bytes(relative_offset),
                }
            })
            .map_err(Into::into)
    }

    fn write(&mut self, time: i64, relative_offset: u32) -> Result<()> {
        debug!(?time, ?relative_offset);
        self.storage
            .write_all(&time.to_be_bytes())
            .and(self.storage.write_all(&relative_offset.to_be_bytes()))
            .map_err(Into::into)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Entry {
    time: i64,
    relative_offset: u32,
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
    fn monotonically_increasing_timestamp() -> Result<()> {
        let _guard = init_tracing()?;

        let mut index = TimeIndex::builder()
            .base_offset(0)
            .in_memory(vec![])
            .build();
        index.append(1, 1)?;
        index.append(2, 2)?;
        index.append(5, 5)?;

        assert!(matches!(
            index.append(3, 3),
            Err(Error::LessThanMaxTime {
                time: 3,
                max_time: Some(5)
            })
        ));
        Ok(())
    }

    #[test]
    fn entry_at() -> Result<()> {
        let _guard = init_tracing()?;

        let base_offset = 54_345;
        let mut index = TimeIndex::builder()
            .base_offset(base_offset)
            .in_memory(vec![])
            .build();

        index.append(0, base_offset + 5)?;
        index.append(1, base_offset + 11)?;
        index.append(2, base_offset + 172)?;
        index.append(5, base_offset + 245)?;

        assert_eq!(5, index.entry_at(0).map(|entry| entry.relative_offset)?);
        assert_eq!(11, index.entry_at(1).map(|entry| entry.relative_offset)?);
        assert_eq!(172, index.entry_at(2).map(|entry| entry.relative_offset)?);
        assert_eq!(245, index.entry_at(3).map(|entry| entry.relative_offset)?);

        assert!(matches!(
            index.entry_at(4),
            Err(Error::NoSuchEntry { nth: 4 })
        ));

        Ok(())
    }

    #[test]
    fn append_must_be_bigger_than_base_offset() -> Result<()> {
        let _guard = init_tracing()?;

        let mut index = TimeIndex::builder()
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

        let base_offset = 32123;
        let mut index = TimeIndex::builder()
            .base_offset(base_offset)
            .in_memory(vec![])
            .build();

        assert_eq!(base_offset, index.offset_for_time(98789)?);
        Ok(())
    }

    #[test]
    fn offset_for_time_with_indexed_position() -> Result<()> {
        let _guard = init_tracing()?;

        let mut index = TimeIndex::builder()
            .base_offset(0)
            .in_memory(vec![])
            .build();
        index.append(55, 1)?;
        index.append(66, 2)?;
        index.append(77, 5)?;
        index.append(88, 7)?;

        assert_eq!(1, index.offset_for_time(55)?);
        assert_eq!(2, index.offset_for_time(66)?);
        assert_eq!(5, index.offset_for_time(77)?);
        assert_eq!(7, index.offset_for_time(88)?);

        assert_eq!(2, index.offset_for_time(70)?);

        Ok(())
    }

    #[test]
    fn offset_for_time_with_unindexed_mid_position() -> Result<()> {
        let _guard = init_tracing()?;

        let mut index = TimeIndex::builder()
            .base_offset(0)
            .in_memory(vec![])
            .build();
        index.append(55, 1)?;
        index.append(66, 2)?;
        index.append(77, 5)?;
        index.append(88, 7)?;

        assert_eq!(2, index.offset_for_time(70)?);

        Ok(())
    }

    #[test]
    fn offset_for_time_bigger_than_indexed() -> Result<()> {
        let _guard = init_tracing()?;

        let mut index = TimeIndex::builder()
            .base_offset(0)
            .in_memory(vec![])
            .build();
        index.append(55, 1)?;
        index.append(66, 2)?;
        index.append(77, 5)?;
        index.append(88, 7)?;

        assert_eq!(7, index.offset_for_time(90)?);
        Ok(())
    }

    #[test]
    fn offset_for_time_smaller_than_indexed() -> Result<()> {
        let _guard = init_tracing()?;

        let base_offset = 23;
        let mut index = TimeIndex::builder()
            .base_offset(base_offset)
            .in_memory(vec![])
            .build();
        index.append(55, base_offset + 1)?;
        index.append(66, base_offset + 2)?;
        index.append(77, base_offset + 5)?;
        index.append(88, base_offset + 7)?;

        assert_eq!(base_offset, index.offset_for_time(50)?);
        Ok(())
    }
}
