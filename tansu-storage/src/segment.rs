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

use crate::{
    index::{
        offset::{FileSystemOffsetProvider, OffsetIndex},
        Offset, OffsetProvider,
    },
    Error, Result, Topition, TopitionOffset,
};
use bytes::Bytes;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{self, Debug, Formatter},
    fs::{create_dir_all, DirEntry, File, OpenOptions},
    future::Future,
    io::{Cursor, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    ops::RangeFrom,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll, Waker},
    time::Instant,
};
use tansu_kafka_sans_io::{
    record::{batch, compression::Batch},
    Decoder, Encoder,
};
use tracing::{debug, instrument};

pub trait Segment: Debug + Send {
    fn append(&mut self, batch: Batch) -> Result<i64>;
    fn read(&mut self, starting_offset: i64) -> Result<Batch>;
    fn base_offset(&self) -> i64;
    fn max_offset(&self) -> Option<i64>;
    fn register_pending_offset(&self, waker: Waker) -> Result<()>;
    fn bytes_since_last_index_entry(&self) -> u64;
    fn truncate_from_offset(&mut self, range: RangeFrom<i64>) -> Result<()>;
}

impl<T: Segment + ?Sized> Segment for Box<T> {
    fn append(&mut self, batch: Batch) -> Result<i64> {
        (**self).append(batch)
    }

    fn read(&mut self, starting_offset: i64) -> Result<Batch> {
        (**self).read(starting_offset)
    }

    fn base_offset(&self) -> i64 {
        (**self).base_offset()
    }

    fn max_offset(&self) -> Option<i64> {
        (**self).max_offset()
    }

    fn register_pending_offset(&self, waker: Waker) -> Result<()> {
        (**self).register_pending_offset(waker)
    }

    fn bytes_since_last_index_entry(&self) -> u64 {
        (**self).bytes_since_last_index_entry()
    }

    fn truncate_from_offset(&mut self, range: RangeFrom<i64>) -> Result<()> {
        (**self).truncate_from_offset(range)
    }
}

pub trait SegmentProvider: Debug + Send {
    #[allow(clippy::type_complexity)]
    fn init(&self) -> Result<BTreeMap<Topition, BTreeMap<i64, Box<dyn Segment>>>>;

    fn provide_segment(&self, tpo: &TopitionOffset) -> Result<Box<dyn Segment>>;
}

impl<T: SegmentProvider + ?Sized> SegmentProvider for Box<T> {
    fn init(&self) -> Result<BTreeMap<Topition, BTreeMap<i64, Box<dyn Segment>>>> {
        (**self).init()
    }

    fn provide_segment(&self, tpo: &TopitionOffset) -> Result<Box<dyn Segment>> {
        (**self).provide_segment(tpo)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Builder;

pub struct LogSegment<S, O> {
    storage: S,
    base_offset: i64,
    created: Instant,
    offsets: O,
    bytes_since_last_index_entry: u64,
    index_interval_bytes: u64,
    max_offset: Option<i64>,
    pending_offset: Arc<Mutex<Vec<Waker>>>,
}

impl<S, O> Debug for LogSegment<S, O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(LogSegment))
            .field("base_offset", &self.base_offset)
            .field("created", &self.created)
            .field(
                "bytes_since_last_index_entry",
                &self.bytes_since_last_index_entry,
            )
            .field("index_interval_bytes", &self.index_interval_bytes)
            .field("max_offset", &self.max_offset)
            .finish()
    }
}

impl LogSegment<PhantomData<Builder>, PhantomData<Builder>> {
    pub fn builder() -> LogSegmentBuilder<Builder, Builder> {
        LogSegmentBuilder {
            storage: Builder,
            base_offset: 0,
            created: Instant::now(),
            offsets: Builder,
            index_interval_bytes: 4_096,
        }
    }
}

pub struct LogSegmentBuilder<S, O> {
    storage: S,
    base_offset: i64,
    created: Instant,
    offsets: O,
    index_interval_bytes: u64,
}

impl<S, O> Debug for LogSegmentBuilder<S, O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(LogSegment))
            .field("base_offset", &self.base_offset)
            .field("created", &self.created)
            .field("index_interval_bytes", &self.index_interval_bytes)
            .finish()
    }
}

impl<S, O> LogSegmentBuilder<S, O> {
    pub fn base_offset(self, base_offset: i64) -> Self {
        Self {
            base_offset,
            ..self
        }
    }

    pub fn index_interval_bytes(self, index_interval_bytes: u64) -> Self {
        Self {
            index_interval_bytes,
            ..self
        }
    }
}

impl<O> LogSegmentBuilder<Builder, O> {
    pub fn file_system<P: AsRef<Path>>(self, path: P) -> Result<LogSegmentBuilder<File, O>> {
        OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .map(|storage| LogSegmentBuilder {
                storage,
                base_offset: self.base_offset,
                created: self.created,
                offsets: self.offsets,
                index_interval_bytes: self.index_interval_bytes,
            })
            .map_err(Into::into)
    }

    pub fn in_memory(self, data: &[u8]) -> LogSegmentBuilder<Cursor<Vec<u8>>, O> {
        debug!(?data);

        LogSegmentBuilder {
            storage: Cursor::new(data.to_vec()),
            base_offset: self.base_offset,
            created: self.created,
            offsets: self.offsets,
            index_interval_bytes: self.index_interval_bytes,
        }
    }
}

impl<S> LogSegmentBuilder<S, Builder> {
    pub fn offsets<O>(self, offsets: O) -> LogSegmentBuilder<S, O>
    where
        O: Offset,
    {
        LogSegmentBuilder {
            storage: self.storage,
            base_offset: self.base_offset,
            created: self.created,
            offsets,
            index_interval_bytes: self.index_interval_bytes,
        }
    }
}

impl<S, O> LogSegmentBuilder<S, O>
where
    S: Read + Seek + Write,
    O: Offset,
{
    pub fn build(self) -> LogSegment<S, O> {
        LogSegment {
            storage: self.storage,
            base_offset: self.base_offset,
            created: self.created,
            offsets: self.offsets,
            bytes_since_last_index_entry: 0,
            index_interval_bytes: self.index_interval_bytes,
            max_offset: None,
            pending_offset: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

pub trait Truncate {
    fn truncate_from(&mut self, position: u64) -> Result<()>;
}

impl Truncate for File {
    fn truncate_from(&mut self, position: u64) -> Result<()> {
        self.set_len(position).map_err(Into::into)
    }
}

impl Truncate for Cursor<Vec<u8>> {
    fn truncate_from(&mut self, position: u64) -> Result<()> {
        usize::try_from(position)
            .map_err(Into::into)
            .map(|position| self.get_mut().truncate(position))
    }
}

impl<S, O> LogSegment<S, O>
where
    S: Read + Seek + Send + Truncate + Write,
    O: Offset,
{
    fn batch_serialize(&mut self, batch: &Batch) -> Result<()> {
        let mut encoder = Encoder::new(&mut self.storage);
        batch.serialize(&mut encoder).map_err(Into::into)
    }

    fn batch_deserialize(&mut self) -> Result<Batch> {
        let mut decoder = Decoder::new(&mut self.storage);
        Batch::deserialize(&mut decoder).map_err(Into::into)
    }

    fn pending_current_state_lock(&self) -> Result<MutexGuard<'_, Vec<Waker>>> {
        self.pending_offset.lock().map_err(|error| error.into())
    }

    #[instrument]
    fn recover(&mut self) -> Result<()> {
        self.check().and_then(|position| {
            if let Some(position) = position {
                debug!(?position);
                self.storage.truncate_from(position).map_err(Into::into)
            } else {
                Ok(())
            }
        })
    }

    fn check(&mut self) -> Result<Option<u64>> {
        self.storage.rewind()?;

        let mut decoder = Decoder::new(&mut self.storage);
        let mut position = None;

        loop {
            match Batch::deserialize(&mut decoder) {
                Ok(batch) => {
                    let delta = i64::from(batch.last_offset_delta);
                    _ = self.max_offset.replace(
                        self.max_offset
                            .map_or(self.base_offset + delta, |max_offset| {
                                delta + max_offset + 1
                            }),
                    );
                    _ = position.replace(decoder.position());
                }

                Err(error) => {
                    debug!(?error, ?position);
                    return Ok(position);
                }
            }
        }
    }

    #[allow(dead_code)]
    fn compact<T, P>(
        &mut self,
        output: &mut LogSegment<T, P>,
        keys_to_be_removed_from_tail: &BTreeSet<Bytes>,
    ) -> Result<usize>
    where
        T: Read + Seek + Send + Truncate + Write,
        P: Offset,
    {
        let mut offset = self.base_offset;
        let mut records = 0;
        debug!(?offset);

        while let Ok(batch) = self.read(offset) {
            offset += i64::from(batch.last_offset_delta) + 1;

            _ = batch::Batch::try_from(batch)
                .map_err(Into::into)
                .and_then(|inflated| inflated.compact(keys_to_be_removed_from_tail))
                .map_err(Into::into)
                .and_then(|compacted| {
                    records += compacted.records;
                    compacted
                        .batch
                        .try_into()
                        .map_err(Into::into)
                        .and_then(|deflated| output.append(deflated))
                })?;

            debug!(?offset);
        }

        Ok(records)
    }

    pub fn iter(&mut self) -> LogSegmentIter<'_, S, O> {
        LogSegmentIter::new(self)
    }

    #[allow(dead_code)]
    fn into_storage(self) -> S {
        self.storage
    }
}

impl<S, O> IntoIterator for LogSegment<S, O>
where
    S: Read + Seek + Send + Truncate + Write,
    O: Offset,
{
    type Item = Batch;

    type IntoIter = LogSegmentIterator<S, O>;

    fn into_iter(self) -> Self::IntoIter {
        let offset = self.base_offset;
        LogSegmentIterator {
            segment: self,
            offset,
        }
    }
}

#[derive(Debug)]
pub struct LogSegmentIterator<S, O> {
    segment: LogSegment<S, O>,
    offset: i64,
}

impl<S, O> LogSegmentIterator<S, O>
where
    S: Read + Seek + Send + Truncate + Write,
    O: Offset,
{
    pub fn new(segment: LogSegment<S, O>) -> Self {
        let offset = segment.base_offset;

        Self { segment, offset }
    }
}

impl<S, O> Iterator for LogSegmentIterator<S, O>
where
    S: Read + Seek + Send + Truncate + Write,
    O: Offset,
{
    type Item = Batch;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.segment.read(self.offset).ok();

        if let Some(ref batch) = next {
            self.offset += i64::from(batch.last_offset_delta) + 1;
        }

        next
    }
}

#[derive(Debug)]
pub struct LogSegmentIter<'s, S, O> {
    segment: &'s mut LogSegment<S, O>,
    offset: i64,
}

impl<'s, S, O> LogSegmentIter<'s, S, O> {
    fn new(segment: &'s mut LogSegment<S, O>) -> Self {
        let offset = segment.base_offset;
        Self { segment, offset }
    }
}

impl<'s, S, O> Iterator for LogSegmentIter<'s, S, O>
where
    S: Read + Seek + Send + Truncate + Write,
    O: Offset,
{
    type Item = Batch;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.segment.read(self.offset).ok();
        debug!(?self.offset, ?next);

        if let Some(ref batch) = next {
            debug!(?batch.last_offset_delta);
            self.offset += i64::from(batch.last_offset_delta) + 1;
        }

        next
    }
}

impl<S, O> Segment for LogSegment<S, O>
where
    S: Read + Seek + Send + Truncate + Write,
    O: Offset,
{
    #[instrument]
    fn append(&mut self, mut batch: Batch) -> Result<i64> {
        self.storage
        .seek(SeekFrom::End(0))
        .map_err(Into::into)
        .and_then(|start| {
            batch.base_offset = self
                .max_offset
                .map_or(self.base_offset, |max_offset| max_offset + 1);

            self.batch_serialize(&batch)?;

            let delta = i64::from(batch.last_offset_delta);
            _ = self.max_offset.replace(
                self.max_offset
                    .map_or(self.base_offset + delta, |max_offset| {
                        delta + max_offset + 1
                    }),
            );

            let end = self.storage.stream_position()?;

            self.bytes_since_last_index_entry += end - start;
            debug!(?self.bytes_since_last_index_entry);

            if self.bytes_since_last_index_entry > self.index_interval_bytes {
                debug!(self.bytes_since_last_index_entry, self.index_interval_bytes, ?batch.base_offset, ?start);

                self.offsets.append(batch.base_offset, start)?;
                self.bytes_since_last_index_entry = 0;
            }

            Ok(batch.base_offset)
        })
    }

    #[instrument]
    fn read(&mut self, starting_offset: i64) -> Result<Batch> {
        debug!(?starting_offset, ?self.base_offset, ?self.max_offset);

        if self.max_offset.is_none()
            || self
                .max_offset
                .is_some_and(|max_offset| starting_offset > max_offset)
        {
            Err(Error::NoSuchOffset(starting_offset))
        } else {
            self.offsets
                .position_for_offset(starting_offset)
                .inspect(|position| debug!(?position))
                .and_then(|position| {
                    self.storage
                        .seek(SeekFrom::Start(position))
                        .map_err(Into::into)
                        .and_then(|start| {
                            debug!(?start);

                            let mut batch = self.batch_deserialize()?;

                            while batch.max_offset() < starting_offset {
                                batch = self.batch_deserialize()?;
                            }

                            debug!(?batch);

                            Ok(batch)
                        })
                })
        }
    }

    fn base_offset(&self) -> i64 {
        self.base_offset
    }

    fn max_offset(&self) -> Option<i64> {
        self.max_offset
    }

    fn register_pending_offset(&self, waker: Waker) -> Result<()> {
        self.pending_current_state_lock()
            .map(|mut pending| pending.push(waker))
            .and(Ok(()))
    }

    fn bytes_since_last_index_entry(&self) -> u64 {
        self.bytes_since_last_index_entry
    }

    #[instrument]
    fn truncate_from_offset(&mut self, range: RangeFrom<i64>) -> Result<()> {
        self.offsets
            .position_for_offset(range.start)
            .inspect(|position| debug!(?position))
            .and_then(|position| {
                self.storage
                    .seek(SeekFrom::Start(position))
                    .map_err(Into::into)
                    .and_then(|start| {
                        debug!(?start);

                        let mut decoder = Decoder::new(&mut self.storage);
                        let mut batch = Batch::deserialize(&mut decoder)?;

                        while batch.max_offset() < range.start {
                            batch = Batch::deserialize(&mut decoder)?;
                        }

                        self.storage
                            .stream_position()
                            .map_err(Into::into)
                            .and_then(|current| self.storage.truncate_from(current))
                    })
            })
    }
}

#[derive(Debug)]
pub struct SegmentRead {
    segment: Box<dyn Segment>,
    starting_offset: i64,
}

impl SegmentRead {
    pub fn new(starting_offset: i64, segment: Box<dyn Segment>) -> Self {
        Self {
            segment,
            starting_offset,
        }
    }

    pub fn starting_offset(&self) -> i64 {
        self.starting_offset
    }
}

impl Future for SegmentRead {
    type Output = Result<Batch>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();

        match me.segment.read(me.starting_offset) {
            Err(Error::NoSuchOffset(_)) => {
                me.segment
                    .register_pending_offset(cx.waker().clone())
                    .unwrap();
                Poll::Pending
            }

            otherwise => Poll::Ready(otherwise),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MemorySegmentProvider<'data> {
    pub index_interval_bytes: u64,
    pub data: &'data [u8],
}

impl<'data> Default for MemorySegmentProvider<'data> {
    fn default() -> Self {
        Self {
            index_interval_bytes: 8_192,
            data: &[],
        }
    }
}

impl<'data> MemorySegmentProvider<'data> {
    pub fn new(index_interval_bytes: u64, data: &'data [u8]) -> Self {
        Self {
            index_interval_bytes,
            data,
        }
    }
}

impl<'data> SegmentProvider for MemorySegmentProvider<'data> {
    fn init(&self) -> Result<BTreeMap<Topition, BTreeMap<i64, Box<dyn Segment>>>> {
        Ok(BTreeMap::new())
    }

    fn provide_segment(&self, tpo: &TopitionOffset) -> Result<Box<dyn Segment>> {
        debug!(?tpo);

        let mut log_segment = LogSegment::builder()
            .base_offset(tpo.offset())
            .index_interval_bytes(self.index_interval_bytes)
            .offsets(OffsetIndex::builder().in_memory(vec![]).build())
            .in_memory(self.data)
            .build();

        log_segment.recover()?;

        Ok(Box::new(log_segment))
    }
}

#[derive(Debug)]
pub struct FileSystemSegmentProvider<P> {
    index_interval_bytes: u64,
    dir: P,
    offset_provider: FileSystemOffsetProvider<P>,
}

impl<P> FileSystemSegmentProvider<P>
where
    P: AsRef<Path> + Clone + Debug + Send + Sync + 'static,
{
    pub fn new(index_interval_bytes: u64, dir: P) -> Result<Self> {
        let offset_provider = FileSystemOffsetProvider::new(dir.clone());

        Ok(Self {
            index_interval_bytes,
            dir,
            offset_provider,
        })
    }
}

impl<P> FileSystemSegmentProvider<P>
where
    P: AsRef<Path>,
{
    fn filename(&self, tpo: &TopitionOffset) -> PathBuf {
        self.dir
            .as_ref()
            .join(PathBuf::from(tpo))
            .with_extension("log")
    }

    fn ends_with_log(entry: &DirEntry) -> Result<bool> {
        entry
            .file_name()
            .into_string()
            .map_err(Error::OsString)
            .map(|s| s.ends_with(".log"))
    }

    fn base_offset_for_log(entry: &DirEntry) -> Result<i64> {
        Regex::new(r"^(?<offset>\d{20}).log$")
            .map_err(Into::into)
            .and_then(|re| {
                entry
                    .file_name()
                    .into_string()
                    .map_err(Error::OsString)
                    .and_then(|ref file_name| {
                        re.captures(file_name)
                            .ok_or(Error::Message(format!("no captures for {file_name}")))
                            .and_then(|ref captures| {
                                captures
                                    .name("offset")
                                    .ok_or(Error::Message(format!(
                                        "missing offset for: {file_name}"
                                    )))
                                    .map(|s| s.as_str())
                                    .and_then(|s| str::parse(s).map_err(Into::into))
                            })
                    })
            })
    }
}

impl<P> SegmentProvider for FileSystemSegmentProvider<P>
where
    P: AsRef<Path> + Debug + Send + Sync,
{
    fn init(&self) -> Result<BTreeMap<Topition, BTreeMap<i64, Box<dyn Segment>>>> {
        let mut segments: BTreeMap<Topition, BTreeMap<i64, Box<dyn Segment>>> = BTreeMap::new();

        let read_dir = self.dir.as_ref().read_dir()?;
        for entry in read_dir {
            let entry = entry?;

            if entry.file_type()?.is_dir() {
                if let Ok(tp) = Topition::try_from(&entry) {
                    debug!(?entry, ?tp);

                    for entry in entry.path().read_dir()? {
                        let entry = entry?;

                        if Self::ends_with_log(&entry)? {
                            if let Ok(offset) = Self::base_offset_for_log(&entry) {
                                debug!(?entry, ?offset);

                                let tpo = TopitionOffset::new(tp.clone(), offset);

                                let offset_index = self.offset_provider.provide_offset(&tpo)?;

                                let log_name = self
                                    .dir
                                    .as_ref()
                                    .join(PathBuf::from(&tpo))
                                    .with_extension("log");
                                debug!(?log_name);

                                let mut log_segment = LogSegment::builder()
                                    .base_offset(tpo.offset())
                                    .index_interval_bytes(self.index_interval_bytes)
                                    .offsets(offset_index)
                                    .file_system(log_name)?
                                    .build();

                                log_segment.recover()?;

                                _ = segments
                                    .entry(tp.clone())
                                    .or_default()
                                    .insert(offset, Box::new(log_segment) as Box<dyn Segment>);
                            }
                        }
                    }
                }
            }
        }

        Ok(segments)
    }

    fn provide_segment(&self, tpo: &TopitionOffset) -> Result<Box<dyn Segment>> {
        debug!(?tpo);

        create_dir_all(self.dir.as_ref().join(PathBuf::from(tpo.topition())))?;

        let offset_index = self.offset_provider.provide_offset(tpo)?;

        let log_name = self.filename(tpo);
        debug!(?log_name);

        let mut log_segment = LogSegment::builder()
            .base_offset(tpo.offset())
            .index_interval_bytes(self.index_interval_bytes)
            .offsets(offset_index)
            .file_system(log_name)?
            .build();

        log_segment.recover()?;

        Ok(Box::new(log_segment))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Error, Topition};
    use bytes::Bytes;
    use std::{fs::write, sync::Arc, thread};
    use tansu_kafka_sans_io::record::Record;
    use tempfile::tempdir;
    use tracing::{subscriber::DefaultGuard, Level};
    use tracing_subscriber::{filter::Targets, fmt, prelude::*, registry};

    #[cfg(miri)]
    fn init_tracing() -> Result<()> {
        Ok(())
    }

    #[cfg(not(miri))]
    fn init_tracing() -> Result<DefaultGuard> {
        let subscriber = registry()
            .with(
                Targets::new()
                    .with_target("tansu_storage", Level::DEBUG)
                    .with_target("tansu_kafka_sans_io::record::batch", Level::DEBUG),
            )
            .with(
                thread::current()
                    .name()
                    .ok_or(Error::Message(String::from("unnamed thread")))
                    .and_then(|name| {
                        File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME")))
                            .map_err(Into::into)
                    })
                    .map(Arc::new)
                    .map(|writer| {
                        fmt::layer()
                            .with_writer(writer)
                            .with_level(true)
                            .with_line_number(true)
                            .with_ansi(false)
                    })?,
            );
        Ok(tracing::subscriber::set_default(subscriber))
    }

    #[test]
    fn append_and_read_in_memory_indexed_offsets() -> Result<()> {
        let _guard = init_tracing()?;

        let base_offset = 32123;
        let index_interval_bytes = 48;

        let data = [];

        let mut segment = LogSegment::builder()
            .base_offset(base_offset)
            .index_interval_bytes(index_interval_bytes)
            .offsets(OffsetIndex::builder().in_memory(vec![]).build())
            .in_memory(&data)
            .build();

        assert!(segment.max_offset().is_none());

        let def = &b"def"[..];

        assert_eq!(
            segment.append(
                batch::Batch::builder()
                    .record(Record::builder().value(def.into()))
                    .build()
                    .and_then(TryInto::try_into)?
            )?,
            base_offset
        );
        assert_eq!(0, segment.bytes_since_last_index_entry);
        assert_eq!(Some(base_offset), segment.max_offset());

        assert_eq!(
            Some(def.into()),
            segment
                .read(base_offset)
                .and_then(|deflated| batch::Batch::try_from(deflated).map_err(Into::into))
                .map(|batch| batch.records[0].value.clone())?
        );

        let efg = &b"efg"[..];

        assert_eq!(
            segment.append(
                batch::Batch::builder()
                    .record(Record::builder().value(efg.into()))
                    .build()
                    .and_then(TryInto::try_into)?
            )?,
            base_offset + 1
        );
        assert_eq!(0, segment.bytes_since_last_index_entry);
        assert_eq!(Some(base_offset + 1), segment.max_offset());

        assert_eq!(
            Some(efg.into()),
            segment
                .read(base_offset + 1)
                .and_then(|deflated| batch::Batch::try_from(deflated).map_err(Into::into))
                .map(|batch| batch.records[0].value.clone())?
        );

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn append_and_read_file_system_indexed_offsets() -> Result<()> {
        let _guard = init_tracing()?;

        let base_offset = 32123;
        let index_interval_bytes = 48;

        let dir = tempdir()?;
        let provider = FileSystemSegmentProvider::new(index_interval_bytes, dir.path().to_owned())?;
        assert!(provider.init()?.is_empty());

        let topic = "asdf";
        let partition = 767676;

        let topition = Topition::new(topic, partition);
        let tpo = TopitionOffset::new(topition, base_offset);

        let mut segment = provider.provide_segment(&tpo)?;
        assert!(segment.max_offset().is_none());

        let def = &b"def"[..];

        assert_eq!(
            segment.append(
                batch::Batch::builder()
                    .record(Record::builder().value(def.into()))
                    .build()
                    .and_then(TryInto::try_into)?
            )?,
            base_offset
        );
        assert_eq!(0, segment.bytes_since_last_index_entry());
        assert_eq!(Some(base_offset), segment.max_offset());

        assert_eq!(
            Some(def.into()),
            segment
                .read(base_offset)
                .and_then(|deflated| batch::Batch::try_from(deflated).map_err(Into::into))
                .map(|batch| batch.records[0].value.clone())?
        );

        let efg = &b"efg"[..];

        assert_eq!(
            segment.append(
                batch::Batch::builder()
                    .record(Record::builder().value(efg.into()))
                    .build()
                    .and_then(TryInto::try_into)?
            )?,
            base_offset + 1
        );
        assert_eq!(Some(base_offset + 1), segment.max_offset());
        assert_eq!(0, segment.bytes_since_last_index_entry());

        assert_eq!(
            Some(efg.into()),
            segment
                .read(base_offset + 1)
                .and_then(|deflated| batch::Batch::try_from(deflated).map_err(Into::into))
                .map(|batch| batch.records[0].value.clone())?
        );

        Ok(())
    }

    #[test]
    fn append_and_read_unindexed_offsets() -> Result<()> {
        let _guard = init_tracing()?;

        let base_offset = 54345;
        let index_interval_bytes = 8_192;
        let data = [];

        let mut segment = LogSegment::builder()
            .base_offset(base_offset)
            .index_interval_bytes(index_interval_bytes)
            .offsets(OffsetIndex::builder().in_memory(vec![]).build())
            .in_memory(&data)
            .build();
        assert!(segment.max_offset().is_none());

        let def = &b"def"[..];

        let offset = batch::Batch::builder()
            .record(Record::builder().value(def.into()))
            .build()
            .and_then(TryInto::try_into)
            .map_err(Into::into)
            .and_then(|batch| segment.append(batch))?;

        assert_eq!(Some(base_offset), segment.max_offset());
        assert_eq!(base_offset, offset);

        let bytes_since_last_index_entry = segment.bytes_since_last_index_entry;

        assert!(0 != bytes_since_last_index_entry);

        assert_eq!(
            Some(def.into()),
            segment
                .read(base_offset)
                .and_then(|deflated| batch::Batch::try_from(deflated).map_err(Into::into))
                .map(|batch| batch.records[0].value.clone())?
        );

        let efg = &b"efg"[..];

        let offset = batch::Batch::builder()
            .base_offset(1)
            .record(Record::builder().value(efg.into()))
            .build()
            .and_then(TryInto::try_into)
            .map_err(Into::into)
            .and_then(|batch| segment.append(batch))?;

        assert_eq!(Some(base_offset + 1), segment.max_offset());
        assert_eq!(base_offset + 1, offset);

        assert!(segment.bytes_since_last_index_entry > bytes_since_last_index_entry);

        assert_eq!(
            Some(efg.into()),
            segment
                .read(base_offset + 1)
                .and_then(|deflated| batch::Batch::try_from(deflated).map_err(Into::into))
                .map(|batch| batch.records[0].value.clone())?
        );

        Ok(())
    }

    #[test]
    fn read_empty_in_memory_segment() -> Result<()> {
        let _guard = init_tracing()?;

        let base_offset = 7543457;
        let index_interval_bytes = 8_192;
        let data = [];

        let mut segment = LogSegment::builder()
            .base_offset(base_offset)
            .index_interval_bytes(index_interval_bytes)
            .offsets(OffsetIndex::builder().in_memory(vec![]).build())
            .in_memory(&data)
            .build();
        assert!(segment.max_offset().is_none());

        assert!(matches!(
            segment.read(base_offset),
            Err(Error::NoSuchOffset(_))
        ));

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn read_empty_file_system_segment() -> Result<()> {
        let _guard = init_tracing()?;

        let base_offset = 7543457;
        let index_interval_bytes = 8_192;

        let dir = tempdir()?;

        let provider = FileSystemSegmentProvider::new(index_interval_bytes, dir.path().to_owned())?;
        assert!(provider.init()?.is_empty());

        let topic = "asdf";
        let partition = 767676;

        let topition = Topition::new(topic, partition);
        let tpo = TopitionOffset::new(topition, base_offset);

        let mut segment = provider.provide_segment(&tpo)?;
        assert!(segment.max_offset().is_none());

        assert!(matches!(
            segment.read(base_offset),
            Err(Error::NoSuchOffset(_))
        ));
        Ok(())
    }

    #[test]
    fn single_record_in_memory_recovery() -> Result<()> {
        let _guard = init_tracing()?;

        let index_interval_bytes = 8_192;
        let base_offset = 78987;

        let data = [].as_slice();

        let mut segment = LogSegment::builder()
            .base_offset(base_offset)
            .index_interval_bytes(index_interval_bytes)
            .offsets(OffsetIndex::builder().in_memory(vec![]).build())
            .in_memory(data)
            .build();
        assert!(segment.max_offset().is_none());

        let value = &b"abc"[..];

        let offset = batch::Batch::builder()
            .record(Record::builder().value(value.into()))
            .build()
            .and_then(TryInto::try_into)
            .map_err(Into::into)
            .and_then(|batch| segment.append(batch))?;
        assert_eq!(Some(base_offset), segment.max_offset());
        debug!(?offset);

        let mut data = segment.into_storage().into_inner();
        debug!(?data);

        let mut corrupt = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        data.append(&mut corrupt);

        let provider = Box::new(MemorySegmentProvider {
            index_interval_bytes,
            data: &data,
        }) as Box<dyn SegmentProvider>;
        assert!(provider.init()?.is_empty());

        let topic = "pqr";
        let partition = 321;
        let topition = Topition::new(topic, partition);
        let tpo = TopitionOffset::new(topition, base_offset);
        let mut segment = provider.provide_segment(&tpo)?;
        assert_eq!(Some(base_offset), segment.max_offset());

        let actual = segment
            .read(offset)
            .and_then(|deflated| batch::Batch::try_from(deflated).map_err(Into::into))?;
        assert_eq!(Some(value.into()), actual.records[0].value);

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn single_record_file_system_recovery() -> Result<()> {
        let _guard = init_tracing()?;

        let index_interval_bytes = 8_192;
        let base_offset = 78987;

        let data = [].as_slice();

        let mut segment = LogSegment::builder()
            .base_offset(base_offset)
            .index_interval_bytes(index_interval_bytes)
            .offsets(OffsetIndex::builder().in_memory(vec![]).build())
            .in_memory(data)
            .build();
        assert!(segment.max_offset().is_none());

        let value = &b"abc"[..];

        let offset = batch::Batch::builder()
            .record(Record::builder().value(value.into()))
            .build()
            .and_then(TryInto::try_into)
            .map_err(Into::into)
            .and_then(|batch| segment.append(batch))?;
        assert_eq!(Some(base_offset), segment.max_offset());
        debug!(?offset);

        let mut data = segment.into_storage().into_inner();
        debug!(?data);

        let mut corrupt = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        data.append(&mut corrupt);

        let topic = "pqr";
        let partition = 321;
        let tp = Topition::new(topic, partition);
        let tpo = TopitionOffset::new(tp.clone(), base_offset);

        let dir = tempdir()?;
        create_dir_all(dir.as_ref().join(PathBuf::from(&tp)))?;
        let mut log_name = dir.as_ref().join(PathBuf::from(&tpo));
        _ = log_name.set_extension("log");
        write(log_name, data)?;

        let provider = FileSystemSegmentProvider::new(index_interval_bytes, dir.path().to_owned())?;
        let mut segments = provider.init()?;
        assert!(segments.contains_key(&tp));

        let segment = segments
            .get_mut(&tp)
            .and_then(|offsets| offsets.get_mut(&base_offset))
            .unwrap();
        assert_eq!(Some(base_offset), segment.max_offset());
        let actual = segment
            .read(offset)
            .and_then(|deflated| batch::Batch::try_from(deflated).map_err(Into::into))?;
        assert_eq!(Some(value.into()), actual.records[0].value);

        Ok(())
    }

    #[test]
    fn multi_record_recovery() -> Result<()> {
        let _guard = init_tracing()?;

        let index_interval_bytes = 8_192;
        let base_offset = 4321234;

        let data = [].as_slice();

        let mut segment = LogSegment::builder()
            .base_offset(base_offset)
            .index_interval_bytes(index_interval_bytes)
            .offsets(OffsetIndex::builder().in_memory(vec![]).build())
            .in_memory(data)
            .build();
        assert!(segment.max_offset().is_none());

        let first_value = Bytes::from("one");

        let first_offset = batch::Batch::builder()
            .record(Record::builder().value(first_value.clone().into()))
            .build()
            .and_then(TryInto::try_into)
            .map_err(Into::into)
            .and_then(|batch| segment.append(batch))?;
        assert_eq!(Some(base_offset), segment.max_offset());
        debug!(?first_offset);

        let second_value = Bytes::from("two");

        let second_offset = batch::Batch::builder()
            .record(Record::builder().value(second_value.clone().into()))
            .build()
            .and_then(TryInto::try_into)
            .map_err(Into::into)
            .and_then(|batch| segment.append(batch))?;
        assert_eq!(Some(base_offset + 1), segment.max_offset());
        debug!(?second_offset);

        let mut data = segment.into_storage().into_inner();
        debug!(?data);

        let mut corrupt = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        data.append(&mut corrupt);

        let provider = Box::new(MemorySegmentProvider {
            index_interval_bytes,
            data: &data,
        }) as Box<dyn SegmentProvider>;
        assert!(provider.init()?.is_empty());

        let topic = "pqr";
        let partition = 321;
        let topition = Topition::new(topic, partition);
        let tpo = TopitionOffset::new(topition.clone(), base_offset);
        let mut segment = provider.provide_segment(&tpo)?;
        assert_eq!(Some(base_offset + 1), segment.max_offset());

        let actual = segment
            .read(first_offset)
            .and_then(|deflated| batch::Batch::try_from(deflated).map_err(Into::into))?;
        assert_eq!(Some(first_value), actual.records[0].value);

        let actual = segment
            .read(second_offset)
            .and_then(|deflated| batch::Batch::try_from(deflated).map_err(Into::into))?;
        assert_eq!(Some(second_value), actual.records[0].value);

        Ok(())
    }

    #[test]
    fn simple_invalid_data_recovery() -> Result<()> {
        let _guard = init_tracing()?;

        let index_interval_bytes = 8_192;
        let data = &b"123"[..];

        let provider = Box::new(MemorySegmentProvider {
            index_interval_bytes,
            data,
        }) as Box<dyn SegmentProvider>;
        assert!(provider.init()?.is_empty());

        let base_offset = 0;
        let topic = "pqr";
        let partition = 321;
        let topition = Topition::new(topic, partition);
        let tpo = TopitionOffset::new(topition, base_offset);
        let segment = provider.provide_segment(&tpo)?;
        assert!(segment.max_offset().is_none());
        Ok(())
    }

    #[test]
    fn iter() -> Result<()> {
        let _guard = init_tracing()?;

        let index_interval_bytes = 8_192;
        let base_offset = 4321234;

        let data = [].as_slice();

        let mut segment = LogSegment::builder()
            .base_offset(base_offset)
            .index_interval_bytes(index_interval_bytes)
            .offsets(OffsetIndex::builder().in_memory(vec![]).build())
            .in_memory(data)
            .build();
        assert!(segment.max_offset().is_none());

        let base_timestamp = 1721978771334;

        let keys: Vec<String> = (0..=6).map(|i| format!("k{i}")).collect();
        let values: Vec<String> = (0..=11).map(|i| format!("v{i}")).collect();

        let indexes = [
            (1, 1),
            (2, 2),
            (1, 3),
            (1, 4),
            (3, 5),
            (2, 6),
            (4, 7),
            (5, 8),
            (5, 9),
            (2, 10),
            (6, 11),
        ];

        let mut builder = batch::Batch::builder()
            .base_offset(base_offset)
            .partition_leader_epoch(-1)
            .magic(2)
            .attributes(0)
            .last_offset_delta(i32::try_from(indexes.len() - 1)?)
            .base_timestamp(base_timestamp)
            .max_timestamp(base_timestamp + i64::try_from(indexes.len())? - 1)
            .producer_id(-1)
            .producer_epoch(0)
            .base_sequence(0);

        for (offset_delta, (key_index, value_index)) in indexes.iter().enumerate() {
            builder = builder.record(
                Record::builder()
                    .offset_delta(i32::try_from(offset_delta)?)
                    .timestamp_delta(i64::try_from(offset_delta)?)
                    .key(keys[*key_index].as_bytes().into())
                    .value(values[*value_index].as_bytes().into()),
            );
        }

        assert_eq!(
            base_offset,
            builder
                .build()
                .and_then(TryInto::try_into)
                .map_err(Into::into)
                .and_then(|batch| segment.append(batch))?
        );
        assert_eq!(Some(base_offset + 10), segment.max_offset());
        assert_eq!(1, segment.iter().count());

        assert_eq!(
            Some(11),
            segment
                .iter()
                .map(|deflated| batch::Batch::try_from(deflated)
                    .map_or(0, |inflated| inflated.records.len()))
                .reduce(|acc, len| acc + len)
        );

        let mut builder = batch::Batch::builder()
            .base_offset(base_offset + i64::try_from(indexes.len())?)
            .partition_leader_epoch(-1)
            .magic(2)
            .attributes(0)
            .last_offset_delta(i32::try_from(indexes.len() - 1)?)
            .base_timestamp(base_timestamp)
            .max_timestamp(base_timestamp + i64::try_from(indexes.len())? - 1)
            .producer_id(-1)
            .producer_epoch(0)
            .base_sequence(0);

        for (offset_delta, (key_index, value_index)) in indexes.iter().enumerate() {
            builder = builder.record(
                Record::builder()
                    .offset_delta(i32::try_from(offset_delta)?)
                    .timestamp_delta(i64::try_from(offset_delta)?)
                    .key(keys[*key_index].as_bytes().into())
                    .value(values[*value_index].as_bytes().into()),
            );
        }

        assert_eq!(
            base_offset + i64::try_from(indexes.len())?,
            builder
                .build()
                .and_then(TryInto::try_into)
                .map_err(Into::into)
                .and_then(|batch| segment.append(batch))?
        );

        assert_eq!(2, segment.iter().count());
        assert_eq!(
            Some(22),
            segment
                .iter()
                .map(|deflated| batch::Batch::try_from(deflated)
                    .map_or(0, |inflated| inflated.records.len()))
                .reduce(|acc, len| acc + len)
        );

        Ok(())
    }

    #[test]
    fn compaction() -> Result<()> {
        let _guard = init_tracing()?;

        let index_interval_bytes = 8_192;
        let base_offset = 4321234;

        let data = [].as_slice();

        let mut segment = LogSegment::builder()
            .base_offset(base_offset)
            .index_interval_bytes(index_interval_bytes)
            .offsets(OffsetIndex::builder().in_memory(vec![]).build())
            .in_memory(data)
            .build();
        assert!(segment.max_offset().is_none());

        let base_timestamp = 1721978771334;

        let keys: Vec<String> = (0..=6).map(|i| format!("k{i}")).collect();
        let values: Vec<String> = (0..=11).map(|i| format!("v{i}")).collect();

        let indexes = [
            (1, 1),
            (2, 2),
            (1, 3),
            (1, 4),
            (3, 5),
            (2, 6),
            (4, 7),
            (5, 8),
            (5, 9),
            (2, 10),
            (6, 11),
        ];

        let mut builder = batch::Batch::builder()
            .base_offset(base_offset)
            .partition_leader_epoch(-1)
            .magic(2)
            .attributes(0)
            .last_offset_delta(i32::try_from(indexes.len() - 1)?)
            .base_timestamp(base_timestamp)
            .max_timestamp(base_timestamp + i64::try_from(indexes.len())? - 1)
            .producer_id(-1)
            .producer_epoch(0)
            .base_sequence(0);

        for (offset_delta, (key_index, value_index)) in indexes.into_iter().enumerate() {
            builder = builder.record(
                Record::builder()
                    .offset_delta(i32::try_from(offset_delta)?)
                    .timestamp_delta(i64::try_from(offset_delta)?)
                    .key(keys[key_index].as_bytes().into())
                    .value(values[value_index].as_bytes().into()),
            );
        }

        assert_eq!(
            base_offset,
            builder
                .build()
                .and_then(TryInto::try_into)
                .map_err(Into::into)
                .and_then(|batch| segment.append(batch))?
        );

        assert_eq!(Some(base_offset + 10), segment.max_offset());

        assert_eq!(
            Some(11),
            segment
                .iter()
                .map(|deflated| batch::Batch::try_from(deflated)
                    .map_or(0, |inflated| inflated.records.len()))
                .reduce(|acc, len| acc + len)
        );

        let mut compacted = LogSegment::builder()
            .base_offset(base_offset)
            .index_interval_bytes(index_interval_bytes)
            .offsets(OffsetIndex::builder().in_memory(vec![]).build())
            .in_memory(data)
            .build();

        let keys_to_be_removed_from_tail = BTreeSet::new();
        let records = segment.compact(&mut compacted, &keys_to_be_removed_from_tail)?;
        assert_eq!(5, records);

        assert_eq!(
            Some(6),
            compacted
                .iter()
                .map(|deflated| batch::Batch::try_from(deflated)
                    .map_or(0, |inflated| inflated.records.len()))
                .reduce(|acc, len| acc + len)
        );

        assert_eq!(Some(base_offset + 10), compacted.max_offset());

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn file_system_provider() -> Result<()> {
        let _guard = init_tracing()?;

        let base_offset = 32123;
        let index_interval_bytes = 48;

        let dir = tempdir()?;
        let provider = FileSystemSegmentProvider::new(index_interval_bytes, dir.path().to_owned())?;
        assert!(provider.init()?.is_empty());

        let topic = "asdf";
        let partition = 767676;

        let tp = Topition::new(topic, partition);
        let tpo = TopitionOffset::new(tp.clone(), base_offset);

        let mut segment = provider.provide_segment(&tpo)?;

        let def = &b"def"[..];

        assert_eq!(
            segment.append(
                batch::Batch::builder()
                    .record(Record::builder().value(def.into()))
                    .build()
                    .and_then(TryInto::try_into)?
            )?,
            base_offset
        );
        assert_eq!(0, segment.bytes_since_last_index_entry());

        let efg = &b"efg"[..];

        assert_eq!(
            segment.append(
                batch::Batch::builder()
                    .record(Record::builder().value(efg.into()))
                    .build()
                    .and_then(TryInto::try_into)?
            )?,
            base_offset + 1
        );

        let provider = FileSystemSegmentProvider::new(index_interval_bytes, dir.path().to_owned())?;
        let mut segments = provider.init()?;
        debug!(?segments);
        assert!(segments.contains_key(&tp));

        let segment = segments
            .get_mut(&tp)
            .and_then(|offsets| offsets.get_mut(&base_offset))
            .unwrap();

        assert_eq!(Some(base_offset + 1), segment.max_offset());

        assert_eq!(
            Some(def.into()),
            segment
                .read(base_offset)
                .and_then(|deflated| batch::Batch::try_from(deflated).map_err(Into::into))
                .map(|batch| batch.records[0].value.clone())?
        );

        assert_eq!(
            Some(efg.into()),
            segment
                .read(base_offset + 1)
                .and_then(|deflated| batch::Batch::try_from(deflated).map_err(Into::into))
                .map(|batch| batch.records[0].value.clone())?
        );

        assert!(matches!(
            segment
                .read(base_offset + 2)
                .and_then(|deflated| batch::Batch::try_from(deflated).map_err(Into::into))
                .map(|batch| batch.records[0].value.clone()),
            Err(Error::NoSuchOffset(_)),
        ));

        Ok(())
    }
}
