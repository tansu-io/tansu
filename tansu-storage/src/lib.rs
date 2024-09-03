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

use glob::{GlobError, PatternError};
use regex::Regex;
use segment::{Segment, SegmentProvider};
use std::{
    array::TryFromSliceError,
    collections::BTreeMap,
    ffi::OsString,
    fs::DirEntry,
    io,
    num::{ParseIntError, TryFromIntError},
    path::PathBuf,
    result,
    str::FromStr,
    sync::PoisonError,
    task::Waker,
    time::SystemTimeError,
};
use tansu_kafka_sans_io::record::Batch;
use tracing::{debug, instrument};

pub mod index;
pub mod segment;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("glob")]
    Glob(#[from] GlobError),

    #[error("io")]
    Io(#[from] io::Error),

    #[error("kafka sans io")]
    KafkaSansIo(#[from] tansu_kafka_sans_io::Error),

    #[error("offset: {offset}, is less than base offset: {base_offset}")]
    LessThanBaseOffset { offset: i64, base_offset: i64 },

    #[error("offset: {offset}, is less than last offset: {last_offset:?}")]
    LessThanLastOffset {
        offset: i64,
        last_offset: Option<i64>,
    },

    #[error("time: {time}, is less than max time: {max_time:?}")]
    LessThanMaxTime { time: i64, max_time: Option<i64> },

    #[error("time: {time}, is less than min time: {min_time:?}")]
    LessThanMinTime { time: i64, min_time: Option<i64> },

    #[error("message: {0}")]
    Message(String),

    #[error("no such entry nth: {nth}")]
    NoSuchEntry { nth: u32 },

    #[error("no such offset: {0}")]
    NoSuchOffset(i64),

    #[error("os string {0:?}")]
    OsString(OsString),

    #[error("pattern")]
    Pattern(#[from] PatternError),

    #[error("parse int: {0}")]
    ParseInt(#[from] ParseIntError),

    #[error("poision")]
    Poison,

    #[error("regex")]
    Regex(#[from] regex::Error),

    #[error("segment empty: {0:?}")]
    SegmentEmpty(Topition),

    #[error("segment missing: {topition:?}, at offset: {offset:?}")]
    SegmentMissing {
        topition: Topition,
        offset: Option<i64>,
    },

    #[error("system time: {0}")]
    SystemTime(#[from] SystemTimeError),

    #[error("try from int: {0}")]
    TryFromInt(#[from] TryFromIntError),

    #[error("try from slice: {0}")]
    TryFromSlice(#[from] TryFromSliceError),
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Topition {
    topic: String,
    partition: i32,
}

impl Topition {
    pub fn new(topic: &str, partition: i32) -> Self {
        let topic = topic.to_owned();
        Self { topic, partition }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }
}

impl TryFrom<&DirEntry> for Topition {
    type Error = Error;

    fn try_from(value: &DirEntry) -> result::Result<Self, Self::Error> {
        Regex::new(r"^(?<topic>.+)-(?<partition>\d{10})$")
            .map_err(Into::into)
            .and_then(|re| {
                value
                    .file_name()
                    .into_string()
                    .map_err(Error::OsString)
                    .and_then(|ref file_name| {
                        re.captures(file_name)
                            .ok_or(Error::Message(format!("no captures for {file_name}")))
                            .and_then(|ref captures| {
                                let topic = captures
                                    .name("topic")
                                    .ok_or(Error::Message(format!("missing topic for {file_name}")))
                                    .map(|s| s.as_str().to_owned())?;

                                let partition = captures
                                    .name("partition")
                                    .ok_or(Error::Message(format!(
                                        "missing partition for: {file_name}"
                                    )))
                                    .map(|s| s.as_str())
                                    .and_then(|s| str::parse(s).map_err(Into::into))?;

                                Ok(Self { topic, partition })
                            })
                    })
            })
    }
}

impl FromStr for Topition {
    type Err = Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        i32::from_str(&s[s.len() - 10..])
            .map(|partition| {
                let topic = String::from(&s[..s.len() - 11]);

                Self { topic, partition }
            })
            .map_err(Into::into)
    }
}

impl From<&Topition> for PathBuf {
    fn from(value: &Topition) -> Self {
        let topic = value.topic.as_str();
        let partition = value.partition;
        PathBuf::from(format!("{topic}-{partition:0>10}"))
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TopitionOffset {
    topition: Topition,
    offset: i64,
}

impl TopitionOffset {
    pub fn new(topition: Topition, offset: i64) -> Self {
        Self { topition, offset }
    }

    pub fn topition(&self) -> &Topition {
        &self.topition
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }
}

impl From<&TopitionOffset> for PathBuf {
    fn from(value: &TopitionOffset) -> Self {
        let offset = value.offset;
        PathBuf::from(value.topition()).join(format!("{offset:0>20}"))
    }
}

#[derive(Debug)]
pub struct Storage {
    provider: Box<dyn SegmentProvider>,
    segments: BTreeMap<Topition, BTreeMap<i64, Box<dyn Segment>>>,
    pending_fetch: Vec<Waker>,
}

impl Storage {
    pub fn with_segment_provider(provider: Box<dyn SegmentProvider>) -> Result<Self> {
        let segments = provider.init()?;

        Ok(Self {
            provider,
            segments,
            pending_fetch: Vec::new(),
        })
    }

    #[instrument]
    pub fn produce(&mut self, topition: &'_ Topition, batch: Batch) -> Result<i64> {
        let base_offset = if let Some(segments) = self.segments.get_mut(topition) {
            segments
                .last_entry()
                .as_mut()
                .ok_or_else(|| Error::SegmentEmpty(topition.to_owned()))
                .and_then(|segment| segment.get_mut().append(batch).map_err(Into::into))?
        } else {
            let tpo = TopitionOffset::new(topition.clone(), batch.base_offset);
            let mut segment = self.provider.provide_segment(&tpo)?;
            let base_offset = segment.append(batch)?;

            let mut mapping = BTreeMap::new();
            _ = mapping.insert(base_offset, segment);

            _ = self.segments.insert(topition.to_owned(), mapping);
            base_offset
        };

        for ws in self.pending_fetch.drain(..) {
            ws.wake()
        }

        debug!(?base_offset);

        Ok(base_offset)
    }

    fn segments(&self, topition: &'_ Topition) -> Result<&BTreeMap<i64, Box<dyn Segment>>> {
        self.segments.get(topition).ok_or_else(|| {
            debug!(?topition);

            Error::SegmentMissing {
                topition: topition.to_owned(),
                offset: None,
            }
        })
    }

    fn segments_mut(
        &mut self,
        topition: &'_ Topition,
    ) -> Result<&mut BTreeMap<i64, Box<dyn Segment>>> {
        self.segments.get_mut(topition).ok_or_else(|| {
            debug!(?topition);

            Error::SegmentMissing {
                topition: topition.to_owned(),
                offset: None,
            }
        })
    }

    fn segment_mut(
        &mut self,
        topition: &'_ Topition,
        offset: i64,
    ) -> Result<&mut Box<dyn Segment>> {
        self.segments_mut(topition).and_then(|segments| {
            segments
                .range_mut(..=offset)
                .last()
                .ok_or_else(|| {
                    debug!(?topition, ?offset);

                    Error::SegmentMissing {
                        topition: topition.to_owned(),
                        offset: Some(offset),
                    }
                })
                .map(|(_, segment)| segment)
        })
    }

    #[instrument]
    pub fn fetch(&mut self, topition: &'_ Topition, offset: i64) -> Result<Batch> {
        self.segment_mut(topition, offset)
            .and_then(|segment| segment.read(offset).map_err(Into::into))
    }

    #[instrument]
    pub fn last_stable_offset(&self, topition: &'_ Topition) -> Result<i64> {
        self.segments(topition).map(|segments| {
            if segments.len() > 1 {
                segments.last_key_value().map_or(0, |(_, segment)| {
                    segment.max_offset().unwrap_or(segment.base_offset() - 1)
                })
            } else {
                segments
                    .last_key_value()
                    .map_or(0, |(_, segment)| segment.max_offset().unwrap_or(0))
            }
        })
    }

    #[instrument]
    pub fn high_watermark(&self, topition: &'_ Topition) -> Result<i64> {
        self.last_stable_offset(topition)
    }

    #[instrument]
    pub fn register_pending_fetch(&mut self, waker: Waker) {
        self.pending_fetch.push(waker)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::MemorySegmentProvider;
    use bytes::Bytes;
    use tansu_kafka_sans_io::record::{Batch, Record};

    #[test]
    fn produce_fetch() -> Result<()> {
        let mut manager =
            Storage::with_segment_provider(Box::new(MemorySegmentProvider::default()))?;

        let topic = "abc";
        let partition = 6;
        let topition = Topition::new(topic, partition);

        let values = (0..10)
            .map(|i| i.to_string())
            .try_fold(Vec::new(), |mut acc, value| {
                Batch::builder()
                    .record(Record::builder().value(value.clone().as_bytes().into()))
                    .build()
                    .map_err(Into::into)
                    .and_then(|batch| {
                        manager.produce(&topition, batch).map(|offset| {
                            acc.push((offset, Bytes::copy_from_slice(value.as_bytes())));
                            acc
                        })
                    })
            })?;

        for (offset, expecting) in values {
            let actual = manager.fetch(&topition, offset)?.records[0]
                .value
                .clone()
                .unwrap();
            assert_eq!(actual, expecting);
        }

        Ok(())
    }

    #[test]
    fn as_path_buf() {
        let topic = "qwerty";
        let partition = i32::MAX;
        let topition = Topition::new(topic, partition);

        assert_eq!(PathBuf::from("qwerty-2147483647"), PathBuf::from(&topition));

        let base_offset = i64::MAX;
        let tpo = TopitionOffset::new(topition, base_offset);

        assert_eq!(
            PathBuf::from("qwerty-2147483647/09223372036854775807"),
            PathBuf::from(&tpo)
        );
    }

    #[test]
    fn topition_from_str() -> Result<()> {
        let topition = Topition::from_str("qwerty-2147483647")?;
        assert_eq!("qwerty", topition.topic());
        assert_eq!(i32::MAX, topition.partition());
        Ok(())
    }
}
