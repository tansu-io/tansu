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

use super::Record;
use crate::{primitive::ByteSize, record::codec::Sequence, Encoder, Result};
use bytes::Bytes;
use crc::{Crc, Digest, CRC_32_ISCSI};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
};
use tracing::debug;

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Frame {
    pub batches: Vec<Batch>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Batch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,

    #[serde(serialize_with = "Sequence::<Record>::serialize")]
    #[serde(deserialize_with = "Sequence::<Record>::deserialize")]
    pub records: Vec<Record>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Compaction {
    pub batch: Batch,
    pub records: usize,
}

impl Batch {
    #[must_use]
    pub fn builder() -> Builder {
        Builder::default()
    }

    pub fn into_builder(self) -> Builder {
        self.into()
    }

    pub fn max_offset(&self) -> i64 {
        self.base_offset + i64::from(self.last_offset_delta)
    }

    pub fn keys(&self) -> BTreeSet<Bytes> {
        self.records
            .iter()
            .fold(BTreeSet::new(), |mut acc, record| {
                if let Some(key) = record.key() {
                    acc.insert(key);
                }

                acc
            })
    }

    pub fn compact(mut self, head: &BTreeSet<Bytes>) -> Result<Compaction> {
        let mut last_delta_offset_for_key = BTreeMap::new();
        let mut records = 0;

        for record in self.records.iter() {
            if let Some(key) = record.key() {
                if head.contains(&key) {
                    records += 1;
                    continue;
                }

                if last_delta_offset_for_key
                    .insert(key, record.offset_delta)
                    .is_some()
                {
                    records += 1;
                }
            }
        }

        debug!(?records);

        if records > 0 {
            let delta_offsets_to_retain: BTreeSet<i32> =
                last_delta_offset_for_key.into_values().collect();
            debug!(?delta_offsets_to_retain);

            self.records
                .retain(|record| delta_offsets_to_retain.contains(&record.offset_delta));

            self.into_builder()
                .build()
                .map(|batch| Compaction { batch, records })
        } else {
            Ok(Compaction {
                batch: self,
                records,
            })
        }
    }
}

impl From<Batch> for Builder {
    fn from(value: Batch) -> Self {
        Self {
            base_offset: value.base_offset,
            partition_leader_epoch: value.partition_leader_epoch,
            magic: value.magic,
            attributes: value.attributes,
            last_offset_delta: value.last_offset_delta,
            base_timestamp: value.base_timestamp,
            max_timestamp: value.max_timestamp,
            producer_id: value.producer_id,
            producer_epoch: value.producer_epoch,
            base_sequence: value.base_sequence,
            records: value.records.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Builder {
    base_offset: i64,
    partition_leader_epoch: i32,
    magic: i8,
    attributes: i16,
    last_offset_delta: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    records: Sequence<super::Builder>,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            base_offset: 0,
            partition_leader_epoch: -1,
            magic: 2,
            attributes: 0,
            last_offset_delta: 0,
            base_timestamp: 1_707_058_170_165,
            max_timestamp: 1_707_058_170_165,
            producer_id: -1,
            producer_epoch: 0,
            base_sequence: 0,
            records: Sequence::default(),
        }
    }
}

impl ByteSize for Builder {
    fn size_in_bytes(&self) -> Result<usize> {
        Ok(size_of_val(&self.partition_leader_epoch)
            + size_of_val(&self.magic)
            // size of crc is u32
            + size_of::<u32>()
            + size_of_val(&self.attributes)
            + size_of_val(&self.last_offset_delta)
            + size_of_val(&self.base_timestamp)
            + size_of_val(&self.max_timestamp)
            + size_of_val(&self.producer_id)
            + size_of_val(&self.producer_epoch)
            + size_of_val(&self.base_sequence)
            + self.records.size_in_bytes()?)
    }
}

impl Builder {
    #[must_use]
    pub fn base_offset(mut self, base_offset: i64) -> Self {
        self.base_offset = base_offset;
        self
    }

    #[must_use]
    pub fn partition_leader_epoch(mut self, partition_leader_epoch: i32) -> Self {
        self.partition_leader_epoch = partition_leader_epoch;
        self
    }

    #[must_use]
    pub fn magic(mut self, magic: i8) -> Self {
        self.magic = magic;
        self
    }

    #[must_use]
    pub fn attributes(mut self, attributes: i16) -> Self {
        self.attributes = attributes;
        self
    }

    #[must_use]
    pub fn last_offset_delta(mut self, last_offset_delta: i32) -> Self {
        self.last_offset_delta = last_offset_delta;
        self
    }

    #[must_use]
    pub fn base_timestamp(mut self, base_timestamp: i64) -> Self {
        self.base_timestamp = base_timestamp;
        self
    }

    #[must_use]
    pub fn max_timestamp(mut self, max_timestamp: i64) -> Self {
        self.max_timestamp = max_timestamp;
        self
    }

    #[must_use]
    pub fn producer_id(mut self, producer_id: i64) -> Self {
        self.producer_id = producer_id;
        self
    }

    #[must_use]
    pub fn producer_epoch(mut self, producer_epoch: i16) -> Self {
        self.producer_epoch = producer_epoch;
        self
    }

    #[must_use]
    pub fn base_sequence(mut self, base_sequence: i32) -> Self {
        self.base_sequence = base_sequence;
        self
    }

    #[must_use]
    pub fn record(mut self, record: super::Builder) -> Self {
        self.records.extend(vec![record]);
        self
    }

    fn crc(&self) -> Result<u32> {
        struct CrcUpdate<'a> {
            digest: Digest<'a, u32>,
        }

        impl<'a> io::Write for CrcUpdate<'a> {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.digest.update(buf);
                Ok(buf.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let crc = Crc::<u32>::new(&CRC_32_ISCSI);

        let mut digester = CrcUpdate {
            digest: crc.digest(),
        };

        let mut serializer = Encoder::new(&mut digester);

        self.attributes
            .serialize(&mut serializer)
            .and(self.last_offset_delta.serialize(&mut serializer))
            .and(self.base_timestamp.serialize(&mut serializer))
            .and(self.max_timestamp.serialize(&mut serializer))
            .and(self.producer_id.serialize(&mut serializer))
            .and(self.producer_epoch.serialize(&mut serializer))
            .and(self.base_sequence.serialize(&mut serializer))
            .and(self.records.serialize(&mut serializer))
            .map(|()| digester.digest.finalize())
    }

    pub fn build(self) -> Result<Batch> {
        let batch_length = self
            .size_in_bytes()
            .and_then(|size| i32::try_from(size).map_err(Into::into))?;

        let crc = self.crc()?;

        let records = self
            .records
            .0
            .into_iter()
            .try_fold(Vec::new(), |mut acc, record| {
                Record::try_from(record).map(|record| {
                    acc.push(record);
                    acc
                })
            })?;

        Ok(Batch {
            base_offset: self.base_offset,
            batch_length,
            partition_leader_epoch: self.partition_leader_epoch,
            magic: self.magic,
            crc,
            attributes: self.attributes,
            last_offset_delta: self.last_offset_delta,
            base_timestamp: self.base_timestamp,
            max_timestamp: self.max_timestamp,
            producer_id: self.producer_id,
            producer_epoch: self.producer_epoch,
            base_sequence: self.base_sequence,
            records,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compaction() -> Result<()> {
        let keys: Vec<String> = (0..=6).map(|i| format!("k{i}")).collect();
        let values: Vec<String> = (0..=11).map(|i| format!("v{i}")).collect();

        let base_offset = 98789;
        let base_timestamp = 1721978771334;

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

        let mut builder = Batch::builder()
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

        let compacted = builder
            .build()
            .and_then(|batch| batch.compact(&[].into()))?;

        assert_eq!(5, compacted.records);

        let retained: BTreeSet<i32> = compacted
            .batch
            .records
            .iter()
            .map(|record| record.offset_delta)
            .collect();

        assert_eq!(BTreeSet::from([3, 4, 6, 8, 9, 10]), retained);

        Ok(())
    }

    #[test]
    fn compaction_with_key_in_head_of_log() -> Result<()> {
        let keys: Vec<String> = (0..=6).map(|i| format!("k{i}")).collect();
        let values: Vec<String> = (0..=11).map(|i| format!("v{i}")).collect();

        let base_offset = 98789;
        let base_timestamp = 1721978771334;

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

        let mut builder = Batch::builder()
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

        let compacted = builder.build().and_then(|batch| {
            batch.compact(&[Bytes::copy_from_slice(keys[6].as_bytes())].into())
        })?;

        assert_eq!(6, compacted.records);

        let retained: BTreeSet<i32> = compacted
            .batch
            .records
            .iter()
            .map(|record| record.offset_delta)
            .collect();

        assert_eq!(BTreeSet::from([3, 4, 6, 8, 9]), retained);

        Ok(())
    }
}
