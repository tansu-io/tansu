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

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::iter;
use tansu_sans_io::record::{Record, deflated, inflated};
use tansu_storage::{
    Topition, TopitionOffset,
    index::offset::OffsetIndex,
    segment::{LogSegment, MemorySegmentProvider, Segment, SegmentProvider},
};

fn append_in_memory(c: &mut Criterion) {
    static KB: usize = 1024;

    let mut group = c.benchmark_group("append_in_memory");

    for size in [KB, 2 * KB, 4 * KB, 8 * KB, 16 * KB].iter() {
        _ = group.throughput(Throughput::Bytes(*size as u64));

        _ = group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let base_offset = 32123;
                let index_interval_bytes = 48;

                let data = [];

                let mut segment = LogSegment::builder()
                    .base_offset(base_offset)
                    .index_interval_bytes(index_interval_bytes)
                    .offsets(OffsetIndex::builder().in_memory(vec![]).build())
                    .in_memory(&data)
                    .build();

                let value = iter::repeat_n(0u8, size).take(size).collect::<Vec<_>>();

                _ = segment
                    .append(
                        inflated::Batch::builder()
                            .record(Record::builder().value(Some(Bytes::from(value))))
                            .build()
                            .and_then(deflated::Batch::try_from)
                            .unwrap(),
                    )
                    .unwrap();
            })
        });
    }

    group.finish();
}

fn append_in_memory_provider(c: &mut Criterion) {
    static KB: usize = 1024;

    let mut group = c.benchmark_group("append_in_memory_provider");

    for size in [KB, 2 * KB, 4 * KB, 8 * KB, 16 * KB].iter() {
        _ = group.throughput(Throughput::Bytes(*size as u64));

        _ = group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let base_offset = 32123;
                let index_interval_bytes = 48;

                let data = [];

                let provider = Box::new(MemorySegmentProvider {
                    index_interval_bytes,
                    data: &data,
                }) as Box<dyn SegmentProvider>;

                let topic = "pqr";
                let partition = 321;
                let topition = Topition::new(topic, partition);
                let tpo = TopitionOffset::new(topition, base_offset);
                let mut segment = provider.provide_segment(&tpo).unwrap();

                let value = iter::repeat_n(0u8, size).collect::<Vec<_>>();

                _ = segment
                    .append(
                        inflated::Batch::builder()
                            .record(Record::builder().value(Some(Bytes::from(value))))
                            .build()
                            .and_then(deflated::Batch::try_from)
                            .unwrap(),
                    )
                    .unwrap();
            })
        });
    }

    group.finish();
}

criterion_group!(benches, append_in_memory, append_in_memory_provider);
criterion_main!(benches);
