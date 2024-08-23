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

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tansu_kafka_sans_io::Frame;

fn api_versions_request_v3_000(c: &mut Criterion) {
    _ = c.bench_function("api_versions_request_v3_000", |b| {
        b.iter(|| {
            Frame::request_from_bytes(black_box(&[
                0, 0, 0, 52, 0, 18, 0, 3, 0, 0, 0, 3, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45,
                112, 114, 111, 100, 117, 99, 101, 114, 0, 18, 97, 112, 97, 99, 104, 101, 45, 107,
                97, 102, 107, 97, 45, 106, 97, 118, 97, 6, 51, 46, 54, 46, 49, 0,
            ]))
        })
    });
}

criterion_group!(benches, api_versions_request_v3_000);
criterion_main!(benches);
