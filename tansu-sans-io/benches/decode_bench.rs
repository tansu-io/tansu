// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use tansu_sans_io::Frame;

fn api_versions_request_v3_000(c: &mut Criterion) {
    _ = c.bench_function("api_versions_request_v3_000", |b| {
        b.iter(|| {
            Frame::request_from_bytes(black_box(
                &[
                    0, 0, 0, 52, 0, 18, 0, 3, 0, 0, 0, 3, 0, 16, 99, 111, 110, 115, 111, 108, 101,
                    45, 112, 114, 111, 100, 117, 99, 101, 114, 0, 18, 97, 112, 97, 99, 104, 101,
                    45, 107, 97, 102, 107, 97, 45, 106, 97, 118, 97, 6, 51, 46, 54, 46, 49, 0,
                ][..],
            ))
            .is_ok()
        })
    });
}

fn produce_request_v10_001(c: &mut Criterion) {
    _ = c.bench_function("produce_request_v10_001", |b| {
        b.iter(|| {
            black_box(
                Frame::request_from_bytes(
                    &[
                        0, 0, 1, 84, 0, 0, 0, 10, 0, 0, 0, 6, 0, 16, 99, 111, 110, 115, 111, 108,
                        101, 45, 112, 114, 111, 100, 117, 99, 101, 114, 0, 0, 255, 255, 0, 0, 5,
                        220, 2, 5, 116, 101, 115, 116, 2, 0, 0, 0, 0, 163, 2, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 1, 22, 255, 255, 255, 255, 2, 185, 194, 249, 184, 0, 0, 0, 0, 0,
                        4, 0, 0, 1, 144, 237, 238, 215, 134, 0, 0, 1, 144, 237, 238, 215, 142, 0,
                        0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 76, 0, 0, 0, 12, 113,
                        119, 101, 114, 116, 121, 10, 112, 111, 105, 117, 121, 6, 4, 104, 49, 6,
                        112, 113, 114, 4, 104, 50, 6, 106, 107, 108, 4, 104, 51, 6, 117, 105, 111,
                        76, 0, 16, 2, 12, 97, 115, 100, 102, 103, 104, 10, 108, 107, 106, 104, 103,
                        6, 4, 104, 49, 6, 114, 116, 121, 4, 104, 50, 6, 100, 102, 103, 4, 104, 51,
                        6, 108, 107, 106, 76, 0, 16, 4, 12, 106, 107, 108, 106, 107, 108, 10, 105,
                        117, 105, 117, 105, 6, 4, 104, 49, 6, 122, 120, 99, 4, 104, 50, 6, 99, 118,
                        98, 4, 104, 51, 6, 109, 110, 98, 84, 0, 16, 6, 12, 113, 119, 101, 114, 116,
                        121, 18, 121, 116, 114, 114, 114, 119, 113, 101, 101, 6, 4, 104, 49, 6,
                        101, 114, 105, 4, 104, 50, 6, 101, 105, 117, 4, 104, 51, 6, 112, 112, 111,
                        134, 1, 0, 16, 8, 18, 113, 119, 114, 114, 114, 116, 105, 105, 112, 62, 106,
                        107, 108, 106, 107, 108, 106, 107, 108, 107, 106, 107, 106, 107, 108, 107,
                        106, 108, 106, 108, 107, 106, 108, 106, 107, 108, 106, 108, 107, 106, 106,
                        6, 4, 104, 49, 6, 105, 105, 111, 4, 104, 50, 6, 101, 114, 116, 4, 104, 51,
                        6, 113, 119, 101, 0, 0, 0,
                    ][..],
                )
                .is_ok(),
            )
        })
    });
}

criterion_group!(decode, api_versions_request_v3_000, produce_request_v10_001);
criterion_main!(decode);
