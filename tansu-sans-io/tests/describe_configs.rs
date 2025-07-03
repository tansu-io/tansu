// Copyright ⓒ 2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use common::init_tracing;
use std::collections::BTreeMap;
use tansu_model::{MessageKind, VersionRange};
use tansu_sans_io::MESSAGE_META;

pub mod common;

#[test]
fn response() {
    let _guard = init_tracing().unwrap();

    let message_name = "DescribeConfigsResponse";
    assert!(BTreeMap::from(MESSAGE_META).contains_key(message_name));

    let meta = BTreeMap::from(MESSAGE_META);

    let message = meta.get(message_name).unwrap();
    assert_eq!(32, message.api_key);
    assert_eq!(MessageKind::Response, message.message_kind);

    let structures = message.structures();
    let keys: Vec<&&str> = structures.keys().collect();
    assert_eq!(
        vec![
            &"DescribeConfigsResourceResult",
            &"DescribeConfigsResult",
            &"DescribeConfigsSynonym",
        ],
        keys
    );

    assert_eq!(
        "DescribeConfigsResult",
        message
            .field("results")
            .and_then(|field| field.kind.kind_of_sequence())
            .map(|kind_meta| kind_meta.0)
            .unwrap()
    );

    assert_eq!(
        "DescribeConfigsResourceResult",
        message
            .field("results")
            .and_then(|results| results.field("configs"))
            .and_then(|field| field.kind.kind_of_sequence())
            .map(|kind_meta| kind_meta.0)
            .unwrap()
    );

    assert_eq!(
        "bool",
        message
            .field("results")
            .and_then(|results| results.field("configs"))
            .and_then(|configs| configs.field("read_only"))
            .map(|read_only| read_only.kind.0)
            .unwrap()
    );

    assert_eq!(
        VersionRange {
            start: 0,
            end: i16::MAX
        },
        message
            .field("results")
            .and_then(|results| results.field("configs"))
            .and_then(|configs| configs.field("read_only"))
            .map(|read_only| read_only.version)
            .unwrap()
    );

    assert_eq!(
        "bool",
        message
            .field("results")
            .and_then(|results| results.field("configs"))
            .and_then(|configs| configs.field("is_default"))
            .map(|read_only| read_only.kind.0)
            .unwrap()
    );

    assert_eq!(
        VersionRange { start: 0, end: 0 },
        message
            .field("results")
            .and_then(|results| results.field("configs"))
            .and_then(|configs| configs.field("is_default"))
            .map(|read_only| read_only.version)
            .unwrap()
    );
}
