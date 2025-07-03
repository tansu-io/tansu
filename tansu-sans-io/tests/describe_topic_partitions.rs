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
use tansu_model::MessageKind;
use tansu_sans_io::MESSAGE_META;

pub mod common;

#[test]
fn request() {
    let _guard = init_tracing().unwrap();

    assert!(BTreeMap::from(MESSAGE_META).contains_key("DescribeTopicPartitionsRequest"));

    let meta = BTreeMap::from(MESSAGE_META);

    let message = meta.get("DescribeTopicPartitionsRequest").unwrap();
    assert_eq!(75, message.api_key);
    assert_eq!(MessageKind::Request, message.message_kind);

    let structures = message.structures();
    let keys: Vec<&&str> = structures.keys().collect();
    assert_eq!(vec![&"Cursor", &"TopicRequest",], keys);

    assert!(
        message
            .field("topics")
            .map(|field| field.kind.is_sequence())
            .unwrap()
    );

    assert!(
        message
            .field("topics")
            .map(|field| field.is_structure())
            .unwrap()
    );

    assert!(
        !message
            .field("response_partition_limit")
            .map(|field| field.kind.is_sequence())
            .unwrap()
    );

    assert!(
        !message
            .field("response_partition_limit")
            .map(|field| field.is_structure())
            .unwrap()
    );

    assert!(
        !message
            .field("cursor")
            .map(|field| field.kind.is_sequence())
            .unwrap()
    );

    assert!(
        message
            .field("cursor")
            .map(|field| field.is_nullable(i16::MAX))
            .unwrap()
    );

    assert!(
        message
            .field("cursor")
            .map(|field| field.is_structure())
            .unwrap()
    );
}
