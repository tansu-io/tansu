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

use std::collections::BTreeMap;
use tansu_kafka_model::{MessageKind, VersionRange};
use tansu_kafka_sans_io::MESSAGE_META;

#[test]
fn check_message_meta() {
    assert!(BTreeMap::from(MESSAGE_META).contains_key("AddPartitionsToTxnRequest"));

    let meta = BTreeMap::from(MESSAGE_META);

    let message = meta.get("AddPartitionsToTxnRequest").unwrap();
    assert_eq!(24, message.api_key);
    assert_eq!(MessageKind::Request, message.message_kind);
    assert_eq!(VersionRange { start: 0, end: 5 }, message.version.valid);
    assert_eq!(
        VersionRange {
            start: 3,
            end: i16::MAX
        },
        message.version.flexible
    );

    let fields = BTreeMap::from_iter(message.fields.iter().copied());
    assert!(fields.contains_key("transactions"));

    assert_eq!(
        VersionRange {
            start: 4,
            end: i16::MAX
        },
        fields["transactions"].version
    );

    let transactions = BTreeMap::from_iter(fields["transactions"].fields.iter().copied());
    assert!(transactions.contains_key("transactional_id"));
    assert_eq!(0, transactions["transactional_id"].fields.len());

    assert!(transactions.contains_key("producer_id"));
    assert_eq!(0, transactions["producer_id"].fields.len());

    assert!(transactions.contains_key("producer_epoch"));
    assert_eq!(0, transactions["producer_epoch"].fields.len());

    assert!(transactions.contains_key("verify_only"));
    assert_eq!(0, transactions["verify_only"].fields.len());

    assert!(transactions.contains_key("topics"));
    assert_eq!(2, transactions["topics"].fields.len());

    let topics = BTreeMap::from_iter(transactions["topics"].fields.iter().copied());
    assert!(topics.contains_key("name"));
    assert!(topics.contains_key("partitions"));
}
