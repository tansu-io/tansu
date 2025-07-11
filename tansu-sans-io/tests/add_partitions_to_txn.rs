use common::init_tracing;
use std::collections::BTreeMap;
use tansu_model::{MessageKind, VersionRange};
use tansu_sans_io::MESSAGE_META;

pub mod common;

#[test]
fn check_message_meta() {
    let _guard = init_tracing().unwrap();

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
