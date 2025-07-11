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
