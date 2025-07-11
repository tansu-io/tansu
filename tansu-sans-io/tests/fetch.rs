use common::init_tracing;
use std::collections::BTreeMap;
use tansu_model::MessageKind;
use tansu_sans_io::MESSAGE_META;

pub mod common;

#[test]
fn request() {
    let _guard = init_tracing().unwrap();

    assert!(BTreeMap::from(MESSAGE_META).contains_key("FetchRequest"));

    let meta = BTreeMap::from(MESSAGE_META);

    let message = meta.get("FetchRequest").unwrap();
    assert_eq!(1, message.api_key);
    assert_eq!(MessageKind::Request, message.message_kind);

    let structures = message.structures();
    let keys: Vec<&&str> = structures.keys().collect();
    assert_eq!(
        vec![
            &"FetchPartition",
            &"FetchTopic",
            &"ForgottenTopic",
            &"ReplicaState"
        ],
        keys
    );

    assert_eq!(
        "ForgottenTopic",
        message
            .field("forgotten_topics_data")
            .and_then(|field| field.kind.kind_of_sequence())
            .map(|kind_meta| kind_meta.0)
            .unwrap()
    );

    let partitions = message
        .field("forgotten_topics_data")
        .and_then(|forgotten_topic_data| forgotten_topic_data.field("partitions"))
        .unwrap();

    assert!(!partitions.is_structure());
    assert!(partitions.kind.is_sequence());
    assert!(
        partitions
            .kind
            .kind_of_sequence()
            .map(|sk| sk.is_primitive())
            .unwrap()
    );
}
