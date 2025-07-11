use common::init_tracing;
use std::collections::BTreeMap;
use tansu_model::MessageKind;
use tansu_sans_io::MESSAGE_META;

pub mod common;

#[test]
fn check_message_meta() {
    let _guard = init_tracing().unwrap();

    assert!(BTreeMap::from(MESSAGE_META).contains_key("CreateTopicsRequest"));

    let meta = BTreeMap::from(MESSAGE_META);

    let message = meta.get("CreateTopicsRequest").unwrap();
    assert_eq!(19, message.api_key);
    assert_eq!(MessageKind::Request, message.message_kind);

    let structures = message.structures();
    let keys: Vec<&&str> = structures.keys().collect();
    assert_eq!(
        vec![
            &"CreatableReplicaAssignment",
            &"CreatableTopic",
            &"CreatableTopicConfig"
        ],
        keys
    );

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
            .field("topics")
            .map(|field| field.kind.is_primitive())
            .unwrap()
    );

    assert_eq!(
        "CreatableTopic",
        message
            .field("topics")
            .and_then(|field| field.kind.kind_of_sequence())
            .map(|kind_meta| kind_meta.0)
            .unwrap()
    );

    assert!(
        !message
            .field("timeout_ms")
            .map(|field| field.kind.is_sequence())
            .unwrap()
    );

    assert!(
        !message
            .field("timeout_ms")
            .map(|field| field.is_structure())
            .unwrap()
    );

    assert!(
        !message
            .field("validate_only")
            .map(|field| field.kind.is_sequence())
            .unwrap()
    );

    assert!(
        !message
            .field("validate_only")
            .map(|field| field.is_structure())
            .unwrap()
    );
}
