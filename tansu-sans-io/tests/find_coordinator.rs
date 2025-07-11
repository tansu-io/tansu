use common::init_tracing;
use std::collections::BTreeMap;
use tansu_model::{MessageKind, VersionRange};
use tansu_sans_io::MESSAGE_META;

pub mod common;

#[test]
fn check_message_meta() {
    let _guard = init_tracing().unwrap();

    assert!(BTreeMap::from(MESSAGE_META).contains_key("FindCoordinatorRequest"));

    let meta = BTreeMap::from(MESSAGE_META);

    let message = meta.get("FindCoordinatorRequest").unwrap();
    assert_eq!(10, message.api_key);
    assert_eq!(MessageKind::Request, message.message_kind);

    assert_eq!(VersionRange { start: 0, end: 6 }, message.version.valid);

    assert_eq!(
        Some(VersionRange { start: 0, end: 3 }),
        message.field("key").map(|field| field.version)
    );

    assert_eq!(
        Some(VersionRange {
            start: 1,
            end: i16::MAX
        }),
        message.field("key_type").map(|field| field.version)
    );
    assert!(
        !message
            .field("key_type")
            .is_some_and(|field| field.is_mandatory(None))
    );

    assert_eq!(
        Some(VersionRange {
            start: 4,
            end: i16::MAX
        }),
        message.field("coordinator_keys").map(|field| field.version)
    );
    assert!(
        !message
            .field("coordinator_keys")
            .is_some_and(|field| field.is_mandatory(None))
    );
}
