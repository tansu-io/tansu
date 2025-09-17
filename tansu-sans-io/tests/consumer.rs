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

use bytes::Bytes;
use common::init_tracing;
use tansu_sans_io::{
    Result,
    consumer::{MemberAssignment, MemberMetadata, TopicPartition},
};

pub mod common;

#[test]
fn decode_range_metadata_001() -> Result<()> {
    let _guard = init_tracing().unwrap();

    let encoded =
        Bytes::from_static(b"\0\x03\0\0\0\x01\0\tbenchmark\0\0\0\0\0\0\0\0\xff\xff\xff\xff\0\0");

    let decoded = MemberMetadata::try_from(encoded.clone())?;

    assert_eq!(3, decoded.version);
    assert_eq!(vec![String::from("benchmark")], decoded.subscription.topics);
    assert_eq!(
        Some(Bytes::from_static(b"")),
        decoded.subscription.user_data
    );
    assert!(
        decoded
            .subscription
            .owned_partitions
            .as_ref()
            .is_some_and(|owned_partitions| owned_partitions.is_empty())
    );

    assert_eq!(Some(-1), decoded.subscription.generation_id);
    assert_eq!(Some(""), decoded.subscription.rack_id.as_deref());

    assert_eq!(encoded, Bytes::try_from(&decoded)?);

    Ok(())
}

#[test]
fn decode_range_metadata_002() -> Result<()> {
    let _guard = init_tracing().unwrap();

    let encoded = Bytes::from_static(
        b"\0\x03\0\0\0\x01\0\x04test\xff\xff\xff\xff\0\0\0\0\xff\xff\xff\xff\xff\xff",
    );

    let decoded = MemberMetadata::try_from(encoded.clone())?;

    assert_eq!(3, decoded.version);
    assert_eq!(vec![String::from("test")], decoded.subscription.topics);
    assert_eq!(None, decoded.subscription.user_data);
    assert!(
        decoded
            .subscription
            .owned_partitions
            .as_ref()
            .is_some_and(|owned_partitions| owned_partitions.is_empty())
    );

    assert_eq!(Some(-1), decoded.subscription.generation_id);
    assert_eq!(None, decoded.subscription.rack_id.as_deref());

    assert_eq!(encoded, Bytes::try_from(&decoded)?);

    Ok(())
}

#[test]
fn cooperative_sticky_001() -> Result<()> {
    let _guard = init_tracing().unwrap();

    let encoded = Bytes::from_static(
        b"\0\x03\0\0\0\x01\0\x04test\0\0\0\x04\xff\xff\xff\xff\0\0\0\0\xff\xff\xff\xff\xff\xff",
    );

    let decoded = MemberMetadata::try_from(encoded.clone())?;

    assert_eq!(3, decoded.version);
    assert_eq!(vec![String::from("test")], decoded.subscription.topics);
    assert_eq!(
        Some(Bytes::from_static(b"\xff\xff\xff\xff")),
        decoded.subscription.user_data
    );
    assert!(
        decoded
            .subscription
            .owned_partitions
            .as_ref()
            .is_some_and(|owned_partitions| owned_partitions.is_empty())
    );

    assert_eq!(Some(-1), decoded.subscription.generation_id);
    assert_eq!(None, decoded.subscription.rack_id.as_deref());

    assert_eq!(encoded, Bytes::try_from(&decoded)?);

    Ok(())
}

#[test]
fn member_assignment_001() -> Result<()> {
    let _guard = init_tracing().unwrap();

    let encoded = Bytes::from_static(
        b"\0\x03\0\0\0\x01\0\x04test\0\0\0\x03\0\0\0\0\0\0\0\x01\0\0\0\x02\xff\xff\xff\xff",
    );

    let decoded = MemberAssignment::try_from(encoded.clone())?;

    assert_eq!(3, decoded.version);
    assert_eq!(1, decoded.assignment.assigned_partitions.len());
    assert_eq!(
        Some(TopicPartition {
            topic: "test".into(),
            partitions: vec![0, 1, 2]
        }),
        decoded.assignment.assigned_partitions.get(0).cloned()
    );
    assert!(decoded.assignment.user_data.is_none());

    assert_eq!(encoded, Bytes::try_from(&decoded)?);

    Ok(())
}
