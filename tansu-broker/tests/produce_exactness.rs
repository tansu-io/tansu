// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::{collections::BTreeSet, sync::Arc};

use bytes::Bytes;
use common::{alphanumeric_string, init_tracing};
use rama::{Context, Layer as _, Service as _};
use tansu_broker::{Error, Result, service::advertised_versions, service::storage};
use tansu_sans_io::{
    Ack, ApiKey as _, ApiVersionsRequest, ApiVersionsResponse, ErrorCode, Frame, Header,
    ProduceRequest, ProduceResponse,
    create_topics_request::CreatableTopic,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated, inflated},
};
use tansu_service::{BytesFrameLayer, BytesFrameService, FrameRouteService};
use tansu_storage::{Storage, StorageContainer};
use url::Url;

mod common;

type Broker = BytesFrameService<FrameRouteService<(), Error>>;

fn broker<S>(storage: S) -> Result<Broker>
where
    S: Storage + Clone,
{
    storage::services(
        FrameRouteService::<(), Error>::builder().with_advertised_versions(advertised_versions()),
        storage,
    )
    .and_then(|builder| builder.build().map_err(Into::into))
    .map(|frame_route| BytesFrameLayer::default().into_layer(frame_route))
}

async fn storage() -> Result<Arc<Box<dyn Storage>>> {
    StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("null://")?)
        .build()
        .await
        .map_err(Into::into)
}

async fn storage_with_topic(topic: &str) -> Result<Arc<Box<dyn Storage>>> {
    let storage = StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("slatedb://memory")?)
        .build()
        .await
        .map_err(Error::from)?;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.into())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await
        .map_err(Error::from)?;

    Ok(storage)
}

fn record_frame(value: &[u8]) -> Result<deflated::Frame> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::copy_from_slice(value))))
        .build()
        .map(|batch| inflated::Frame {
            batches: vec![batch],
        })
        .and_then(deflated::Frame::try_from)
        .map_err(Into::into)
}

fn produce_request(topic: &str, acks: i16, value: &[u8], api_version: i16) -> Result<Bytes> {
    let frame = record_frame(value)?;

    let request = ProduceRequest::default()
        .acks(acks)
        .timeout_ms(0)
        .topic_data(Some(
            [TopicProduceData::default()
                .name(topic.into())
                .partition_data(Some(
                    [PartitionProduceData::default()
                        .index(0)
                        .records(Some(frame))]
                    .into(),
                ))]
            .into(),
        ));

    Frame::request(
        Header::Request {
            api_key: ProduceRequest::KEY,
            api_version,
            correlation_id: 0,
            client_id: Some(env!("CARGO_PKG_NAME").into()),
        },
        request.into(),
    )
    .map_err(Into::into)
}

async fn api_versions(broker: &Broker) -> Result<ApiVersionsResponse> {
    let request = Frame::request(
        Header::Request {
            api_key: ApiVersionsRequest::KEY,
            api_version: 4,
            correlation_id: 0,
            client_id: Some(env!("CARGO_PKG_NAME").into()),
        },
        ApiVersionsRequest::default()
            .client_software_name(Some(env!("CARGO_PKG_NAME").into()))
            .client_software_version(Some(env!("CARGO_PKG_VERSION").into()))
            .into(),
    )?;

    let response = broker.serve(Context::default(), request).await?;
    let response = Frame::response_from_bytes(response, ApiVersionsRequest::KEY, 4)?;
    ApiVersionsResponse::try_from(response.body).map_err(Into::into)
}

#[tokio::test]
async fn api_versions_do_not_advertise_produce() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage().await?;
    let broker = broker(storage)?;

    let response = api_versions(&broker).await?;
    let api_keys = response.api_keys.unwrap_or_default();
    let api_keys = api_keys
        .iter()
        .map(|api_version| api_version.api_key)
        .collect::<BTreeSet<_>>();

    assert!(
        api_keys.contains(&ApiVersionsRequest::KEY),
        "ApiVersions must still advertise itself"
    );
    assert!(api_keys.contains(&tansu_sans_io::MetadataRequest::KEY));
    assert_eq!(2, api_keys.len());
    assert!(!api_keys.contains(&ProduceRequest::KEY));

    Ok(())
}

#[tokio::test]
async fn produce_acks_zero_suppresses_wire_response() -> Result<()> {
    let _guard = init_tracing()?;
    let topic = alphanumeric_string(15);
    let storage = storage().await?;
    let broker = broker(storage)?;

    for api_version in 0..=11 {
        let response = broker
            .serve(
                Context::default(),
                produce_request(&topic, Ack::None.into(), b"one", api_version)?,
            )
            .await?;

        assert!(
            response.is_empty(),
            "acks=0 must suppress version {api_version}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn produce_acks_leader_and_full_isr_return_wire_response() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage().await?;
    let broker = broker(storage.clone())?;

    for ack in [Ack::Leader, Ack::FullIsr] {
        let topic = alphanumeric_string(15);

        let response = broker
            .serve(
                Context::default(),
                produce_request(&topic, i16::from(ack), b"two", 11)?,
            )
            .await?;

        assert!(!response.is_empty());

        let response = Frame::response_from_bytes(response, ProduceRequest::KEY, 11)?;
        let response = ProduceResponse::try_from(response.body)?;
        let topics = response.responses.as_deref().unwrap_or_default();

        assert_eq!(1, topics.len());
        assert_eq!(topic.as_str(), topics[0].name.as_str());

        let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(0, partitions[0].index);
        assert_ne!(i16::from(ErrorCode::None), partitions[0].error_code);
    }

    Ok(())
}

#[tokio::test]
async fn produce_request_versions_decode_across_supported_versions() -> Result<()> {
    let _guard = init_tracing()?;

    for api_version in 0..=11 {
        let topic = alphanumeric_string(15);
        let storage = storage_with_topic(&topic).await?;
        let broker = broker(storage)?;

        let response = broker
            .serve(
                Context::default(),
                produce_request(&topic, Ack::Leader.into(), b"matrix", api_version)?,
            )
            .await?;

        assert!(
            !response.is_empty(),
            "Produce v{api_version} should return a wire response"
        );

        let response = Frame::response_from_bytes(response, ProduceRequest::KEY, api_version)?;
        let response = ProduceResponse::try_from(response.body)?;
        let topics = response.responses.as_deref().unwrap_or_default();

        assert_eq!(1, topics.len());
        assert_eq!(topic.as_str(), topics[0].name.as_str());

        let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(0, partitions[0].index);
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(0, partitions[0].base_offset);
    }

    Ok(())
}
