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

use rama::{
    Context, Layer, Service,
    layer::{HijackLayer, MapResponseLayer},
};
use tansu_sans_io::{
    ApiKey as _, ApiVersionsRequest, ErrorCode, Frame, Header, MetadataRequest, MetadataResponse,
    metadata_response::MetadataResponseBroker,
};
use tansu_service::{
    BytesFrameLayer, BytesLayer, FrameApiKeyMatcher, FrameBytesLayer, FrameRequestLayer,
    FrameRouteService, FrameService, RequestFrameLayer, RequestLayer, ResponseService,
};
use tracing::debug;

use crate::common::{Error, init_tracing};

mod common;

mod doctest_code_a {
    use rama::{Context, Layer as _, Service as _};
    use tansu_sans_io::{ApiKey as _, ApiVersionsRequest, MetadataRequest, MetadataResponse};
    use tansu_service::{
        BytesFrameLayer, BytesFrameService, BytesLayer, BytesService, FrameBytesLayer,
        FrameBytesService, FrameRouteService, RequestFrameLayer, RequestFrameService, RequestLayer,
        ResponseService,
    };

    use crate::common::Error;

    async fn frame_route() -> Result<FrameRouteService<(), Error>, Error> {
        let frame_route = FrameRouteService::<(), Error>::builder()
            .with_service(
                RequestLayer::<MetadataRequest>::new().into_layer(ResponseService::new(|_, _| {
                    Ok(MetadataResponse::default()
                        .brokers(Some([].into()))
                        .topics(Some([].into()))
                        .cluster_id(Some("tansu".into()))
                        .controller_id(Some(111))
                        .throttle_time_ms(Some(0))
                        .cluster_authorized_operations(Some(-1)))
                })),
            )
            .and_then(|builder| builder.build())?;

        Ok(frame_route)
    }

    async fn layers() -> Result<
        RequestFrameService<
            FrameBytesService<BytesService<BytesFrameService<FrameRouteService<(), Error>>>>,
        >,
        Error,
    > {
        let frame_route = frame_route().await?;

        let layers = (
            RequestFrameLayer,
            FrameBytesLayer,
            BytesLayer,
            BytesFrameLayer,
        )
            .into_layer(frame_route);

        Ok(layers)
    }

    #[tokio::test]
    async fn metadata_request() -> Result<(), Error> {
        let service = layers().await?;

        let ctx = Context::default();

        let request = MetadataRequest::default()
            .topics(Some([].into()))
            .allow_auto_topic_creation(Some(false))
            .include_cluster_authorized_operations(Some(false))
            .include_topic_authorized_operations(Some(false));

        let response = service.serve(ctx, request).await?;
        assert_eq!(Some("tansu".into()), response.cluster_id);

        Ok(())
    }

    #[tokio::test]
    async fn api_versions_request() -> Result<(), Error> {
        let service = layers().await?;

        let client_software_name = "abcba";
        let client_software_version = "12321";

        let response = service
            .serve(
                Context::default(),
                ApiVersionsRequest::default()
                    .client_software_name(Some(client_software_name.into()))
                    .client_software_version(Some(client_software_version.into())),
            )
            .await?;

        let api_versions = response
            .api_keys
            .unwrap_or_default()
            .into_iter()
            .map(|api_version| api_version.api_key)
            .collect::<Vec<_>>();

        assert_eq!(2, api_versions.len());
        assert!(api_versions.contains(&ApiVersionsRequest::KEY));
        assert!(api_versions.contains(&MetadataRequest::KEY));

        Ok(())
    }
}

#[tokio::test]
async fn simple_routes() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let cluster_id = "abc";

    type State = ();

    let service = (
        RequestFrameLayer,
        FrameBytesLayer,
        BytesLayer,
        BytesFrameLayer,
    )
        .into_layer(
            FrameRouteService::builder()
                .with_service(RequestLayer::<MetadataRequest>::new().into_layer(
                    ResponseService::new(|_ctx: Context<State>, _req: MetadataRequest| {
                        Ok::<_, Error>(
                            MetadataResponse::default()
                                .brokers(Some([].into()))
                                .topics(Some([].into()))
                                .cluster_id(Some(cluster_id.into()))
                                .controller_id(Some(111))
                                .throttle_time_ms(Some(0))
                                .cluster_authorized_operations(Some(-1)),
                        )
                    }),
                ))
                .and_then(|builder| builder.build())?,
        );

    let ctx = Context::default();

    {
        let client_software_name = "abcba";
        let client_software_version = "12321";

        let request = ApiVersionsRequest::default()
            .client_software_name(Some(client_software_name.into()))
            .client_software_version(Some(client_software_version.into()));

        let response = service.serve(ctx.clone(), request).await?;

        assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

        let api_versions = response
            .api_keys
            .unwrap_or_default()
            .into_iter()
            .map(|api_version| api_version.api_key)
            .collect::<Vec<_>>();

        assert_eq!(2, api_versions.len());
        assert!(api_versions.contains(&ApiVersionsRequest::KEY));
        assert!(api_versions.contains(&MetadataRequest::KEY));
    }

    let request = MetadataRequest::default()
        .topics(Some([].into()))
        .allow_auto_topic_creation(Some(false))
        .include_cluster_authorized_operations(Some(false))
        .include_topic_authorized_operations(Some(false));

    let response = service.serve(ctx, request).await?;
    assert_eq!(Some(cluster_id.into()), response.cluster_id);

    Ok(())
}

#[tokio::test]
async fn route_request_map_response() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let cluster_id = "abc";

    let node_id = 12321;
    let host = "defgfed";
    let port = 32123;

    type State = ();

    let rl = (
        RequestLayer::<MetadataRequest>::new(),
        MapResponseLayer::new(move |response: MetadataResponse| {
            response.brokers(Some(vec![
                MetadataResponseBroker::default()
                    .node_id(node_id)
                    .host(host.into())
                    .port(port)
                    .rack(None),
            ]))
        }),
    )
        .into_layer(ResponseService::new(
            |_ctx: Context<State>, _req: MetadataRequest| {
                Ok::<_, Error>(
                    MetadataResponse::default()
                        .brokers(Some([].into()))
                        .topics(Some([].into()))
                        .cluster_id(Some(cluster_id.into()))
                        .controller_id(Some(111))
                        .throttle_time_ms(Some(0))
                        .cluster_authorized_operations(Some(-1)),
                )
            },
        ));

    let service = (
        RequestFrameLayer,
        FrameBytesLayer,
        BytesLayer,
        BytesFrameLayer,
    )
        .into_layer(
            FrameRouteService::<(), Error>::builder()
                .with_service(rl)
                .and_then(|builder| builder.build())?,
        );

    let ctx = Context::default();

    {
        let client_software_name = "abcba";
        let client_software_version = "12321";

        let request = ApiVersionsRequest::default()
            .client_software_name(Some(client_software_name.into()))
            .client_software_version(Some(client_software_version.into()));

        let response = service.serve(ctx.clone(), request).await?;

        assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

        let api_versions = response
            .api_keys
            .unwrap_or_default()
            .into_iter()
            .map(|api_version| api_version.api_key)
            .collect::<Vec<_>>();

        assert_eq!(2, api_versions.len());
        assert!(api_versions.contains(&ApiVersionsRequest::KEY));
        assert!(api_versions.contains(&MetadataRequest::KEY));
    }

    let request = MetadataRequest::default()
        .topics(Some([].into()))
        .allow_auto_topic_creation(Some(false))
        .include_cluster_authorized_operations(Some(false))
        .include_topic_authorized_operations(Some(false));

    let response = service.serve(ctx, request).await?;
    assert_eq!(Some(cluster_id.into()), response.cluster_id);
    assert_eq!(Some(111), response.controller_id);

    let brokers = response.brokers.unwrap_or_default();
    assert_eq!(1, brokers.len());

    assert_eq!(node_id, brokers[0].node_id);
    assert_eq!(host, brokers[0].host);
    assert_eq!(port, brokers[0].port);

    Ok(())
}

#[tokio::test]
async fn simple_layers() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let cluster_id = "abc";

    type State = ();

    let service = (
        RequestFrameLayer,
        FrameBytesLayer,
        BytesLayer,
        BytesFrameLayer,
    )
        .into_layer(
            FrameRouteService::builder()
                .with_service(RequestLayer::<MetadataRequest>::new().into_layer(
                    ResponseService::new(|_ctx: Context<State>, _req: MetadataRequest| {
                        Ok::<_, Error>(
                            MetadataResponse::default()
                                .brokers(Some([].into()))
                                .topics(Some([].into()))
                                .cluster_id(Some(cluster_id.into()))
                                .controller_id(Some(111))
                                .throttle_time_ms(Some(0))
                                .cluster_authorized_operations(Some(-1)),
                        )
                    }),
                ))
                .and_then(|builder| builder.build())?,
        );

    let ctx = Context::default();

    let request = MetadataRequest::default()
        .topics(Some([].into()))
        .allow_auto_topic_creation(Some(false))
        .include_cluster_authorized_operations(Some(false))
        .include_topic_authorized_operations(Some(false));

    let response = service.serve(ctx, request).await?;
    assert_eq!(Some(cluster_id.into()), response.cluster_id);

    Ok(())
}

#[tokio::test]
async fn api_key_hijack() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const CLUSTER_ID: &str = "abc";
    const NODE_ID: i32 = 111;
    const HOST: &str = "localhost";
    const PORT: i32 = 9092;

    let service = (
        FrameRequestLayer::<MetadataRequest>::new(),
        MapResponseLayer::new(move |response: MetadataResponse| {
            response.brokers(Some(vec![
                MetadataResponseBroker::default()
                    .node_id(NODE_ID)
                    .host(HOST.into())
                    .port(PORT)
                    .rack(None),
            ]))
        }),
    )
        .into_layer(ResponseService::new(|_, _req: MetadataRequest| {
            Ok::<_, Error>(
                MetadataResponse::default()
                    .brokers(Some([].into()))
                    .topics(Some([].into()))
                    .cluster_id(Some(CLUSTER_ID.into()))
                    .controller_id(Some(NODE_ID))
                    .throttle_time_ms(Some(0))
                    .cluster_authorized_operations(Some(-1)),
            )
        }));

    let hijack = HijackLayer::new(FrameApiKeyMatcher(MetadataRequest::KEY), service.clone())
        .into_layer(FrameService::new(|_, req: Frame| {
            debug!(?req);
            Err(Error::Message("unmapped".into()))
        }));

    let frame = hijack
        .serve(
            Context::default(),
            Frame {
                header: Header::Request {
                    api_key: MetadataRequest::KEY,
                    api_version: 12,
                    correlation_id: 0,
                    client_id: Some("tansu".into()),
                },
                body: MetadataRequest::default().into(),
                size: 0,
            },
        )
        .await?;

    let response: MetadataResponse = frame.body.try_into()?;
    assert_eq!(Some(111), response.controller_id);

    let brokers = response.brokers.unwrap_or_default();
    assert_eq!(1, brokers.len());
    assert_eq!(111, brokers[0].node_id);

    Ok(())
}
