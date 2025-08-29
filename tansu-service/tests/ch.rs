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

use rama::{Context, Layer as _, Service as _};
use tansu_sans_io::{ApiKey as _, Frame, Header, MetadataRequest, MetadataResponse};
use tansu_service::{
    ChannelFrameLayer, FrameChannelService, FrameReceiver, FrameRouteService, RequestLayer,
    ResponseService, bounded_channel,
};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::common::{Error, init_tracing};

mod common;

async fn server(cancellation: CancellationToken, rx: FrameReceiver) -> Result<(), Error> {
    let service =
        ChannelFrameLayer::new(cancellation).into_layer(
            FrameRouteService::builder()
                .with_service(RequestLayer::<MetadataRequest>::new().into_layer(
                    ResponseService::new(|_ctx: Context<()>, _req: MetadataRequest| {
                        Ok::<_, Error>(
                            MetadataResponse::default()
                                .brokers(Some([].into()))
                                .topics(Some([].into()))
                                .cluster_id(Some("abc".into()))
                                .controller_id(Some(111))
                                .throttle_time_ms(Some(0))
                                .cluster_authorized_operations(Some(-1)),
                        )
                    }),
                ))
                .and_then(|builder| builder.build())?,
        );

    service.serve(Context::default(), rx).await
}

#[tokio::test]
async fn client_server() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (tx, rx) = bounded_channel(100);

    let cancellation = CancellationToken::new();

    let mut join = JoinSet::new();

    let _server = {
        let cancellation = cancellation.clone();
        join.spawn(async move { server(cancellation, rx).await })
    };

    let client = FrameChannelService::new(tx);

    let frame = client
        .serve(
            Context::default(),
            Frame {
                header: Header::Request {
                    api_key: MetadataRequest::KEY,
                    api_version: 12,
                    correlation_id: 0,
                    client_id: Some(env!("CARGO_PKG_NAME").into()),
                },
                body: MetadataRequest::default()
                    .topics(Some([].into()))
                    .allow_auto_topic_creation(Some(false))
                    .include_cluster_authorized_operations(Some(false))
                    .include_topic_authorized_operations(Some(false))
                    .into(),
                size: 0,
            },
        )
        .await?;

    let response = MetadataResponse::try_from(frame.body)?;
    assert_eq!(Some("abc"), response.cluster_id.as_deref());
    assert_eq!(Some(111), response.controller_id);

    cancellation.cancel();

    let joined = join.join_all().await;
    debug!(?joined);

    Ok(())
}
