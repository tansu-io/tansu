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

use std::{result, time::Duration};

use clap::Parser;
use dotenv::dotenv;
use rama::Layer as _;
use tansu_client::{
    BytesConnectionService, Client, ConnectionManager, FrameConnectionLayer, FramePoolLayer,
};
use tansu_sans_io::{MetadataRequest, NULL_TOPIC_ID, metadata_request::MetadataRequestTopic};
use tansu_service::FrameBytesLayer;
use tracing::debug;
use tracing_subscriber::{
    EnvFilter, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
};
use url::Url;

type Result<T> = result::Result<T, anyhow::Error>;

fn parse_duration(s: &str) -> Result<Duration> {
    humantime::parse_duration(s).map_err(anyhow::Error::new)
}

#[derive(Clone, Debug, Parser)]
#[command(
    version,
    about = "Group Consumer",
    long_about = None,
)]
struct Arg {
    /// The URL of the broker
    #[arg(long, default_value = "tcp://localhost:9092")]
    broker: Url,

    /// The topics within the group
    topics: Vec<String>,

    #[arg(long, default_value = "10s", value_parser = parse_duration)]
    heartbeat_interval: Duration,
}

#[tokio::main]
async fn main() -> Result<()> {
    _ = dotenv().ok();

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(
            tracing_subscriber::fmt::layer()
                .with_level(true)
                .with_line_number(true)
                .with_thread_ids(false)
                .with_span_events(FmtSpan::NONE),
        )
        .init();

    let arg = Arg::parse();

    let pool = ConnectionManager::builder(arg.broker)
        .client_id(Some(env!("CARGO_PKG_NAME").into()))
        .build()
        .await
        .inspect(|pool| debug!(?pool))?;

    let origin = Client::new(pool.clone());

    let metadata = origin
        .call(
            MetadataRequest::default()
                .allow_auto_topic_creation(Some(false))
                .include_cluster_authorized_operations(Some(false))
                .include_topic_authorized_operations(Some(false))
                .topics(Some(
                    arg.topics
                        .iter()
                        .map(|topic| {
                            MetadataRequestTopic::default()
                                .name(Some(topic.to_owned()))
                                .topic_id(Some(NULL_TOPIC_ID))
                        })
                        .collect(),
                )),
        )
        .await?;

    let frame_origin = (
        FramePoolLayer::new(pool.clone()),
        FrameConnectionLayer,
        FrameBytesLayer,
    )
        .into_layer(BytesConnectionService);

    Ok(())
}
