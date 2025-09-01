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

use crate::common::{Error, init_tracing};
use rama::{Context, Layer as _, Service, layer::MapStateLayer};
use tansu_sans_io::{ErrorCode, GetTelemetrySubscriptionsRequest};
use tansu_storage::{GetTelemetrySubscriptionsService, StorageContainer};
use url::Url;

mod common;

#[tokio::test]
async fn req() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;

    let storage = StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(Url::parse("memory://tansu/")?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(GetTelemetrySubscriptionsService);

    let client_instance_id = [0; 16];

    let response = service
        .serve(
            Context::default(),
            GetTelemetrySubscriptionsRequest::default().client_instance_id(client_instance_id),
        )
        .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

    Ok(())
}
