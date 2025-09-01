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
use tansu_sans_io::{
    ConfigResource, DescribeConfigsRequest, ErrorCode,
    describe_configs_request::DescribeConfigsResource,
};
use tansu_storage::{DescribeConfigsService, StorageContainer};
use url::Url;

mod common;

#[tokio::test]
async fn req() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://tansu/")?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(DescribeConfigsService);

    let response = service
        .serve(
            Context::default(),
            DescribeConfigsRequest::default()
                .include_documentation(Some(false))
                .include_synonyms(Some(false))
                .resources(Some(
                    [DescribeConfigsResource::default()
                        .resource_name("abcba".into())
                        .resource_type(ConfigResource::Topic.into())
                        .configuration_keys(Some([].into()))]
                    .into(),
                )),
        )
        .await?;

    let results = response.results.unwrap_or_default();
    assert_eq!(1, results.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(results[0].error_code)?);
    assert!(results[0].configs.as_deref().unwrap_or_default().is_empty());

    Ok(())
}
