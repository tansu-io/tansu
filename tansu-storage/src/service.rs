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

mod consumer_group_describe;
mod create_topics;
mod delete_groups;
mod delete_records;
mod delete_topics;
mod describe_cluster;
mod describe_configs;
mod describe_groups;
mod describe_topic_partitions;
mod fetch;
mod find_coordinator;
mod incremental_alter_configs;
mod init_producer_id;
mod list_groups;
mod list_offsets;
mod list_partition_reassignments;
mod metadata;
mod produce;
mod telemetry;
mod txn;

use rama::{Context, Layer, Service};
use std::marker::PhantomData;
use tansu_sans_io::{ApiKey, Body, Frame, Request, Response};

pub use consumer_group_describe::ConsumerGroupDescribeService;
pub use create_topics::CreateTopicsService;
pub use delete_groups::DeleteGroupsService;
pub use delete_records::DeleteRecordsService;
pub use delete_topics::DeleteTopicsService;
pub use describe_cluster::DescribeClusterService;
pub use describe_configs::DescribeConfigsService;
pub use describe_groups::DescribeGroupsService;
pub use describe_topic_partitions::DescribeTopicPartitionsService;
pub use fetch::FetchService;
pub use find_coordinator::FindCoordinatorService;
pub use incremental_alter_configs::IncrementalAlterConfigsService;
pub use init_producer_id::InitProducerIdService;
pub use list_groups::ListGroupsService;
pub use list_offsets::ListOffsetsService;
pub use list_partition_reassignments::ListPartitionReassignmentsService;
pub use metadata::MetadataService;
pub use produce::ProduceService;
pub use telemetry::GetTelemetrySubscriptionsService;
pub use txn::add_offsets::AddOffsetsService as TxnAddOffsetsService;
pub use txn::add_partitions::AddPartitionService as TxnAddPartitionService;
pub use txn::offset_commit::OffsetCommitService as TxnOffsetCommitService;

use crate::Storage;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StorageService<S, G, Q> {
    inner: S,
    storage: G,
    request: PhantomData<Q>,
}

impl<S, G, Q> ApiKey for StorageService<S, G, Q>
where
    Q: ApiKey,
{
    const KEY: i16 = Q::KEY;
}

impl<S, G, State, Q> Service<State, Frame> for StorageService<S, G, Q>
where
    Q: Request,
    Q::Response: Response,
    S: Service<G, Q>,
    Body: From<S::Response>,
    Q::Response: Into<Body>,
    S::Error: From<<Q as TryFrom<Body>>::Error>,
    G: Storage,
    State: Send + Sync + 'static,
{
    type Response = Body;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let (ctx, _) = ctx.swap_state(self.storage.clone());

        let req = Q::try_from(req.body)?;
        self.inner.serve(ctx, req).await.map(Into::into)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StorageLayer<G, Q> {
    storage: G,
    request: PhantomData<Q>,
}

impl<G, Q> StorageLayer<G, Q> {
    pub fn new(storage: G) -> Self {
        Self {
            storage,
            request: PhantomData,
        }
    }
}

impl<S, G, Q> Layer<S> for StorageLayer<G, Q>
where
    G: Storage,
{
    type Service = StorageService<S, G, Q>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            storage: self.storage.clone(),
            inner,
            request: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use tansu_sans_io::{ConsumerGroupDescribeRequest, Frame, Header};

    use crate::{Error, dynostore::DynoStore};

    #[tokio::test]
    async fn abc() -> Result<(), Error> {
        let cluster = "abc";
        let node = 12321;

        let storage = DynoStore::new(cluster, node, InMemory::new());

        let cgd = ConsumerGroupDescribeService;

        let sl = StorageLayer::<_, ConsumerGroupDescribeRequest>::new(storage);

        let q = sl.into_layer(cgd);

        let p = q
            .serve(
                Context::default(),
                Frame {
                    header: Header::Request {
                        api_key: ConsumerGroupDescribeRequest::KEY,
                        api_version: 7,
                        correlation_id: 123,
                        client_id: Some("hello".into()),
                    },
                    size: 0,
                    body: ConsumerGroupDescribeRequest::default().into(),
                },
            )
            .await?;

        println!("{p:?}");

        Ok(())
    }
}
