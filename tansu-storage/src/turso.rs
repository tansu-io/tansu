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

use std::{collections::BTreeMap, marker::PhantomData, time::Duration};

use crate::{
    BrokerRegistrationRequest, GroupDetail, ListOffsetRequest, ListOffsetResponse,
    MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse,
    Result, Storage, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, UpdateError, Version, sql::remove_comments,
};
use async_trait::async_trait;
use tansu_sans_io::{
    ConfigResource, ErrorCode, IsolationLevel, create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult, delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup, record::deflated,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use tansu_schema::{Registry, lake::House};
use tracing::{debug, error};
use turso::Database;
use url::Url;
use uuid::Uuid;

macro_rules! include_sql {
    ($e: expr) => {
        remove_comments(include_str!($e))
    };
}

/// Turso storage engine
///
#[derive(Clone, Debug)]
pub struct Engine {
    cluster: String,
    node: i32,
    advertised_listener: Url,
    db: Database,
    #[allow(dead_code)]
    schemas: Option<Registry>,
    #[allow(dead_code)]
    lake: Option<House>,
}

impl Engine {
    pub fn builder()
    -> Builder<PhantomData<String>, PhantomData<i32>, PhantomData<Url>, PhantomData<Url>> {
        Builder::default()
    }
}

#[derive(Clone, Default, Debug)]
pub struct Builder<C, N, L, D> {
    cluster: C,
    node: N,
    advertised_listener: L,
    storage: D,
    schemas: Option<Registry>,
    lake: Option<House>,
}

impl<C, N, L, D> Builder<C, N, L, D> {
    pub(crate) fn cluster(self, cluster: impl Into<String>) -> Builder<String, N, L, D> {
        Builder {
            cluster: cluster.into(),
            node: self.node,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub(crate) fn node(self, node: i32) -> Builder<C, i32, L, D> {
        debug!(node);
        Builder {
            cluster: self.cluster,
            node,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub(crate) fn advertised_listener(self, advertised_listener: Url) -> Builder<C, N, Url, D> {
        debug!(%advertised_listener);
        Builder {
            cluster: self.cluster,
            node: self.node,
            advertised_listener,
            storage: self.storage,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub(crate) fn storage(self, storage: Url) -> Builder<C, N, L, Url> {
        debug!(%storage);
        Builder {
            cluster: self.cluster,
            node: self.node,
            advertised_listener: self.advertised_listener,
            storage,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub(crate) fn schemas(self, schemas: Option<Registry>) -> Builder<C, N, L, D> {
        Self { schemas, ..self }
    }

    pub(crate) fn lake(self, lake: Option<House>) -> Self {
        Self { lake, ..self }
    }
}

impl Builder<String, i32, Url, Url> {
    pub(crate) async fn build(self) -> Result<Engine> {
        let db = turso::Builder::new_local("tansu.db").build().await?;

        let connection = db.connect()?;

        _ = connection
            .execute(include_sql!("ddl/010-cluster.sql").as_str(), ())
            .await
            .inspect(|rows| debug!(rows))
            .inspect_err(|err| error!(?err));

        _ = connection
            .execute(include_sql!("ddl/020-topic.sql").as_str(), ())
            .await
            .inspect(|rows| debug!(rows))
            .inspect_err(|err| error!(?err));

        Ok(Engine {
            cluster: self.cluster,
            node: self.node,
            advertised_listener: self.advertised_listener,
            db,
            schemas: self.schemas,
            lake: self.lake,
        })
    }
}

#[async_trait]
impl Storage for Engine {
    async fn register_broker(
        &mut self,
        broker_registration: BrokerRegistrationRequest,
    ) -> Result<()> {
        debug!(?broker_registration);

        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(include_sql!("pg/register_broker.sql").as_str())
            .await?;

        let params = broker_registration.cluster_id.as_str();

        _ = statement
            .execute(&[params])
            .await
            .inspect(|rows| debug!(rows))
            .inspect_err(|err| error!(?err));

        Ok(())
    }

    async fn brokers(&mut self) -> Result<Vec<DescribeClusterBroker>> {
        debug!(cluster = self.cluster);

        let broker_id = self.node;
        let host = self
            .advertised_listener
            .host_str()
            .unwrap_or("0.0.0.0")
            .into();
        let port = self.advertised_listener.port().unwrap_or(9092).into();
        let rack = None;

        Ok(vec![
            DescribeClusterBroker::default()
                .broker_id(broker_id)
                .host(host)
                .port(port)
                .rack(rack),
        ])
    }

    async fn create_topic(&mut self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        debug!(cluster = self.cluster, ?topic, validate_only);

        let connection = self
            .db
            .connect()
            .inspect(|connection| debug!(?connection))
            .inspect_err(|err| debug!(?err))?;

        let mut statement = connection
            .prepare(include_sql!("pg/topic_insert.sql").as_str())
            .await
            .inspect_err(|err| debug!(?err))?;

        let parameters = (
            self.cluster.as_str(),
            topic.name.as_str(),
            topic.num_partitions,
            (topic.replication_factor as i32),
        );

        let result = statement
            .query(parameters)
            .await
            .inspect_err(|err| debug!(?err))?;

        let _ = result;

        todo!()
    }

    async fn delete_records(
        &mut self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        debug!(?topics);
        todo!()
    }

    async fn delete_topic(&mut self, topic: &TopicId) -> Result<ErrorCode> {
        debug!(cluster = self.cluster, ?topic);
        todo!()
    }

    async fn incremental_alter_resource(
        &mut self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        debug!(?resource);
        todo!()
    }

    async fn produce(
        &mut self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        debug!(cluster = self.cluster, transaction_id, ?topition, ?deflated);
        todo!()
    }

    async fn fetch(
        &mut self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        debug!(?topition, offset, min_bytes, max_bytes, ?isolation_level);
        todo!()
    }

    async fn offset_stage(&mut self, topition: &Topition) -> Result<OffsetStage> {
        debug!(cluster = self.cluster, ?topition);
        todo!()
    }

    async fn offset_commit(
        &mut self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        debug!(cluster = self.cluster, ?group, ?retention, ?offsets);
        todo!()
    }

    async fn committed_offset_topitions(
        &mut self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, i64>> {
        debug!(group_id);
        todo!()
    }

    async fn offset_fetch(
        &mut self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        debug!(cluster = self.cluster, ?group_id, ?topics, ?require_stable);
        todo!()
    }

    async fn list_offsets(
        &mut self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffsetRequest)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        debug!(cluster = self.cluster, ?isolation_level, ?offsets);
        todo!()
    }

    async fn metadata(&mut self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        debug!(cluster = self.cluster, ?topics);
        todo!()
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        debug!(cluster = self.cluster, name, ?resource, ?keys);
        todo!()
    }

    async fn describe_topic_partitions(
        &mut self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        debug!(?topics, partition_limit, ?cursor);
        todo!()
    }

    async fn list_groups(&mut self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        debug!(?states_filter);
        todo!()
    }

    async fn delete_groups(
        &mut self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        debug!(?group_ids);
        todo!()
    }

    async fn describe_groups(
        &mut self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        debug!(?group_ids, include_authorized_operations);
        todo!()
    }

    async fn update_group(
        &mut self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        debug!(cluster = self.cluster, group_id, ?detail, ?version);
        todo!()
    }

    async fn init_producer(
        &mut self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        debug!(
            cluster = self.cluster,
            transaction_id, transaction_timeout_ms, producer_id, producer_epoch
        );
        todo!()
    }

    async fn txn_add_offsets(
        &mut self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        debug!(
            cluster = self.cluster,
            transaction_id, producer_id, producer_epoch, group_id
        );

        Ok(ErrorCode::None)
    }

    async fn txn_add_partitions(
        &mut self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        debug!(cluster = self.cluster, ?partitions);
        todo!()
    }

    async fn txn_offset_commit(
        &mut self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        debug!(cluster = self.cluster, ?offsets);
        todo!()
    }

    async fn txn_end(
        &mut self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        debug!(cluster = ?self.cluster, transaction_id, producer_id, producer_epoch, committed);
        todo!()
    }

    async fn maintain(&self) -> Result<()> {
        Ok(())
    }
}
