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

use std::{collections::BTreeMap, time::Duration};

use async_trait::async_trait;
use tansu_sans_io::{
    ConfigResource, ErrorCode, IsolationLevel, ListOffset, ScramMechanism,
    create_topics_request::CreatableTopic, delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic, delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup, record::deflated::Batch,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use tracing::instrument;
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, Error, GroupDetail, ListOffsetResponse, MetadataResponse,
    NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result,
    ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, UpdateError, Version,
};

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Engine;

const FEATURE: &str = "storage";
const MESSAGE: &str = "storage has not been defined";

#[async_trait]
impl Storage for Engine {
    #[instrument(ret)]
    async fn register_broker(&self, _broker_registration: BrokerRegistrationRequest) -> Result<()> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn create_topic(&self, _topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn incremental_alter_resource(
        &self,
        _resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn produce(
        &self,
        _transaction_id: Option<&str>,
        topition: &Topition,
        _deflated: Batch,
    ) -> Result<i64> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn fetch(
        &self,
        topition: &Topition,
        _offset: i64,
        _min_bytes: u32,
        _max_bytes: u32,
        _isolation_level: IsolationLevel,
    ) -> Result<Vec<Batch>> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn offset_commit(
        &self,
        group: &str,
        _retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn offset_fetch(
        &self,
        _group_id: Option<&str>,
        topics: &[Topition],
        _require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn list_offsets(
        &self,
        _isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn metadata(&self, _topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn describe_config(
        &self,
        name: &str,
        _resource: ConfigResource,
        _keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn describe_topic_partitions(
        &self,
        _topics: Option<&[TopicId]>,
        _partition_limit: i32,
        _cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn list_groups(&self, _states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn delete_groups(
        &self,
        _group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn describe_groups(
        &self,
        _group_ids: Option<&[String]>,
        _include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn update_group(
        &self,
        group_id: &str,
        _detail: GroupDetail,
        _version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        Err(UpdateError::Error(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        }))
    }

    #[instrument(ret)]
    async fn init_producer(
        &self,
        _transaction_id: Option<&str>,
        _transaction_timeout_ms: i32,
        _producer_id: Option<i64>,
        _producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn txn_add_partitions(
        &self,
        _partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn txn_offset_commit(
        &self,
        _offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn txn_end(
        &self,
        transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _committed: bool,
    ) -> Result<ErrorCode> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn maintain(&self) -> Result<()> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn cluster_id(&self) -> Result<String> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn node(&self) -> Result<i32> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn advertised_listener(&self) -> Result<Url> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        _mechanism: ScramMechanism,
        _credential: ScramCredential,
    ) -> Result<()> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }
}
