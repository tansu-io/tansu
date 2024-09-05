// Copyright ⓒ 2024 Peter Morgan <peter.james.morgan@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

pub mod administrator;
pub mod consumer;

use crate::{Error, Result, CONSUMER_OFFSETS};
use administrator::{Controller, Forming, Inner};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{
    collections::BTreeMap,
    fmt::Debug,
    sync::{Arc, Mutex, MutexGuard},
};
use tansu_kafka_sans_io::{
    join_group_request::JoinGroupRequestProtocol,
    leave_group_request::MemberIdentity,
    offset_commit_request::OffsetCommitRequestTopic,
    offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopic},
    record::inflated,
    sync_group_request::SyncGroupRequestAssignment,
    Body,
};
use tansu_storage::{Storage, Topition};
use tracing::debug;

#[derive(Debug)]
pub struct OffsetCommit<'a> {
    pub group_id: &'a str,
    pub generation_id_or_member_epoch: Option<i32>,
    pub member_id: Option<&'a str>,
    pub group_instance_id: Option<&'a str>,
    pub retention_time_ms: Option<i64>,
    pub topics: Option<&'a [OffsetCommitRequestTopic]>,
}

pub trait Coordinator: Debug + Send + Sync {
    #[allow(clippy::too_many_arguments)]
    fn join(
        &mut self,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> Result<Body>;

    #[allow(clippy::too_many_arguments)]
    fn sync(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> Result<Body>;

    fn heartbeat(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<Body>;

    fn leave(
        &mut self,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> Result<Body>;

    fn offset_commit(&mut self, detail: OffsetCommit<'_>) -> Result<Body>;

    fn offset_fetch(
        &mut self,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> Result<Body>;
}

impl<T: Coordinator + ?Sized> Coordinator for Box<T> {
    fn join(
        &mut self,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> Result<Body> {
        (**self).join(
            client_id,
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            protocols,
            reason,
        )
    }

    fn sync(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> Result<Body> {
        (**self).sync(
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            protocol_type,
            protocol_name,
            assignments,
        )
    }

    fn leave(
        &mut self,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> Result<Body> {
        (**self).leave(group_id, member_id, members)
    }

    fn offset_commit(&mut self, detail: OffsetCommit<'_>) -> Result<Body> {
        (**self).offset_commit(detail)
    }

    fn offset_fetch(
        &mut self,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> Result<Body> {
        (**self).offset_fetch(group_id, topics, groups, require_stable)
    }

    fn heartbeat(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<Body> {
        (**self).heartbeat(group_id, generation_id, member_id, group_instance_id)
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GroupTopition {
    pub group: String,
    pub topition: Topition,
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OffsetCommitKey {
    group: String,
    topic: String,
    partition: i32,
}

impl From<OffsetCommitKey> for GroupTopition {
    fn from(offset_commit_key: OffsetCommitKey) -> Self {
        Self {
            group: offset_commit_key.group,
            topition: Topition::new(
                offset_commit_key.topic.as_str(),
                offset_commit_key.partition,
            ),
        }
    }
}

impl TryFrom<Bytes> for OffsetCommitKey {
    type Error = Error;

    fn try_from(mut value: Bytes) -> Result<Self, Self::Error> {
        let len = usize::try_from(value.get_i16())?;
        let group = if len > 0 {
            String::from_utf8(value.copy_to_bytes(len).to_vec())?
        } else {
            "".to_owned()
        };

        let len = usize::try_from(value.get_i16())?;

        let topic = if len > 0 {
            String::from_utf8(value.copy_to_bytes(len).to_vec())?
        } else {
            "".to_owned()
        };

        let partition = value.get_i32();

        Ok(Self {
            group,
            topic,
            partition,
        })
    }
}

impl TryFrom<OffsetCommitKey> for Bytes {
    type Error = Error;

    fn try_from(value: OffsetCommitKey) -> Result<Self, Self::Error> {
        let mut b = BytesMut::with_capacity(
            size_of::<i16>()
                + value.group.len()
                + size_of::<i16>()
                + value.topic.len()
                + size_of::<i32>(),
        );

        let len = i16::try_from(value.group.len())?;
        b.put_i16(len);
        if len > 0 {
            b.put(value.group.as_bytes());
        }

        let len = i16::try_from(value.topic.len())?;
        b.put_i16(len);
        if len > 0 {
            b.put(value.topic.as_bytes());
        }

        b.put_i32(value.partition);
        Ok(b.into())
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OffsetCommitValue {
    offset: i64,
    leader_epoch: i32,
    metadata: String,
    commit_timestamp: i64,
    expire_timestamp: i64,
}

impl TryFrom<Bytes> for OffsetCommitValue {
    type Error = Error;

    fn try_from(mut value: Bytes) -> Result<Self, Self::Error> {
        let offset = value.get_i64();
        let leader_epoch = value.get_i32();

        let len = usize::try_from(value.get_i16())?;
        let metadata = if len > 0 {
            String::from_utf8(value.copy_to_bytes(len).to_vec())?
        } else {
            "".to_owned()
        };

        let commit_timestamp = value.get_i64();
        let expire_timestamp = value.get_i64();

        Ok(Self {
            offset,
            leader_epoch,
            metadata,
            commit_timestamp,
            expire_timestamp,
        })
    }
}

impl TryFrom<OffsetCommitValue> for Bytes {
    type Error = Error;

    fn try_from(value: OffsetCommitValue) -> Result<Self, Self::Error> {
        let mut b = BytesMut::with_capacity(
            size_of::<i64>()
                + size_of::<i32>()
                + size_of::<i16>()
                + value.metadata.len()
                + size_of::<i64>()
                + size_of::<i64>(),
        );

        b.put_i64(value.offset);
        b.put_i32(value.leader_epoch);

        let len = i16::try_from(value.metadata.len())?;
        b.put_i16(len);
        if len > 0 {
            b.put(value.metadata.as_bytes());
        }

        b.put_i64(value.commit_timestamp);
        b.put_i64(value.expire_timestamp);

        Ok(b.into())
    }
}

pub trait ProvideCoordinator: Debug + Send + Sync {
    fn provide_coordinator(&mut self) -> Result<Box<dyn Coordinator>>;
}

#[derive(Clone, Debug)]
pub struct GroupProvider {
    storage: Arc<Mutex<Storage>>,
    partitions: i32,
    offsets: Arc<Mutex<BTreeMap<GroupTopition, OffsetCommitValue>>>,
}

impl GroupProvider {
    fn recover(
        storage: &mut MutexGuard<'_, Storage>,
        partitions: i32,
    ) -> Result<BTreeMap<GroupTopition, OffsetCommitValue>> {
        let mut offsets = BTreeMap::new();

        for partition in 0..partitions {
            let topition = Topition::new(CONSUMER_OFFSETS, partition);
            debug!(?topition);

            let offset = 0;

            let Ok(batch) = storage
                .fetch(&topition, offset)
                .and_then(|deflated| inflated::Batch::try_from(deflated).map_err(Into::into))
            else {
                continue;
            };

            for record in batch.records {
                let Some(key) = record.key() else {
                    continue;
                };

                let Ok(key) = OffsetCommitKey::try_from(key) else {
                    continue;
                };

                let gtp = GroupTopition::from(key);

                let Some(value) = record.value() else {
                    continue;
                };

                let Ok(value) = OffsetCommitValue::try_from(value) else {
                    continue;
                };

                debug!(?gtp, ?value);

                _ = offsets.insert(gtp, value);
            }
        }

        Ok(offsets)
    }

    pub fn new(storage: Arc<Mutex<Storage>>, partitions: i32) -> Result<Self> {
        storage
            .lock()
            .map_err(Into::into)
            .inspect(|storage| debug!(?storage))
            .and_then(|mut storage| Self::recover(&mut storage, partitions))
            .inspect(|offsets| debug!(?offsets))
            .map(Mutex::new)
            .map(Arc::new)
            .map(|offsets| Self {
                storage,
                partitions,
                offsets,
            })
    }
}

impl ProvideCoordinator for GroupProvider {
    fn provide_coordinator(&mut self) -> Result<Box<dyn Coordinator>> {
        Inner::<Forming>::with_storage(self.storage.clone(), self.partitions, self.offsets.clone())
            .map(|inner| Controller::new(inner.into()))
            .map(|controller| Box::new(controller) as Box<dyn Coordinator>)
    }
}

#[derive(Debug)]
pub struct Manager {
    provider: Box<dyn ProvideCoordinator>,
    groups: BTreeMap<Option<String>, Box<dyn Coordinator>>,
}

impl Manager {
    pub fn new(provider: Box<dyn ProvideCoordinator>) -> Self {
        Self {
            provider,
            groups: BTreeMap::new(),
        }
    }
}

impl Coordinator for Manager {
    fn join(
        &mut self,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> Result<Body> {
        self.groups
            .entry(Some(group_id.to_owned()))
            .or_insert(self.provider.provide_coordinator()?)
            .join(
                client_id,
                group_id,
                session_timeout_ms,
                rebalance_timeout_ms,
                member_id,
                group_instance_id,
                protocol_type,
                protocols,
                reason,
            )
    }

    fn sync(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> Result<Body> {
        self.groups
            .entry(Some(group_id.to_owned()))
            .or_insert(self.provider.provide_coordinator()?)
            .sync(
                group_id,
                generation_id,
                member_id,
                group_instance_id,
                protocol_type,
                protocol_name,
                assignments,
            )
    }

    fn leave(
        &mut self,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> Result<Body> {
        self.groups
            .entry(Some(group_id.to_owned()))
            .or_insert(self.provider.provide_coordinator()?)
            .leave(group_id, member_id, members)
    }

    fn offset_commit(&mut self, detail: OffsetCommit<'_>) -> Result<Body> {
        self.groups
            .entry(Some(detail.group_id.to_owned()))
            .or_insert(self.provider.provide_coordinator()?)
            .offset_commit(detail)
    }

    fn offset_fetch(
        &mut self,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> Result<Body> {
        if let Some(groups) = groups {
            let group = &groups[..1];

            self.groups
                .entry(Some(group[0].group_id.to_owned()))
                .or_insert(self.provider.provide_coordinator()?)
                .offset_fetch(group_id, topics, Some(group), require_stable)
        } else {
            self.groups
                .entry(group_id.map(|group_id| group_id.to_owned()))
                .or_insert(self.provider.provide_coordinator()?)
                .offset_fetch(group_id, topics, groups, require_stable)
        }
    }

    fn heartbeat(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<Body> {
        self.groups
            .entry(Some(group_id.to_owned()))
            .or_insert(self.provider.provide_coordinator()?)
            .heartbeat(group_id, generation_id, member_id, group_instance_id)
    }
}
