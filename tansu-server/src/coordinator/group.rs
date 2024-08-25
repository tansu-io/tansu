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

use crate::Result;
use administrator::{Controller, Fresh, Inner, Wrapper};
use std::{collections::BTreeMap, fmt::Debug};
use tansu_kafka_sans_io::{
    join_group_request::JoinGroupRequestProtocol,
    leave_group_request::MemberIdentity,
    offset_commit_request::OffsetCommitRequestTopic,
    offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopic},
    sync_group_request::SyncGroupRequestAssignment,
    Body,
};

pub trait Coordinator: Debug + Send + Sync {
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

    fn offset_commit(
        &mut self,
        group_id: &str,
        generation_id_or_member_epoch: Option<i32>,
        member_id: Option<&str>,
        group_instance_id: Option<&str>,
        retention_time_ms: Option<i64>,
        topics: Option<&[OffsetCommitRequestTopic]>,
    ) -> Result<Body>;

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

    fn offset_commit(
        &mut self,
        group_id: &str,
        generation_id_or_member_epoch: Option<i32>,
        member_id: Option<&str>,
        group_instance_id: Option<&str>,
        retention_time_ms: Option<i64>,
        topics: Option<&[OffsetCommitRequestTopic]>,
    ) -> Result<Body> {
        (**self).offset_commit(
            group_id,
            generation_id_or_member_epoch,
            member_id,
            group_instance_id,
            retention_time_ms,
            topics,
        )
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

pub trait ProvideCoordinator: Debug + Send + Sync {
    fn provide_coordinator(&mut self) -> Result<Box<dyn Coordinator>>;
}

#[derive(Copy, Clone, Debug)]
pub struct GroupProvider;

impl ProvideCoordinator for GroupProvider {
    fn provide_coordinator(&mut self) -> Result<Box<dyn Coordinator>> {
        let wrapper: Wrapper = Inner::<Fresh>::default().into();
        Ok(Box::new(Controller::new(wrapper)) as Box<dyn Coordinator>)
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

    fn offset_commit(
        &mut self,
        group_id: &str,
        generation_id_or_member_epoch: Option<i32>,
        member_id: Option<&str>,
        group_instance_id: Option<&str>,
        retention_time_ms: Option<i64>,
        topics: Option<&[OffsetCommitRequestTopic]>,
    ) -> Result<Body> {
        self.groups
            .entry(Some(group_id.to_owned()))
            .or_insert(self.provider.provide_coordinator()?)
            .offset_commit(
                group_id,
                generation_id_or_member_epoch,
                member_id,
                group_instance_id,
                retention_time_ms,
                topics,
            )
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
