-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright ⓒ 2024 Peter Morgan <peter.james.morgan@gmail.com>
--
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU Affero General Public License as
-- published by the Free Software Foundation, either version 3 of the
-- License, or (at your option) any later version.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU Affero General Public License for more details.
--
-- You should have received a copy of the GNU Affero General Public License
-- along with this program.  If not, see <https://www.gnu.org/licenses/>.

insert into consumer_offset
(consumer_group,
topition,
committed_offset,
leader_epoch,
timestamp,
metadata)

select

cg.id,
tp.id,
txn_oc_tp.committed_offset,
txn_oc_tp.leader_epoch,
txn_oc_tp.created_at,
txn_oc_tp.metadata

from

cluster c,
consumer_group cg,
producer p,
topic t,
topition tp,
txn,
txn_offset_commit txn_oc,
txn_offset_commit_tp txn_oc_tp

where

c.name = $1
and txn.name = $2
and p.id = $3
and p.epoch = $4

and cg.cluster = c.id
and p.cluster = c.id
and t.cluster = c.id
and txn.cluster = c.id

and tp.topic = t.id

and txn_oc.transaction = txn.id
and txn_oc.consumer_group = cg.id
and txn_oc.producer_id = p.id

and txn_oc_tp.offset_commit = txn_oc.id
and txn_oc_tp.topition = tp.id

on conflict (consumer_group, topition)

do update set
committed_offset = excluded.committed_offset,
leader_epoch = excluded.leader_epoch,
timestamp = excluded.timestamp,
metadata = excluded.metadata;
