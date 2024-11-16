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

cluster c
join producer p on p.cluster = c.id
join producer_epoch pe on pe.producer = p.id
join consumer_group cg on cg.cluster = c.id
join topic t on t.cluster = c.id
join txn on txn.cluster = c.id and txn.producer = p.id
join txn_detail txn_d on txn_d.transaction = txn.id and txn_d.producer_epoch = pe.id
join topition tp on tp.topic = t.id
join txn_offset_commit txn_oc on txn_oc.txn_detail = txn_d.id and txn_oc.consumer_group = cg.id
join txn_offset_commit_tp txn_oc_tp on txn_oc_tp.offset_commit = txn_oc.id and txn_oc_tp.topition = tp.id

where

c.name = $1
and txn.name = $2
and p.id = $3
and pe.epoch = $4

on conflict (consumer_group, topition)

do update set

committed_offset = excluded.committed_offset,
leader_epoch = excluded.leader_epoch,
timestamp = excluded.timestamp,
last_updated = excluded.last_updated,
metadata = excluded.metadata;
