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

insert into txn_offset_commit_tp
(offset_commit, topition, committed_offset, leader_epoch, metadata)
select oc.id, tp.id, $8, $9, $10
from cluster c,
consumer_group cg,
producer p,
topic t,
topition tp,
txn_offset_commit oc,
txn
where c.name = $1
and txn.name = $2
and cg.name = $3
and p.id = $4
and p.epoch = $5
and t.name = $6
and tp.partition = $7
and txn.cluster = c.id
and cg.cluster = c.id
and p.cluster = c.id
and t.cluster = c.id
and tp.topic = t.id
and oc.transaction = txn.id
and oc.consumer_group = cg.id
