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
(consumer_group, topition, committed_offset, leader_epoch, timestamp, metadata)
select cg.id, tp.id, $5, $6, $7, $8
from cluster c, consumer_group cg, topic t, topition tp
where c.name = $1
and t.name = $2
and tp.partition = $3
and cg.name = $4
and t.cluster = c.id
and tp.topic = t.id
and cg.cluster = c.id
on conflict (consumer_group, topition)
do update set
committed_offset = excluded.committed_offset,
leader_epoch = excluded.leader_epoch,
timestamp = excluded.timestamp,
metadata = excluded.metadata;
