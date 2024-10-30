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

select
id as offset, timestamp
from
record
join (
select
coalesce(min(record.id), (select last_value from record_id_seq)) as offset
from record, topic, cluster
where
topic.cluster = cluster.id
and cluster.name = $1
and topic.name = $2
and record.partition = $3
and record.timestamp >= $4
and record.topic = topic.id) as minimum
on record.id = minimum.offset;
