-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

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
