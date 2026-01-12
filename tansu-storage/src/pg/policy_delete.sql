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

-- retention.ms

with

ancient as (
    select tp.id as topition, r.offset_id as offset_id
    from record r
    join topition tp on tp.id = r.topition
    join topic t on t.id = tp.topic
    join cluster c on t.cluster = c.id
    join topic_configuration tc_policy on tc_policy.topic = t.id
    left join topic_configuration tc_retention on tc_retention.topic = t.id
    where c.name = $1
    and tc_policy.name = 'cleanup.policy'
    and tc_policy.value like '%delete%'
    and tc_retention.name = 'retention.ms'
    and (extract(epoch from cast($2 as timestamp)) - extract(epoch from r.timestamp)) > coalesce(cast(tc_retention.value as integer) / 1000, $3)
)

delete from record
where (record.topition, record.offset_id) in (select * from ancient);
