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

create table if not exists txn_offset_commit (
    id integer primary key autoincrement not null,
    txn_detail integer references txn_detail (id),
    consumer_group integer references consumer_group (id),
    generation_id integer,
    member_id text,
    last_updated text default current_timestamp not null,
    created_at text default current_timestamp not null,
    unique (txn_detail, consumer_group)
);
