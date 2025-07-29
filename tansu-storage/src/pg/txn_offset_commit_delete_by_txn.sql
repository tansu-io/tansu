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

delete from txn_offset_commit
where txn_offset_commit.txn_detail in (
    select txn_d.id

    from cluster c
    join producer p on p.cluster = c.id
    join producer_epoch pe on pe.producer = p.id
    join txn on txn.cluster = c.id and txn.producer = p.id
    join txn_detail txn_d on txn_d.txn = txn.id and txn_d.producer_epoch = pe.id

    where

    c.name = $1
    and txn.name = $2
    and p.id = $3
    and pe.epoch = $4
);
