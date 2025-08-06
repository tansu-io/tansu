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


-- prepare txn_detail_update_sequence (text, integer, integer) as

update txn_detail

set

sequence = txn_detail.sequence + $5

from

cluster c,
producer p,
txn

where

c.name = $1
and txn.name = $2
and p.id = $3
and txn_detail.epoch = $4

and p.cluster = c.id
and txn.cluster = c.id
and txn.producer = p.id
and txn_detail."transaction" = txn.id;
