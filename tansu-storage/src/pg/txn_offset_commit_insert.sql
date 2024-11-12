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

insert into txn_offset_commit
(txn_detail, consumer_group, generation_id, member_id)

select
txn_detail.id,
cg.id,
$6,
$7

from

cluster c
join consumer_group cg on cg.cluster = c.id
join txn on txn.cluster = c.id
join txn_detail on txn_detail.transaction = txn.id

where

c.name = $1
and txn.name = $2
and cg.name = $3
and txn.id = $4
and txn_detail.epoch = $5;
