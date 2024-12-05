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

-- Last Stable Offset Tracking: To do this, the broker must maintain in
-- memory the set of active transactions along with their initial
-- offsets. The LSO is always equal to the minimum of the initial offsets
-- across all active transactions.
--

-- prepare list_latest_offset (text, text, integer) as


select offset_id, timestamp

from

(select

1 as o, r.offset_id, r.timestamp

from

cluster c

join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id

join txn on txn.cluster = c.id
join txn_detail txn_d on txn_d.transaction = txn.id
join txn_topition txn_tp on txn_tp.txn_detail = txn_d.id and txn_tp.topition = tp.id
join txn_produce_offset txn_po on txn_po.txn_topition = txn_tp.id
join record r on r.topition = tp.id and r.offset_id = txn_po.offset_start

where

c.name = $1
and t.name = $2
and tp.partition = $3
and (txn_d.status = 'PREPARE_COMMIT' or txn_d.status = 'PREPARE_ABORT' or txn_d.status = 'BEGIN')

union

select

2 as o, r.offset_id + 1, r.timestamp

from

cluster c
join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id
join watermark w on w.topition = tp.id
join record r on r.topition = tp.id and r.offset_id = w.high - 1

where

c.name = $1
and t.name = $2
and tp.partition = $3

order by o, offset_id asc
limit 1);
