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
-- prepare watermark (text, text, integer) as
select

min(txn_po.offset_start) as stable

from cluster c

join topic t on t.cluster = c.id
join txn on txn.cluster = c.id
join txn_topition txn_tp on txn_tp.transaction = txn.id
join txn_produce_offset txn_po on txn_po.txn_topition = txn_tp.id
join topition tp on tp.topic = t.id and txn_tp.topition = tp.id
join watermark w on w.topition = tp.id

where

c.name = $1
and t.name = $2
and tp.partition = $3;
