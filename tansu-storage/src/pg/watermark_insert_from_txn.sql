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

insert into watermark
(topition, stable)

select tp.id, txn_po.offset_end

from

cluster c,
producer p,
topic t,
topition tp,
txn,
txn_produce_offset txn_po,
txn_topition txn_tp

where

c.name = $1
and txn.name = $2
and p.id = $3
and p.epoch = $4

and p.cluster = c.id
and t.cluster = c.id
and txn.cluster = c.id

and tp.topic = t.id

and txn_po.txn_topition = txn_tp.id

and txn_tp.transaction = txn.id
and txn_tp.topition = tp.id;
