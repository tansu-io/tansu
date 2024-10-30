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

select count(distinct txn.name)
from cluster c,
producer p,
topic t,
topition tp,
txn,
txn_topition txn_tp
where c.name = $1
and p.id = $2
and p.epoch = $3
and t.name = $4
and tp.partition = $5
and txn.cluster = c.id
and txn.producer = p.id
and p.cluster = c.id
and t.cluster = c.id
and tp.topic = t.id
and txn_tp.transaction = txn.id
and txn_tp.topition = tp.id;
