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

update txn_detail

set

status = $5,
last_updated = current_timestamp

from cluster c, producer p, producer_epoch pe, txn

where c.name = $1
and txn.name = $2
and p.id = $3
and pe.epoch = $4
and p.cluster = c.id
and pe.producer = p.id
and txn.cluster = c.id
and txn.producer = p.id
and txn_detail.producer_epoch = pe.id
and txn_detail.transaction = txn.id;
