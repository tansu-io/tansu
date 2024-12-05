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

insert into producer_detail (producer_epoch, topition, sequence)

select pe.id, tp.id, $6

from

cluster c
join producer p on p.cluster = c.id
join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id
join producer_epoch pe on pe.producer = p.id

where

c.name = $1
and t.name = $2
and tp.partition = $3
and p.id = $4
and pe.epoch = $5

on conflict (producer_epoch, topition)

do update set

sequence = producer_detail.sequence + $6,
last_updated = excluded.last_updated
