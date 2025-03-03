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

-- prepare producer_epoch_insert(text, integer) as

with curr as (

select

p.id,
coalesce(max(pe.epoch) + 1, 0) as epoch

from

cluster c
join producer p on p.cluster = c.id
left join producer_epoch pe on pe.producer = p.id

where

c.name = $1
and p.id = $2

group by p.id

)

insert into producer_epoch (producer, epoch)
select * from curr
returning epoch;
