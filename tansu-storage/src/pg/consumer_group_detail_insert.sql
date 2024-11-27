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

-- prepare cg_update (text, text, uuid, uuid, json) as
insert into consumer_group_detail
(consumer_group, e_tag, detail)

select cg.id, $4, $5

from cluster c
join consumer_group cg on cg.cluster = c.id

where c.name = $1
and cg.name = $2

on conflict (consumer_group)

do update set

detail = excluded.detail,
last_updated = excluded.last_updated,
e_tag = $4

where consumer_group_detail.e_tag = $3

returning $2, $1, e_tag, detail;
