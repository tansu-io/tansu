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

-- prepare list_latest_offset (text, text, integer) as
select r.offset_id, r.timestamp
from cluster c, record r, topic t, topition tp, watermark w
where c.name = $1
and t.name = $2
and tp.partition = $3
and t.cluster = c.id
and tp.topic = t.id
and w.topition = tp.id
and r.offset_id = w.high
and r.topition = tp.id;
