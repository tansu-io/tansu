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

begin;

create table topic (
    id uuid primary key default gen_random_uuid(),
    name text not null unique,
    partitions integer,
    created_at timestamp default current_timestamp
);

create table topic_configuration (
    topic uuid references topic(id),
    name text,
    value text,
    created_at timestamp default current_timestamp
);

create type "Compression" as enum (
    'Gzip',
    'Snappy',
    'Lz4',
    'Zstd'
);

create table record (
    id bigserial primary key not null,
    topic uuid references topic(id),
    partition integer,
    producer_id bigint,
    sequence integer,
    timestamp timestamp,
    k bytea,
    v bytea,
    created_at timestamp default current_timestamp
);

create table header (
    record bigint references record(id),
    k bytea,
    v bytea,
    created_at timestamp default current_timestamp
);

create table consumer_offset (
    grp text,
    topic uuid references topic(id),
    partition integer,
    primary key (grp, topic, partition),
    committed_offset bigint,
    leader_epoch integer,
    timestamp timestamp,
    metadata text,
    created_at timestamp default current_timestamp
);

create table consumer_group (
    grp text primary key,
    generation integer,
    created_at timestamp default current_timestamp
);

commit;
