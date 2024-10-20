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

begin;

create table cluster (
  id int generated always as identity primary key,
  name text not null,
  unique (name),
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table broker (
  id int generated always as identity primary key,
  cluster integer references cluster(id) not null,
  node integer not null,
  rack text,
  incarnation uuid not null,
  unique (cluster, node),
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table listener (
  id int generated always as identity primary key,
  broker integer references broker(id) not null,
  name text not null,
  host text not null,
  port integer not null,
  unique (broker, name),
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table topic (
  id uuid primary key default gen_random_uuid(),
  cluster integer references cluster(id) not null,
  name text not null,
  unique (cluster, name),
  partitions integer not null,
  replication_factor integer not null,
  is_internal bool default false not null,
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table topic_leader (
  topic uuid references topic(id) not null,
  partition integer not null,
  leader integer not null,
  epoch integer,
  primary key (topic, partition, leader),
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table topic_replica_node (
  topic uuid references topic(id),
  partition integer,
  replica integer,
  primary key (topic, partition),
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table topic_isr_node (
  topic uuid references topic(id),
  partition integer,
  replica integer,
  primary key (topic, partition),
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table topic_configuration (
  topic uuid references topic(id),
  name text not null,
  value text,
  primary key (topic, name),
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create type "Compression" as enum (
  'Gzip',
  'Snappy',
  'Lz4',
  'Zstd'
);

create table record (
  id bigint primary key generated always as identity (minvalue 0),
  topic uuid references topic(id),
  partition integer,
  producer_id bigint,
  sequence integer,
  timestamp timestamp,
  k bytea,
  v bytea,
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table header (
  record bigint references record(id),
  k bytea,
  v bytea,
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
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
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table consumer_group (
  grp text not null,
  cluster integer references cluster(id) not null,
  primary key (grp, cluster),
  e_tag uuid not null,
  detail json not null,
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table producer (
  id bigint generated always as identity primary key,
  transaction_id text,
  transaction_timeout_ms int,
  epoch int default 0,
  unique (id, epoch),
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);


commit;
