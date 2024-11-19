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

create table if not exists cluster (
  id int generated always as identity primary key,
  name text not null,
  unique (name),
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table if not exists broker (
  id int generated always as identity primary key,

  cluster int references cluster(id) not null,
  node int not null,
  unique (cluster, node),

  rack text,
  incarnation uuid not null,
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table if not exists listener (
  id int generated always as identity primary key,

  broker int references broker(id) not null,
  name text not null,
  unique (broker, name),

  host text not null,
  port int not null,
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table if not exists topic (
  id int generated always as identity primary key,

  cluster int references cluster(id) not null,
  name text not null,
  unique (cluster, name),

  uuid uuid default gen_random_uuid(),
  partitions int not null,
  replication_factor int not null,
  is_internal bool default false not null,
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table if not exists topition (
  id int generated always as identity primary key,

  topic int references topic(id),
  partition int,
  unique (topic, partition),

  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);


create table if not exists watermark (
  id int generated always as identity primary key,

  topition int references topition(id),
  unique(topition),

  low bigint,
  high bigint,

  -- Last Stable Offset Tracking: To do this, the broker must maintain in
  -- memory the set of active transactions along with their initial
  -- offsets. The LSO is always equal to the minimum of the initial offsets
  -- across all active transactions.
  --
  stable bigint,

  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table if not exists topic_configuration (
  id int generated always as identity primary key,

  topic int references topic(id),
  name text not null,
  unique (topic, name),

  value text,
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table if not exists record (
  id bigint generated always as identity primary key,

  topition int references topition(id),
  offset_id bigint not null,
  unique (topition, offset_id),

  -- if the record was produced as part of a transaction
  --
  producer_id bigint,
  producer_epoch smallint,

  timestamp timestamp,
  k bytea,
  v bytea,

  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table if not exists header (
  id bigint generated always as identity primary key,

  record int references record(id),
  k bytea,
  unique (record, k),

  v bytea,

  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table if not exists consumer_group (
  id int generated always as identity primary key,

  cluster int references cluster(id) not null,
  name text not null,
  unique (cluster, name),

  e_tag uuid not null,
  detail json not null,
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table if not exists consumer_offset (
  id int generated always as identity primary key,

  consumer_group int references consumer_group(id),
  topition int references topition(id),
  unique (consumer_group, topition),

  committed_offset bigint,
  leader_epoch int,
  timestamp timestamp,
  metadata text,
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);


-- non transactional idempotent producer
--
create table if not exists producer (
  id bigint generated by default as identity primary key,

  cluster int references cluster(id) not null,

  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table if not exists producer_epoch (
  id int generated by default as identity primary key,

  producer bigint references producer(id),
  epoch smallint default 0 not null,
  unique (producer, epoch),

  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table if not exists producer_detail (
  id bigint generated by default as identity primary key,

  producer_epoch int references producer_epoch(id),
  topition int references topition(id),
  unique (producer_epoch, topition),

  sequence int default 0 not null,

  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

-- transactional, including idempotent producer
--
create table if not exists txn (
  id bigint generated always as identity primary key,

  cluster int references cluster(id),
  name text,
  unique (cluster, name),

  producer bigint references producer(id),

  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table if not exists txn_detail (
  id bigint generated always as identity primary key,

  transaction bigint references txn(id),
  producer_epoch int references producer_epoch(id),
  unique (transaction, producer_epoch),

  transaction_timeout_ms int not null,

  -- AddPartitionsToTxnRequest:
  -- If this is the first partition added to the transaction,
  -- the coordinator will also start the transaction timer
  started_at timestamp,

  -- BEGIN, PREPARE_COMMIT, PREPARE_ABORT, COMMITTED or ABORTED
  --
  status text,

  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

-- AddPartitionsToTxnRequest
--
create table if not exists txn_topition (
  id int generated always as identity primary key,

  txn_detail int references txn_detail(id),
  topition int references topition(id),
  unique (txn_detail, topition),

  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table if not exists txn_produce_offset (
  id int generated always as identity primary key,

  txn_topition int references txn_topition(id),
  unique (txn_topition),

  offset_start bigint,
  offset_end bigint,

  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table if not exists txn_offset_commit (
  id int generated always as identity primary key,

  txn_detail int references txn_detail(id),
  consumer_group int references consumer_group(id),
  unique (txn_detail, consumer_group),

  generation_id int,
  member_id text,

  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create table if not exists txn_offset_commit_tp (
  id int generated always as identity primary key,

  offset_commit int references txn_offset_commit(id),
  topition int references topition(id),
  unique (offset_commit, topition),

  committed_offset bigint,
  leader_epoch int,
  metadata text,
  last_updated timestamp default current_timestamp not null,
  created_at timestamp default current_timestamp not null
);

create or replace view cluster_txn_produce_offset as
select
c.name as cluster,
p.id producer_id,
pe.epoch as producer_epoch,
txn.name as txn,
txn_d.status as status,
t.name as topic,
tp.partition as partition,
txn_po.offset_start as offset_start,
txn_po.offset_end as offset_end
from cluster c
join topic t on t.cluster = c.id
join producer p on p.cluster = c.id
join producer_epoch pe on pe.producer = pe.id
join txn on txn.cluster = c.id and txn.producer = p.id
join txn_detail txn_d on txn_d.transaction = txn.id and txn_d.producer_epoch = pe.id
join txn_topition txn_tp on txn_tp.txn_detail = txn_d.id
join txn_produce_offset txn_po on txn_po.txn_topition = txn_tp.id
join topition tp on tp.topic = t.id and txn_tp.topition = tp.id
order by c.name, txn.name, t.name, tp.partition, p.id, pe.epoch;

create or replace view cluster_txn_consumer_offset as
select
c.name as cluster,
p.id as producer_id,
pe.epoch as producer_epoch,
txn.name as txn,
t.name as topic,
tp.partition as partition,
txn_oc_tp.committed_offset as committed_offset,
txn_oc_tp.leader_epoch,
txn_oc_tp.created_at,
txn_oc_tp.metadata
from
cluster c
join consumer_group cg on cg.cluster = c.id
join producer p on p.cluster = c.id
join producer_epoch pe on pe.producer = pe.id
join txn on txn.cluster = c.id and txn.producer = p.id
join txn_detail txn_d on txn_d.transaction = txn.id and txn_d.producer_epoch = pe.id
join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id
join txn_offset_commit txn_oc on txn_oc.txn_detail = txn_d.id and txn_oc.consumer_group = cg.id
join txn_offset_commit_tp txn_oc_tp on txn_oc_tp.offset_commit = txn_oc.id and txn_oc_tp.topition = tp.id
order by c.name, txn.name, t.name, tp.partition, p.id, pe.epoch;

create or replace view cluster_watermark as
select
c.name as cluster,
t.name as topic,
tp.partition as partition,
w.low as low,
w.high as high
from
cluster c
join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id
join watermark w on w.topition = tp.id
where w.low is not null and w.high is not null
order by c.name, t.name, tp.partition;

create or replace view v_txn_topition as
select
c.name as cluster,
txn.name as txn,
p.id as producer_id,
pe.epoch as producer_epoch,
t.name as topic,
tp.partition as partition
from
cluster c
join producer p on p.cluster = c.id
join producer_epoch pe on pe.producer = p.id
join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id
join txn on txn.cluster = c.id and txn.producer = p.id
join txn_detail txn_d on txn_d.transaction = txn.id and txn_d.producer_epoch = pe.id
join txn_topition txn_tp on txn_tp.txn_detail = txn_d.id and txn_tp.topition = tp.id;

create or replace view t_txn_detail as
select
c.name as cluster,
txn.name as txn,
p.id as producer_id,
pe.epoch as producer_epoch,
txn_d.status,
txn_d.started_at
from
cluster c
join producer p on p.cluster = c.id
join producer_epoch pe on pe.producer = p.id
join txn on txn.cluster = c.id and txn.producer = p.id
join txn_detail txn_d on txn_d.transaction = txn.id and txn_d.producer_epoch = pe.id;

commit;
