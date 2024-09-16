# Tansu

[Tansu][github-com-tansu-io] is an Apache Kafka API compatible broker
with a Postgres storage engine. Acting as a drop in replacement,
existing clients connect to Tansu, producing and fetching messages
stored in Postgres.  Tansu is in **early** development, licensed under
the [GNU AGPL][agpl-license]. Written in async 🦀
[Rust][rust-lang-org] 🚀.

While retaining API compatibility, the current storage engine
implemented for Postgres is very different when compared to Apache
Kafka:

- Messages are not stored in segments, so that retention and
  compaction polices can be applied immediately.
- Message ordering is total over all topics and not restricted to a
  single topic partition.
- Brokers do not replicate messages, relying on [continous
  archiving][continuous-archiving] instead.
  
Our initial use cases are relatively low volume Kafka deployments
where total message ordering could be useful. Other non-functional
requirements might require a different storage engine. Tansu has been
designed to work with multiple storage engines which are also in
development:

- A Postgres engine where message ordering is either per topic,
  or per topic partition (as in Kafka).
- An object store for S3 or compatible services.
- A segmented disk store (as in Kafka with broker replication).

We store a Kafka message using the following `record` schema:

```sql
create table record (
  id bigserial primary key not null,
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
```

The `k` and `v` are the key and value being stored by the client, with
the SQL being used for a fetch looks like:

```sql
with sized as (
 select
 record.id,
 timestamp,
 k,
 v,
 sum(coalesce(length(k), 0) + coalesce(length(v), 0)),
 over (order by record.id) as bytes
 from cluster, record, topic
 where
 cluster.name = $1
 and topic.name = $2
 and record.partition = $3
 and record.id >= $4
 and topic.cluster = cluster.id
 and record.topic = topic.id
) select * from sized where bytes < $5;
```

One of the parameters for the [Kafka Fetch API][kafka-fetch] is the
maximum number of bytes being returned. We use a [with
query][with-queries] here to restrict the size of the result set being
returned, with a running total of the size.

Tansu is available as a [minimal from scratch][docker-from-scratch]
docker image. With a `compose.yaml`, available from [here][compose]:

```shell
docker compose up
```

Using the regular Apache Kafka CLI you can create topics, produce and consume
messages with Tansu:

```shell
kafka-topics \
  --bootstrap-server localhost:9092 \
  --partitions=3 \
  --replication-factor=1 \
  --create --topic test
```

Producer:

```shell
echo "hello world" | kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic test
```

Consumer:

```shell
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic test \
  --from-beginning \
  --property print.timestamp=true \
  --property print.key=true \
  --property print.offset=true \
  --property print.partition=true \
  --property print.headers=true \
  --property print.value=true
```

Or using [librdkafka][librdkafka] to produce:

```shell
echo "Lorem ipsum dolor..." | \
  ./examples/rdkafka_example -P \
  -t test \
  -b localhost:9092 \
  -z gzip
```

Consumer:

```shell
./examples/rdkafka_example \
  -C \
  -t test \
  -b localhost:9092
```


[compose]: https://github.com/tansu-io/tansu/blob/main/compose.yaml
[librdkafka]: https://github.com/confluentinc/librdkafka
[docker-from-scratch]: https://docs.docker.com/build/building/base-images/#create-a-minimal-base-image-using-scratch
[agpl-license]: https://www.gnu.org/licenses/agpl-3.0.en.html
[continuous-archiving]: https://www.postgresql.org/docs/current/continuous-archiving.html
[github-com-tansu-io]: https://github.com/tansu-io/tansu
[kafka-fetch]: https://kafka.apache.org/protocol.html#The_Messages_Fetch
[rust-lang-org]: https://www.rust-lang.org
[with-queries]: https://www.postgresql.org/docs/current/queries-with.html
