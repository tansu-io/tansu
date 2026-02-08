<div align="center">

# Tansu 🗃️
stateless Kafka-compatible broker with pluggable storage (PostgreSQL, SQLite, S3, memory)

<br>

[![License](https://img.shields.io/badge/License-Apache-165dfc.svg)](https://github.com/tansu-io/tansu/blob/main/LICENSE)
&nbsp;
[![Built with Rust](https://img.shields.io/badge/built_with-Rust-165dfc.svg?logo=rust)](https://www.rust-lang.org/)
&nbsp;
<br>
[![Docs](https://img.shields.io/badge/📖%20docs-docs.tansu.io-165dfc.svg)](https://docs.tansu.io/)
&nbsp;
[![Blog](https://img.shields.io/badge/%F0%9F%93%98%20blog-blog.tansu.io-165dfc.svg)](https://blog.tansu.io/articles)
&nbsp;
<br>
[![GitHub stars](https://img.shields.io/github/stars/tansu-io/tansu?style=social)](https://github.com/tansu-io/tansu)
&nbsp;
[![Bluesky](https://img.shields.io/bluesky/followers/tansu.io)](https://bsky.app/profile/tansu.io)

<br>

</div>

# What is Tansu?

[Tansu][github-com-tansu-io] is a drop-in replacement for
Apache Kafka with PostgreSQL, libSQL (SQLite), S3 or memory storage engines.
Schema backed topics (Avro, JSON or Protocol buffers) can
be written as [Apache Iceberg](https://iceberg.apache.org) or [Delta Lake](https://delta.io) tables.

Features:

- Apache Kafka API compatible
- Available with [PostgreSQL](https://www.postgresql.org), [libSQL](https://docs.turso.tech/libsql), [S3](https://en.wikipedia.org/wiki/Amazon_S3) or memory storage engines
- Topics [validated](docs/schema-registry.md) by [JSON Schema][json-schema-org], [Apache Avro](https://avro.apache.org)
  or [Protocol buffers](protocol-buffers) can be written as [Apache Iceberg](https://iceberg.apache.org) or [Delta Lake](https://delta.io) tables

See [examples using pyiceberg](https://github.com/tansu-io/example-pyiceberg), [examples using Apache Spark](https://github.com/tansu-io/example-spark) or 🆕 [examples using Delta Lake](https://github.com/tansu-io/example-delta-lake).

For data durability:

- S3 is designed to exceed [99.999999999% (11 nines)][aws-s3-storage-classes]
- PostgreSQL with [continuous archiving][continuous-archiving]
  streaming transaction logs files to an archive
- The memory storage engine is designed for ephemeral non-production environments

Tansu is a single statically linked binary containing the following:

- **broker** an Apache Kafka API compatible broker and schema registry
- **topic** a CLI to create/delete Topics
- **cat** a CLI to consume or produce Avro, JSON or Protobuf messages to a topic
- **proxy** an Apache Kafka compatible proxy

## broker

The broker subcommand is default if no other command is supplied.

```shell
Usage: tansu [OPTIONS]
       tansu <COMMAND>

Commands:
  broker  Apache Kafka compatible broker with Avro, JSON, Protobuf schema validation [default if no command supplied]
  cat     Easily consume or produce Avro, JSON or Protobuf messages to a topic
  topic   Create or delete topics managed by the broker
  proxy   Apache Kafka compatible proxy
  help    Print this message or the help of the given subcommand(s)

Options:
      --kafka-cluster-id <KAFKA_CLUSTER_ID>
          All members of the same cluster should use the same id [env: CLUSTER_ID=RvQwrYegSUCkIPkaiAZQlQ] [default: tansu_cluster]
      --kafka-listener-url <KAFKA_LISTENER_URL>
          The broker will listen on this address [env: LISTENER_URL=] [default: tcp://[::]:9092]
      --kafka-advertised-listener-url <KAFKA_ADVERTISED_LISTENER_URL>
          This location is advertised to clients in metadata [env: ADVERTISED_LISTENER_URL=tcp://localhost:9092] [default: tcp://localhost:9092]
      --storage-engine <STORAGE_ENGINE>
          Storage engine examples are: postgres://postgres:postgres@localhost, memory://tansu/ or s3://tansu/ [env: STORAGE_ENGINE=s3://tansu/] [default: memory://tansu/]
      --schema-registry <SCHEMA_REGISTRY>
          Schema registry examples are: file://./etc/schema or s3://tansu/, containing: topic.json, topic.proto or topic.avsc [env: SCHEMA_REGISTRY=file://./etc/schema]
      --data-lake <DATA_LAKE>
          Apache Parquet files are written to this location, examples are: file://./lake or s3://lake/ [env: DATA_LAKE=s3://lake/]
      --iceberg-catalog <ICEBERG_CATALOG>
          Apache Iceberg Catalog, examples are: http://localhost:8181/ [env: ICEBERG_CATALOG=http://localhost:8181/]
      --iceberg-namespace <ICEBERG_NAMESPACE>
          Iceberg namespace [env: ICEBERG_NAMESPACE=] [default: tansu]
      --prometheus-listener-url <PROMETHEUS_LISTENER_URL>
          Broker metrics can be scraped by Prometheus from this URL [env: PROMETHEUS_LISTENER_URL=tcp://0.0.0.0:9100] [default: tcp://[::]:9100]
  -h, --help
          Print help
  -V, --version
          Print version
```

A broker can be started by simply running `tansu`, all options have defaults. Tansu pickup any existing environment,
loading any found in `.env`. An [example.env](example.env) is provided as part of the distribution
and can be copied into `.env` for local modification. Sample schemas can be found in [etc/schema](etc/schema), used in the examples.

If an Apache Avro, Protobuf or JSON schema has been assigned to a topic, the
broker will reject any messages that are invalid. Schema backed topics are written
as Apache Parquet when the `-data-lake` option is provided.

## topic

The `tansu topic` command has the following subcommands:

```shell
Create or delete topics managed by the broker

Usage: tansu topic <COMMAND>

Commands:
  create  Create a topic
  delete  Delete an existing topic
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

To create a topic use:

```shell
tansu topic create taxi
```

## cat

The `tansu cat` command, has the following subcommands:

```shell
tansu cat --help
Easily consume or produce Avro, JSON or Protobuf messages to a topic

Usage: tansu cat <COMMAND>

Commands:
  produce  Produce Avro/JSON/Protobuf messages to a topic
  consume  Consume Avro/JSON/Protobuf messages from a topic
  help     Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

The `produce` subcommand reads JSON formatted messages encoding them into
Apache Avro, Protobuf or JSON depending on the schema used by the topic.

For example, the `taxi` topic is backed by [taxi.proto](etc/schema/taxi.proto).
Using [trips.json](etc/data/trips.json) containing a JSON array of objects,
`tansu cat produce` encodes each message into protobuf into the broker:

```
tansu cat produce taxi etc/data/trips.json
```

Using [duckdb](https://duckdb.org) we can read the
[Apache Parquet](https://parquet.apache.org) files
created by the broker:

```shell
duckdb :memory: "SELECT * FROM 'data/taxi/*/*.parquet'"
```

Results in the following output:

```shell
|-----------+---------+---------------+-------------+---------------|
| vendor_id | trip_id | trip_distance | fare_amount | store_and_fwd |
|     int64 |   int64 |         float |      double |         int32 |
|-----------+---------+---------------+-------------+---------------|
|         1 | 1000371 |           1.8 |       15.32 |             0 |
|         2 | 1000372 |           2.5 |       22.15 |             0 |
|         2 | 1000373 |           0.9 |        9.01 |             0 |
|         1 | 1000374 |           8.4 |       42.13 |             1 |
|-----------+---------+---------------+-------------+---------------|
```


### s3

The following will configure a S3 storage engine
using the "tansu" bucket (full context is in
[compose.yaml](compose.yaml) and [example.env](example.env)):

Copy `example.env` into `.env` so that you have a local working copy:

```shell
cp example.env .env
```

Edit `.env` so that `STORAGE_ENGINE` is defined as:

```shell
STORAGE_ENGINE="s3://tansu/"
```

First time startup, you'll need to create a bucket, an access key
and a secret in minio.

Just bring minio up, without tansu:

```shell
docker compose up -d minio
```

Create a minio `local` alias representing `http://localhost:9000` with the default credentials of `minioadmin`:

```shell
docker compose exec minio \
   /usr/bin/mc \
   alias \
   set \
   local \
   http://localhost:9000 \
   minioadmin \
   minioadmin
```

Create a `tansu` bucket in minio using the `local` alias:

```shell
docker compose exec minio \
   /usr/bin/mc mb local/tansu
```

Once this is done, you can start tansu with:

```shell
docker compose up -d tansu
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

Describe the `test` topic:

```shell
kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic test
```

Note that node 111 is the leader and ISR for each topic partition.
This node represents the broker handling your request. All brokers are node 111.

Producer:

```shell
echo "hello world" | kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic test
```

Group consumer using `test-consumer-group`:

```shell
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --group test-consumer-group \
  --topic test \
  --from-beginning \
  --property print.timestamp=true \
  --property print.key=true \
  --property print.offset=true \
  --property print.partition=true \
  --property print.headers=true \
  --property print.value=true
```

Describe the consumer `test-consumer-group` group:

```shell
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group test-consumer-group \
  --describe
```

### PostgreSQL

To switch between the minio and PostgreSQL examples, firstly
shutdown Tansu:

```shell
docker compose down tansu
```

Switch to the PostgreSQL storage engine by updating [.env](.env):

```env
# minio storage engine
# STORAGE_ENGINE="s3://tansu/"

# PostgreSQL storage engine -- NB: @db and NOT @localhost :)
STORAGE_ENGINE="postgres://postgres:postgres@db"
```

Start PostgreSQL:

```shell
docker compose up -d db
```

Bring Tansu back up:

```shell
docker compose up -d tansu
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
  --group test-consumer-group \
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
  -t test -p 1 \
  -b localhost:9092 \
  -z gzip
```

Consumer:

```shell
./examples/rdkafka_example \
  -C \
  -t test -p 1 \
  -b localhost:9092
```

## Feedback

Please [raise an issue][tansu-issues] if you encounter a problem.

## License

Tansu is licensed under [Apache 2.0][apache-license].

[apache-license]: https://www.apache.org/licenses/LICENSE-2.0
[apache-zookeeper]: https://en.wikipedia.org/wiki/Apache_ZooKeeper
[aws-s3-conditional-requests]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html
[aws-s3-conditional-writes]: https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/
[aws-s3-storage-classes]: https://aws.amazon.com/s3/storage-classes/
[cloudflare-r2]: https://developers.cloudflare.com/r2/
[continuous-archiving]: https://www.postgresql.org/docs/current/continuous-archiving.html
[crates-io-object-store]: https://crates.io/crates/object_store
[github-com-tansu-io]: https://github.com/tansu-io/tansu
[json-schema-org]: https://json-schema.org/
[librdkafka]: https://github.com/confluentinc/librdkafka
[min-io]: https://min.io
[minio-create-access-key]: https://min.io/docs/minio/container/administration/console/security-and-access.html#id1
[minio-create-bucket]: https://min.io/docs/minio/container/administration/console/managing-objects.html#creating-buckets
[object-store-dynamo-conditional-put]: https://docs.rs/object_store/0.11.0/object_store/aws/struct.DynamoCommit.html
[protocol-buffers]: https://protobuf.dev
[raft-consensus]: https://raft.github.io
[rust-lang-org]: https://www.rust-lang.org
[tansu-issues]: https://github.com/tansu-io/tansu/issues
[tigris-conditional-writes]: https://www.tigrisdata.com/blog/s3-conditional-writes/
