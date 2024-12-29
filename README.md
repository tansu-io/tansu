# Tansu

[Tansu][github-com-tansu-io] is a drop-in replacement for
Apache Kafka with PostgreSQL, S3 or memory storage engines.
Without the cost of broker replicated storage for
durability. Licensed under the [GNU AGPL][agpl-license].
Written in 100% safe 🦺 async 🚀 [Rust][rust-lang-org] 🦀

Features:

- Kafka API compatible
- Elastic stateless brokers: no more planning and reassigning partitions to a broker
- Embedded JSON [schema registration and validation](docs/schema-registry.md) of messages
- Consensus free without the overhead of [Raft][raft-consensus] or [ZooKeeper][apache-zookeeper]
- All brokers are the leader and ISR of any topic partition
- All brokers are the transaction and group coordinator
- No network replication or duplicate data storage charges
- Spin up a broker for the duration of a Kafka API request: no more idle brokers
- Available with PostgreSQL, S3 or memory storage engines

For data durability:

- S3 is designed to exceed [99.999999999% (11 nines)][aws-s3-storage-classes]
- PostgreSQL with [continuous archiving][continuous-archiving]
  streaming transaction logs files to an archive
- The memory storage engine is designed for ephemeral non-production environments

## configuration

The `storage-engine` parameter is a S3 URL that specifies the bucket
to be used. The following will configure a S3 storage engine
using the "tansu" bucket (full context is in
[compose.yaml](compose.yaml) and [.env](.env)):

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

The minio console should now be running on
[http://localhost:9001](http://localhost:9001), login using
the default user credentials of "minioadmin", with password "minioadmin". Follow
the [bucket creation instructions][minio-create-bucket]
to create a bucket called "tansu", and then
[create an access key and secret][minio-create-access-key].
Use your newly created access key and
secret to update the following environment in [.env](.env):

```bash
# Your AWS access key:
AWS_ACCESS_KEY_ID="access key"

# Your AWS secret:
AWS_SECRET_ACCESS_KEY="secret"

# The endpoint URL of the S3 service:
AWS_ENDPOINT="http://minio:9000"

# Allow HTTP requests to the S3 service:
AWS_ALLOW_HTTP="true"
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

## PostgreSQL

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

Tansu is licensed under the [GNU AGPL][agpl-license].

[agpl-license]: https://www.gnu.org/licenses/agpl-3.0.en.html
[apache-zookeeper]: https://en.wikipedia.org/wiki/Apache_ZooKeeper
[aws-s3-conditional-requests]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html
[aws-s3-conditional-writes]: https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/
[aws-s3-storage-classes]: https://aws.amazon.com/s3/storage-classes/
[cloudflare-r2]: https://developers.cloudflare.com/r2/
[continuous-archiving]: https://www.postgresql.org/docs/current/continuous-archiving.html
[crates-io-object-store]: https://crates.io/crates/object_store
[github-com-tansu-io]: https://github.com/tansu-io/tansu
[librdkafka]: https://github.com/confluentinc/librdkafka
[min-io]: https://min.io
[minio-create-access-key]: https://min.io/docs/minio/container/administration/console/security-and-access.html#id1
[minio-create-bucket]: https://min.io/docs/minio/container/administration/console/managing-objects.html#creating-buckets
[object-store-dynamo-conditional-put]: https://docs.rs/object_store/0.11.0/object_store/aws/struct.DynamoCommit.html
[raft-consensus]: https://raft.github.io
[rust-lang-org]: https://www.rust-lang.org
[tansu-issues]: https://github.com/tansu-io/tansu/issues
[tigris-conditional-writes]: https://www.tigrisdata.com/blog/s3-conditional-writes/
