# Tansu

[Tansu][github-com-tansu-io] is a modern drop-in replacement for
Apache Kafka. Without the cost of broker replicated storage for
durability. Tansu is in **early** development, licensed under
the [GNU AGPL][agpl-license]. Written in async 🚀
[Rust][rust-lang-org] 🦀

Tansu brokers are:

- Kafka API compatible (exceptions: transactions and idempotent
  producer)
- Stateless with instant scaling up or down. No more planning and
  reassigning partitions to a broker
- Available with PostgreSQL or S3 storage engines

For data durability:

- S3 is designed to exceed [99.999999999% (11
  nines)][aws-s3-storage-classes]
- PostgreSQL with [continuous archiving][continuous-archiving]
  streaming transaction logs files to an archive.

Stateless brokers are cost effective, with no network replication and
duplicate data storage charges.

Stateless brokers do not have the ceremony of [Raft][raft-consensus]
or [ZooKeeper][apache-zookeeper].

## S3

You can have 3 brokers running in separate [Availability
Zones][aws-regions-zones] for resilience:

![Tansu S3](https://shortishly.com/assets/images/tansu-s3-2024-10-09.svg)

Each broker is stateless. Brokers can come and go. Without affecting
leadership of consumer groups. The leader and In-Sync-Replica is the
broker serving your request. No more client broker ping pong.

With Tansu there is no replication between brokers. The data transfer
cost between the [Availability Zones][aws-regions-zones] is a $0 line
item. There are $0 in duplicated storage charges.

With stateless brokers, you can run Tansu in a [server-less
architecture][wikipedia-serverless]:

![Tansu Lambda S3](https://shortishly.com/assets/images/tansu-lambda-s3-2024-10-09.svg)

Spin up a broker for the duration of a Kafka API request. Then spin
down. No more idle brokers.

Tansu requires that the underlying S3 service support conditional
PUT requests. While
[AWS S3 does now support conditional writes][aws-s3-conditional-writes],
the support is
[limited to not overwriting an existing object][aws-s3-conditional-requests].
To have stateless brokers we need to
[use a compare and set operation][tigris-conditional-writes],
which is not currently available in AWS S3.

Much like the Kafka protocol, the S3 protocol allows vendors to
differentiate. Different levels of service while retaining
compatibility with the underlying API. You can use [minio][min-io] or
[tigis][tigris-conditional-writes], among a number of other vendors
supporting conditional put.

Tansu uses [object store][crates-io-object-store], providing a
multi-cloud API for storage. There is an alternative option to use a
[DynamoDB-based commit protocol, to provide conditional write support
for AWS S3][object-store-dynamo-conditional-put] instead.

### configuration

The `storage-engine` parameter is a named S3 URL that specifies the bucket
to be used. The following will configure a S3 storage engine called "minio"
using the "tansu" bucket (full context is in
[compose.yaml](compose.yaml)):

```shell
--storage-engine minio=s3://tansu/
```

First time startup, with the above `compose.yaml`, you'll need to
create a bucket, an access key and a secret in minio.

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
AWS_ENDPOINT="http://localhost:9000"

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

## PostgreSQL

The major differences between Apache Kafka the Tansu PostgreSQL
storage engine are:

- Messages are not stored in segments, so that retention and
  compaction polices can be applied immediately.
- Message ordering is total over all topics and not restricted to a
  single topic partition.
- Brokers do not replicate messages, relying on [continous
  archiving][continuous-archiving] instead.

To switch between the minio and PostgreSQL examples, firstly
shutdown Tansu:

```shell
docker compose down tansu
```

Switch to the PostgreSQL storage engine by updating [.env](.env):

```env
# minio storage engine
# STORAGE_ENGINE="minio=s3://tansu/"

# PostgreSQL storage engine
STORAGE_ENGINE="pg=postgres://postgres:postgres@db"
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

## Feedback

Please [raise an issue][tansu-issues] if you encounter a problem.

## License

Tansu is licensed under the [GNU AGPL][agpl-license].

[agpl-license]: https://www.gnu.org/licenses/agpl-3.0.en.html
[apache-zookeeper]: https://en.wikipedia.org/wiki/Apache_ZooKeeper
[aws-regions-zones]: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html
[aws-s3-conditional-requests]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html
[aws-s3-conditional-writes]: https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/
[aws-s3-storage-classes]: https://aws.amazon.com/s3/storage-classes/
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
[wikipedia-serverless]: https://en.wikipedia.org/wiki/Serverless_computing
