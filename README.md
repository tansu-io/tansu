# Tansu

[Tansu][github-com-tansu-io] is an Apache Kafka API compatible broker
with PostgreSQL and S3 storage engines. Acting as a drop in replacement,
existing clients connect to Tansu, producing and fetching messages.
Written in 100% safe async 🦀 [Rust][rust-lang-org] 🚀.

## S3

The storage engine implemented for S3 is very different when
compared to Apache Kafka:

- Brokers do not replicate messages, instead relying on the underlying
  storage for resilience.
- Brokers do not require a consensus protocol (e.g., Raft or
  Zookeeper), instead conditional PUTs are used to coordinate state.
- Brokers are stateless.
- All brokers are leaders.

Note that, Tansu requires that the underlying S3 service support conditional
PUT requests. While
[AWS S3 does now support conditional writes][aws-s3-conditional-writes],
the support is
[limited to not overwriting an existing object][aws-s3-conditional-requests].
To have stateless brokers we need to
[use a compare and set operation][tigris-conditional-writes],
which is not currently available in AWS S3.

Much like the Kafka protocol, the open nature of the S3 protocol allows vendors
to differentiate with different levels of service while retaining compatibility
with the underlying API. We have tried [minio][min-io]
and [tigis][tigris-conditional-writes], among a number of other vendors now supporting
conditional put.

### configuration

The `storage-engine` parameter is a named S3 URL that specifies the bucket
to be used. The above will configure a S3 storage engine called "minio"
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

The minio console should now be running on http://localhost:9001, login using
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

## License

Tansu is licensed under the [GNU AGPL][agpl-license].

[agpl-license]: https://www.gnu.org/licenses/agpl-3.0.en.html
[aws-s3-conditional-requests]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html
[aws-s3-conditional-writes]: https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/
[continuous-archiving]: https://www.postgresql.org/docs/current/continuous-archiving.html
[github-com-tansu-io]: https://github.com/tansu-io/tansu
[librdkafka]: https://github.com/confluentinc/librdkafka
[min-io]: https://min.io
[rust-lang-org]: https://www.rust-lang.org
[tigris-conditional-writes]: https://www.tigrisdata.com/blog/s3-conditional-writes/
[minio-create-bucket]: https://min.io/docs/minio/container/administration/console/managing-objects.html#creating-buckets
[minio-create-access-key]: https://min.io/docs/minio/container/administration/console/security-and-access.html#id1
