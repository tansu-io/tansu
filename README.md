# Tansu

[Tansu][github-com-tansu-io] is an Apache Kafka API compatible broker
with multiple storage engines. Acting as a drop in replacement,
existing clients connect to Tansu, producing and fetching messages.
Tansu is licensed under the [GNU AGPL][agpl-license].
Written in 100% safe async 🦀 [Rust][rust-lang-org] 🚀.

Available storage engines:

- S3
- PostgreSQL (additional [discussion][tansu-postgres])

## S3

While retaining API compatibility, the storage engine
implemented for S3 is very different when compared to Apache
Kafka:

- Brokers do not replicate messages, instead relying on the underlying
  storage for resilience.
- Brokers do not require a consensus protocol (e.g., Raft or
  Zookeeper), instead conditional PUTs are used to coordinate state.
- Brokers are stateless.
- All brokers are leaders.

Note that, Tansu requires that the underlying S3 service support conditional
PUT requests. While
[AWS S3 does now support conditional writes,][aws-s3-conditional-writes],
the support is
[limited to not overwriting an existing object][aws-s3-conditional-requests].
To have stateless brokers we need to use a compare and set operation,
which is not currently available in AWS S3.

Much like the Kafka protocol, the open nature of the S3 protocol allows vendors
to differentiate with different levels of service while retaining compatibility
with the underlying API. A couple of other S3 vendors provide conditional updates,
that we have tried:

- [minio][min-io]
- [tigis][tigris-conditional-writes]

### configuration

The `storage-engine` parameter is a named S3 URL that specifies the bucket
to be used. The above will configure a S3 storage engine called "minio"
using the "tansu" bucket (full context is in
[compose.yaml](compose.yaml)):

```shell
--storage-engine minio=s3://tansu/
```

Tansu can be configured to use a local minio service with the following environment:

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

## PostgreSQL

While retaining API compatibility, the current storage engine
implemented for PostgreSQL is very different when compared to Apache
Kafka:

- Messages are not stored in segments, so that retention and
  compaction polices can be applied immediately.
- Message ordering is total over all topics and not restricted to a
  single topic partition.
- Brokers do not replicate messages, relying on [continous
  archiving][continuous-archiving] instead.

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

[agpl-license]: https://www.gnu.org/licenses/agpl-3.0.en.html
[aws-s3-conditional-requests]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html
[aws-s3-conditional-writes]: https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/
[compose]: https://github.com/tansu-io/tansu/blob/main/compose.yaml
[continuous-archiving]: https://www.postgresql.org/docs/current/continuous-archiving.html
[docker-from-scratch]: https://docs.docker.com/build/building/base-images/#create-a-minimal-base-image-using-scratch
[github-com-tansu-io]: https://github.com/tansu-io/tansu
[librdkafka]: https://github.com/confluentinc/librdkafka
[min-io]: https://min.io
[rust-lang-org]: https://www.rust-lang.org
[tansu-postgres]: https://shortishly.com/blog/tansu-postgres/
[tigris-conditional-writes]: https://www.tigrisdata.com/blog/s3-conditional-writes/
