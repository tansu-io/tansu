# Tansu

Tansu is an Apache Kafka API compatible server written in 🦀 Rust, licensed under the GNU AGPL.

> [!CAUTION]
>
> This project is still in early development.
> Expect breaking changes and bugs, and please report any issues you encounter.
> Thank you!

---

## Quick start

Assuming that you have Apache Kafka command line tools and [just](https://github.com/casey/just) installed
for the following, while in the root of the repository:

Start a Tansu server that will listen for connections on 127.0.0.1:9092:

```shell
just tansu-1
```

In another shell:

```shell
just test-topic-create
```

The above will use `kafka-topics` to create a test topic by connecting to 127.0.0.1:9092.
Output should look like:

```shell
kafka-topics --bootstrap-server 127.0.0.1:9092 --config cleanup.policy=compact --partitions=1 --replication-factor=1 --create --topic test
Created topic test.
```
To produce some data onto the test topic:

```shell
just test-topic-produce
```

Output will look like:

```shell
echo "h1:pqr,h2:jkl,h3:uio  qwerty  poiuy" | kafka-console-producer --bootstrap-server localhost:9092 --topic test --property parse.headers=true --property parse.key=true
>>
```

The above produces a record with 3 headers (h1 -> pqr, h2 -> jkl, h3 -> uio) with a key of "qwerty" and value of "poiuy".

To consume the data:

```shell
just test-topic-consume
```

Output should look like:

```shell
kafka-console-consumer --consumer.config /usr/local/etc/kafka/consumer.properties --bootstrap-server localhost:9092 --topic test --from-beginning --property print.timestamp=true --property print.key=true --property print.offset=true --property print.partition=true --property print.headers=true --property print.value=true
CreateTime:1724251212963        Partition:0     Offset:0        h1:pqr,h2:jkl,h3:uio    qwerty  poiuy
```
