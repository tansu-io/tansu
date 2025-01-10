# Sarama

[Sarama][sarama] is a [Go][golang] client library for Apache Kafka.

## console producer

The console producer can be built and installed with:

```shell
go install github.com/IBM/sarama/tools/kafka-console-producer
```

Messages can be produced from the console:

```shell
$GOPATH/bin/kafka-console-producer \
  -verbose \
  -topic=test \
  -key=abc \
  -value=def \
  -headers=foo:bar,bar:foo \
  -brokers=localhost:9092
```

## console consumer

The console consumer can be built and installed with:

```shell
go install github.com/IBM/sarama/tools/kafka-console-consumer
```

Messages can be consumed from a topic with:

```shell
$GOPATH/bin/kafka-console-consumer \
  -verbose \
  -topic=test \
  -offset=oldest \
  -brokers=localhost:9092
```

[sarama]: https://github.com/IBM/sarama
[golang]: https://go.dev
