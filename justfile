test:
    cargo test --workspace --all-features --all-targets

clippy:
    cargo clippy --all-targets --all-features

miri:
    cargo +nightly miri test --no-fail-fast --all-features

work-dir:
    rm -rf work-dir/tansu-*/*

list-topics:
    kafka-topics --bootstrap-server 127.0.0.1:9092 --list

test-topic-describe:
    kafka-topics --bootstrap-server 127.0.0.1:9092 --describe --topic test

test-topic-create:
    kafka-topics --bootstrap-server 127.0.0.1:9092 --config cleanup.policy=compact --partitions=3 --replication-factor=1 --create --topic test

test-topic-produce:
    echo "h1:pqr,h2:jkl,h3:uio	qwerty	poiuy\nh1:def,h2:lmn,h3:xyz	asdfgh	lkj\nh1:stu,h2:fgh,h3:ijk	zxcvbn	mnbvc" | kafka-console-producer --bootstrap-server localhost:9092 --topic test --property parse.headers=true --property parse.key=true

test-topic-consume:
    kafka-console-consumer --consumer.config /usr/local/etc/kafka/consumer.properties --bootstrap-server localhost:9092 --topic test --from-beginning --property print.timestamp=true --property print.key=true --property print.offset=true --property print.partition=true --property print.headers=true --property print.value=true

tansu-1:
    RUST_BACKTRACE=1 ./target/debug/tansu-server \
        --kafka-cluster-id RvQwrYegSUCkIPkaiAZQlQ \
        --kafka-listener-url tcp://127.0.0.1:9092/ \
        --kafka-node-id 4343 \
        --raft-listener-url tcp://localhost:4567/ \
        --raft-peer-url tcp://localhost:4568/ \
        --raft-peer-url tcp://localhost:4569/ \
        --work-dir work-dir/tansu-1

kafka-proxy:
    docker run -d -p 19092:9092 apache/kafka:3.8.0

proxy:
    ./target/debug/tansu-proxy


all: test miri
