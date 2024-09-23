default: fmt build test clippy

build:
    cargo build --workspace --all-targets

test:
    cargo test --workspace --all-targets

clippy:
    cargo clippy --all-targets

fmt:
    cargo fmt --all

miri:
    cargo +nightly miri test --no-fail-fast --all-features

work-dir:
    rm -rf work-dir/tansu-*/*

docker-build:
    docker build --tag ghcr.io/tansu-io/tansu --no-cache --progress plain .

docker-compose-db-up:
    docker compose up --detach db

docker-compose-db-down:
    docker compose down --volumes db

docker-compose-up:
    docker compose up --detach

docker-compose-down:
    docker compose down --volumes

psql:
    docker compose exec db psql $*

docker-run-postgres:
    docker run \
        --detach \
        --name postgres \
        --publish 5432:5432 \
        --env PGUSER=postgres \
        --env POSTGRES_PASSWORD=postgres \
        --volume ./work-dir/initdb.d/:/docker-entrypoint-initdb.d/ \
        postgres:16.4

docker-run:
    docker run --detach --name tansu --publish 9092:9092 tansu

docker-rm-f:
    docker rm --force tansu

list-topics:
    kafka-topics --bootstrap-server 127.0.0.1:9092 --command-config command.properties --list

list-topics-plain:
    kafka-topics --bootstrap-server 127.0.0.1:9092 --command-config command-plain.properties --list

list-topics-scram-256:
    kafka-topics --bootstrap-server 127.0.0.1:9092 --command-config command-scram-256.properties --list

test-topic-describe:
    kafka-topics --bootstrap-server 127.0.0.1:9092 --describe --topic test

test-topic-create:
    kafka-topics --bootstrap-server 127.0.0.1:9092 --config cleanup.policy=compact --partitions=3 --replication-factor=1 --create --topic test

test-topic-get-offsets:
    kafka-get-offsets --bootstrap-server 127.0.0.1:9092 --topic test
   

test-topic-produce:
    echo "h1:pqr,h2:jkl,h3:uio	qwerty	poiuy\nh1:def,h2:lmn,h3:xyz	asdfgh	lkj\nh1:stu,h2:fgh,h3:ijk	zxcvbn	mnbvc" | kafka-console-producer --bootstrap-server localhost:9092 --topic test --property parse.headers=true --property parse.key=true

test-topic-consume:
    kafka-console-consumer --consumer.config /usr/local/etc/kafka/consumer.properties --bootstrap-server localhost:9092 --topic test --from-beginning --property print.timestamp=true --property print.key=true --property print.offset=true --property print.partition=true --property print.headers=true --property print.value=true

tansu-1:
    RUST_BACKTRACE=1 RUST_LOG=warn,tansu_server=debug ./target/debug/tansu-server \
        --kafka-cluster-id RvQwrYegSUCkIPkaiAZQlQ \
        --kafka-listener-url tcp://127.0.0.1:9092/ \
        --kafka-node-id 4343 \
        --raft-listener-url tcp://127.0.0.1:4567/ \
        --raft-peer-url tcp://127.0.0.1:4568/ \
        --raft-peer-url tcp://127.0.0.1:4569/ \
        --storage-engine pg=postgres://postgres:postgres@localhost \
        --work-dir work-dir/tansu-1

kafka-proxy:
    docker run -d -p 19092:9092 apache/kafka:3.8.0

proxy:
    ./target/debug/tansu-proxy


all: test miri
