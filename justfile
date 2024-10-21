set dotenv-load

default: fmt build test clippy

build:
    cargo build --workspace --all-targets

test:
    cargo test --workspace --all-targets

clippy:
    cargo clippy --all-targets -- -D warnings

fmt:
    cargo fmt --all

miri:
    cargo +nightly miri test --no-fail-fast --all-features

work-dir:
    rm -rf work-dir/tansu-*/*

docker-build:
    docker build --tag ghcr.io/tansu-io/tansu --no-cache --progress plain .

docker-compose-minio-up:
    docker compose up --detach minio

docker-compose-minio-down:
    docker compose down --volumes minio

docker-compose-tansu-up:
    docker compose up --detach tansu

docker-compose-tansu-down:
    docker compose down --volumes tansu

docker-compose-sr-up:
    docker compose up --detach sr

docker-compose-sr-down:
    docker compose down --volumes sr

docker-compose-c3-up:
    docker compose up --detach c3

docker-compose-c3-down:
    docker compose down --volumes c3

docker-compose-db-up:
    docker compose up --detach db

docker-compose-db-down:
    docker compose down --volumes db

docker-compose-up:
    docker compose up --detach

docker-compose-down:
    docker compose down --volumes

docker-compose-ps:
    docker compose ps

docker-compose-logs:
    docker compose logs

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
    kafka-topics --bootstrap-server 127.0.0.1:9092 --list

test-topic-describe:
    kafka-topics --bootstrap-server 127.0.0.1:9092 --describe --topic test

test-topic-create:
    kafka-topics --bootstrap-server 127.0.0.1:9092 --config cleanup.policy=compact --partitions=3 --replication-factor=1 --create --topic test

test-topic-delete:
    kafka-topics --bootstrap-server 127.0.0.1:9092 --delete --topic test

test-topic-get-offsets-earliest:
    kafka-get-offsets --bootstrap-server 127.0.0.1:9092 --topic test --time earliest

test-topic-get-offsets-latest:
    kafka-get-offsets --bootstrap-server 127.0.0.1:9092 --topic test --time latest

test-topic-produce:
    echo "h1:pqr,h2:jkl,h3:uio	qwerty	poiuy\nh1:def,h2:lmn,h3:xyz	asdfgh	lkj\nh1:stu,h2:fgh,h3:ijk	zxcvbn	mnbvc" | kafka-console-producer --bootstrap-server localhost:9092 --topic test --property parse.headers=true --property parse.key=true

test-topic-consume:
    kafka-console-consumer --bootstrap-server localhost:9092 --consumer-property fetch.max.wait.ms=15000 --group test-consumer-group --topic test --from-beginning --property print.timestamp=true --property print.key=true --property print.offset=true --property print.partition=true --property print.headers=true --property print.value=true

tansu-1:
    ./target/debug/tansu-server \
        --kafka-cluster-id ${CLUSTER_ID} \
        --kafka-listener-url tcp://0.0.0.0:9092/ \
        --kafka-advertised-listener-url tcp:://127.0.0.1:9092/ \
        --kafka-node-id ${NODE_ID} \
        --storage-engine ${STORAGE_ENGINE} \
        --work-dir work-dir/tansu-1 2>&1 | tee tansu.log

kafka-proxy:
    docker run -d -p 19092:9092 apache/kafka:3.8.0

proxy:
    ./target/debug/tansu-proxy 2>&1 | tee proxy.log


all: test miri
