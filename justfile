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

docker-compose-db-up:
    docker compose up --detach db

docker-compose-db-down:
    docker compose down --volumes db

docker-compose-jaeger-up:
    docker compose up --detach jaeger

docker-compose-jaeger-down:
    docker compose down --volumes jaeger

docker-compose-prometheus-up:
    docker compose up --detach prometheus

docker-compose-prometheus-down:
    docker compose down --volumes prometheus

docker-compose-grafana-up:
    docker compose up --detach grafana

docker-compose-grafana-down:
    docker compose down --volumes grafana

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
        --volume ./etc/initdb.d/:/docker-entrypoint-initdb.d/ \
        postgres:16.4

docker-run:
    docker run --detach --name tansu --publish 9092:9092 tansu

docker-rm-f:
    docker rm --force tansu

list-topics:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --list

test-topic-describe:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --describe --topic test

test-topic-create:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --config cleanup.policy=compact --partitions=3 --replication-factor=1 --create --topic test

test-topic-delete:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --delete --topic test

test-topic-get-offsets-earliest:
    kafka-get-offsets --bootstrap-server ${ADVERTISED_LISTENER} --topic test --time earliest

test-topic-get-offsets-latest:
    kafka-get-offsets --bootstrap-server ${ADVERTISED_LISTENER} --topic test --time latest

test-topic-produce:
    echo "h1:pqr,h2:jkl,h3:uio	qwerty	poiuy\nh1:def,h2:lmn,h3:xyz	asdfgh	lkj\nh1:stu,h2:fgh,h3:ijk	zxcvbn	mnbvc" | kafka-console-producer --bootstrap-server ${ADVERTISED_LISTENER} --topic test --property parse.headers=true --property parse.key=true

test-topic-consume:
    kafka-console-consumer --bootstrap-server ${ADVERTISED_LISTENER} --consumer-property fetch.max.wait.ms=15000 --group test-consumer-group --topic test --from-beginning --property print.timestamp=true --property print.key=true --property print.offset=true --property print.partition=true --property print.headers=true --property print.value=true

test-consumer-group-describe:
    kafka-consumer-groups --bootstrap-server ${ADVERTISED_LISTENER} --group test-consumer-group --describe

consumer-group-list:
    kafka-consumer-groups --bootstrap-server ${ADVERTISED_LISTENER} --list

test-reset-offsets-to-earliest:
    kafka-consumer-groups --bootstrap-server ${ADVERTISED_LISTENER} --group test-consumer-group --topic test:0 --reset-offsets --to-earliest --execute

person-topic-create:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --partitions=3 --replication-factor=1 --create --topic person

person-topic-produce-valid:
    echo 'h1:pqr,h2:jkl,h3:uio	"ABC-123"	{"firstName": "John", "lastName": "Doe", "age": 21}' | kafka-console-producer --bootstrap-server ${ADVERTISED_LISTENER} --topic person --property parse.headers=true --property parse.key=true

person-topic-produce-invalid:
    echo 'h1:pqr,h2:jkl,h3:uio	"ABC-123"	{"firstName": "John", "lastName": "Doe", "age": -1}' | kafka-console-producer --bootstrap-server ${ADVERTISED_LISTENER} --topic person --property parse.headers=true --property parse.key=true

person-topic-consume:
    kafka-console-consumer \
        --bootstrap-server ${ADVERTISED_LISTENER} \
        --consumer-property fetch.max.wait.ms=15000 \
        --group person-consumer-group --topic person \
        --from-beginning \
        --property print.timestamp=true \
        --property print.key=true \
        --property print.offset=true \
        --property print.partition=true \
        --property print.headers=true \
        --property print.value=true

tansu-server:
    ./target/debug/tansu-server \
        --schema-registry file://./etc/schema 2>&1 >tansu.log

kafka-proxy:
    docker run -d -p 19092:9092 apache/kafka:3.9.0

tansu-proxy:
    ./target/debug/tansu-proxy 2>&1 | tee proxy.log

codespace-create:
    gh codespace create \
        --repo $(gh repo view --json nameWithOwner --jq .nameWithOwner) \
        --branch $(git branch --show-current) \
        --machine basicLinux32gb

codespace-delete:
    gh codespace ls \
        --repo $(gh repo view \
            --json nameWithOwner \
            --jq .nameWithOwner) \
        --json name \
        --jq '.[].name' | xargs --no-run-if-empty -n1 gh codespace delete --codespace

codespace-logs:
    gh codespace logs \
        --codespace $(gh codespace ls \
            --repo $(gh repo view \
                --json nameWithOwner \
                --jq .nameWithOwner) \
            --json name \
            --jq '.[].name')

codespace-ls:
    gh codespace list \
        --repo $(gh repo view \
            --json nameWithOwner \
            --jq .nameWithOwner)

codespace-ssh:
    gh codespace ssh \
        --codespace $(gh codespace ls \
            --repo $(gh repo view \
                --json nameWithOwner \
                --jq .nameWithOwner) \
            --json name \
            --jq '.[].name')

all: test miri
