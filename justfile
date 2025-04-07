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

minio-up:
    docker compose up --detach --wait minio

minio-down:
    docker compose down --volumes minio

minio-local-alias:
    docker compose exec minio /usr/bin/mc alias set local http://localhost:9000 minioadmin minioadmin

minio-tansu-bucket:
    docker compose exec minio /usr/bin/mc mb local/tansu

minio-lake-bucket:
    docker compose exec minio /usr/bin/mc mb local/lake

minio-ready-local:
    docker compose exec minio /usr/bin/mc ready local

tansu-up:
    docker compose up --detach tansu

tansu-down:
    docker compose down --volumes tansu

db-up:
    docker compose up --detach db

db-down:
    docker compose down --volumes db

jaeger-up:
    docker compose up --detach jaeger

jaeger-down:
    docker compose down --volumes jaeger

prometheus-up:
    docker compose up --detach prometheus

prometheus-down:
    docker compose down --volumes prometheus

grafana-up:
    docker compose up --detach grafana

grafana-down:
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

test-topic-alter:
    kafka-configs --bootstrap-server ${ADVERTISED_LISTENER} --alter --entity-type topics --entity-name test --add-config retention.ms=3600000,retention.bytes=524288000

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

search-topic-create:
    ./target/debug/tansu topic create --broker ${ADVERTISED_LISTENER_URL} --name search

search-topic-produce:
    echo '{"value": {"query": "abc/def", "page_number": 6, "results_per_page": 13, "corpus": "CORPUS_WEB"}}' | target/debug/tansu cat produce --topic search --partition 0 --schema-registry file://./etc/schema

tansu-server:
    ./target/debug/tansu broker \
        --schema-registry file://./etc/schema 2>&1 | tee tansu.log

kafka-proxy:
    docker run -d -p 19092:9092 apache/kafka:3.9.0

tansu-proxy:
    ./target/debug/tansu proxy 2>&1 | tee proxy.log

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

benchmark-flamegraph: build docker-compose-down minio-up minio-ready-local minio-local-alias minio-tansu-bucket prometheus-up grafana-up
	flamegraph -- ./target/debug/tansu broker --schema-registry file://./etc/schema 2>&1  | tee tansu.log

benchmark: build docker-compose-down minio-up minio-ready-local minio-local-alias minio-tansu-bucket prometheus-up grafana-up
	./target/debug/tansu broker --schema-registry file://./etc/schema 2>&1  | tee tansu.log

otel: build docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-tansu-bucket prometheus-up grafana-up
	./target/debug/tansu broker --schema-registry file://./etc/schema 2>&1  | tee tansu.log

otel-up: docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-tansu-bucket prometheus-up grafana-up tansu-up

server: build docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-tansu-bucket minio-lake-bucket
	./target/debug/tansu broker --schema-registry file://./etc/schema 2>&1  | tee tansu.log

people-cat:
    echo '{"key": {"id": 32123}, "value": {"name": "alice", "email": "alice@example.com"}}' | target/debug/tansu cat produce --topic people --partition 0 --schema-registry file://./etc/schema

people-topic-create:
    ./target/debug/tansu topic create --broker ${ADVERTISED_LISTENER_URL} --name people

observation-produce:
    cat etc/data/observations.json | target/debug/tansu cat produce --topic observation --partition 0 --schema-registry file://./etc/schema

observation-consume:
    target/debug/tansu cat consume --topic observation --partition 0 --schema-registry file://./etc/schema --max-wait-time-ms=5000

observation-topic-create:
    ./target/debug/tansu topic create --broker ${ADVERTISED_LISTENER_URL} --name observation

observation-duckdb-parquet:
    duckdb -init duckdb-init.sql :memory: "SELECT key,unnest(value) FROM 's3://lake/observation/*/*.parquet'"

duckdb:
    duckdb -init duckdb-init.sql


taxi-produce:
    cat etc/data/trips.json | target/debug/tansu cat produce --topic taxi --partition 0 --schema-registry file://./etc/schema

taxi-consume:
    target/debug/tansu cat consume --topic taxi --partition 0 --schema-registry file://./etc/schema --max-wait-time-ms=5000

taxi-topic-create:
    ./target/debug/tansu topic create --broker ${ADVERTISED_LISTENER_URL} --name taxi

taxi-topic-delete:
    ./target/debug/tansu topic delete --broker ${ADVERTISED_LISTENER_URL} --name taxi

taxi-duckdb-parquet:
    duckdb -init duckdb-init.sql :memory: "SELECT * FROM 's3://lake/taxi/*/*.parquet'"
