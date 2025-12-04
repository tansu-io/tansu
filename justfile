set dotenv-load

default: fmt build test clippy

about:
    cargo about generate about.hbs > license.html

cargo-build +args:
    cargo build {{args}}

license:
    cargo about generate about.hbs > license.html

build: (cargo-build "--bin" "tansu" "--no-default-features" "--features" "delta,dynostore,iceberg,libsql,parquet,postgres")

build-examples: (cargo-build "--examples")

release: (cargo-build "--release" "--bin" "tansu" "--no-default-features" "--features" "delta,dynostore,iceberg,libsql,parquet,postgres")

release-sqlite: (cargo-build "--release" "--bin" "tansu" "--no-default-features" "--features" "libsql")

test: test-workspace test-doc

test-workspace:
    cargo test --workspace --all-targets --all-features

test-doc:
    cargo test --workspace --doc --all-features

doc:
    cargo doc --all-features --open

check:
    cargo check --workspace --all-features --all-targets

clippy:
    cargo clippy --workspace --all-features --all-targets -- -D warnings

fmt:
    cargo fmt --all --check

miri:
    cargo +nightly miri test --no-fail-fast --all-features

docker-build:
    docker build --tag ghcr.io/tansu-io/tansu --no-cache --progress plain --debug .

docker-build-cross:
    docker build --tag ghcr.io/tansu-io/tansu --no-cache --progress plain --platform linux/amd64,linux/arm64 --debug .

minio-up: (docker-compose-up "minio")

minio-down: (docker-compose-down "minio")

minio-mc +args:
    docker compose exec minio /usr/bin/mc {{args}}

minio-local-alias: (minio-mc "alias" "set" "local" "http://localhost:9000" "minioadmin" "minioadmin")

minio-tansu-bucket: (minio-mc "mb" "local/tansu")

minio-lake-bucket: (minio-mc "mb" "local/lake")

minio-ready-local: (minio-mc "ready" "local")

tansu-up: (docker-compose-up "tansu")

tansu-down: (docker-compose-down "tansu")

db-up: (docker-compose-up "db")

db-down: (docker-compose-down "db")

jaeger-up: (docker-compose-up "jaeger")

jaeger-down: (docker-compose-down "jaeger")

prometheus-up: (docker-compose-up "prometheus")

prometheus-down: (docker-compose-down "prometheus")

grafana-up: (docker-compose-up "grafana")

grafana-down: (docker-compose-down "grafana")

lakehouse-catalog-up: (docker-compose-up "lakehouse-catalog")

lakehouse-catalog-down: (docker-compose-down "lakehouse-catalog")

lakehouse-accept-terms-of-use:
    curl http://localhost:8181/management/v1/bootstrap -H "Content-Type: application/json" --data '{"accept-terms-of-use": true}'

lakehouse-create-warehouse:
    curl http://localhost:8181/management/v1/warehouse -H "Content-Type: application/json" --data @etc/lakekeeper/create-default-warehouse.json

lakehouse-migrate:
    docker compose exec lakehouse-catalog /home/nonroot/iceberg-catalog migrate

docker-compose-up *args:
    docker compose --ansi never --progress plain up --no-color --quiet-pull --wait --detach {{args}}

docker-compose-down *args:
    docker compose down --remove-orphans --volumes {{args}}

docker-compose-ps:
    docker compose ps

docker-compose-logs *args:
    docker compose logs {{args}}

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

docker-prune:
    docker system prune --force

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

topic-create topic *args:
    target/debug/tansu topic create {{topic}} {{args}}

topic-delete topic:
    target/debug/tansu topic delete {{topic}}

cat-produce topic file:
    target/debug/tansu cat produce {{topic}} {{file}}

cat-consume topic:
    target/debug/tansu cat consume {{topic}} --max-wait-time-ms=5000

generator topic *args:
    target/debug/tansu generator {{args}} {{topic}} 2>&1 >generator.log

duckdb-k-unnest-v-parquet topic:
    duckdb -init duckdb-init.sql :memory: "SELECT key,unnest(value) FROM '{{replace(env("DATA_LAKE"), "file://./", "")}}/{{topic}}/*/*.parquet'"

duckdb-parquet topic:
    duckdb -init duckdb-init.sql :memory: "SELECT * FROM '{{replace(env("DATA_LAKE"), "file://./", "")}}/{{topic}}/*/*.parquet'"

# create person topic with schema etc/schema/person.json
person-topic-create: (topic-create "person")

# delete person topic
person-topic-delete: (topic-delete "person")

# produce etc/data/persons.json with schema etc/schema/person.json
person-topic-populate: (cat-produce "person" "etc/data/persons.json")

# produce valid data, that is accepted by the broker
person-topic-produce-valid:
    echo '{"key": "345-67-6543", "value": {"firstName": "John", "lastName": "Doe", "age": 21}}' | target/debug/tansu cat produce person

# produce invalid data, that is rejected by the broker
person-topic-produce-invalid:
    echo '{"key": "ABC-12-4242", "value": {"firstName": "John", "lastName": "Doe", "age": -1}}' | target/debug/tansu cat produce person

# person parquet
person-duckdb-parquet: (duckdb-k-unnest-v-parquet "person")

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

# create search topic with etc/schema/search.proto
search-topic-create: (topic-create "search")

# delete search topic
search-topic-delete: (topic-delete "search")

# produce data to search topic with etc/schema/search.proto
search-topic-produce:
    echo '{"value": {"query": "abc/def", "page_number": 6, "results_per_page": 13, "corpus": "CORPUS_WEB"}}' | target/debug/tansu cat produce search

# search parquet
search-duckdb-parquet: (duckdb-parquet "search")

tansu-server:
    target/debug/tansu broker --schema-registry file://./etc/schema 2>&1 | tee broker.log

kafka-proxy:
    docker run -d -p 19092:9092 apache/kafka:3.9.0

kafka39:
    docker run --rm -p 9092:9092 apache/kafka:3.9.0

kafka41:
    docker run --rm -p 9092:9092 apache/kafka:4.1.0

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

flamegraph *args:
    cargo flamegraph {{args}}

benchmark-flamegraph: build docker-compose-down minio-up minio-ready-local minio-local-alias minio-tansu-bucket prometheus-up grafana-up
	flamegraph -- target/debug/tansu broker 2>&1  | tee broker.log

benchmark: build docker-compose-down minio-up minio-ready-local minio-local-alias minio-tansu-bucket prometheus-up grafana-up
	target/debug/tansu broker 2>&1  | tee broker.log

otel: build docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-tansu-bucket prometheus-up grafana-up
	target/debug/tansu broker 2>&1  | tee broker.log

otel-up: docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-tansu-bucket prometheus-up grafana-up tansu-up

tansu-broker kind *args:
    target/{{kind}}/tansu broker {{args}} 2>&1 | tee broker.log

# run a debug broker with configuration from .env
broker *args: build docker-compose-down prometheus-up grafana-up db-up minio-up minio-ready-local minio-local-alias minio-tansu-bucket minio-lake-bucket lakehouse-catalog-up (tansu-broker "debug" args)

# run a release broker with configuration from .env
broker-release *args: release docker-compose-down prometheus-up grafana-up db-up minio-up minio-ready-local minio-local-alias minio-tansu-bucket minio-lake-bucket lakehouse-catalog-up (tansu-broker "release" args)


# run a proxy with configuration from .env
proxy *args:
    target/debug/tansu proxy {{args}} 2>&1 | tee proxy.log


# teardown compose, rebuild: minio, db, tansu and lake buckets
server: (cargo-build "--bin" "tansu") docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-tansu-bucket minio-lake-bucket lakehouse-catalog-up
	target/debug/tansu broker 2>&1  | tee broker.log

gdb: (cargo-build "--bin" "tansu") docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-tansu-bucket minio-lake-bucket
    rust-gdb --args target/debug/tansu broker

lldb: (cargo-build "--bin" "tansu") docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-tansu-bucket minio-lake-bucket lakehouse-catalog-up
    rust-lldb target/debug/tansu broker

ci: docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-tansu-bucket minio-lake-bucket lakehouse-catalog-up lakehouse-accept-terms-of-use lakehouse-create-warehouse

# produce etc/data/observations.json with schema etc/schema/observation.avsc
observation-produce: (cat-produce "observation" "etc/data/observations.json")

# consume observation topic with schema etc/schema/observation.avsc
observation-consume: (cat-consume "observation")

# create observation topic with schema etc/schema/observation.avsc
observation-topic-create: (topic-create "observation")

# observation parquet
observation-duckdb-parquet: (duckdb-k-unnest-v-parquet "observation")

duckdb *sql:
    duckdb -init duckdb-init.sql -markdown :memory: {{sql}}

# produce etc/data/trips.json with schema etc/schema/taxi.proto
taxi-topic-populate: (cat-produce "taxi" "etc/data/trips.json")

# consume taxi topic with schema etc/schema/taxi.proto
taxi-topic-consume: (cat-consume "taxi")

# create taxi topic with generated fields with schema etc/schema/taxi.proto
taxi-topic-create: (topic-create "taxi" "--partitions=1"  "--config=tansu.lake.normalize=true" "--config=tansu.lake.partition=meta.day" "--config=tansu.lake.z_order=vendor_id" "--config=tansu.lake.sink=true" "--config=tansu.batch=true" "--config=tansu.batch.max_records=200" "--config=tansu.batch.timeout_ms=1000")

# create taxi topic with schema etc/schema/taxi.proto
taxi-topic-create-plain: (topic-create "taxi" "--partitions" "1" "--config" "tansu.lake.sink=true")

# create taxi topic with a flattened schema etc/schema/taxi.proto
taxi-topic-create-normalize: (topic-create "taxi" "--partitions" "1" "--config" "tansu.lake.sink=true" "--config" "tansu.lake.normalize=true" "--config" "tansu.lake.normalize.separator=_" "--config" "tansu.lake.z_order=value_vendor_id")

taxi-topic-generator: (generator "taxi" "--broker=tcp://localhost:9092" "--per-second=1" "--producers=16" "--batch-size=1" "--duration-seconds=60")

# delete taxi topic
taxi-topic-delete: (topic-delete "taxi")

# taxi parquet
taxi-duckdb-parquet: (duckdb-parquet "taxi")

# taxi duckdb delta lake
taxi-duckdb-delta: (duckdb "\"select * from delta_scan('s3://lake/tansu.taxi');\"")

# create employee topic with etc/schema/employee.proto
employee-topic-create: (topic-create "employee")

# produce etc/data/persons.json with etc/schema/person.json
employee-produce: (cat-produce "employee" "etc/data/employees.json")

# employee duckdb delta lake
employee-duckdb-delta: (duckdb "\"select * from delta_scan('s3://lake/tansu.employee');\"")


# create customer topic with schema etc/schema/customer.proto
customer-topic-create *args: (topic-create "customer" "--partitions=1"  "--config=tansu.lake.normalize=true" "--config=tansu.lake.partition=meta.day" "--config=tansu.lake.sink=true" "--config=tansu.batch=true" "--config=tansu.batch.max_records=200" "--config=tansu.batch.timeout_ms=1000" args)

customer-topic-generator *args: (generator "customer" args)

customer-duckdb-delta: (duckdb "\"select * from delta_scan('s3://lake/tansu.customer');\"")

broker-null-debug:
    ./target/debug/tansu --storage-engine=null://sink 2>&1 | tee broker.log

broker-null profile="profiling":
    cargo build --profile {{profile}} --bin tansu
    ./target/{{profile}}/tansu --storage-engine=null://sink 2>&1 | tee broker.log

broker-sqlite profile="profiling":
    rm -f tansu.db*
    cargo build --profile {{profile}} --features libsql --bin tansu
    ./target/{{profile}}/tansu --storage-engine=sqlite://tansu.db 2>&1 | tee broker.log

samply-null profile="profiling":
    cargo build --profile {{profile}} --bin tansu
    RUST_LOG=warn samply record ./target/{{profile}}/tansu --storage-engine=null://sink

flamegraph-null-debug:
    cargo build --bin tansu
    RUST_LOG=warn flamegraph -- ./target/debug/tansu --storage-engine=null://sink

flamegraph-null profile="profiling":
    RUSTFLAGS="-C force-frame-pointers=yes" cargo build --profile {{profile}} --bin tansu
    RUST_LOG=warn flamegraph -- ./target/{{profile}}/tansu --storage-engine=null://sink

flamegraph-sqlite-debug:
    rm -f tansu.db*
    cargo build --bin tansu --features libsql
    RUST_LOG=warn flamegraph -- ./target/debug/tansu --storage-engine=sqlite://tansu.db

flamegraph-sqlite profile="profiling":
    rm -f tansu.db*
    RUSTFLAGS="-C force-frame-pointers=yes" cargo build --profile {{profile}} --features libsql --bin tansu
    RUST_LOG=warn flamegraph -- ./target/{{profile}}/tansu --storage-engine=sqlite://tansu.db 2>&1

samply-produce profile="profiling":
    RUSTFLAGS="-C force-frame-pointers=yes" cargo build --profile {{profile}} --bin bench_produce_v11
    RUST_LOG=warn samply record ./target/{{profile}}/bench_produce_v11

flamegraph-produce profile="profiling":
    RUSTFLAGS="-C force-frame-pointers=yes" cargo build --profile {{profile}} --bin bench_produce_v11
    RUST_LOG=warn flamegraph -- ./target/{{profile}}/bench_produce_v11

bench:
    cargo bench --profile profiling --all-features --package tansu-sans-io --quiet

producer-perf  throughput="1000" record_size="1024" num_records="100000":
    kafka-producer-perf-test --topic test --num-records {{num_records}} --record-size {{record_size}} --throughput {{throughput}} --producer-props bootstrap.servers=${ADVERTISED_LISTENER}

producer-perf-1000: (producer-perf "1000")

producer-perf-2000: (producer-perf "2000")

producer-perf-3000: (producer-perf "3000")

producer-perf-5000: (producer-perf "5000")

producer-perf-10000: (producer-perf "10000" "1024" "100000")

producer-perf-20000: (producer-perf "20000" "1024" "500000")

producer-perf-40000: (producer-perf "40000" "1024" "1000000")

producer-perf-45000: (producer-perf "45000" "1024" "1100000")

producer-perf-50000: (producer-perf "50000" "1024" "1250000")

producer-perf-100000: (producer-perf "100000" "1024" "2500000")

producer-perf-200000: (producer-perf "200000" "1024" "5000000")

producer-perf-500000: (producer-perf "500000" "1024" "12500000")
