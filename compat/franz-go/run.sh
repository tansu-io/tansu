#!/usr/bin/env bash
#
# Run the franz-go (pkg/kgo) integration test suite against a running
# tansu broker.
#
# The broker must be reachable at ${BOOTSTRAP_SERVERS} (default
# 127.0.0.1:9092):
#
#   tansu broker --storage-engine=memory:// \
#       --advertised-listener-url=tcp://127.0.0.1:9092
#
# Only tests listed in tests.allow are run; FINDINGS.md records the
# compatibility gaps behind the excluded tests. Grow the allowlist as
# broker compatibility improves.
#
# KGO_TEST_RF=1 is forced: tansu is a single-node broker and the suite
# creates topics with replication factor 3 by default.

set -euo pipefail

FRANZ_GO_VERSION="${FRANZ_GO_VERSION:-v1.21.3}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-127.0.0.1:9092}"
COMPAT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="${WORK_DIR:-${COMPAT_DIR}/../../target/compat}"
SRC_DIR="${WORK_DIR}/franz-go"

mkdir -p "${WORK_DIR}"

if [[ ! -d "${SRC_DIR}" ]]; then
    git clone --depth 1 --branch "${FRANZ_GO_VERSION}" \
        https://github.com/twmb/franz-go.git "${SRC_DIR}"
fi

for _ in $(seq 1 100); do
    if (exec 3<> "/dev/tcp/${BOOTSTRAP_SERVERS%:*}/${BOOTSTRAP_SERVERS##*:}") \
           2> /dev/null; then
        break
    fi
    sleep 0.1
done

tests=$(grep -Ev '^[[:space:]]*(#|$)' "${COMPAT_DIR}/tests.allow" |
            awk '{print $1}' | paste -s -d '|' -)

count=$(grep -Ev '^[[:space:]]*(#|$)' "${COMPAT_DIR}/tests.allow" | wc -l)
echo "running ${count// /} franz-go test(s) against ${BOOTSTRAP_SERVERS}"

cd "${SRC_DIR}/pkg/kgo"
KGO_SEEDS="${BOOTSTRAP_SERVERS}" \
KGO_TEST_RF=1 \
KGO_TEST_RECORDS="${KGO_TEST_RECORDS:-10000}" \
KGO_LOG_LEVEL="${KGO_LOG_LEVEL:-none}" \
    go test -count=1 -timeout 600s -run "^(${tests})\$" .
