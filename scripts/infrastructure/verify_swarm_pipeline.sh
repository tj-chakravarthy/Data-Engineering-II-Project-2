#!/bin/bash
# verify_swarm_pipeline.sh
# Run from the Swarm manager after the Pulsar stack is deployed.
set -e

if [[ $# -ne 2 || $1 == "--help" ]]; then
    echo "Usage: ./verify_swarm_pipeline.sh <DEPLOY_STARTED_AT> <AGGREGATOR_HOST>"
    exit 1
fi
DEPLOY_STARTED_AT="$1"
AGGREGATOR_NODE="$2"

REMOTE_REPO_DIR="/home/ubuntu/app"
TARGET_PATH="${REMOTE_REPO_DIR}/scripts/infrastructure"
ENV_FILE="${TARGET_PATH}/.env"
RESULTS_DIR="${REMOTE_REPO_DIR}/data/results"
SMOKE_ATTEMPTS=${SMOKE_ATTEMPTS:-30}
SMOKE_DELAY_SECONDS=${SMOKE_DELAY_SECONDS:-15}
SMOKE_MIN_RAW_MESSAGES=${SMOKE_MIN_RAW_MESSAGES:-1}

env_value() {
    local key=$1
    if [ ! -f "$ENV_FILE" ]; then
        return 0
    fi
    grep -E "^${key}=" "$ENV_FILE" | tail -1 | cut -d'=' -f2-
}

PULSAR_TOPIC=${PULSAR_TOPIC:-$(env_value PULSAR_TOPIC)}
PULSAR_TOPIC=${PULSAR_TOPIC:-repos.raw}

topic_path() {
    if [[ "$PULSAR_TOPIC" == persistent://* ]]; then
        echo "$PULSAR_TOPIC"
    else
        echo "persistent://public/default/${PULSAR_TOPIC}"
    fi
}

pulsar_container_id() {
    docker ps \
        --filter label=com.docker.swarm.service.name=pulsar_pulsar \
        --format '{{.ID}}' \
        | head -1
}

raw_topic_message_count() {
    local container=$1
    local topic=$2
    local stats
    stats=$(docker exec "$container" bin/pulsar-admin topics stats "$topic" 2>/dev/null || true)
    if [ -z "$stats" ]; then
        echo 0
        return
    fi
    printf '%s' "$stats" | python3 -c 'import json, sys; print(int(json.load(sys.stdin).get("msgInCounter", 0)))' 2>/dev/null || echo 0
}

print_debug_logs() {
    echo ""
    echo "==== crawler logs ===="
    docker service logs pulsar_crawler --tail 50 || true
    echo ""
    echo "==== analytics logs ===="
    docker service logs pulsar_analytics --tail 50 || true
    # fixed by TJ tail the aggregator too so failures show its diagnostics
    echo ""
    echo "==== analytics-aggregator logs ===="
    docker service logs pulsar_analytics-aggregator --tail 50 || true
    echo ""
    echo "==== service tasks ===="
    docker service ps pulsar_crawler --no-trunc || true
    docker service ps pulsar_analytics --no-trunc || true
    docker service ps pulsar_analytics-aggregator --no-trunc || true
}

wait_for_raw_messages() {
    local container=$1
    local topic=$2
    echo "Smoke check: waiting for at least ${SMOKE_MIN_RAW_MESSAGES} message(s) on ${topic}..."
    for attempt in $(seq 1 "$SMOKE_ATTEMPTS"); do
        count=$(raw_topic_message_count "$container" "$topic")
        if [ "$count" -ge "$SMOKE_MIN_RAW_MESSAGES" ]; then
            echo "  Raw topic has ${count} message(s)."
            return 0
        fi
        echo "  Attempt $attempt/$SMOKE_ATTEMPTS: raw message count=${count}; retrying in ${SMOKE_DELAY_SECONDS}s..."
        sleep "$SMOKE_DELAY_SECONDS"
    done
    echo "ERROR: raw topic did not receive enough messages."
    print_debug_logs
    exit 1
}

wait_for_aggregator_results() {
    local host=$1
    echo "Smoke check: waiting for aggregator results on ${host}:${RESULTS_DIR}..."
    for attempt in $(seq 1 "$SMOKE_ATTEMPTS"); do
        if ssh "$host" "test -s '${RESULTS_DIR}/all_results.json' && [ \$(stat -c %Y '${RESULTS_DIR}/all_results.json') -ge ${DEPLOY_STARTED_AT} ]"; then
            echo "  Aggregator results exist:"
            ssh "$host" "ls -lh '${RESULTS_DIR}'"
            return 0
        fi
        echo "  Attempt $attempt/$SMOKE_ATTEMPTS: current-run all_results.json not ready; retrying in ${SMOKE_DELAY_SECONDS}s..."
        sleep "$SMOKE_DELAY_SECONDS"
    done
    echo "ERROR: Aggregator did not produce a current-run ${RESULTS_DIR}/all_results.json."
    print_debug_logs
    exit 1
}

echo "Verifying deployed Swarm pipeline..."
PULSAR_CONTAINER=$(pulsar_container_id)
if [ -z "$PULSAR_CONTAINER" ]; then
    echo "ERROR: could not find pulsar_pulsar container."
    docker service ps pulsar_pulsar --no-trunc || true
    exit 1
fi

if ! docker node ls --filter label=aggregator=true --format '{{.Hostname}}' | grep -q .; then
    echo "ERROR: no Swarm node has aggregator=true label."
    docker node ls
    exit 1
fi

TOPIC=$(topic_path)
wait_for_raw_messages "$PULSAR_CONTAINER" "$TOPIC"
wait_for_aggregator_results "$AGGREGATOR_NODE"

echo "Swarm pipeline verification passed."
