#!/bin/bash
# run_experiment.sh

REMOTE_REPO_DIR="/home/ubuntu/app"
INFRA_DIR="${REMOTE_REPO_DIR}/scripts/infrastructure"
ENV_FILE="${INFRA_DIR}/.env"
STACK_FILE="${INFRA_DIR}/cluster-stack.yml"
STACK_NAME="pulsar"

WORKERS=("group16-worker-1" "group16-worker-2" "group16-worker-3" "group16-worker-4")

EXPERIMENT_ID="${EXPERIMENT_ID:-manual}"
RUN_SECONDS="${RUN_SECONDS:-600}" # 10 min

# Use TMP prefix to avoid .env overriding our values when sourcing
TMP_ANALYTICS_NUM_RUNNERS="${ANALYTICS_NUM_RUNNERS:-1}"
TMP_NUM_WORKERS="${NUM_WORKERS:-1}"
TMP_TOKEN_COUNT="${TOKEN_COUNT:-5}"
TMP_FLUSH_EVERY="${FLUSH_EVERY:-20}"

RESULTS_DIR="${REMOTE_REPO_DIR}/data/results"
FIGURES_DIR="${REMOTE_REPO_DIR}/data/figures"
OUTPUT_DIR="${REMOTE_REPO_DIR}/data/output"
EXPERIMENTS_DIR="${RESULTS_DIR}/experiments/${EXPERIMENT_ID}"

MASTER_IP=$(hostname -I | awk '{print $1}')

if [ -z "$MASTER_IP" ]; then
    echo "ERROR: could not determine master IP"
    exit 1
fi

echo "==== Starting experiment: ${EXPERIMENT_ID} ===="
echo "RUN_SECONDS=${RUN_SECONDS}"
echo "ANALYTICS_NUM_RUNNERS=${ANALYTICS_NUM_RUNNERS}"
echo "NUM_WORKERS=${NUM_WORKERS}"
echo "TOKEN_COUNT=${TOKEN_COUNT}"
echo "FLUSH_EVERY=${FLUSH_EVERY}"
echo "PARTITION_TOKENS=${PARTITION_TOKENS}"

# ------------------------------------------------------------
# 1. Remove previous stack only, not the whole swarm.
# ------------------------------------------------------------
echo "Removing previous stack if it exists..."
docker stack rm "$STACK_NAME" >/dev/null 2>&1 || true

for attempt in $(seq 1 60); do
    remaining=$(docker service ls --filter label=com.docker.stack.namespace="$STACK_NAME" -q 2>/dev/null)
    if [ -z "$remaining" ]; then
        echo "  Previous stack removed."
        break
    fi

    echo "  Waiting for previous stack to disappear... attempt $attempt/60"
    sleep 5
done

# ------------------------------------------------------------
# 2. Select active analytics workers using labels.
# ------------------------------------------------------------
echo "Selecting ${NUM_WORKERS} active analytics worker(s)..."

index=1
for worker in "${WORKERS[@]}"; do
    node_name=$(ssh "$worker" hostname)

    if [ "$index" -le "$NUM_WORKERS" ]; then
        docker node update --label-add analytics_worker=true "$node_name" >/dev/null
        echo "  ACTIVE:   $worker ($node_name)"
    else
        docker node update --label-add analytics_worker=false "$node_name" >/dev/null
        echo "  INACTIVE: $worker ($node_name)"
    fi

    index=$((index + 1))
done

# ------------------------------------------------------------
# 3. Clear state for clean experiment.
# ------------------------------------------------------------
echo "Clearing old results and checkpoints..."
mkdir -p "$RESULTS_DIR" "$FIGURES_DIR" "$OUTPUT_DIR" "$EXPERIMENTS_DIR"

rm -rf "${RESULTS_DIR:?}/"*
rm -rf "${FIGURES_DIR:?}/"*
rm -f "${OUTPUT_DIR}/repos.published.json"

mkdir -p "$EXPERIMENTS_DIR"

# ------------------------------------------------------------
# 4. Load .env, then override values for this experiment.
# ------------------------------------------------------------
set -a
source "$ENV_FILE"
set +a

export PULSAR_SERVICE_URL="pulsar://${MASTER_IP}:6650"
export PULSAR_ADMIN_URL="http://${MASTER_IP}:8080"

export ANALYTICS_NUM_RUNNERS="$TMP_ANALYTICS_NUM_RUNNERS"
export NUM_WORKERS="$TMP_NUM_WORKERS"
export FLUSH_EVERY="$TMP_FLUSH_EVERY"
export TOKEN_COUNT="$TMP_TOKEN_COUNT"

# Use unique topics/subscriptions per run to avoid old Pulsar cursors/backlog.
export PULSAR_TOPIC="repos.raw.${EXPERIMENT_ID}"
export ENRICHED_TOPIC="repos.enriched.${EXPERIMENT_ID}"
export ANALYTICS_SUBSCRIPTION="analytics.${EXPERIMENT_ID}"
export AGGREGATOR_SUBSCRIPTION="aggregator.${EXPERIMENT_ID}"

# Disable extra tokens when TOKEN_COUNT is smaller than available tokens.
for i in 1 2 3 4 5; do
    var="GITHUB_TOKEN_${i}"

    if [ "$i" -gt "$TOKEN_COUNT" ]; then
        export "$var="
    fi
done

# ------------------------------------------------------------
# 5. Deploy stack.
# ------------------------------------------------------------
echo "Deploying stack..."
DEPLOY_STARTED_AT=$(date +%s)
docker stack deploy --detach=true -c "$STACK_FILE" "$STACK_NAME"

# ------------------------------------------------------------
# 6. Wait for services.
# ------------------------------------------------------------
wait_for_replicas() {
    local service="$1"
    local expected="$2"

    echo "Waiting for $service to reach $expected/$expected..."

    for attempt in $(seq 1 60); do
        replicas=$(docker service ls --filter "name=${service}" --format '{{.Replicas}}' | head -1)

        if [ "$replicas" = "${expected}/${expected}" ]; then
            echo "  $service is ready."
            return 0
        fi

        echo "  attempt $attempt/60: $replicas"
        sleep 5
    done

    echo "ERROR: $service did not reach $expected replicas."
    docker service ps "$service" --no-trunc || true
    docker service logs "$service" --tail 50 || true
    exit 1
}

wait_for_replicas "${STACK_NAME}_pulsar" 1
wait_for_replicas "${STACK_NAME}_analytics" "$ANALYTICS_NUM_RUNNERS"
wait_for_replicas "${STACK_NAME}_analytics-aggregator" 1

# ------------------------------------------------------------
# 7. Run for fixed time.
# ------------------------------------------------------------
echo "Running experiment for ${RUN_SECONDS} seconds..."
sleep "$RUN_SECONDS"

# ------------------------------------------------------------
# 8. Stop producers first, then consumers, then whole stack.
# ------------------------------------------------------------
echo "Stopping crawler first..."
docker service scale "${STACK_NAME}_crawler=0" || true

echo "Giving runners time to flush and acknowledge pending work..."
sleep "${DRAIN_SECONDS:-30}"

# fetch results before shutting down the stack
"${REMOTE_REPO_DIR}/scripts/process-results/fetch_results.sh"

echo "Removing stack..."
docker stack rm "$STACK_NAME"

echo "Waiting for old stack services and networks to disappear..."
for attempt in $(seq 1 60); do
    remaining_services=$(docker service ls \
        --filter label=com.docker.stack.namespace=pulsar \
        -q 2>/dev/null || true)

    remaining_networks=$(docker network ls \
        --filter label=com.docker.stack.namespace=pulsar \
        -q 2>/dev/null || true)

    if [ -z "$remaining_services" ] && [ -z "$remaining_networks" ]; then
        echo "Old stack removed."
        break
    fi

    echo "Still removing old stack... attempt $attempt/60"
    sleep 5
done


# ------------------------------------------------------------
# 9. Save experiment results.
# ------------------------------------------------------------
echo "Saving experiment results..."
mkdir -p "$EXPERIMENTS_DIR"

cp -f "${RESULTS_DIR}/*/timestamps_profiling.jsonl" "$EXPERIMENTS_DIR/" 2>/dev/null || true
cp -f "${RESULTS_DIR}/*/results_history.jsonl" "$EXPERIMENTS_DIR/" 2>/dev/null || true
cp -f "${RESULTS_DIR}/*/all_results.json" "$EXPERIMENTS_DIR/" 2>/dev/null || true
cp -f "${RESULTS_DIR}/*/analytics_state.json" "$EXPERIMENTS_DIR/" 2>/dev/null || true

cat > "${EXPERIMENTS_DIR}/experiment_config.json" <<EOF
{
  "run_id": "${EXPERIMENT_ID}",
  "run_seconds": ${RUN_SECONDS},
  "analytics_num_runners": ${ANALYTICS_NUM_RUNNERS},
  "num_workers": ${NUM_WORKERS},
  "token_count": ${TOKEN_COUNT},
  "flush_every": ${FLUSH_EVERY},
  "partition_tokens": "${PARTITION_TOKENS}",
  "pulsar_topic": "${PULSAR_TOPIC}",
  "enriched_topic": "${ENRICHED_TOPIC}",
  "analytics_subscription": "${ANALYTICS_SUBSCRIPTION}",
  "aggregator_subscription": "${AGGREGATOR_SUBSCRIPTION}",
  "deployed_at": ${DEPLOY_STARTED_AT}
}
EOF

echo "Saved results to ${EXPERIMENTS_DIR}"
echo "==== Experiment ${EXPERIMENT_ID} complete ===="
