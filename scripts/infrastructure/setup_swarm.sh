#!/bin/bash
# setup_swarm.sh
# Run once from the master VM after all VMs are provisioned.
# Assumes: repo is unpacked at /home/ubuntu/app
set -e

WORKERS="w1 w2 w3 w4"
REMOTE_REPO_DIR="/home/ubuntu/app"
TARGET_PATH="${REMOTE_REPO_DIR}/scripts/infrastructure"
ENV_FILE="${TARGET_PATH}/.env"
STACK_FILE="${TARGET_PATH}/cluster-stack.yml"
MAX_ATTEMPTS=30

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

wait_for_cloud_init() {
    local host=$1
    echo "Waiting for cloud-init on $host..."
    for attempt in $(seq 1 $MAX_ATTEMPTS); do
        if ssh "$host" 'cloud-init status --wait' > /dev/null 2>&1; then
            echo "  $host ready."
            return 0
        fi
        echo "  Attempt $attempt/$MAX_ATTEMPTS failed, retrying in 10s..."
        sleep 10
    done
    echo "ERROR: cloud-init did not finish on $host within retry limit."
    exit 1
}

wait_for_service_replicas() {
    local service=$1
    local expected=$2
    echo "Waiting for service '$service' to reach $expected/$expected replicas..."
    for attempt in $(seq 1 60); do
        running=$(docker service ls --filter "name=${service}" --format '{{.Replicas}}' 2>/dev/null | head -1)
        if [ "$running" = "${expected}/${expected}" ]; then
            echo "  $service is up ($running)."
            return 0
        fi
        echo "  Attempt $attempt/60: $running - retrying in 10s..."
        sleep 10
    done
    echo "ERROR: service '$service' did not reach $expected replicas in time."
    docker service logs "$service" --tail 30 || true
    exit 1
}

wait_for_service_running_or_completed() {
    local service=$1
    local expected=$2
    echo "Waiting for service '$service' to run or complete successfully..."
    for attempt in $(seq 1 60); do
        running=$(docker service ls --filter "name=${service}" --format '{{.Replicas}}' 2>/dev/null | head -1)
        if [ "$running" = "${expected}/${expected}" ]; then
            echo "  $service is running ($running)."
            return 0
        fi

        completed=$(docker service ps "$service" --no-trunc --format '{{.CurrentState}}' 2>/dev/null | grep -c '^Complete ' || true)
        if [ "$completed" -ge "$expected" ]; then
            echo "  $service completed successfully."
            return 0
        fi

        echo "  Attempt $attempt/60: replicas=$running completed_tasks=$completed - retrying in 10s..."
        sleep 10
    done
    echo "ERROR: service '$service' did not run or complete successfully in time."
    docker service ps "$service" --no-trunc || true
    docker service logs "$service" --tail 30 || true
    exit 1
}

# -------------------------------------------------------------------
# 1. Wait for all workers
# -------------------------------------------------------------------
for worker in $WORKERS; do
    wait_for_cloud_init "$worker"
done

# -------------------------------------------------------------------
# 2. Copy repo to workers
# -------------------------------------------------------------------
echo "Copying repo to workers..."
for worker in $WORKERS; do
    echo "  -> $worker"
    tar -czf - -C "$REMOTE_REPO_DIR" . \
        | ssh "$worker" "mkdir -p ${REMOTE_REPO_DIR} && tar -xzf - -C ${REMOTE_REPO_DIR}"
done
echo "  Done."

# The crawler and analytics services keep cache/checkpoint/result files under
# /app/data, so create the bind-mount source on every possible service node
# before Swarm starts containers.
mkdir -p \
    "${REMOTE_REPO_DIR}/data/cache" \
    "${REMOTE_REPO_DIR}/data/output" \
    "${REMOTE_REPO_DIR}/data/results" \
    "${REMOTE_REPO_DIR}/data/figures"
for worker in $WORKERS; do
    ssh "$worker" "mkdir -p \
        ${REMOTE_REPO_DIR}/data/cache \
        ${REMOTE_REPO_DIR}/data/output \
        ${REMOTE_REPO_DIR}/data/results \
        ${REMOTE_REPO_DIR}/data/figures"
done

# -------------------------------------------------------------------
# 3. Init Docker Swarm on master
# -------------------------------------------------------------------
echo "Initializing Docker Swarm on master..."
docker swarm init
echo "  Swarm initialized."

JOIN_TOKEN=$(docker swarm join-token worker -q)
MASTER_IP=$(hostname -I | awk '{print $1}')

# -------------------------------------------------------------------
# 4. Join workers
# -------------------------------------------------------------------
for worker in $WORKERS; do
    echo "Joining $worker to swarm..."
    ssh "$worker" "docker swarm join --token $JOIN_TOKEN $MASTER_IP:2377"
    echo "  $worker joined."
done

ANALYTICS_SSH_HOST=$(echo "$WORKERS" | awk '{print $1}')
ANALYTICS_NODE=$(ssh "$ANALYTICS_SSH_HOST" hostname)
if [ -z "$ANALYTICS_NODE" ]; then
    echo "ERROR: no Swarm worker node found for analytics placement."
    exit 1
fi
echo "Pinning analytics service to worker '$ANALYTICS_SSH_HOST' (node '$ANALYTICS_NODE')..."
docker node update --label-add analytics=true "$ANALYTICS_NODE"
echo "  Analytics node labeled."

# -------------------------------------------------------------------
# 5. Deploy stack and wait for Pulsar standalone to be ready
# -------------------------------------------------------------------
echo "Creating '/home/ubuntu/data' directory..."
mkdir -p /home/ubuntu/data
echo "  Done"
echo "Deploying pulsar stack..."
(
    # apply side-effect only within subshell
    export $(grep -v '^#' "$ENV_FILE" | xargs)
    export PULSAR_SERVICE_URL="pulsar://$MASTER_IP:6650"
    docker stack deploy --detach=true -c "$STACK_FILE" pulsar
)

wait_for_service_replicas "pulsar_pulsar" 1
wait_for_service_running_or_completed "pulsar_crawler" 1
wait_for_service_replicas "pulsar_analytics" 1
bash "${TARGET_PATH}/smoke_check.sh"

# -------------------------------------------------------------------
# 6. Summary
# -------------------------------------------------------------------
echo ""
echo "==== Deployment complete ===="
echo ""
docker node ls
echo ""
docker service ls
echo ""
echo "Pulsar broker:  pulsar://${MASTER_IP}:6650"
echo "Admin API:      http://${MASTER_IP}:8080"
