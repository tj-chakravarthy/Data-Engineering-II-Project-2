#!/bin/bash
# setup_swarm.sh
# Run once from the master VM after all VMs are provisioned.
# Assumes: cluster-stack.yml is at /home/ubuntu/cluster-stack.yml repo is
# unpacked at /home/ubuntu/app
set -e

WORKERS="w1 w2 w3 w4"
REMOTE_REPO_DIR="/home/ubuntu/app"
STACK_FILE="/home/ubuntu/cluster-stack.yml"
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
    # Polls until all replicas of a service are running.
    # Usage: wait_for_service_replicas <service_name> <expected_replicas>
    local service=$1
    local expected=$2
    echo "Waiting for service '$service' to reach $expected/$expected replicas..."
    for attempt in $(seq 1 60); do
        running=$(docker service ls --filter "name=${service}" --format '{{.Replicas}}' 2>/dev/null | head -1)
        if [ "$running" = "${expected}/${expected}" ]; then
            echo "  $service is up ($running)."
            return 0
        fi
        echo "  Attempt $attempt/60: $running — retrying in 10s..."
        sleep 10
    done
    echo "ERROR: service '$service' did not reach $expected replicas in time."
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
# 2. Copy repo to workers (master already has it at $REMOTE_REPO_DIR)
# -------------------------------------------------------------------
echo "Copying repo to workers..."
for worker in $WORKERS; do
    echo "  -> $worker"
    # Create a fresh tar from the already-unpacked dir and pipe it directly
    tar -czf - -C "$REMOTE_REPO_DIR" . \
        | ssh "$worker" "mkdir -p ${REMOTE_REPO_DIR} && tar -xzf - -C ${REMOTE_REPO_DIR}"
done
echo "  Done."

# -------------------------------------------------------------------
# 3. Init Docker Swarm on master
# -------------------------------------------------------------------
echo "Initializing Docker Swarm on master..."
# Guard: if already a swarm manager, skip
if docker info --format '{{.Swarm.LocalNodeState}}' 2>/dev/null | grep -q 'active'; then
    echo "  Already in a swarm, skipping init."
else
    docker swarm init
    echo "  Swarm initialized."
fi

JOIN_TOKEN=$(docker swarm join-token worker -q)
MASTER_IP=$(hostname -I | awk '{print $1}')

# -------------------------------------------------------------------
# 4. Join workers
# -------------------------------------------------------------------
for worker in $WORKERS; do
    echo "Joining $worker to swarm..."
    # Skip if already joined
    if ssh "$worker" "docker info --format '{{.Swarm.LocalNodeState}}'" 2>/dev/null | grep -q 'active'; then
        echo "  $worker already in swarm, skipping."
    else
        ssh "$worker" "docker swarm join --token $JOIN_TOKEN $MASTER_IP:2377"
        echo "  $worker joined."
    fi
done

# -------------------------------------------------------------------
# 5. Label nodes so placement constraints work
#    cluster-stack.yml uses:
#      node.role == manager  -> zookeeper, broker
#      node.role == worker   -> bookie (x4)
# -------------------------------------------------------------------
echo "Verifying node labels..."
docker node ls

# -------------------------------------------------------------------
# 6. Deploy stack in dependency order
#    Swarm ignores depends_on at runtime, so we deploy services one
#    at a time and wait for each to be healthy before moving on.
# -------------------------------------------------------------------
echo "Deploying pulsar stack (step by step)..."

# Step 1: ZooKeeper
docker stack deploy --detach=true -c "$STACK_FILE" pulsar
# docker stack deploy creates all services at once; we gate progress manually.

echo ""
echo "Waiting for ZooKeeper to be healthy before proceeding..."
wait_for_service_replicas "pulsar_zookeeper" 1

# Step 2: ZooKeeper is up, now verify pulsar-init ran (it's replicas: 1, restart: none)
echo "Waiting for pulsar-init to complete..."
for attempt in $(seq 1 30); do
    state=$(docker service ps pulsar_pulsar-init --format '{{.CurrentState}}' 2>/dev/null | head -1)
    if echo "$state" | grep -qi "complete"; then
        echo "  pulsar-init completed."
        break
    fi
    if echo "$state" | grep -qi "failed"; then
        echo "ERROR: pulsar-init failed. Logs:"
        docker service logs pulsar_pulsar-init --tail 50 || true
        exit 1
    fi
    echo "  State: '$state' — waiting 10s... (attempt $attempt/30)"
    sleep 10
done

# Step 3: Bookies
echo "Waiting for all 4 bookies..."
wait_for_service_replicas "pulsar_bookie" 4

# Step 4: Broker
echo "Waiting for broker..."
wait_for_service_replicas "pulsar_broker" 1

# -------------------------------------------------------------------
# 7. Summary
# -------------------------------------------------------------------
echo ""
echo "==== Deployment complete ===="
echo ""
echo "Node status:"
docker node ls
echo ""
echo "Service status:"
docker service ls
echo ""
echo "Pulsar broker:  pulsar://$(hostname -I | awk '{print $1}'):6650"
echo "Admin API:      http://$(hostname -I | awk '{print $1}'):8080"
echo ""
echo "Test connectivity:"
echo "  docker run --rm --network host apachepulsar/pulsar:4.0.10 \\"
echo "    bin/pulsar-admin --admin-url http://localhost:8080 brokers list pulsar-cluster"
