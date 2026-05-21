#!/bin/bash
# teardown_swarm.sh
# Run from the master VM to fully tear down the stack and swarm.
# After this, setup_swarm.sh can be run again cleanly.
set -e

WORKERS="w1 w2 w3 w4"

# -------------------------------------------------------------------
# 1. Remove the stack (stops and removes all services + networks)
# -------------------------------------------------------------------
echo "Removing pulsar stack..."
if docker stack ls --format '{{.Name}}' | grep -q '^pulsar$'; then
    docker stack rm pulsar
    # Wait for all stack services/networks to be fully removed
    echo "Waiting for stack resources to be removed..."
    for attempt in $(seq 1 30); do
        remaining=$(docker service ls --filter label=com.docker.stack.namespace=pulsar -q 2>/dev/null)
        if [ -z "$remaining" ]; then
            echo "  All services removed."
            break
        fi
        echo "  Still removing... (attempt $attempt/30)"
        sleep 5
    done
else
    echo "  No 'pulsar' stack found, skipping."
fi

# -------------------------------------------------------------------
# 2. Leave swarm on each worker
# -------------------------------------------------------------------
echo "Removing workers from swarm..."
for worker in $WORKERS; do
    echo "  -> $worker"
    ssh "$worker" "docker swarm leave --force" 2>/dev/null && echo "    Left swarm." || echo "    Was not in swarm, skipping."
done

# -------------------------------------------------------------------
# 3. Tear down swarm on master (must come after workers leave)
# -------------------------------------------------------------------
echo "Tearing down swarm on master..."
docker swarm leave --force 2>/dev/null && echo "  Done." || echo "  Master was not in swarm, skipping."

echo ""
echo "==== Teardown complete. Ready for a fresh setup_swarm.sh run. ===="
