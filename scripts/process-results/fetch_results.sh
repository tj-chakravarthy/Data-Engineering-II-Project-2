#!/bin/bash
LOCAL_DIR="/home/ubuntu/app/data"
APP_DIR="/home/ubuntu/app"
REMOTE_DIR="/home/ubuntu/app/data"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOCAL_RESULTS_DIR="${LOCAL_DIR}/results/results_${TIMESTAMP}"

echo "Finding aggregator node..."
WORKER_NAME="$(docker service ps pulsar_analytics-aggregator \
  --filter "desired-state=running" \
  --format "{{.Node}}" | head -1)"

if [ -z "${WORKER_NAME}" ]; then
  echo "ERROR: could not find running aggregator container"
  exit 1
fi

if [[ "${WORKER_NAME}" == "group16-master" ]]; then
  echo "Results already at master."
  echo "Exiting..."
  exit
fi

echo "Aggregator is on ${WORKER_NAME} (${WORKER_IP})"

echo "Fetching results into ${LOCAL_RESULTS_DIR}..."
mkdir -p "${LOCAL_RESULTS_DIR}"

scp "${WORKER_NAME}:${REMOTE_DIR}/results/*" "${LOCAL_RESULTS_DIR}/"

echo "Done. Results are at ${LOCAL_RESULTS_DIR}"
