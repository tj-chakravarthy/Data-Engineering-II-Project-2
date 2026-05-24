#!/bin/bash
set -e

# If $1 is provided, use it. Otherwise, default to 'latest'.
TAG=${1:-latest}
DOCKERHUB_URL="andreashadjoullis1153/pulsar_client"

# Build context is this script's directory (src/); the Dockerfile COPY paths
# are relative to it. Resolve it so the script works from any working dir.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Building image with tag: $TAG..."
docker build -f "${SCRIPT_DIR}/Dockerfile" -t "${DOCKERHUB_URL}:${TAG}" "${SCRIPT_DIR}"

echo "Logging into Docker Hub..."
docker login

echo "Pushing image to registry..."
docker push "${DOCKERHUB_URL}:${TAG}"

echo "Done! Successfully built and pushed ${DOCKERHUB_URL}:${TAG}"
