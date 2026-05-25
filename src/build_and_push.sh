#!/bin/bash

# Define SCRIPT_DIR (points to the directory where this script resides)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CURRENT_BRANCH=$(git branch --show-current)
TAG=$(echo "$CURRENT_BRANCH" | tr '/' '-')
DOCKERHUB_URL="andreashadjoullis1153/pulsar_client"

echo "Building image using context: ${SCRIPT_DIR}"
# Build the primary branch tag
docker build -f "${SCRIPT_DIR}/Dockerfile" -t "${DOCKERHUB_URL}:${TAG}" "${SCRIPT_DIR}"

# If the branch is main, additionally tag it as 'latest'
if [ "$TAG" = "main" ]; then
    echo "Main branch detected. Also tagging as 'latest'..."
    docker tag "${DOCKERHUB_URL}:${TAG}" "${DOCKERHUB_URL}:latest"
    docker push "${DOCKERHUB_URL}:latest"
fi

echo "Pushing primary branch image..."
docker push "${DOCKERHUB_URL}:${TAG}"
