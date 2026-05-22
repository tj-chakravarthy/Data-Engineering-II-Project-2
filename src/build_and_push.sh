#!/bin/bash

# If $1 is provided, use it. Otherwise, default to 'latest'.
TAG=${1:-latest}

echo "Building image with tag: $TAG..."
docker build -t "andreashadjoullis1153/crawler:$TAG" .

echo "Logging into Docker Hub..."
docker login

echo "Pushing image to registry..."
docker push "andreashadjoullis1153/crawler:$TAG"

echo "Done! Successfully built and pushed andreashadjoullis1153/crawler:$TAG"
