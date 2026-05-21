#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# fixed by TJ: build from the repo root so src/Dockerfile can copy
# requirements.txt instead of duplicating dependency versions.
docker build -f "${REPO_ROOT}/src/Dockerfile" -t andreashadjoullis1153/crawler:latest "${REPO_ROOT}"
docker login
docker push andreashadjoullis1153/crawler:latest
