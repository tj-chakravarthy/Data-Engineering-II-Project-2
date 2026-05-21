#!/bin/bash
# run.sh
set -e

VENV_DIR=".venv"
REPO_ARCHIVE="repo.tar.gz"
REMOTE_REPO_DIR="/home/ubuntu/app"
TARGET_PATH="${REMOTE_REPO_DIR}/scripts/infrastructure/"
LOCAL_REPO_ROOT="$(git rev-parse --show-toplevel)"
ROOT_ENV_FILE="${LOCAL_REPO_ROOT}/.env"
INFRA_ENV_FILE="${LOCAL_REPO_ROOT}/scripts/infrastructure/.env"
ENV_FILE="${ENV_FILE:-$ROOT_ENV_FILE}"

if [[ $# -ne 2 || $1 == "--help" ]]; then
    echo "Usage: ./run.sh <PUBLIC_KEY_NAME> <PRIVATE_KEY_PATH>"
    exit 1
fi

PUBLIC_KEY_NAME="$1"
PRIVATE_KEY_PATH="$2"

# fixed by TJ: support either repo-root .env or the course infra .env file,
# because git archive skips ad-hoc local env files but this project tracks one.
if [ ! -f "$ENV_FILE" ] && [ -f "$INFRA_ENV_FILE" ]; then
    ENV_FILE="$INFRA_ENV_FILE"
fi

if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: missing runtime .env"
    echo "Checked ${ROOT_ENV_FILE} and ${INFRA_ENV_FILE}."
    echo "Create it from .env.example and set GitHub/Pulsar values before deploying."
    exit 1
fi

echo "Sourcing 'UPPMAX-openrc.sh.ignore'..."
source UPPMAX-openrc.sh.ignore

# --- Python venv + deps ---
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
    echo "Installing dependencies..."
    "$VENV_DIR/bin/pip" install \
        python-openstackclient \
        python-novaclient \
        python-keystoneclient
fi

# --- Provision master ---
echo "Running start_master_instance.py..."
"$VENV_DIR/bin/python" start_master_instance.py "$PUBLIC_KEY_NAME" "$PRIVATE_KEY_PATH"

# --- Provision workers ---
echo "Running start_worker_instances.py..."
"$VENV_DIR/bin/python" start_worker_instances.py "$PRIVATE_KEY_PATH"

# --- Read master IP written by start_master_instance.py ---
MASTER_IP=$(grep '^MASTER_IP=' master_info.txt | cut -d'=' -f2)
if [ -z "$MASTER_IP" ]; then
    echo "ERROR: could not read MASTER_IP from master_info.txt"
    exit 1
fi

# --- Package repo ---
echo "Packaging repo with git archive..."
pushd "$LOCAL_REPO_ROOT" > /dev/null
git archive --format=tar.gz --output="${OLDPWD}/${REPO_ARCHIVE}" HEAD
popd > /dev/null
echo "  Created ${REPO_ARCHIVE}"

# --- Copy archive to master ---
echo "Copying repo to master (ubuntu@${MASTER_IP})..."
scp -i "$PRIVATE_KEY_PATH" \
    -o StrictHostKeyChecking=accept-new \
    "$REPO_ARCHIVE" "ubuntu@${MASTER_IP}:/home/ubuntu/"
echo "  Done."

# --- Unpack repo on master ---
echo "Unpacking repo on master..."
ssh -i "$PRIVATE_KEY_PATH" \
    -o StrictHostKeyChecking=accept-new \
    "ubuntu@${MASTER_IP}" \
    "mkdir -p ${REMOTE_REPO_DIR} && tar -xzf /home/ubuntu/${REPO_ARCHIVE} -C ${REMOTE_REPO_DIR} && rm /home/ubuntu/${REPO_ARCHIVE}"
echo "  Unpacked to ${REMOTE_REPO_DIR}"

# --- Copy runtime environment to master ---
echo "Copying .env to master..."
scp -i "$PRIVATE_KEY_PATH" \
    -o StrictHostKeyChecking=accept-new \
    "$ENV_FILE" "ubuntu@${MASTER_IP}:${REMOTE_REPO_DIR}/.env"
echo "  Copied .env."

# Clean up local archive
rm -f "$REPO_ARCHIVE"

# --- Run swarm setup ---
echo "Running 'setup_swarm.sh'..."
ssh -i "$PRIVATE_KEY_PATH" \
    -o StrictHostKeyChecking=accept-new \
    "ubuntu@${MASTER_IP}" \
    "${TARGET_PATH}/setup_swarm.sh"
echo "  Ran ${TARGET_PATH}/setup_swarm.sh"
