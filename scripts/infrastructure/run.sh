#!/bin/bash
# run.sh
set -e

VENV_DIR=".venv"

if [[ $# -ne 2 || $1 == "--help" ]]; then
    echo "Usage: ./run <PUBLIC_KEY_NAME> <PRIVATE_KEY_PATH>"
    exit 1
fi
PUBLIC_KEY_NAME="$1"
PRIVATE_KEY_PATH="$2"

echo "Sourcing 'UPPMAX-openrc.sh.ignore'..."
source UPPMAX-openrc.sh.ignore

if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
    echo "Installing dependencies..."
    "$VENV_DIR/bin/pip" install \
        python-openstackclient \
        python-novaclient \
        python-keystoneclient
fi

echo "Running start_master_instance.py..."
"$VENV_DIR/bin/python" start_master_instance.py "$PUBLIC_KEY_NAME" "$PRIVATE_KEY_PATH"

echo "Running start_worker_instances.py..."
"$VENV_DIR/bin/python" start_worker_instances.py "$PRIVATE_KEY_PATH"
