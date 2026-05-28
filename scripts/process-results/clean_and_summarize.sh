#!/bin/bash
if [[ $# -ne 1 || $1 == "--help" ]]; then
    echo "Usage: ./clean_and_summarize.sh <EXPERIMENTS_DIR>"
    exit 1
fi

EXPERIMENTS_DIR="$1"

for dir in "$EXPERIMENTS_DIR"/*/; do
    config="$dir/experiment_config.json"
    deployed_at=$(jq '.deployed_at' "$config")
    run_seconds=$(jq '.run_seconds' "$config")

    python3 results_summary.py \
        --file "$dir/timestamps_profiling.jsonl" \
        --deployed-at "$deployed_at" \
        --run-seconds "$run_seconds" | \
        jq -s '.[0] * .[1]' - "$config"
done
