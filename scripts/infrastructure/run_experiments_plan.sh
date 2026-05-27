#!/usr/bin/env bash
# run_experiments_plan.sh

run_one() {
    local experiment_id="$1"
    local runners="$2"
    local workers="$3"
    local tokens="$4"
    local batch="$5"

    echo ""
    echo "========================================"
    echo "Running $experiment_id"
    echo "========================================"

    EXPERIMENT_ID="$experiment_id" \
    ANALYTICS_NUM_RUNNERS="$runners" \
    NUM_WORKERS="$workers" \
    TOKEN_COUNT="$tokens" \
    FLUSH_EVERY="$batch" \
    ./run_experiment.sh
}

# Runner scaling, scale-up order.
run_one "runners_1_w1_t5_b20" 1 1 5 20
run_one "runners_2_w1_t5_b20" 2 1 5 20
run_one "runners_4_w1_t5_b20" 4 1 5 20
run_one "runners_8_w1_t5_b20" 8 1 5 20

# Token bottleneck.
run_one "tokens_1_r4_w1_b20" 4 4 1 20
run_one "tokens_3_r4_w1_b20" 4 4 3 20
run_one "tokens_5_r4_w1_b20" 4 4 5 20

# Batch size.
run_one "batch_1_r4_w1_t5"   4 4 5 1
run_one "batch_20_r4_w1_t5"  4 4 5 20
run_one "batch_100_r4_w1_t5" 4 4 5 100

# Worker scaling, scale-up order.
run_one "workers_1_r4_t5_b20" 4 1 5 20
run_one "workers_2_r4_t5_b20" 4 2 5 20
run_one "workers_4_r4_t5_b20" 4 4 5 20
