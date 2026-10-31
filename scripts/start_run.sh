#!/usr/bin/env sh
# Start a generator run. usage: scripts/start_run.sh TEST_ID [objects_per_second] [duration_seconds] [scenario]
# TARGET defaults to the compose network address; when the generator runs via plain
# `docker run`, use TARGET=http://host.docker.internal:18000/ingest
set -e
TID=${1:?test_id required}; RATE=${2:-2000}; DUR=${3:-120}; SCEN=${4:-evolving}
TARGET=${TARGET:-http://observatory:8000/ingest}
curl -fsS http://127.0.0.1:9001/start -H 'Content-Type: application/json' -d "{
  \"workload\": \"objects\", \"target_url\": \"$TARGET\", \"test_id\": \"$TID\", \"seed\": 42,
  \"batch_size\": 100, \"workers\": 8, \"objects_per_second\": $RATE, \"rate_profile\": \"bursty\",
  \"ramp_seconds\": 10, \"duration_seconds\": $DUR, \"max_batches\": 200000, \"retries\": 3,
  \"objects\": {\"scenario\": \"$SCEN\", \"tree_every_batches\": 20}}"
echo
