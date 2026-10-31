#!/usr/bin/env sh
# After a run finishes: download the receipt and check stored data against it.
# usage: scripts/fetch_receipt_and_verify.sh TEST_ID
set -e
TID=${1:?test_id required}
mkdir -p generator-runs
until curl -fsS http://127.0.0.1:9001/status | grep -q '"state":"finished"'; do sleep 2; done
curl -fsS http://127.0.0.1:9001/receipt -o "generator-runs/$TID-receipt.json"
docker cp "generator-runs/$TID-receipt.json" cosmos-observatory:/tmp/receipt.json
docker exec cosmos-observatory python /srv/scripts/verify_receipt.py /tmp/receipt.json /data/observatory.db
