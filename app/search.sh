#!/bin/bash
echo "=== Searching for: $1 ==="

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit --master yarn --deploy-mode client --archives /app/.venv.tar.gz#.venv query.py "$1"

echo "=== Search complete ==="
