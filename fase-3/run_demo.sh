#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
: "${JAVA_HOME:?Configure JAVA_HOME para Java 17}"
: "${NYC311_RUNTIME:?Defina una carpeta local fuera de iCloud para Kafka}"
PYTHON="${NYC311_PYTHON:-python}"
export SPARK_LOCAL_IP=127.0.0.1
"$PYTHON" setup_kafka.py --runtime "$NYC311_RUNTIME"
"$PYTHON" streaming.py --root "$PWD" --jars "$NYC311_RUNTIME/jars" --run-name "stream_run_$(date +%Y%m%d_%H%M%S)"
"$PYTHON" fairness_finops.py
