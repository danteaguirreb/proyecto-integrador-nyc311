#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
: "${JAVA_HOME:?Define JAVA_HOME con un JDK 17 compatible con tu arquitectura}"
export SPARK_LOCAL_IP=127.0.0.1
export MLFLOW_DISABLE_AGENT_HINT=1
export TOKENIZERS_PARALLELISM=false
export OMP_NUM_THREADS=1
exec "${NYC311_PYTHON:-.venv/bin/python}" nyc311_phase2.py "$@"
