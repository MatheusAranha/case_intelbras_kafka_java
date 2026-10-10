#!/usr/bin/env bash
# Runs the full test suite locally: unit tests plus AuditFlowIT, a
# Testcontainers integration test that spins up real Kafka and Postgres
# containers (separate from run.sh's docker-compose stack).
set -euo pipefail

echo "==> Checking prerequisites"

if ! command -v java >/dev/null 2>&1; then
  echo "ERROR: Java is not installed or not on PATH (need Java 21)."
  exit 1
fi

if ! command -v mvn >/dev/null 2>&1; then
  echo "ERROR: Maven is not installed or not on PATH."
  exit 1
fi

echo "    $(java -version 2>&1 | head -1)"
echo "    $(mvn -version | head -1)"

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker is not running."
  echo "The integration test (AuditFlowIT) needs it to start real Kafka and Postgres"
  echo "containers. Start Docker and re-run this script."
  exit 1
fi

echo "    Docker is running."

echo "==> Running the full test suite (unit tests + Testcontainers integration test)"
mvn test
