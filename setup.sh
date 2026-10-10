#!/usr/bin/env bash
# Checks prerequisites and prepares everything needed to run the app
# (builds the app image, pulls the rest). Run this once before run.sh.
set -euo pipefail

echo "==> Checking prerequisites"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: Docker is not installed."
  echo "Install it from https://www.docker.com/get-started/ and re-run this script."
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker is installed but not running."
  echo "Start Docker (Docker Desktop, or the docker service) and re-run this script."
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "ERROR: 'docker compose' is not available."
  echo "Update Docker to a version that includes Compose v2."
  exit 1
fi

echo "    Docker and Docker Compose found."

echo "==> Checking that the required ports look free (5432, 9092, 8080, 9090, 3000)"
for port in 5432 9092 8080 9090 3000; do
  if docker ps --format '{{.Ports}}' 2>/dev/null | grep -q ":${port}->"; then
    echo "    WARNING: port ${port} looks like it's already used by another running container."
    echo "    If 'docker compose up' later fails to bind this port, stop whatever is using it first."
  fi
done

echo "==> Building the application image"
docker compose build

echo "==> Pulling the remaining images (Kafka, Postgres, Prometheus, Grafana)"
docker compose pull kafka postgres prometheus grafana

echo ""
echo "Setup complete. Next: run ./run.sh to start everything."
