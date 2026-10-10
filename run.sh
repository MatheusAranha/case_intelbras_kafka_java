#!/usr/bin/env bash
# Starts the full stack (Kafka, Postgres, the app, Prometheus, Grafana),
# waits for the app to become healthy, triggers ingestion of the sample
# dataset, and prints a summary of what's running and where.
set -euo pipefail

echo "==> Starting Kafka, Postgres, the app, Prometheus, and Grafana"
docker compose up -d --build

echo "==> Waiting for the app to become healthy (this can take up to ~1 minute on first run)"
healthy=false
for _ in $(seq 1 60); do
  if curl -s -f http://localhost:8080/actuator/health >/dev/null 2>&1; then
    healthy=true
    break
  fi
  sleep 2
done

if [ "$healthy" != "true" ]; then
  echo "ERROR: the app did not become healthy within 2 minutes."
  echo "Check what's wrong with: docker compose logs app"
  exit 1
fi

echo "    App is healthy."

echo "==> Triggering ingestion of the sample dataset (100 real events + 4 synthetic demo events)"
response=$(curl -s -X POST http://localhost:8080/events/simulate)
echo "    Response: ${response}"

echo "==> Waiting ~15s for Kafka delivery and consumer processing to settle"
sleep 15

audit_count=$(curl -s http://localhost:8080/audits | python3 -c "import json,sys; print(len(json.load(sys.stdin)))" 2>/dev/null) || audit_count="?"

echo ""
echo "Everything is running:"
echo "  App:        http://localhost:8080"
echo "  Audits:     http://localhost:8080/audits  (currently: ${audit_count} entries, expect 100)"
echo "  Metrics:    http://localhost:8080/actuator/prometheus"
echo "  Prometheus: http://localhost:9090"
echo "  Grafana:    http://localhost:3000  (login: admin / admin, dashboard: \"IoT Device Events\")"
echo ""
echo "Re-trigger ingestion any time with:"
echo "  curl -X POST http://localhost:8080/events/simulate"
echo ""
echo "Run the test suite with:    ./test.sh"
echo "Tear everything down with:  docker compose down -v"
