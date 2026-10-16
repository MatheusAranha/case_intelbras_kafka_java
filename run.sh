#!/usr/bin/env bash
# Starts the full stack (Kafka, Postgres, the app, Prometheus, Grafana) and
# waits for the app to become healthy. Does NOT publish the CSV dataset —
# trigger that yourself (curl, or the "Publish CSV" button in
# case_intelbras_frontend) once you're ready to see data flow.
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

echo ""
echo "Everything is running:"
echo "  App:        http://localhost:8080"
echo "  Audits:     http://localhost:8080/audits  (empty until you trigger ingestion)"
echo "  Metrics:    http://localhost:8080/actuator/prometheus"
echo "  Prometheus: http://localhost:9090"
echo "  Grafana:    http://localhost:3000  (login: admin / admin, dashboard: \"IoT Device Events\")"
echo ""
echo "Trigger ingestion of the sample dataset (100 real events + 4 synthetic demo events) with:"
echo "  curl -X POST http://localhost:8080/events/simulate"
echo "or the \"Publish CSV\" button in case_intelbras_frontend (./run.sh there)."
echo ""
echo "Run the test suite with:    ./test.sh"
echo "Tear everything down with:  docker compose down -v"
