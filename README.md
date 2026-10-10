# case_intelbras_kafka_java

Backend case for the Intelbras Backend Sênior process: an IoT device event processing
pipeline built with Java, Spring Boot, and Apache Kafka. Architecture rationale is in
`docs/superpowers/specs/2026-10-10-iot-device-event-kafka-design.md`; coding
conventions are in `CLAUDE.md`.

## The dataset

`src/main/resources/events.csv` is the real 100-event dataset provided with the case,
with 4 appended synthetic rows (`SYNTHETIC-DLQ-001..004`) used only to demonstrate the
retry/dead-letter path live — one per event category, clearly labeled, never mixed into
the real data without being called out. See `DECISIONS.md` for why.

## Running locally (Docker Compose)

```bash
docker compose up -d --build
```

Starts Kafka (KRaft), Postgres, the app, Prometheus, and Grafana.

1. Trigger ingestion:

   ```bash
   curl -X POST http://localhost:8080/events/simulate
   ```

   Returns `{"publishedCount":104}` — 100 real events + 4 synthetic DLQ-demo events.

2. Query the audit trail (only successfully processed events appear — the 4 synthetic
   rows are dead-lettered, never audited):

   ```bash
   curl http://localhost:8080/audits
   curl "http://localhost:8080/audits?deviceId=DEVICE-00014"
   ```

3. Metrics: `http://localhost:8080/actuator/prometheus`
4. Grafana: `http://localhost:3000` (admin/admin), "IoT Device Events" dashboard.

Re-running step 1 demonstrates idempotency (the audit count stays at 100).

## Running tests

```bash
mvn test
```

Includes unit tests and `AuditFlowIT`, a Testcontainers end-to-end test — requires a
running Docker daemon.

## Tearing down

```bash
docker compose down -v
```
