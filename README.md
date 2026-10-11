# case_intelbras_kafka_java

Backend case for the Intelbras Backend Sênior process: an IoT device event processing
pipeline built with Java, Spring Boot, and Apache Kafka. It ingests the real 100-event
dataset provided with the case, publishes events to Kafka, processes them idempotently
in an independent `audit` domain with retry and dead-letter handling, persists an audit
trail in Postgres, and exposes it over REST — observable via Prometheus and Grafana.

This README has two parts: a **project review** (what was built, how, and how it was
validated) and a **step-by-step guide** to actually run it, use it, and test it,
assuming no prior familiarity with this codebase.

---

## Part 1 — Project Review

### What this covers from the case brief

| Requirement | Where |
|---|---|
| RF01 — read/dispatch CSV events | `POST /events/simulate` → `SimulateIngestionUseCase` |
| RF02 — publish to Kafka | `KafkaEventPublisherAdapter`, topic-per-`EventCategory` |
| RF03 — consumer(s) | 4 `@KafkaListener`s in the `audit` domain |
| RF04 — handle different event types without one bad message blocking others | per-row CSV tolerance + per-category handlers + DLQ |
| RF05 — logs to track processing | `ExecutionLog` — one structured log line per use-case execution |
| RT01–RT04 — Java, Kafka, clear responsibilities, error handling | hexagonal architecture, see below |
| RT05 — idempotency / duplicate handling | `eventId` dedup key + DB unique constraint, proven in `AuditFlowIT` |
| RT06 — automated tests | 43 tests: unit (no infra) + Testcontainers integration test |
| Architecture write-up (scaling, partitioning, consumer groups, ordering, idempotency, retry, observability, fault tolerance) | `ARCHITECTURE.md` |
| Technical decisions & trade-offs | `DECISIONS.md` |
| Bonus: git history, Docker Compose, metrics/observability, Kafka integration tests | all four done — see below |

### Architecture at a glance

Hexagonal (ports & adapters), two independent bounded contexts — `device` (owns
ingestion/publishing) and `audit` (owns dedup/processing/persistence) — communicating
**only** through the Kafka message itself, never through shared Java types. Each
domain's `core/` package has zero Spring/Kafka/JPA imports and is unit-testable with no
infrastructure running; `adapters/` hold all the framework code.

```
events.csv (100 real + 4 synthetic) → POST /events/simulate → Kafka (4 topics,
partitioned by deviceId) → 4 listeners → dedup check → per-category handler →
Postgres audit_records  (or, on exhausted retries: <topic>.DLT, nothing persisted)
```

The real dataset has no device-type column — `msgType` (always present) is grouped
into the four kinds of event the case brief itself names (property change, status
change, motion detection, other). Full rationale, the Kafka partitioning/scaling story,
and the production-scale discussion are in `ARCHITECTURE.md`.

### Key decisions (full table and trade-offs in `DECISIONS.md`)

- **Idempotency**: `eventId` is the dataset's own native id when present (`body_completo.id`/`alarmId`, verified unique), else a deterministic SHA-256 hash — checked via `existsByDedupKey` before processing, backed by a DB unique constraint.
- **Duplicates and failures are never persisted as audit rows** — only successfully processed events exist in `audit_records`. Duplicates are logged and skipped; failures retry 3× with backoff, then move to `<topic>.DLT`, logged, nothing written to the DB.
- **Logging convention**: every use case (`RecordDeviceEventAuditUseCase`, `SimulateIngestionUseCase`, `QueryAuditsUseCase`) emits exactly one structured `ExecutionLog` line per execution — never zero, never more than one, success or failure. Kafka listeners and REST controllers are deliberately thin delegators with **no log of their own** — they contain no decision-making, and the use case they call already logs the full behaviour, including the Kafka topic/partition/offset/key context the listener handed it. Adding a second log per listener/controller would double log volume with no new signal.
- **DLQ demo without touching real data**: the real 100 rows are expected to all succeed. 4 clearly-labeled synthetic rows (`SYNTHETIC-DLQ-001..004`, one per event category) are appended to `events.csv` specifically to exercise the retry → dead-letter path live, rather than fabricating a failure inside anonymized real data.

### Testing & quality

- **Unit tests**: pure domain logic (`core/`), handlers, dedup-key generation, CSV field mapping — no infrastructure required.
- **Integration test** (`AuditFlowIT`, Testcontainers — real Kafka + Postgres): proves the happy path (104 published → 100 audited), that duplicates are truly skipped (asserts the `device_events_duplicate_total` counter increments, not just that the audit count happens to stay flat — a weaker assertion could pass even with broken dedup), and that the 4 synthetic rows land on their dead-letter topics with zero audit rows.
- **43/43 tests pass**, confirmed stable across repeated full-suite runs.
- Built test-first throughout: every production class has a test that was watched fail before the code existed.

### Independent code review

Before calling this done, the full implementation was reviewed by a fresh, independent
review pass (dispatched on Claude Opus, with no memory of the implementation session,
given only the diff, the spec, and the plan). It found real issues, which were then
fixed with the same test-first discipline:

**Fixed:**
- A genuinely flaky `mvn test` run — two tests captured logs from the JVM-wide root
  logger, and leaked Kafka consumer threads from a prior test class could pollute them.
  Fixed by scoping log capture to the specific logger under test and forcing Spring to
  tear down the integration test's context immediately after it finishes.
- The Kafka producer logged "published" and returned *before* the broker acknowledged
  the send — a failed send would disappear silently. Fixed to wait for the delivery
  acknowledgement before reporting success.
- The dedup integration test only proved the audit count stayed flat, which would also
  pass if dedup were broken and the database's unique constraint silently absorbed the
  duplicate. Strengthened to assert the duplicate-skip metric itself increments.
- `audit/core` imported Spring (`@Component` on handler classes, a concrete Spring
  bean as a use-case dependency) — violated its own "zero framework imports in core"
  rule. Fixed with a `MetricsPort` abstraction and explicit bean registration.
- Malformed Kafka messages were retried 3 times with backoff even though a parse
  failure can never succeed on retry. Fixed to dead-letter them immediately.
- `GET /audits?from=&to=` filtered by *when you clicked simulate*, not by when the
  device event actually happened. Fixed by persisting and filtering on the event's own
  timestamp (new column, Flyway migration).
- `docker-compose.yml` had no readiness gating — the app could race Postgres/Kafka
  startup. Fixed with real healthchecks and `condition: service_healthy`.

**Decided, not fixed**: whether Kafka listeners and REST controllers should each get
their own structured log. Settled on no — see "Logging convention" above.

**Deferred as minor** (not blocking, your call if you want them addressed later): the
DLQ recoverer logs its own successful-recovery path at `error` level rather than
`info`; `TIMESTAMP` (no timezone) columns could in principle drift on a non-UTC host
outside Docker (unverified, Docker itself runs UTC); `GET /audits` has no pagination;
an explicit JSON `null` payload would NPE instead of defaulting to empty; Kafka's
advertised listener is internal-only, so host tools like `kcat` can't connect directly
from outside the Docker network.

---

## Part 2 — Quick Start (step by step, assuming nothing)

This section assumes you have never seen this project before. Every command is exact;
every expected output is what you should actually see.

### Step 0 — What you need installed

- **Docker** (Docker Desktop on Mac/Windows, or Docker Engine + Docker Compose on
  Linux). Check with:
  ```bash
  docker --version
  docker compose version
  ```
  If either command fails, install Docker from https://www.docker.com/get-started/
  and make sure the Docker daemon is running before continuing.
- **Java 21** and **Maven** — only needed if you want to run the tests *outside*
  Docker (Step 8). Not needed just to run the app itself, since Docker builds it for
  you. Check with `java -version` (should say `21`) and `mvn -version`.

  If you don't have them yet, the easiest cross-platform way is
  [SDKMAN](https://sdkman.io):
  ```bash
  curl -s "https://get.sdkman.io" | bash
  source "$HOME/.sdkman/bin/sdkman-init.sh"
  sdk install java 21.0.4-tem
  sdk install maven
  ```
  Or with a platform package manager: `brew install openjdk@21 maven` (macOS),
  `sudo apt install openjdk-21-jdk maven` (Ubuntu/Debian), or
  `choco install openjdk maven` (Windows, via Chocolatey).

  **Project (Maven) dependencies are a separate thing from Java/Maven themselves** —
  there's no `npm install`-style step for them. Maven downloads everything declared in
  `pom.xml` automatically into `~/.m2/repository` the first time you build or test, and
  caches it for every build after that. If you want to pre-download them without
  compiling or testing anything yet, run:
  ```bash
  mvn dependency:resolve
  ```
  Otherwise, just running `./test.sh` (Step 8) or `mvn test` downloads whatever's
  missing as part of the normal run.
- A terminal, and `curl` (preinstalled on macOS/Linux; on Windows use Git Bash, WSL,
  or PowerShell's `curl.exe`).

### Step 1 — Get the code

```bash
git clone git@github.com:MatheusAranha/case_intelbras_kafka_java.git
cd case_intelbras_kafka_java
```

(If you already have it locally, just `cd` into the folder instead.)

### Fastest path: three scripts

If you just want this running with minimal typing, run these exact commands in your
terminal, from inside the `case_intelbras_kafka_java` folder, in this order:

1. Check prerequisites and build/pull every image:

   ```bash
   ./setup.sh
   ```

2. Start everything, trigger the demo ingestion, and print the URLs/results:

   ```bash
   ./run.sh
   ```

3. Run the full test suite locally (unit tests + the Testcontainers integration test):

   ```bash
   ./test.sh
   ```

`./run.sh` ends by printing exactly what Step 4–5 below show manually (the audit count,
the metrics, the Grafana URL). The rest of this section walks through the same thing
by hand, one command at a time, so you can see what each script is actually doing and
interact with the running app directly.

### Step 2 — Start everything

```bash
docker compose up -d --build
```

(This is exactly what `./run.sh` does first, plus the health-wait and demo trigger
below — use whichever you prefer.)

This single command builds the application image and starts five containers: Kafka,
Postgres, the application, Prometheus, and Grafana. The first run takes a minute or two
(downloading images, compiling the app); later runs are fast.

Watch it come up:

```bash
docker compose ps
```

Wait until all five show `Up` (and `kafka`, `postgres`, `app` show `(healthy)`) before
continuing — this can take up to ~30 seconds after the containers start. To watch it
happen live instead of polling, run `docker compose logs -f app` and wait for a line
containing `Started IotEventsApplication`, then `Ctrl+C` to stop following.

### Step 3 — Trigger the demo

This tells the app to read its bundled sample dataset (100 real anonymized IoT events
plus 4 clearly-labeled synthetic ones) and publish every row to Kafka:

```bash
curl -X POST http://localhost:8080/events/simulate
```

You should see exactly:

```json
{"publishedCount":104}
```

That's "I successfully published 104 messages to Kafka" — it does **not** mean they've
all finished processing yet (that happens asynchronously, in the background, over the
next few seconds).

### Step 4 — See the results

Wait about 10–15 seconds for Kafka to deliver the messages and the consumers to process
them, then look at the audit trail:

```bash
curl http://localhost:8080/audits
```

You'll get back a JSON array of 100 entries (one per successfully processed real
event) — the 4 synthetic ones are deliberately *not* here (see Step 7). Count them:

```bash
curl -s http://localhost:8080/audits | python3 -c "import json,sys; print(len(json.load(sys.stdin)))"
```

Expected: `100`.

Each entry looks like this (one example):

```json
{
  "dedupKey": "...",
  "deviceId": "DEVICE-00014",
  "eventCategory": "PROPERTY_CHANGE",
  "eventType": "iotProperty",
  "eventTimestamp": "2026-09-25T08:47:51.157Z",
  "generalInfo": "Device DEVICE-00014 reported a property change ...",
  "consumedAt": "...",
  "createdAt": "..."
}
```

You can filter by device or event type:

```bash
curl "http://localhost:8080/audits?deviceId=DEVICE-00014"
curl "http://localhost:8080/audits?eventType=videoMotion"
```

### Step 5 — Explore metrics and the Grafana dashboard

Raw metrics (Prometheus text format):

```bash
curl http://localhost:8080/actuator/prometheus | grep device_events
```

You should see (among other lines):

```
device_events_processed_total 100.0
device_events_duplicate_total 0.0
device_events_dlq_total 4.0
```

That's: 100 processed and audited, 0 duplicates so far (you haven't re-ingested yet —
see Step 6), 4 sent to the dead-letter topic (the synthetic demo rows — see Step 7).

For the visual dashboard: open **http://localhost:3000** in a browser. Log in with
username `admin`, password `admin` (it won't ask you to change it for this demo). In
the left sidebar, go to **Dashboards** and open **"IoT Device Events"** — it's
pre-provisioned, you don't need to build anything. You'll see four live panels: events
processed, duplicates skipped, dead-lettered events, and Kafka consumer lag, all
updating in real time as you trigger more ingestions.

Prometheus's own UI (for writing ad-hoc queries) is at **http://localhost:9090**.

### Step 6 — Prove idempotency (duplicate handling)

Run the exact same ingestion again:

```bash
curl -X POST http://localhost:8080/events/simulate
```

Wait ~10 seconds, then check the audit count again:

```bash
curl -s http://localhost:8080/audits | python3 -c "import json,sys; print(len(json.load(sys.stdin)))"
```

Expected: still `100` — nothing new was added, because every event was recognized as
already processed. Confirm it in the metrics instead of just trusting the count:

```bash
curl http://localhost:8080/actuator/prometheus | grep device_events_duplicate_total
```

Expected: `device_events_duplicate_total 100.0` — the 100 real events were each
correctly flagged as duplicates and skipped (the 4 synthetic ones aren't "duplicates"
in this count, since they're never successfully saved in the first place — see Step 7).

### Step 7 — Prove the retry / dead-letter path

The 4 synthetic rows (`SYNTHETIC-DLQ-001` through `SYNTHETIC-DLQ-004`, one per event
category) are deliberately built to always fail processing. Each time you call
`/events/simulate`, the app retries each of them 3 times (with 1s/2s/4s backoff), then
gives up and routes them to that topic's dead-letter topic (`<topic>.DLT`) instead —
and, per the design, **never** writes an audit row for them. You already saw this in
the metrics: `device_events_dlq_total 4.0` after Step 3, and it will keep growing by 4
every time you re-run Step 3 or Step 6 (each attempt fails again and gets dead-lettered
again), while `device_events_processed_total` stays at 100 forever — confirming failed
events never pollute the audit trail.

If you want to look at the actual dead-lettered messages, you can use any Kafka client
pointed at `localhost:9092`, topic `device.property-change.events.DLT` (and the other
three `.DLT` topics), to see the raw messages — but this isn't necessary to validate
the behavior, the metric already proves it.

### Step 8 — Run the tests locally

This requires Java 21, Maven, and Docker running (the test suite includes a
Testcontainers-based integration test that spins up real Kafka and Postgres
containers — separate from the `docker compose` stack above, so you don't need to stop
that first).

```bash
./test.sh
# or, equivalently:
mvn test
```

Expected, after ~35 seconds:

```
Tests run: 43, Failures: 0, Errors: 0
BUILD SUCCESS
```

This runs every unit test (pure logic, instant) plus `AuditFlowIT`, which really spins
up Kafka and Postgres in Docker, really runs the full ingestion → Kafka → consumption →
audit flow, and asserts on it — not mocks.

### Step 9 — Shut everything down

```bash
docker compose down -v
```

The `-v` also removes the Postgres data volume, so the next `docker compose up` starts
completely fresh (empty audit trail, as if this were the first run ever).

---

## Reference

- **Scripts**: `./setup.sh` (check prerequisites, build/pull images), `./run.sh` (start everything, trigger the demo, print URLs/results), `./test.sh` (run the full test suite locally).
- **Architecture**: `ARCHITECTURE.md`
- **Decisions & trade-offs**: `DECISIONS.md`
- **Coding conventions**: `CLAUDE.md`
- **Design spec**: `docs/superpowers/specs/2026-10-10-iot-device-event-kafka-design.md`
- **REST API**:
  - `POST /events/simulate` — reads the bundled dataset, publishes every row to Kafka. Returns `{"publishedCount": <int>}`.
  - `GET /audits?deviceId=&eventType=&from=&to=` — queries the audit trail. All parameters optional; `from`/`to` are ISO-8601 instants, filtered on each event's own timestamp.
  - `GET /actuator/health`, `GET /actuator/prometheus` — standard Spring Boot Actuator endpoints.
