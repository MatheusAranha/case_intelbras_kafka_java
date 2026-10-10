# IoT Device Event Processing — Design Spec

Date: 2026-10-10
Status: Approved by user, ready for implementation planning

## 1. Context

This is a take-home case for an Intelbras Backend Sênior (Java) position
(full brief in `case_information/case.md`). The goal: ingest a CSV of IoT
device events ("Casa Inteligente" ecosystem — cameras, sensors, alarms),
publish them to Apache Kafka, and process them through consumers in a way
that demonstrates production-grade event-driven architecture: ordering,
idempotency, retry/DLQ, observability, and testability.

**Deliverables required by the case** (mapped to this design):
- Bloco A (Implementation): Sections 3–8 below.
- Bloco B (Architecture): Sections 3–5, write-up to be produced separately
  describing partitioning/scaling, consumer groups, ordering, idempotency,
  retry/DLQ, observability, fault tolerance for a millions-events/day scale.
- Bloco C (Technical decisions): this spec + its "Decisions & trade-offs"
  section (9) serves as the source material.
- Bonus targets: GitHub versioning (already a git repo), Docker Compose
  local run, metrics/observability (Prometheus + Grafana), Kafka
  integration tests (Testcontainers).

**Known constraint**: the real 100-event CSV has not been provided yet.
The schema below is a plausible IoT event model inferred from the case
brief (property changes, status changes, motion detection). When the real
CSV arrives, only `device.adapters.csv.CsvEventSourceAdapter` and the
sample CSV resource should need to change — this is the seam the design
isolates for exactly that reason.

## 2. Tech stack

- Java, Maven, Spring Boot (spring-kafka, spring-data-jpa, spring-boot-actuator)
- PostgreSQL (audit persistence)
- Apache Kafka (KRaft mode — no ZooKeeper)
- Micrometer + Prometheus + Grafana (observability)
- JUnit 5 + Testcontainers (Kafka, Postgres) for integration tests
- Docker Compose for local orchestration

## 3. Architecture style

Hexagonal (ports & adapters), organized **domain-per-feature**, not
layer-per-feature. Two bounded contexts, each independent:

```
com.intelbras.iotevents
├── domains/
│   ├── device/                      # owns event creation & publishing
│   │   ├── core/
│   │   │   ├── DeviceEvent.java             entity: eventId, deviceId, deviceType, eventType, timestamp, payload
│   │   │   ├── DeviceType.java              enum: CAMERA, SENSOR, ALARM, GENERIC
│   │   │   ├── EventType.java               enum: PROPERTY_CHANGE, STATUS_CHANGE, MOTION_DETECTED, ... (extensible)
│   │   │   ├── error/
│   │   │   │   └── DeviceEventValidationError.java
│   │   │   ├── port/
│   │   │   │   ├── EventSourcePort.java     interface: List<DeviceEvent> readAll()
│   │   │   │   └── EventPublisherPort.java  interface: void publish(DeviceEvent)
│   │   │   └── usecase/
│   │   │       └── SimulateIngestionUseCase.java
│   │   └── adapters/
│   │       ├── csv/CsvEventSourceAdapter.java          implements EventSourcePort
│   │       ├── kafka/KafkaEventPublisherAdapter.java   implements EventPublisherPort
│   │       └── web/EventSimulationController.java      POST /events/simulate
│   │
│   └── audit/                       # owns dedup + audit trail of processed events
│       ├── core/
│       │   ├── AuditRecord.java             entity: dedupKey, deviceId, deviceType, eventType, generalInfo, consumedAt, createdAt
│       │   ├── IncomingDeviceEvent.java     audit's OWN value object for an event it consumes (NOT device.core.DeviceEvent)
│       │   ├── port/
│       │   │   └── AuditRepositoryPort.java interface: save, existsByDedupKey, query(filters)
│       │   ├── handler/
│       │   │   ├── DeviceEventAuditHandler.java   interface: String handle(IncomingDeviceEvent)
│       │   │   ├── CameraEventAuditHandler.java
│       │   │   ├── SensorEventAuditHandler.java
│       │   │   ├── AlarmEventAuditHandler.java
│       │   │   └── GenericEventAuditHandler.java
│       │   └── usecase/
│       │       ├── RecordDeviceEventAuditUseCase.java   dedup check -> handler -> save (only on success)
│       │       └── QueryAuditsUseCase.java
│       └── adapters/
│           ├── kafka/
│           │   ├── listener/
│           │   │   ├── CameraEventListener.java
│           │   │   ├── SensorEventListener.java
│           │   │   ├── AlarmEventListener.java
│           │   │   └── GenericEventListener.java
│           │   └── DeviceEventDlqRecoverer.java   logs + republishes to <topic>.DLT, no DB write
│           ├── persistence/
│           │   ├── JpaAuditRepositoryAdapter.java   implements AuditRepositoryPort
│           │   ├── AuditJpaEntity.java              JPA entity, unique constraint on event_id
│           │   └── AuditMapper.java
│           └── web/AuditController.java             GET /audits
│
└── config/                           # cross-cutting Spring wiring, not business logic
    ├── KafkaConfig.java              topics, error handler, DLQ recoverer, consumer factory (concurrency)
    ├── MetricsConfig.java            Micrometer custom metrics registration
    └── JacksonConfig.java
```

**Rules**:
- `core/` packages contain zero Spring/Kafka/JPA imports — pure Java, unit-testable without infra.
- `core/usecase` depends only on `core/port` interfaces, never on `adapters/`.
- Ports are Java interfaces (not abstract classes) — idiomatic for Spring's `@Component`-based autowiring, and doesn't block a class from implementing more than one port.
- DI is standard Spring autowiring (constructor injection, `@Service`/`@Component`/`@Repository` stereotypes) — no manual composition root. The use case still only ever references the port interface, so the dependency-inversion property holds regardless of who performs the wiring.
- **No cross-domain Java imports.** `audit` never imports anything from `device.core`. The only thing crossing the boundary is the Kafka message itself (JSON), which `audit.adapters.kafka.listener.*` deserializes into `audit.core.IncomingDeviceEvent`. This is what allows a future third domain (e.g. `notification`) to consume the same topics independently, with its own interpretation of the event, without any coupling to `device` or `audit` Java types.

## 4. Domain models & wire contract

**`DeviceEvent` (device.core)**:
```
eventId: String       // CSV's own id column if present; otherwise sha256(deviceId+eventType+timestamp+payload)
deviceId: String
deviceType: DeviceType // CAMERA | SENSOR | ALARM | GENERIC
eventType: EventType
timestamp: Instant
payload: Map<String,Object>
```
`eventId` is computed once, in `device.core`, at event-construction time — it is the identity every independent consumer dedupes on.

**Wire contract (Kafka message value, JSON)** — this is the actual contract between domains, not a shared Java class:
```json
{
  "eventId": "string",
  "deviceId": "string",
  "deviceType": "CAMERA|SENSOR|ALARM|GENERIC",
  "eventType": "string",
  "timestamp": "ISO-8601",
  "payload": { "...": "..." }
}
```

**`IncomingDeviceEvent` (audit.core)**: structurally mirrors the wire contract's fields but is a distinct type owned by `audit`, built by `audit`'s own Kafka listeners. Deliberately not shared with `device.core.DeviceEvent`.

**`AuditRecord` (audit.core)**:
```
dedupKey: String    // == eventId from the wire contract
deviceId: String
deviceType: String
eventType: String
generalInfo: String  // produced by the matching DeviceEventAuditHandler
consumedAt: Instant
createdAt: Instant
```
No status field — every row that exists is, by construction, a successfully processed event (see Section 6).

**Assumed CSV columns** (sample CSV to be created under `src/main/resources/events.csv` for the demo, ~20–30 rows covering all four device types and at least one intentionally malformed row for the DLQ demo):
```
id, device_id, device_type, event_type, timestamp, payload_json
```
`id` and `payload_json` may be absent/malformed in the real dataset — `CsvEventSourceAdapter` must tolerate a missing `id` (falls back to the hash) and a missing/invalid `payload_json` (defaults to an empty map), without ever throwing out of the whole ingestion run for one bad row (RT04).

## 5. Kafka topology

| Topic | Partitions | Key |
|---|---|---|
| `device.camera.events` | 3 | `deviceId` |
| `device.sensor.events` | 3 | `deviceId` |
| `device.alarm.events` | 3 | `deviceId` |
| `device.generic.events` | 3 | `deviceId` |

- Partitioning by `deviceId` guarantees per-device ordering while spreading different devices across partitions for parallelism.
- Single consumer group `audit-service` shared across all four listener classes (one logical consuming application).
- Listener concurrency = 3 per topic (matches partition count) for intra-topic parallel processing.
- Each topic has a matching `<topic>.DLT` dead-letter topic (e.g. `device.camera.events.DLT`), 1 partition each (low volume by design).

## 6. Data flow

**Happy path**: `POST /events/simulate` → `CsvEventSourceAdapter.readAll()` → `SimulateIngestionUseCase` builds a validated `DeviceEvent` per row (computing `eventId`) → `KafkaEventPublisherAdapter.publish()` resolves topic from `deviceType`, publishes keyed by `deviceId` → matching `audit` listener deserializes into `IncomingDeviceEvent` → `RecordDeviceEventAuditUseCase`: `existsByDedupKey(eventId)` is false → matching `DeviceEventAuditHandler.handle()` produces `generalInfo` → `AuditRecord` saved.

**Duplicate path**: same `eventId` arrives again → `existsByDedupKey(eventId)` is true → **log only** ("skipped duplicate event eventId=... deviceId=...") → handler is never invoked, no DB write, use case returns without error.

**Failure path**: handler throws → Spring Kafka `DefaultErrorHandler` retries with backoff (3 attempts, exponential 1s/2s/4s) → still failing → `DeviceEventDlqRecoverer` logs the failure (deviceId, eventType, exception) and publishes the original record to `<topic>.DLT` → **no DB write at all**. The DLT topic is the durable record of the failure; replay/inspection is a manual operational action, described (not automated) in the Bloco B write-up.

To make the failure path demoable, one row in the sample CSV carries a deliberately unrecognized `event_type` value that the matching handler treats as unprocessable and throws on.

## 7. REST API

- `POST /events/simulate` — triggers `SimulateIngestionUseCase`: reads the bundled CSV, publishes every row to Kafka. Can be called repeatedly to demonstrate the duplicate path.
- `GET /audits` — `QueryAuditsUseCase`, filterable by `deviceId`, `eventType`, and a date range; returns persisted `AuditRecord`s. Exists to support a future separate frontend project; no frontend is built as part of this deliverable.

## 8. Observability, testing, and local run

**Observability**: Spring Boot Actuator + Micrometer, `/actuator/prometheus` endpoint. Custom metrics: events-processed counter (tagged `deviceType`, `eventType`), duplicates-skipped counter, DLQ counter, handler-processing-duration timer, plus Micrometer's built-in Kafka consumer-lag metrics. `docker-compose.yml` includes `prometheus` (scrape config targeting the app) and `grafana` (pre-provisioned datasource + one dashboard JSON checked into the repo).

**Testing**:
- Unit (no infra): CSV row → `DeviceEvent` mapping including malformed-row tolerance; `eventId` generation (CSV id vs. hash fallback); each `DeviceEventAuditHandler`'s `generalInfo` output; deviceType → topic resolution.
- Integration (Testcontainers — real Kafka + Postgres): full simulate → publish → consume → audit-persisted flow; a duplicate-event test asserting exactly one `AuditRecord` exists and the handler is invoked only once; a forced-failure test asserting the record lands on `<topic>.DLT` and that no `AuditRecord` is written for it.

**Docker Compose services**: `kafka` (KRaft), `postgres`, `prometheus`, `grafana`, `app`. Single `docker compose up` brings up the full stack (bonus ★★).

## 9. Decisions & trade-offs (seed material for Bloco C)

| Decision | Alternative considered | Why this way |
|---|---|---|
| Topic-per-device-type (not per-event-type, not single topic) | Single topic with type field; one topic per event type | Matches how device domains naturally evolve/scale independently in a real IoT platform; still preserves per-device ordering via the `deviceId` key within each topic |
| Two independent domains (`device`, `audit`), communicating only via the Kafka wire contract | One domain covering ingestion+processing+audit | Models audit as one of potentially several independent consumers (e.g. a future `notification` domain) of the same event stream, without cross-domain Java coupling |
| Ports as Java interfaces, Spring-autowired | Abstract classes; manual `@Configuration`/`@Bean` composition root (both from the user's TS template conventions) | Idiomatic Java/Spring; same dependency-inversion guarantee without fighting the framework |
| `eventId` computed once in `device.core`, carried as wire data | Each consumer independently derives its own dedup key | Keeps identity computation in one place; consumers trust the message instead of re-deriving, while still able to interpret the event independently |
| Duplicates: log only, no audit row | Persist a `DUPLICATE`-status row | Matches the user's explicit requirement — the audit table should only ever reflect successfully processed events |
| Failures: log + DLQ only, no audit row, no `status` field on `AuditRecord` | Persist a `FAILED`-status row | Same as above — simplifies the schema since only one outcome (success) is ever persisted |
| PostgreSQL for the audit store | H2 | Real persistence across restarts, works with Testcontainers integration tests and the Docker Compose bonus |
| Spring Kafka `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` | Manual try/catch per listener | Standard, well-documented retry/DLQ pattern; strong material for the Bloco B architecture discussion |

## 10. Out of scope

- The frontend for viewing audits (mentioned by the user as a future, separate project).
- The real CSV dataset (not yet provided) — the sample CSV and `CsvEventSourceAdapter` are the seam to swap when it arrives.
- Authentication/authorization on the REST endpoints (not required by the case).
- Automated DLQ replay (covered as a design discussion in Bloco B, not implemented).
