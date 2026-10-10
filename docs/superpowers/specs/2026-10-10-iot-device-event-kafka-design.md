# IoT Device Event Processing — Design Spec

Date: 2026-10-10 (revised after receiving the real dataset)
Status: Approved by user, ready for implementation planning

## 0. Revision note

This spec was originally written before the real CSV dataset arrived, against an
assumed schema (`id, device_id, device_type, event_type, timestamp, payload_json`).
The real dataset (`case_information/case_backend_senior_eventos_sanitizados 1.csv`,
100 rows) has a completely different shape. This revision replaces every section
that depended on the assumed schema. The core architecture (hexagonal, two domains,
4-topic Kafka layout, idempotency/retry/DLQ strategy) is unchanged — only the field
mapping and the classification dimension change.

## 1. Context

This is a take-home case for an Intelbras Backend Sênior (Java) position
(full brief in `case_information/case.md`). The goal: ingest a CSV of IoT
device events ("Casa Inteligente" ecosystem), publish them to Apache Kafka,
and process them through consumers in a way that demonstrates production-grade
event-driven architecture: ordering, idempotency, retry/DLQ, observability,
and testability.

**Deliverables required by the case** (mapped to this design):
- Bloco A (Implementation): Sections 3–8 below.
- Bloco B (Architecture): Sections 3–5, write-up in `ARCHITECTURE.md` describing
  partitioning/scaling, consumer groups, ordering, idempotency, retry/DLQ,
  observability, fault tolerance for a millions-events/day scale.
- Bloco C (Technical decisions): this spec + `DECISIONS.md`.
- Bonus targets: GitHub versioning (already a git repo), Docker Compose
  local run, metrics/observability (Prometheus + Grafana), Kafka
  integration tests (Testcontainers).

## 2. The real dataset

`case_information/case_backend_senior_eventos_sanitizados 1.csv` — 100 rows,
columns: `event_number, timestamp_log, msgType, did, dname, pid, parentDid,
parentPid, utcTime, userId, oauthProject, content, body_completo`.

Key facts, established by direct inspection:

- **`msgType`** is the real categorical dimension (always present, never blank).
  Distribution across the 100 rows: `iotProperty`(45), `online`(18),
  `videoMotion`(12), `offline`(7), `iotEvent`(6), `mobileDetect`(4), `human`(3),
  `informationTransfer`(2), `openCamera`(1), `smartVideoMotion`(1), `upgradeFail`(1).
  There is no `device_type` column, and the `deviceType` string occasionally
  found nested inside `body_completo` (`IPC`, `XFAMTXB5`, ...) is an anonymized
  product code, not a usable camera/sensor/alarm classification.
- **`timestamp_log`** is present and ISO-8601-with-offset (e.g.
  `2026-09-25T08:47:51.157000+00:00`) on all 100 rows — the reliable, uniform
  timestamp source. Timestamps nested inside `body_completo` vary in format by
  message family (epoch seconds for camera/alarm events, compact
  `yyyyMMdd'T'HHmmss` strings for IoT-platform events) and are NOT used as the
  parsed timestamp.
- **`content`** is always valid JSON (verified on all 100 rows), sometimes `{}`.
  It is the event's payload.
- **`body_completo`** is the full raw envelope as originally emitted. It is only
  consulted for two fallback lookups (see Section 4) — never stored as the
  event's payload.
- **A native unique id exists only for alarm-style events**: `body_completo.id`
  (present on `videoMotion`/`mobileDetect`/`human`/`smartVideoMotion`, 20 rows,
  globally unique, JSON number) and `body_completo.alarmId` (present on
  `iotEvent`, 6 rows, unique, JSON string). The other 74 rows have no native id.
  `event_number` is a CSV row index, not a usable business id — it is ignored.
- **2 rows** (`informationTransfer`) have a blank `did`; both carry a `deviceId`
  field inside `body_completo` instead (`FJ1K2001704PS`, `IA8L4001890ZN`).

## 3. Classification: `EventCategory`, derived from `msgType`

The case brief itself names four kinds of event: "alterações de propriedades,
mudanças de status, detecção de movimento e outros eventos." That maps directly
onto `msgType`, and reuses the same 4-topic Kafka layout this design already
called for — just with a real classification instead of a fabricated one:

| `EventCategory` | `msgType` values | Row count |
|---|---|---|
| `PROPERTY_CHANGE` | `iotProperty` | 45 |
| `STATUS_CHANGE` | `online`, `offline` | 25 |
| `MOTION_DETECTION` | `videoMotion`, `mobileDetect`, `human`, `smartVideoMotion` (all four carry `labelType: motionAlarm`/`humanAlarm` in `body_completo`) | 20 |
| `OTHER` | `iotEvent`, `informationTransfer`, `openCamera`, `upgradeFail`, and any unrecognized future `msgType` (fallback) | 10 |

## 4. Tech stack

Unchanged: Java, Maven, Spring Boot (spring-kafka, spring-data-jpa,
spring-boot-actuator), PostgreSQL, Apache Kafka (KRaft), Micrometer + Prometheus
+ Grafana, JUnit 5 + Testcontainers, Docker Compose.

## 5. Architecture style

Unchanged: hexagonal (ports & adapters), domain-per-feature. Two bounded
contexts, each independent:

```
com.intelbras.iotevents
├── domains/
│   ├── device/                      # owns event creation & publishing
│   │   ├── core/
│   │   │   ├── DeviceEvent.java             entity: eventId, deviceId, eventCategory, eventType (raw msgType), timestamp, payload
│   │   │   ├── EventCategory.java           enum: PROPERTY_CHANGE, STATUS_CHANGE, MOTION_DETECTION, OTHER — EventCategory.fromMsgType(String)
│   │   │   ├── error/DeviceEventValidationError.java
│   │   │   ├── port/EventSourcePort.java / EventPublisherPort.java
│   │   │   └── usecase/SimulateIngestionUseCase.java
│   │   └── adapters/
│   │       ├── csv/CsvEventSourceAdapter.java          reads events.csv, parses content + body_completo fallbacks
│   │       ├── kafka/KafkaEventPublisherAdapter.java   resolves topic from eventCategory
│   │       └── web/EventSimulationController.java      POST /events/simulate
│   │
│   └── audit/                       # owns dedup + audit trail of processed events
│       ├── core/
│       │   ├── AuditRecord.java             entity: dedupKey, deviceId, eventCategory, eventType, generalInfo, consumedAt, createdAt
│       │   ├── IncomingDeviceEvent.java     audit's OWN value object (eventId, deviceId, eventCategory:String, eventType:String, timestamp, payload)
│       │   ├── port/AuditRepositoryPort.java
│       │   ├── handler/
│       │   │   ├── DeviceEventAuditHandler.java   interface: String handle(IncomingDeviceEvent); boolean supports(String eventCategory)
│       │   │   ├── PropertyChangeEventAuditHandler.java
│       │   │   ├── StatusChangeEventAuditHandler.java
│       │   │   ├── MotionDetectionEventAuditHandler.java
│       │   │   └── OtherEventAuditHandler.java   (also the designated fallback)
│       │   └── usecase/RecordDeviceEventAuditUseCase.java / QueryAuditsUseCase.java
│       └── adapters/
│           ├── kafka/listener/PropertyChangeEventListener.java / StatusChangeEventListener.java / MotionDetectionEventListener.java / OtherEventListener.java
│           ├── kafka/DeviceEventDlqRecoverer.java
│           ├── persistence/JpaAuditRepositoryAdapter.java + AuditJpaEntity.java + AuditMapper.java
│           └── web/AuditController.java             GET /audits
│
└── config/                           KafkaTopics, KafkaTopicConfig, KafkaErrorHandlingConfig
```

Same rules as before: `core/` has zero framework imports; no cross-domain Java
imports (`audit` never imports `device.core.*`); ports are Java interfaces,
Spring-autowired.

## 6. Field mapping (real CSV → domain model)

| Domain field | Source | Notes |
|---|---|---|
| `DeviceEvent.deviceId` | `did` column | If blank, fall back to `body_completo.deviceId`; if still absent, `"UNKNOWN"`. Only `informationTransfer` rows hit the fallback in the real data, and both resolve successfully. |
| `DeviceEvent.eventType` | `msgType` column, unmodified casing | Kept verbatim (e.g. `"videoMotion"`) — it's the platform's own real vocabulary, not free text to normalize. |
| `DeviceEvent.eventCategory` | `EventCategory.fromMsgType(msgType)` | Drives topic routing (Section 3). |
| `DeviceEvent.timestamp` | `timestamp_log` column | Parsed via `OffsetDateTime.parse(...).toInstant()` — NOT `Instant.parse`, because the format uses explicit `+00:00` offsets rather than literal `Z`. |
| `DeviceEvent.payload` | `content` column, parsed as JSON | Always present and valid in the real data; parse failure (shouldn't occur) falls back to an empty map, consistent with CSV-row tolerance. |
| `DeviceEvent.eventId` (dedup key) | `body_completo.id` → else `body_completo.alarmId` → else `DedupKeyGenerator.generate(deviceId, eventType, timestampRaw, payload)` | Both native-id fields are confirmed globally unique within their respective rows; the hash fallback covers the other 74 rows (and the synthetic DLQ-demo rows, Section 7). |

`body_completo` is parsed by the CSV adapter solely to extract `deviceId`/`id`/`alarmId`
when needed — it is never stored on `DeviceEvent` and never reaches Kafka.

## 7. Demonstrating the DLQ path without touching real data

The real 100 rows are all expected to process successfully — there's no
naturally-occurring "poison pill" in anonymized production data, and fabricating
a failure inside a real row would misrepresent the dataset. Per the case's own
"Importante" section, additional events beyond the original 100 are allowed, so
`src/main/resources/events.csv` is the real 100 rows **plus 4 appended synthetic
rows** (`event_number` 101–104), one per `EventCategory`, each carrying
`"simulateFailure": true` in its `content`. They use obviously fake identifiers
(`SYNTHETIC-DLQ-001..004`) and are called out as fabricated in `README.md` and
`ARCHITECTURE.md` — never mixed into the real data without being labeled as such.

The existing `RecordDeviceEventAuditUseCase` check
(`payload.get("simulateFailure") == true` → throw `SimulatedProcessingFailureError`)
needed no change for this — it was already generic.

## 8. Kafka topology

| Topic | Partitions | Key |
|---|---|---|
| `device.property-change.events` | 3 | `deviceId` |
| `device.status-change.events` | 3 | `deviceId` |
| `device.motion-detection.events` | 3 | `deviceId` |
| `device.other.events` | 3 | `deviceId` |

Each topic has a matching `<topic>.DLT` (1 partition). Single consumer group
`audit-service`, concurrency 3 per listener — unchanged from the original design.

## 9. Data flow

Unchanged in shape from the original design (happy / duplicate / failure paths);
only the classification label changed:

**Happy path**: `POST /events/simulate` → `CsvEventSourceAdapter.readAll()` parses
all 104 rows (100 real + 4 synthetic) into validated `DeviceEvent`s → published
keyed by `deviceId` to the topic resolved from `eventCategory` → matching `audit`
listener → dedup check → category handler builds `generalInfo` → `AuditRecord` saved.

**Duplicate path**: same `eventId` arrives again → logged only, no audit row,
no error (unchanged).

**Failure path**: only the 4 synthetic rows exercise this — handler throws
`SimulatedProcessingFailureError` → retry with backoff → dead-lettered to
`<topic>.DLT` → logged only, no audit row (unchanged).

## 10. REST API

Unchanged: `POST /events/simulate`, `GET /audits?deviceId=&eventType=&from=&to=`.
`eventType` here filters on the raw `msgType` value (e.g. `videoMotion`), not the
broader category.

## 11. Observability, testing, and local run

Unchanged from the original design (Actuator/Micrometer/Prometheus/Grafana;
unit tests for `core/`; Testcontainers integration tests for the full flow).
Integration test assertions are now based on the real dataset's actual counts
(104 total published; 100 real rows audited; 4 synthetic rows dead-lettered,
zero audit rows for them) rather than a fabricated row-count.

## 12. Decisions & trade-offs (seed material for Bloco C)

| Decision | Alternative considered | Why this way |
|---|---|---|
| `EventCategory` derived from `msgType`, not a fabricated `device_type` | Keep the originally-assumed `device_type` column | The real data has no such column; `msgType` is the only always-present categorical field, and it maps directly onto the case brief's own four event kinds |
| 4 topics, same layout as originally designed | Redesign topic count/shape around `msgType`'s 11 raw values | Topic-per-raw-msgType would be 11 thin topics for very uneven volumes (1 row up to 45 rows); grouping into the case's own 4 named categories keeps partitioning meaningful |
| `timestamp_log` as the single timestamp source, parsed via `OffsetDateTime` | Use the nested `utcTime`/`time` fields inside `body_completo` | Those vary in format (epoch seconds vs. compact strings) by message family; `timestamp_log` is uniform across all 100 rows |
| Native id (`body_completo.id`/`alarmId`) preferred over hash, when present | Always hash | Real native ids exist for 26/100 rows and are verified unique; preferring them over a hash is more faithful to the source system's own identity |
| Blank `did` → fallback to `body_completo.deviceId` → `"UNKNOWN"` | Reject the row at ingestion | Demonstrates RT04 tolerance on real, mildly inconsistent data rather than silently dropping 2 real rows |
| DLQ demo via 4 appended synthetic rows, clearly labeled | Force a failure into a real row; only cover DLQ in the integration test | The case explicitly allows adding events beyond the original 100; a labeled synthetic row keeps the real dataset unmodified and still demonstrates retry/DLQ live via `docker compose` |
| Two independent domains (`device`, `audit`) | One domain covering ingestion+processing+audit | Unchanged rationale: audit is one of potentially several independent consumers of the event stream |

## 13. Out of scope

- The frontend for viewing audits (future, separate project).
- Authentication/authorization on the REST endpoints.
- Automated DLQ replay (discussed in `ARCHITECTURE.md`, not implemented).
