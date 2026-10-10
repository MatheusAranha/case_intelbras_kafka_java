# Intelbras IoT Device Event Processing — Claude Code Guidelines

Java / Spring Boot / Apache Kafka backend for the Intelbras Backend Sênior
case. Full architecture decisions, Kafka topology, and rationale live in
`docs/superpowers/specs/2026-10-10-iot-device-event-kafka-design.md` —
read that first for *what* we're building. This file is about *how* to
write the code.

---

## Core Architecture Principles

**YOU MUST** follow these:

1. **Dependency Rule**: dependencies point inward. `core/` knows nothing
   about Spring, Kafka, or JPA.
2. **Domain-per-feature, not layer-per-feature**: two independent bounded
   contexts — `domains/device` and `domains/audit` — each with its own
   `core/` and `adapters/`. Never import one domain's `core` types from
   another domain. Cross-domain communication happens only through the
   Kafka message (wire JSON), never through shared Java types.
3. **Behavior Over Implementation**: design around what a use case does,
   not how its adapter happens to do it today.
4. **Explicit Over Clever**: if it needs a comment to explain *what* it
   does, simplify it. Comments are for *why*, only when non-obvious.

### Layer boundaries

```
domains/{device|audit}/
├── core/        entities, enums/value objects, ports (interfaces), use cases, domain errors
│                ZERO Spring / Kafka / JPA imports. Must be unit-testable with no infra running.
└── adapters/    everything that touches the outside world: Kafka producer/consumer,
                 JPA repositories, REST controllers. Implements core's port interfaces.
```

`core/usecase/*` depends only on `core/port/*` interfaces. `adapters/*`
implement those interfaces and are wired in automatically by Spring
(constructor injection, `@Service`/`@Component`/`@Repository` — standard
autowiring, no manual composition root).

---

## Project Structure

See the design spec for the full current tree. Summary:

```
com.intelbras.iotevents
├── domains/
│   ├── device/core/            DeviceEvent, DeviceType, EventType, ports, SimulateIngestionUseCase
│   │   └── adapters/           CsvEventSourceAdapter, KafkaEventPublisherAdapter, EventSimulationController
│   └── audit/core/              AuditRecord, IncomingDeviceEvent, AuditRepositoryPort, *EventAuditHandler, use cases
│       └── adapters/           Kafka listeners, DeviceEventDlqRecoverer, JpaAuditRepositoryAdapter, AuditController
└── config/                     KafkaConfig, MetricsConfig, JacksonConfig — cross-cutting Spring wiring only
```

---

## Naming Conventions

| Element | Convention | Example |
|---|---|---|
| Domain packages | lower-case, singular | `device`, `audit` |
| Entities | `{Name}` (no version suffix — single-shot project) | `DeviceEvent`, `AuditRecord` |
| Ports (interfaces) | `{Name}Port` | `EventPublisherPort`, `AuditRepositoryPort` |
| Adapters (implementations) | `{Tech}{PortName without "Port"}Adapter` | `KafkaEventPublisherAdapter`, `JpaAuditRepositoryAdapter` |
| Use cases | `{Verb}{Noun}UseCase`, single `execute()` method | `SimulateIngestionUseCase`, `RecordDeviceEventAuditUseCase` |
| Kafka listeners | `{DeviceType}EventListener` | `CameraEventListener` |
| Domain errors | `{Reason}Error`, extends a shared base | `DeviceEventValidationError` |
| JPA entities | `{Name}JpaEntity`, never exposed outside the owning adapter package | `AuditJpaEntity` |
| Test files | `{Name}Test` (unit), `{Name}IT` (integration, Testcontainers) | `DedupKeyGeneratorTest`, `AuditFlowIT` |

---

## Critical Patterns

### Entity pattern

Entities are immutable (records where possible). Validation happens in
the constructor/factory method — an invalid entity should be
unconstructible, not constructible-then-checked.

```java
public record DeviceEvent(String eventId, String deviceId, DeviceType deviceType,
                           EventType eventType, Instant timestamp, Map<String, Object> payload) {

    public static DeviceEvent from(DeviceEventRow row) {
        if (row.deviceId() == null || row.deviceId().isBlank()) {
            throw new DeviceEventValidationError("deviceId is required: " + row);
        }
        String eventId = row.id() != null ? row.id() : DedupKeyGenerator.hash(row);
        return new DeviceEvent(eventId, row.deviceId(), row.deviceType(), row.eventType(),
                                row.timestamp(), row.payload());
    }
}
```

### Port pattern

Ports are Java interfaces living in `core/port/`, working with domain
types only — never with JPA entities or Kafka records.

```java
public interface AuditRepositoryPort {
    void save(AuditRecord record);
    boolean existsByDedupKey(String eventId);
    List<AuditRecord> query(AuditQueryFilter filter);
}
```

`JpaAuditRepositoryAdapter implements AuditRepositoryPort`, mapping
`AuditJpaEntity` to/from `AuditRecord` internally — the mapping NEVER
leaks into `core`.

### Error hierarchy

Flexible error classes, no fixed messages, one shared base per domain:

```java
public class DeviceDomainError extends RuntimeException {
    public DeviceDomainError(String message) { super(message); }
}
public class DeviceEventValidationError extends DeviceDomainError { /* message built at throw site */ }
```

---

## Logging — ONE structured log per behaviour (CRITICAL)

**This is a hard rule, not a style preference.** Every behaviour — a use
case's `execute()`, a Kafka listener's `onMessage`, a controller's
handler method — emits **exactly one** log line, success or failure,
never zero and never more than one. No scattered `logger.info()` calls
throughout a method.

### The type

```java
package com.intelbras.iotevents.common.logging;

public final class ExecutionLog {
    private final String module;              // class simple name
    private final String method;               // e.g. "execute"
    private final Instant startedAt = Instant.now();
    private final Map<String, Object> context = new LinkedHashMap<>(); // general info: partition, key, topic, deviceId, eventId...
    private final List<LogStep> steps = new ArrayList<>();
    private Throwable error;

    public static ExecutionLog start(Class<?> module, String method) {
        return new ExecutionLog(module.getSimpleName(), method);
    }

    public ExecutionLog context(String key, Object value) { context.put(key, value); return this; }
    public ExecutionLog step(String message) { return step(message, Map.of()); }
    public ExecutionLog step(String message, Map<String, Object> data) {
        steps.add(new LogStep(message, data));
        return this;
    }

    /** Terminal call. Emits exactly once. Use on the success path. */
    public void info(String message) { StructuredLogger.info(message, this); }

    /** Terminal call. Emits exactly once. Use in the catch block. */
    public void error(String message, Throwable error) {
        this.error = error;
        StructuredLogger.error(message, this);
    }
}

public record LogStep(String message, Map<String, Object> data) {}
```

`StructuredLogger` serializes the whole `ExecutionLog` (module, method,
context, steps, error if present, duration since `startedAt`) to a
single JSON log line via SLF4J — one `logger.info(...)` or
`logger.error(...)` call, one line of output.

### Usage pattern — every use case / listener / controller method follows this shape

```java
public void execute(IncomingDeviceEvent event, KafkaMessageContext kafkaCtx) {
    ExecutionLog log = ExecutionLog.start(getClass(), "execute")
        .context("eventId", event.eventId())
        .context("deviceId", event.deviceId())
        .context("deviceType", event.deviceType())
        .context("topic", kafkaCtx.topic())
        .context("partition", kafkaCtx.partition())
        .context("offset", kafkaCtx.offset())
        .context("messageKey", kafkaCtx.key());

    try {
        boolean duplicate = auditRepositoryPort.existsByDedupKey(event.eventId());
        log.step("Checked dedup key against audit store", Map.of("isDuplicate", duplicate));

        if (duplicate) {
            log.step("Skipped processing because event is a duplicate");
            log.info("Skipped duplicate device event");       // ONE terminal log, success path
            return;
        }

        String generalInfo = handler.handle(event);
        log.step("Built general info via device-type handler", Map.of("generalInfo", generalInfo));

        auditRepositoryPort.save(AuditRecord.from(event, generalInfo));
        log.step("Persisted audit record");

        log.info("Processed device event");                    // ONE terminal log, success path
    } catch (Exception ex) {
        log.step("Unhandled exception while processing device event");
        log.error("Failed to process device event", ex);        // ONE terminal log, failure path
        throw ex;  // still propagate so Spring Kafka's retry/DLQ error handler runs
    }
}
```

**Rules**:
- **ALWAYS** build the `ExecutionLog` at the very top of the behaviour, before any logic.
- **ALWAYS** push a `context(...)` entry for anything identifying — for Kafka-triggered behaviours this MUST include `topic`, `partition`, `offset`, `messageKey`; for other behaviours include whatever uniquely identifies what's being processed (`deviceId`, `eventId`, CSV row number, etc).
- **ALWAYS** push a `step(...)` describing an action that has just completed (past tense) — never an intention.
- **ALWAYS** end with exactly one terminal call: `log.info(...)` on every success/early-return path, `log.error(...)` in the catch block — never both, never neither.
- **ALWAYS** rethrow after `log.error(...)` in Kafka listener code — the error handler needs the exception to drive retry/DLQ; the log call itself does not consume or suppress it.
- **NEVER** call `logger.info`/`logger.error`/`System.out` directly inside a use case, listener, or controller — always go through `ExecutionLog`.
- **NEVER** log a step before the action it describes has executed.
- Pass the same `ExecutionLog` instance down to private helper methods so they can add their own steps — the terminal `.info()`/`.error()` call still happens only once, at the top-level behaviour.
- This applies to **behaviours** (use case `execute()`, listener `onMessage`, controller handler methods) — not to every tiny private getter/mapper. A helper that just transforms data doesn't need its own `ExecutionLog`; it receives the caller's and appends steps.

---

## Kafka Patterns

- **Producer side** (`device.adapters.kafka.KafkaEventPublisherAdapter`): resolves topic from `deviceType`, keys the record by `deviceId`.
- **Consumer side** (`audit.adapters.kafka.listener.*EventListener`): one listener class per device-type topic, same consumer group (`audit-service`), concurrency matching partition count.
- **Error handling**: configured once in `config/KafkaConfig` — `DefaultErrorHandler` with backoff, `DeadLetterPublishingRecoverer` wrapped by `DeviceEventDlqRecoverer` (logs via `ExecutionLog`, no DB write — see design spec Section 6).
- Listener methods must extract `topic`/`partition`/`offset`/`key` from the `ConsumerRecord` (or `@Header` params) specifically to feed the logging `context` — don't discard this metadata.

---

## Testing Strategy

- **`core/` (entities, handlers, dedup-key logic)**: plain JUnit 5 unit tests, no mocks needed — pure functions/pure validation.
- **Full flow (ingestion → Kafka → consumption → audit)**: integration tests with Testcontainers (real Kafka + Postgres) — not mocks. A mocked `AuditRepositoryPort` test would miss the actual DB unique-constraint behavior and real Kafka delivery semantics this design relies on.
- **Behavior-driven test names**: describe the scenario, not the method. `shouldSkipDuplicateEventWithoutPersistingAudit()`, not `testExecute2()`.
- Required scenarios (from the design spec): happy path end-to-end, duplicate event → exactly one audit row ever, forced failure → record on `<topic>.DLT` and zero audit rows.

---

## Anti-Patterns to Avoid

- **Scattered logging**: multiple `logger.info` calls in one method instead of one `ExecutionLog`.
- **Cross-domain imports**: `audit` importing `device.core.DeviceEvent`. If you find yourself doing this, you're missing a field on `IncomingDeviceEvent` — add it there instead.
- **JPA entities leaking into `core`**: a `core/usecase` method accepting or returning `AuditJpaEntity`.
- **Business logic in adapters**: a Kafka listener or controller doing anything beyond deserializing/extracting context and calling a use case.
- **Persisting audit rows for duplicates or failures**: per the design, only successfully processed events get an `AuditRecord`.

---

## When Adding a New Use Case / Adapter

1. Define/extend the port interface in the owning domain's `core/port/`.
2. Implement the use case in `core/usecase/`, following the `ExecutionLog` pattern above.
3. Implement or extend the adapter in `adapters/`, wired via a Spring stereotype annotation.
4. Add unit tests for any new `core/` logic; add/extend the relevant Testcontainers integration test if the change affects the end-to-end flow.
5. If it changes the REST interface, update the matching section in the README's run instructions.

---

## Git Workflow

- **One task, one branch.** Every task (a use case, an adapter, a piece of infra, a doc) gets its own branch created from `main` before work starts — never commit task work directly to `main`.
- **Branch naming**: `{type}/{kebab-case-description}`, e.g. `feat/record-device-event-audit-use-case`, `chore/docker-compose-observability`. Types match the commit types below.
- **Merge back only after the task is validated and running**: tests pass (unit + relevant integration tests) and, where applicable, the behaviour has been exercised locally (e.g. via Docker Compose). Do not merge a branch left broken or half-done.
- Merge into `main` once validated — prefer a regular merge (or squash if the branch has noisy intermediate commits) over leaving work stacked on feature branches.
- Conventional commit style: `{type}: {description}` (no ticket scope — this is a single-track case project). Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `perf`, `build`, `ci`.
- Keep commits scoped to one logical change; avoid bundling unrelated domains in one commit.
- **Commit messages contain only the message itself** — no `Co-Authored-By` or other trailer lines.
