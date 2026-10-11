# Random Event Stream — Design Spec

Date: 2026-10-11
Status: Approved by user, ready for implementation planning

## 1. Purpose

A live-demo and load-testing capability, independent of the CSV-based `/events/simulate`
flow. Part 1 is a factory that fabricates plausible `DeviceEvent`s (same shape as the
real dataset), parameterized by category and by whether an event should be duplicated
or dead-lettered. Part 2 is a REST-controlled background publisher that keeps firing
factory-generated events into Kafka at a configurable rate (events/minute), with no
upper cap — not a toy, but a tool to deliberately push the pipeline past the consumer
side's current capacity, watch Kafka consumer lag grow on the existing Grafana
dashboard, and later demonstrate that scaling the consumer (concurrency, partitions,
app instances) makes that lag recover.

**Explicitly not the goal**: making the producer itself the bottleneck. The producer
must be able to outrun the consumer so the consumer's real ceiling — not an artificial
producer-side queue limit — is what becomes visible.

## 2. Scope

- Generated events stay within the 4 real `EventCategory` buckets
  (`PROPERTY_CHANGE`/`STATUS_CHANGE`/`MOTION_DETECTION`/`OTHER`), reusing the existing
  Kafka topics, listeners, and audit handlers unchanged. No new Kafka topics, no new
  consumer-side code.
- A small hand-rolled random generator (no new runtime dependency — the domain's value
  space is narrow enough that a library wouldn't save meaningful effort over a seeded
  `java.util.Random` and a few value pools).
- In-memory only: no persistence of stream state. An app restart simply means the
  stream isn't running anymore.
- No rate cap. The caller is trusted to push the rate as high as they want, specifically
  to find where the consumer side falls behind.

## 3. Component placement

Everything lives inside the `device` domain (it owns event creation/publishing),
following the existing hexagonal structure:

```
domains/device/
├── core/
│   ├── RandomDeviceEventFactory.java        pure: generate(category, forceDlq) → DeviceEvent
│   ├── RandomEventStreamConfig.java         record: eventsPerMinute, dlqRate, duplicateRate, categories
│   ├── RandomEventStreamStatus.java         record: running, config, startedAt, requestedCount, publishedCount
│   ├── port/
│   │   └── RandomEventStreamPort.java       interface: start(config): status, stop(): status, status(): status
│   └── usecase/
│       ├── StartRandomEventStreamUseCase.java
│       ├── StopRandomEventStreamUseCase.java
│       └── GetRandomEventStreamStatusUseCase.java
└── adapters/
    ├── stream/
    │   └── RandomEventStreamScheduler.java  implements RandomEventStreamPort — ticking scheduler + cached
    │                                        thread pool, owns the rolling history buffer for duplicate
    │                                        replay, calls EventPublisherPort and MetricsPort
    └── web/
        └── RandomEventStreamController.java  POST .../start, POST .../stop, GET .../status
```

Same rules as the rest of the codebase: `core/` has zero Spring/Kafka/JPA imports.
`RandomDeviceEventFactory` is pure and deterministic given a `Random` instance — it
builds a `DeviceEventRow` with randomized fields and calls the existing
`DeviceEvent.from(...)`, reusing today's validation and hash-based `eventId` logic
unchanged. The use cases are thin pass-throughs to `RandomEventStreamPort`, consistent
with how every other endpoint in the app is wired (controller → use case → port →
adapter). All the threading/scheduling machinery lives in the adapter, where
infrastructure concerns belong.

## 4. The factory

`RandomDeviceEventFactory.generate(EventCategory category, boolean forceDlq): DeviceEvent`

- **`deviceId`**: `"RANDOM-" + zero-padded number` (e.g. `RANDOM-00042`) — visually
  distinct from the real dataset's `DEVICE-00014` and the CSV's `SYNTHETIC-DLQ-00N`
  rows, so anyone querying `/audits` afterward can tell live-stream events apart from
  the CSV demo at a glance.
- **`msgType`**: picked randomly from the real values that map to the requested
  `category` (e.g. `MOTION_DETECTION` → one of `videoMotion`/`mobileDetect`/`human`/
  `smartVideoMotion`). Reuses `EventCategory.fromMsgType` unchanged.
- **`payload`**: a small per-category template with randomized values (e.g.
  `PROPERTY_CHANGE` → `{"properties": {"<random code>": <random int>}}` matching the
  real shape; `STATUS_CHANGE` → a random status string; others → a minimal plausible
  map). Just enough to flow meaningfully through the existing per-category audit
  handlers, not an exhaustive replica of every real field.
- **`timestamp`**: `Instant.now()` at generation time.
- **DLQ**: `forceDlq=true` adds `"simulateFailure": true` to the payload — the exact
  mechanism the CSV's synthetic rows already use. No changes needed anywhere in
  `audit`.
- **Duplicate**: NOT a factory concern. Handled one level up, by the scheduler (see
  Section 5) — it keeps the last ~50 generated `DeviceEvent`s and, when `duplicateRate`
  fires, republishes one verbatim (same `eventId`) instead of asking the factory for a
  new one. This produces a true duplicate through the real dedup path, not a simulated
  one.

Unit-testable with a seeded `Random` passed into the constructor, with zero
infrastructure — consistent with how `DedupKeyGenerator`/`DeviceEvent.from` are
already tested.

## 5. Scheduler mechanics

- **Tick loop**: a `ScheduledExecutorService` ticks every 100ms. Each tick computes
  `duePerTick = eventsPerMinute / 600.0`, accumulates a running fractional remainder
  across ticks, and dispatches `floor(accumulated)` events this tick (carrying the
  leftover fraction forward) — so even a low rate like `5/min` (0.0083/tick) averages
  out exactly over time instead of rounding to zero forever.
- **Dispatch, deliberately unconstrained**: each due event is submitted to a **cached
  (elastic) thread pool** (`Executors.newCachedThreadPool()`-style — grows threads as
  needed, reuses idle ones, no fixed size, no bounded queue). The producer must never
  be the thing that runs out of room; it keeps publishing as fast as Kafka will accept.
  Each worker: rolls `dlqRate`/`duplicateRate`, asks the factory for a fresh event (or
  picks a replay from history), then calls the existing `EventPublisherPort.publish(...)`
  — same synchronous ack-wait as the CSV path, same correctness guarantee.
- **Proving the producer isn't the bottleneck**: `status` reports `requestedCount`
  (incremented the moment a tick decides an event is due) vs. `publishedCount`
  (incremented only after `publish()` returns successfully). In normal operation these
  track closely. If they diverge meaningfully, the producer itself is struggling —
  which is not the point of this tool, and is a signal to look at the producer side,
  not to claim it as "the pipeline's limit."
- **The actual ceiling being demonstrated**: Kafka's own per-partition consumer lag
  (already scraped via Micrometer's Kafka binder and graphed in Grafana). At high
  rates — especially with a non-trivial `dlqRate`, since each DLQ'd message occupies a
  consumer thread for ~7s of retry backoff (1s+2s+4s) before dead-lettering —
  `audit-service`'s fixed listener concurrency (3 per topic, 12 total) can't drain fast
  enough, and lag climbs. Scaling the consumer afterward (concurrency, partition count,
  more app instances) is the "scaling fixes it" half of the demo — out of scope for
  this feature itself, but exactly what it's built to make visible.
- **Restart semantics**: calling `start` again cancels the current scheduled task and
  worker pool, clears the history buffer, and starts fresh with the new config — no
  409, always replaces (confirmed preference).
- **Lifecycle**: in-memory only, as noted in Scope.

## 6. REST API

```
POST /events/random-stream/start
Body: {
  "eventsPerMinute": 600,
  "dlqRate": 0.05,          // 0–1, fraction of fresh events forced to simulateFailure
  "duplicateRate": 0.10,    // 0–1, fraction that replay a recent event instead of a fresh one
  "categories": ["PROPERTY_CHANGE","STATUS_CHANGE","MOTION_DETECTION","OTHER"]  // optional, default: all 4
}
→ 200 OK, body: current RandomEventStreamStatus

POST /events/random-stream/stop
→ 200 OK, body: final RandomEventStreamStatus (running: false)

GET /events/random-stream/status
→ 200 OK, body: {
  "running": true,
  "config": { "eventsPerMinute": 600, "dlqRate": 0.05, "duplicateRate": 0.10, "categories": [...] },
  "startedAt": "2026-10-11T12:00:00Z",
  "requestedCount": 1800,
  "publishedCount": 1797
}
```

`dlqRate` and `duplicateRate` are independent probabilities rolled per generated slot.
A replay (duplicate) is never also forced-DLQ, since it's a verbatim copy of an
already-generated (and already-processed) event; a *fresh* event independently rolls
DLQ. `start` while already running stops the old stream first and replaces it — never
a 409.

**Validation** (400 Bad Request): `eventsPerMinute <= 0`; `dlqRate`/`duplicateRate`
outside `[0, 1]`; empty `categories` list. `stop` when nothing is running returns 200
with `running: false`, not an error. `status` before ever calling `start` returns
`running: false` with all counts at 0.

## 7. Metrics

Two new counters, registered through the same `MetricsPort`-style abstraction used
elsewhere (keeps `core` framework-free):

- `random_stream_requested_total` — incremented every time the scheduler decides an
  event is due this tick.
- `random_stream_published_total` — incremented only after a successful `publish()`
  call returns (ack confirmed).

`requested - published`, computed in Grafana rather than stored, is the producer-health
signal — should hover near zero. No new consumer-side metrics are needed:
`device_events_processed_total`, `device_events_dlq_total`, and the existing Kafka
consumer lag panel already tell the real story once the stream is pushing volume
through. Add one panel to the existing Grafana dashboard for the two new counters,
matching the style of the current four.

## 8. Testing strategy

**Unit tests (`core/`, no infra)**:
- `RandomDeviceEventFactory` with a seeded `Random`: generates valid `DeviceEvent`s per
  category; `forceDlq=true` sets the `simulateFailure` payload flag; output always
  passes `DeviceEvent.from(...)`'s existing validation.
- `StartRandomEventStreamUseCase`/`StopRandomEventStreamUseCase`/
  `GetRandomEventStreamStatusUseCase`: thin pass-throughs to a mocked
  `RandomEventStreamPort`, mirroring how existing use cases are tested.

**Integration test** (new, alongside `AuditFlowIT`, Testcontainers): start a stream at
a modest rate (e.g. 600/min) with `dlqRate=0`/`duplicateRate=0` for determinism, let it
run a few seconds, stop it, assert `publishedCount` landed in Postgres as real audit
rows with `deviceId` starting `RANDOM-`. A second test with `duplicateRate=1` asserts
the audit count grows slower than `requestedCount` (every replay after an `eventId`'s
first occurrence is a true skip). Exact consumer-lag numbers are not asserted in
automated tests — too timing/environment-sensitive for CI; that observation is for live
demo use, not the test suite.

**Edge cases validated explicitly**: see Section 6's Validation paragraph.

## 9. Out of scope

- Changing consumer-side scaling configuration (listener concurrency, partition count)
  as part of this feature — that's a manual, separate step in the live demo narrative,
  made visible by this tool but not automated by it.
- Persisting stream state across app restarts.
- A rate cap or any producer-side backpressure/queue-limit mechanism — deliberately
  excluded; the producer must be able to outrun the consumer.
- New Kafka topics or new audit-side logic — the random stream is a producer-only
  addition that rides entirely on the existing consumption pipeline.
