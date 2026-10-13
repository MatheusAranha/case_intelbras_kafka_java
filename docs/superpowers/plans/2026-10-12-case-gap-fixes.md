# Case Gap Fixes Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the gaps found when auditing the project against the case brief (`case_information/case.md`): one missing requirement (alerts), stale numbers and wording in the README, the random event stream absent from the architecture and decisions docs, and thin production-scale coverage in `ARCHITECTURE.md`.

**Source of truth for requirements:** `case_information/case.md`. Bloco B in particular lists "Observabilidade: logs, métricas **e alertas**", which is the one item with no coverage today.

**Tech Stack:** No new dependencies. Prometheus `prom/prometheus:v2.54.1` (already in `docker-compose.yml`) and Markdown docs only. No Java changes are expected.

## Global Constraints

- Follow `CLAUDE.md` → Git Workflow: one branch per task, branched from `main`, conventional commit messages, **no `Co-Authored-By` or other trailer lines**, merge back only after validation.
- Docs are written in English, matching the existing `ARCHITECTURE.md` / `DECISIONS.md` / `README.md` tone: short, factual, rationale-first.
- Do not add Intelbras branding or any information that isn't in the case material (see the last bullet of section 7 of the case).
- Don't claim anything the code doesn't do. For production-scale content that isn't implemented, say explicitly that it is the recommended production setup, not the current behavior.
- Metric names used in alert rules must be verified against the live `GET /actuator/prometheus` output, not assumed. Current names in the code: `device_events_processed_total`, `device_events_duplicate_total`, `device_events_dlq_total`, `random_stream_requested_total`, `random_stream_published_total`, plus the Kafka client metric `kafka_consumer_fetch_manager_records_lag` already used by the Grafana dashboard.

---

### Task 0: Commit the pending README change

`README.md` has an uncommitted paragraph in Step 5 (the PromQL "Example debugging query" for the random stream's requested-vs-published gap). Land it before the other tasks so they branch from a clean `main`.

- [ ] Branch `docs/random-stream-promql-example` from `main`
- [ ] Review the diff (`git diff README.md`) and confirm the PromQL matches the metric names above
- [ ] Commit: `docs: add PromQL example for random stream publish gap`
- [ ] Merge into `main`

---

### Task 1: Add Prometheus alert rules (missing requirement)

**Branch:** `feat/prometheus-alert-rules`

**Files:**
- Create: `observability/alerts.yml`
- Modify: `observability/prometheus.yml` (add `rule_files`)
- Modify: `docker-compose.yml` (mount `alerts.yml` into the Prometheus container)
- Modify: `ARCHITECTURE.md` → `## Observability` section
- Modify: `README.md` → Step 5 (tell the user where to see alerts: `http://localhost:9090/alerts`)

**Alert rules to define** (tune thresholds; each needs `summary`/`description` annotations and a `severity` label):

| Alert | Expression idea | Why |
|---|---|---|
| `DeviceEventsDeadLettered` | `increase(device_events_dlq_total[5m]) > 0` | Any dead-lettered event needs a human (DLT replay is manual by design) |
| `AuditConsumerLagHigh` | `sum(kafka_consumer_fetch_manager_records_lag) by (topic) > <N>` for `5m` | Consumers falling behind producers |
| `RandomStreamPublishGap` | `rate(random_stream_requested_total[1m]) - rate(random_stream_published_total[1m]) > 0` for `2m` | Publish failures are swallowed by `RandomEventStreamScheduler.dispatchOne`, so the metrics gap is the only signal |
| `IotEventsAppDown` | `up{job="iot-events-app"} == 0` for `1m` | Scrape target unreachable |
| `DuplicateRateHigh` (optional) | ratio of `device_events_duplicate_total` to `device_events_processed_total` rate above a threshold | Upstream is redelivering or replaying abnormally |

**Steps:**
- [ ] Write `observability/alerts.yml` with the rules above
- [ ] Add `rule_files: ["/etc/prometheus/alerts.yml"]` to `observability/prometheus.yml`
- [ ] Mount the file in `docker-compose.yml` next to the existing `prometheus.yml` volume
- [ ] Validate the syntax: `docker run --rm -v "$PWD/observability:/cfg" --entrypoint promtool prom/prometheus:v2.54.1 check config /cfg/prometheus.yml` (adjust the mount paths so `rule_files` resolves), and `promtool check rules` on `alerts.yml`
- [ ] Run `docker compose up`, trigger `POST /events/simulate`, and confirm at `http://localhost:9090/alerts` that the rules are loaded. The 4 `SYNTHETIC-DLQ-*` rows should make `DeviceEventsDeadLettered` fire. That's expected; document it as the demo for the alert.
- [ ] Update `ARCHITECTURE.md` → Observability: add an "Alerts" paragraph listing each alert and what it means. Also state that in production, Alertmanager (not included in this local stack) would route them to on-call / Slack / email.
- [ ] Update `README.md` Step 5 with a short "Alerts" note pointing to `http://localhost:9090/alerts`
- [ ] Commit: `feat: add Prometheus alert rules for DLQ, consumer lag, and publish gaps`

**Review focus:** the DLQ alert must fire during the standard demo (proves it works). The lag threshold must not fire during a normal 104-event CSV run.

---

### Task 2: Fix stale test count and RF04 wording in the README

**Branch:** `docs/readme-accuracy`

**Files:**
- Modify: `README.md` (lines ~17–30 requirements table, ~59–64 "Testing & quality")

**Steps:**
- [ ] Run the full suite: `./test.sh` (or `mvn verify`). Docker must be running for the Testcontainers ITs.
- [ ] Take the real totals from the Surefire/Failsafe summary. Don't count `@Test` annotations: parameterized tests expand.
- [ ] Replace both "43 tests" / "43/43 tests pass" mentions with the real number. Mention that `RandomEventStreamFlowIT` exists alongside `AuditFlowIT`.
- [ ] Fix the requirements table:
  - RF04 is "Tratar diferentes tipos de evento presentes no dataset" → map it to `EventCategory` (11 `msgType`s → 4 categories) + per-category handlers.
  - Move "without one bad message blocking others" to the RT04 row (that is RT04's wording in the brief).
  - Split the "RT01–RT04" combined row if needed so each requirement is traceable.
- [ ] Add a row for the random event stream ("Scale: millions of events/day" → `POST /events/random-stream/start`, see Step 7b). Verify the exact path against `RandomEventStreamController`.
- [ ] Commit: `docs: correct test count and requirement mapping in README`

**If any test fails:** stop, do not "fix" the count to a failing number. Report the failure; debugging it is a separate task.

---

### Task 3: Document the random event stream in ARCHITECTURE.md and DECISIONS.md

**Branch:** `docs/random-stream-architecture`

**Input:** `docs/superpowers/specs/2026-10-11-random-event-stream-design.md` (read it first, it holds the rationale)

**Files:**
- Modify: `ARCHITECTURE.md` (Mermaid diagram + a new short section)
- Modify: `DECISIONS.md` (new table rows)

**Steps:**
- [ ] Add the stream to the Mermaid diagram: `RandomDeviceEventFactory` → `RandomEventStreamScheduler` → same 4 topics (via the same publisher). Keep it readable; one extra source node is enough.
- [ ] Add a `## Load generation: random event stream` section. Cover what it is for (push consumers past capacity, watch lag grow and then recover after scaling), that it reuses the exact publish → consume → audit path, and the configurable `dlqRate` / `duplicateRate`.
- [ ] Add rows to `DECISIONS.md`. Pull the actual alternatives from the spec. At minimum:
  - Unbounded rate / no producer backpressure vs. rate-capped generator
  - Publish failures swallowed per slot (stream never dies) vs. stop-on-failure. Trade-off: failures are visible only through the `requested − published` metric gap, which Task 1 turns into an alert.
  - In-memory stream state vs. persisted
- [ ] Commit: `docs: document random event stream in architecture and decisions`

---

### Task 4: Deepen the production-scale sections of ARCHITECTURE.md

**Branch:** `docs/production-scale-architecture`

**Files:**
- Modify: `ARCHITECTURE.md`. Sections: Scalability, Consumer groups, Idempotency, Retry, Availability.
- Optionally modify: `DECISIONS.md` → "Known limitations"

The case's evaluation puts 30% on "Arquitetura e Kafka", and the presentation includes "discussão de cenários de escala/falha". Each item below should be 1–3 sentences: what we'd do in production and why. Clearly separate it from what the local setup does.

- [ ] **Sizing math:** "millions of events/day" ≈ tens to low hundreds of events/sec average, with peaks well above that. Show the back-of-envelope numbers and how they map to partition count. Partitions cap consumer parallelism and are hard to reduce later, so over-provision.
- [ ] **Producer durability:** `acks=all`, `enable.idempotence=true`, batching (`linger.ms`, `batch.size`), compression (`lz4`/`zstd`). Note what `application.yml` sets today (defaults) vs. recommended.
- [ ] **Replication:** replication factor 3 + `min.insync.replicas=2`, multi-AZ brokers. Today `KafkaTopicConfig` uses `replicas(1)` for a single local broker. Say so.
- [ ] **Hot partitions:** keying by `deviceId` keeps per-device order but a very chatty device can skew one partition. Name the mitigation and its ordering trade-off.
- [ ] **Consumer scaling:** horizontal `audit-service` instances up to the partition count. Rebalancing cost (cooperative-sticky assignor). Lag-based autoscaling (e.g. KEDA on consumer lag).
- [ ] **Idempotency at scale:** the `existsByDedupKey` check + unique constraint means one DB round trip per event. Discuss the options: rely on the constraint with insert-on-conflict instead of check-then-insert, and dedup-key retention/TTL.
- [ ] **Database as bottleneck:** `audit_records` grows by millions of rows/day. Cover time-based partitioning, a retention/archival policy, batch inserts, and read replicas for `GET /audits` (which also has no pagination today; already listed as deferred in the README).
- [ ] **DLT operations:** how a replay would be done (tooling or a small replay consumer), DLT retention, and that the DLT alert from Task 1 is the trigger.
- [ ] **Failure scenarios** (good for the Q&A section of the presentation): broker down, Postgres down (retries → DLT; discuss pausing the container instead of dead-lettering when the DB itself is down), app crash mid-batch (at-least-once + idempotency covers it).
- [ ] Commit: `docs: expand production-scale guidance in architecture`

**Review focus:** no statement may imply the current code already does something it doesn't. Cross-check every "today we…" claim against `application.yml`, `KafkaTopicConfig`, and `KafkaErrorHandlingConfig`.

---

## Suggested order

Task 0 → Task 1 (only missing requirement) → Task 3 → Task 4 → Task 2 last. Task 2 updates README counts and links, so it should run after everything else has merged and the final test run reflects the end state.
