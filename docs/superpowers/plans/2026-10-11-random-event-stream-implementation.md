# Random Event Stream Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a factory that fabricates plausible `DeviceEvent`s and a REST-controlled background publisher that keeps firing them into Kafka at a configurable, uncapped rate — a live-demo and load-testing tool to push the consumer side past its current capacity and show Kafka consumer lag grow, then recover once the consumer is scaled.

**Architecture:** Everything lives in the existing `device` domain. `RandomDeviceEventFactory` is a pure `core` class reusing `DeviceEvent.from(...)` unchanged. The scheduler (ticking + an unbounded cached thread pool + a rolling history buffer for duplicate replay) is an adapter implementing a new `RandomEventStreamPort`, driven by three thin use cases, exposed via a new REST controller. No new Kafka topics, no audit-side changes — generated events ride the exact same publish → Kafka → consume → audit pipeline the CSV flow already uses.

**Tech Stack:** Same as the rest of the app — Java 21, Spring Boot, spring-kafka, Micrometer. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-10-11-random-event-stream-design.md`

## Global Constraints

- Generated events stay within the 4 real `EventCategory` values; no new Kafka topics, no new audit-side code.
- `deviceId` for generated events is always `"RANDOM-" + zero-padded number` (distinct from `DEVICE-...` and `SYNTHETIC-DLQ-...`).
- No rate cap, no producer-side backpressure/bounded-queue mechanism — the producer must be able to outrun the consumer, not self-limit.
- `core/` packages touched by this plan (`domains/device/core/**`) have zero Spring/Kafka/JPA imports, consistent with the rest of the codebase.
- No cross-domain imports: the new metrics port lives in `domains/device/core/port`, not reused from `domains/audit/core/port/MetricsPort`.
- Tick interval: 100ms. Thread pool: cached/elastic (`Executors.newCachedThreadPool()`), no fixed size, no bounded queue.
- `start` while already running stops the old stream and replaces it with the new config — never a 409.
- In-memory only: no persistence of stream state across restarts.
- Validation (400 Bad Request): `eventsPerMinute <= 0`; `dlqRate`/`duplicateRate` outside `[0, 1]`; empty `categories`.

## Review Focus

- A single generated event's publish failure (e.g. a transient Kafka hiccup) must not crash the whole stream — the next tick's events should still get dispatched. Owned by Task 4.
- An unrecognized category name in the start request (e.g. `"categories": ["BOGUS"]`) must return 400, not a 500 or silent misbehavior. Owned by Task 5.
- `duplicateRate=1.0` requested before any event has ever been generated (empty history) must still produce a real, fresh event — not throw or hang waiting for a history that doesn't exist yet. Owned by Task 4.
- Calling `/stop` when nothing is running, or calling it twice in a row, must be idempotent (200, `running:false`) and never throw. Owned by Task 4.
- Calling `/status` before `/start` has ever been called (e.g. right after app boot) must return a safe `running:false` / zeroed status, not NPE on uninitialized state. Owned by Task 4.

---

### Task 1: Extend `EventCategory` with a public msgType-sampling method

**Files:**
- Modify: `src/main/java/com/intelbras/iotevents/domains/device/core/EventCategory.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/device/core/EventCategoryTest.java` (new)

**Interfaces:**
- Produces: `EventCategory.sampleMsgTypes(): Set<String>` — the real `msgType` values that classify as this category. `fromMsgType(String): EventCategory`'s existing public behavior is unchanged (same test outcomes as before this task).

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b feat/event-category-msg-type-sampling`

- [ ] **Step 2: Failing test**

```java
package com.intelbras.iotevents.domains.device.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EventCategoryTest {

    @Test
    void shouldClassifyEveryMsgTypeItSamplesBackToItself() {
        for (EventCategory category : EventCategory.values()) {
            if (category == EventCategory.OTHER) {
                continue;
            }
            for (String msgType : category.sampleMsgTypes()) {
                assertThat(EventCategory.fromMsgType(msgType)).isEqualTo(category);
            }
        }
    }

    @Test
    void shouldSampleAtLeastOneMsgTypePerCategory() {
        for (EventCategory category : EventCategory.values()) {
            assertThat(category.sampleMsgTypes()).isNotEmpty();
        }
    }

    @Test
    void shouldStillClassifyKnownMsgTypesCorrectly() {
        assertThat(EventCategory.fromMsgType("iotProperty")).isEqualTo(EventCategory.PROPERTY_CHANGE);
        assertThat(EventCategory.fromMsgType("online")).isEqualTo(EventCategory.STATUS_CHANGE);
        assertThat(EventCategory.fromMsgType("videoMotion")).isEqualTo(EventCategory.MOTION_DETECTION);
        assertThat(EventCategory.fromMsgType("somethingUnrecognized")).isEqualTo(EventCategory.OTHER);
    }
}
```

- [ ] **Step 3: Run, verify fails** — `mvn test -Dtest=EventCategoryTest`
Expected: FAIL — `sampleMsgTypes()` does not exist yet, and `OTHER.sampleMsgTypes()` must also be non-empty (not yet true).

- [ ] **Step 4: Rewrite `EventCategory`**

```java
package com.intelbras.iotevents.domains.device.core;

import java.util.Map;
import java.util.Set;

public enum EventCategory {
    PROPERTY_CHANGE, STATUS_CHANGE, MOTION_DETECTION, OTHER;

    private static final Map<EventCategory, Set<String>> MSG_TYPES_BY_CATEGORY = Map.of(
        PROPERTY_CHANGE, Set.of("iotProperty"),
        STATUS_CHANGE, Set.of("online", "offline"),
        MOTION_DETECTION, Set.of("videoMotion", "mobileDetect", "human", "smartVideoMotion"),
        OTHER, Set.of("iotEvent", "informationTransfer", "openCamera", "upgradeFail")
    );

    public static EventCategory fromMsgType(String msgType) {
        if (msgType == null) {
            return OTHER;
        }
        String trimmed = msgType.trim();
        for (EventCategory category : values()) {
            if (category != OTHER && MSG_TYPES_BY_CATEGORY.get(category).contains(trimmed)) {
                return category;
            }
        }
        return OTHER;
    }

    public Set<String> sampleMsgTypes() {
        return MSG_TYPES_BY_CATEGORY.get(this);
    }
}
```

- [ ] **Step 5: Run, verify passes** — `mvn test -Dtest=EventCategoryTest` → 3 passed
- [ ] **Step 6: Run the full suite to confirm no regression** — `mvn test` → all green (`DeviceEventTest`'s existing classification assertions must still pass unchanged)
- [ ] **Step 7: Commit** — `git add src/main/java/com/intelbras/iotevents/domains/device/core/EventCategory.java src/test/java/com/intelbras/iotevents/domains/device/core/EventCategoryTest.java && git commit -m "feat: expose EventCategory.sampleMsgTypes for random event generation"`
- [ ] **Step 8: Merge** — `git checkout main && git merge --no-ff feat/event-category-msg-type-sampling && git branch -d feat/event-category-msg-type-sampling`

---

### Task 2: `RandomDeviceEventFactory`

**Files:**
- Create: `src/main/java/com/intelbras/iotevents/domains/device/core/RandomDeviceEventFactory.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/device/core/RandomDeviceEventFactoryTest.java`

**Interfaces:**
- Consumes: `EventCategory.sampleMsgTypes()` (Task 1), `DeviceEvent`, `DeviceEventRow`, `DeviceEvent.from(...)` (existing).
- Produces: `RandomDeviceEventFactory()` (no-arg, production use) and `RandomDeviceEventFactory(Random random)` (seeded, for tests); `generate(EventCategory category, boolean forceDlq): DeviceEvent`. Task 4 depends on this exact method signature.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b feat/random-device-event-factory`

- [ ] **Step 2: Failing test**

```java
package com.intelbras.iotevents.domains.device.core;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class RandomDeviceEventFactoryTest {

    @Test
    void shouldGenerateAnEventWithARandomDeviceIdPrefix() {
        RandomDeviceEventFactory factory = new RandomDeviceEventFactory(new Random(42));

        DeviceEvent event = factory.generate(EventCategory.PROPERTY_CHANGE, false);

        assertThat(event.deviceId()).startsWith("RANDOM-");
    }

    @Test
    void shouldGenerateAnEventWhoseMsgTypeClassifiesBackToTheRequestedCategory() {
        RandomDeviceEventFactory factory = new RandomDeviceEventFactory(new Random(7));

        for (EventCategory category : EventCategory.values()) {
            DeviceEvent event = factory.generate(category, false);
            assertThat(event.eventCategory()).isEqualTo(category);
            assertThat(category.sampleMsgTypes()).contains(event.eventType());
        }
    }

    @Test
    void shouldNotSetSimulateFailureWhenForceDlqIsFalse() {
        RandomDeviceEventFactory factory = new RandomDeviceEventFactory(new Random(1));

        DeviceEvent event = factory.generate(EventCategory.MOTION_DETECTION, false);

        assertThat(event.payload()).doesNotContainKey("simulateFailure");
    }

    @Test
    void shouldSetSimulateFailureWhenForceDlqIsTrue() {
        RandomDeviceEventFactory factory = new RandomDeviceEventFactory(new Random(1));

        DeviceEvent event = factory.generate(EventCategory.MOTION_DETECTION, true);

        assertThat(event.payload()).containsEntry("simulateFailure", true);
    }

    @Test
    void shouldGenerateDistinctEventIdsAcrossCalls() {
        RandomDeviceEventFactory factory = new RandomDeviceEventFactory(new Random(99));

        DeviceEvent first = factory.generate(EventCategory.OTHER, false);
        DeviceEvent second = factory.generate(EventCategory.OTHER, false);

        assertThat(first.eventId()).isNotEqualTo(second.eventId());
    }
}
```

- [ ] **Step 3: Run, verify fails** — `mvn test -Dtest=RandomDeviceEventFactoryTest`
Expected: FAIL — `RandomDeviceEventFactory` does not exist yet.

- [ ] **Step 4: `RandomDeviceEventFactory`**

```java
package com.intelbras.iotevents.domains.device.core;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class RandomDeviceEventFactory {

    private final Random random;
    private final AtomicLong deviceSequence = new AtomicLong();

    public RandomDeviceEventFactory() {
        this(new Random());
    }

    public RandomDeviceEventFactory(Random random) {
        this.random = random;
    }

    public DeviceEvent generate(EventCategory category, boolean forceDlq) {
        List<String> msgTypes = List.copyOf(category.sampleMsgTypes());
        String msgType = msgTypes.get(random.nextInt(msgTypes.size()));
        String deviceId = "RANDOM-" + String.format("%05d", deviceSequence.incrementAndGet());
        String timestampRaw = Instant.now().toString();
        Map<String, Object> payload = new HashMap<>(buildPayload(category));
        if (forceDlq) {
            payload.put("simulateFailure", true);
        }

        DeviceEventRow row = new DeviceEventRow(null, deviceId, msgType, timestampRaw, payload);
        return DeviceEvent.from(row);
    }

    private Map<String, Object> buildPayload(EventCategory category) {
        return switch (category) {
            case PROPERTY_CHANGE -> Map.of("properties", Map.of(
                String.valueOf(10000 + random.nextInt(9999)), random.nextInt(100)));
            case STATUS_CHANGE -> Map.of("status", random.nextBoolean() ? "ONLINE" : "OFFLINE");
            case MOTION_DETECTION -> Map.of("confidence", Math.round(random.nextDouble() * 100) / 100.0);
            case OTHER -> Map.of();
        };
    }
}
```

- [ ] **Step 5: Run, verify passes** — `mvn test -Dtest=RandomDeviceEventFactoryTest` → 5 passed
- [ ] **Step 6: Commit** — `git add src/main/java/com/intelbras/iotevents/domains/device/core/RandomDeviceEventFactory.java src/test/java/com/intelbras/iotevents/domains/device/core/RandomDeviceEventFactoryTest.java && git commit -m "feat: add RandomDeviceEventFactory"`
- [ ] **Step 7: Merge** — `git checkout main && git merge --no-ff feat/random-device-event-factory && git branch -d feat/random-device-event-factory`

---

### Task 3: Stream config/status types, port, and the three use cases

**Files:**
- Create: `src/main/java/com/intelbras/iotevents/domains/device/core/RandomEventStreamConfig.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/device/core/RandomEventStreamStatus.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/device/core/port/RandomEventStreamPort.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/device/core/usecase/StartRandomEventStreamUseCase.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/device/core/usecase/StopRandomEventStreamUseCase.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/device/core/usecase/GetRandomEventStreamStatusUseCase.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/device/core/RandomEventStreamConfigTest.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/device/core/usecase/RandomEventStreamUseCasesTest.java`

**Interfaces:**
- Consumes: `EventCategory` (Task 1).
- Produces: `RandomEventStreamConfig(double eventsPerMinute, double dlqRate, double duplicateRate, List<EventCategory> categories)` — throws `IllegalArgumentException` on invalid values via its compact constructor; `RandomEventStreamStatus(boolean running, RandomEventStreamConfig config, Instant startedAt, long requestedCount, long publishedCount)`; `RandomEventStreamPort.start(RandomEventStreamConfig): RandomEventStreamStatus`, `.stop(): RandomEventStreamStatus`, `.status(): RandomEventStreamStatus`. Task 4's adapter implements this port; Task 5's controller calls these three use cases.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b feat/random-event-stream-usecases`

- [ ] **Step 2: Failing tests**

```java
package com.intelbras.iotevents.domains.device.core;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RandomEventStreamConfigTest {

    @Test
    void shouldAcceptAValidConfig() {
        RandomEventStreamConfig config =
            new RandomEventStreamConfig(600, 0.05, 0.1, List.of(EventCategory.PROPERTY_CHANGE));

        assertThat(config.eventsPerMinute()).isEqualTo(600);
    }

    @Test
    void shouldRejectNonPositiveEventsPerMinute() {
        assertThatThrownBy(() -> new RandomEventStreamConfig(0, 0.0, 0.0, List.of(EventCategory.OTHER)))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RandomEventStreamConfig(-1, 0.0, 0.0, List.of(EventCategory.OTHER)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectRatesOutsideZeroToOne() {
        assertThatThrownBy(() -> new RandomEventStreamConfig(60, 1.5, 0.0, List.of(EventCategory.OTHER)))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RandomEventStreamConfig(60, 0.0, -0.1, List.of(EventCategory.OTHER)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectEmptyCategories() {
        assertThatThrownBy(() -> new RandomEventStreamConfig(60, 0.0, 0.0, List.of()))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RandomEventStreamConfig(60, 0.0, 0.0, null))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
```

```java
package com.intelbras.iotevents.domains.device.core.usecase;

import com.intelbras.iotevents.domains.device.core.EventCategory;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamConfig;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamStatus;
import com.intelbras.iotevents.domains.device.core.port.RandomEventStreamPort;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RandomEventStreamUseCasesTest {

    @Test
    void startUseCaseShouldDelegateToThePort() {
        RandomEventStreamPort port = mock(RandomEventStreamPort.class);
        RandomEventStreamConfig config = new RandomEventStreamConfig(60, 0.0, 0.0, List.of(EventCategory.OTHER));
        RandomEventStreamStatus expected = new RandomEventStreamStatus(true, config, Instant.now(), 0, 0);
        when(port.start(config)).thenReturn(expected);

        RandomEventStreamStatus result = new StartRandomEventStreamUseCase(port).execute(config);

        assertThat(result).isEqualTo(expected);
        verify(port).start(config);
    }

    @Test
    void stopUseCaseShouldDelegateToThePort() {
        RandomEventStreamPort port = mock(RandomEventStreamPort.class);
        RandomEventStreamStatus expected = new RandomEventStreamStatus(false, null, null, 3, 3);
        when(port.stop()).thenReturn(expected);

        RandomEventStreamStatus result = new StopRandomEventStreamUseCase(port).execute();

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void getStatusUseCaseShouldDelegateToThePort() {
        RandomEventStreamPort port = mock(RandomEventStreamPort.class);
        RandomEventStreamStatus expected = new RandomEventStreamStatus(false, null, null, 0, 0);
        when(port.status()).thenReturn(expected);

        RandomEventStreamStatus result = new GetRandomEventStreamStatusUseCase(port).execute();

        assertThat(result).isEqualTo(expected);
    }
}
```

- [ ] **Step 3: Run, verify fails** — `mvn test -Dtest=RandomEventStreamConfigTest,RandomEventStreamUseCasesTest`
Expected: FAIL — none of the new classes exist yet.

- [ ] **Step 4: `RandomEventStreamConfig`**

```java
package com.intelbras.iotevents.domains.device.core;

import java.util.List;

public record RandomEventStreamConfig(double eventsPerMinute, double dlqRate, double duplicateRate,
                                       List<EventCategory> categories) {

    public RandomEventStreamConfig {
        if (eventsPerMinute <= 0) {
            throw new IllegalArgumentException("eventsPerMinute must be positive, got " + eventsPerMinute);
        }
        if (dlqRate < 0 || dlqRate > 1) {
            throw new IllegalArgumentException("dlqRate must be between 0 and 1, got " + dlqRate);
        }
        if (duplicateRate < 0 || duplicateRate > 1) {
            throw new IllegalArgumentException("duplicateRate must be between 0 and 1, got " + duplicateRate);
        }
        if (categories == null || categories.isEmpty()) {
            throw new IllegalArgumentException("categories must not be empty");
        }
    }
}
```

- [ ] **Step 5: `RandomEventStreamStatus`**

```java
package com.intelbras.iotevents.domains.device.core;

import java.time.Instant;

public record RandomEventStreamStatus(boolean running, RandomEventStreamConfig config, Instant startedAt,
                                       long requestedCount, long publishedCount) {
}
```

- [ ] **Step 6: `RandomEventStreamPort`**

```java
package com.intelbras.iotevents.domains.device.core.port;

import com.intelbras.iotevents.domains.device.core.RandomEventStreamConfig;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamStatus;

public interface RandomEventStreamPort {
    RandomEventStreamStatus start(RandomEventStreamConfig config);

    RandomEventStreamStatus stop();

    RandomEventStreamStatus status();
}
```

- [ ] **Step 7: The three use cases**

```java
package com.intelbras.iotevents.domains.device.core.usecase;

import com.intelbras.iotevents.domains.device.core.RandomEventStreamConfig;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamStatus;
import com.intelbras.iotevents.domains.device.core.port.RandomEventStreamPort;

public class StartRandomEventStreamUseCase {

    private final RandomEventStreamPort port;

    public StartRandomEventStreamUseCase(RandomEventStreamPort port) {
        this.port = port;
    }

    public RandomEventStreamStatus execute(RandomEventStreamConfig config) {
        return port.start(config);
    }
}
```

```java
package com.intelbras.iotevents.domains.device.core.usecase;

import com.intelbras.iotevents.domains.device.core.RandomEventStreamStatus;
import com.intelbras.iotevents.domains.device.core.port.RandomEventStreamPort;

public class StopRandomEventStreamUseCase {

    private final RandomEventStreamPort port;

    public StopRandomEventStreamUseCase(RandomEventStreamPort port) {
        this.port = port;
    }

    public RandomEventStreamStatus execute() {
        return port.stop();
    }
}
```

```java
package com.intelbras.iotevents.domains.device.core.usecase;

import com.intelbras.iotevents.domains.device.core.RandomEventStreamStatus;
import com.intelbras.iotevents.domains.device.core.port.RandomEventStreamPort;

public class GetRandomEventStreamStatusUseCase {

    private final RandomEventStreamPort port;

    public GetRandomEventStreamStatusUseCase(RandomEventStreamPort port) {
        this.port = port;
    }

    public RandomEventStreamStatus execute() {
        return port.status();
    }
}
```

**Note on these use cases and `ExecutionLog`**: per `CLAUDE.md`'s logging rule, only behaviours with actual logic log. These three use cases are pure one-line pass-throughs to the port (identical in spirit to how a listener/controller delegates to a use case) — the real logic (and its own `ExecutionLog`, added in Task 4) lives in `RandomEventStreamScheduler.start/stop/status`. No `ExecutionLog` call in these three classes.

- [ ] **Step 8: Run, verify passes** — `mvn test -Dtest=RandomEventStreamConfigTest,RandomEventStreamUseCasesTest` → 7 passed
- [ ] **Step 9: Commit** — `git add src/main/java/com/intelbras/iotevents/domains/device/core/RandomEventStreamConfig.java src/main/java/com/intelbras/iotevents/domains/device/core/RandomEventStreamStatus.java src/main/java/com/intelbras/iotevents/domains/device/core/port/RandomEventStreamPort.java src/main/java/com/intelbras/iotevents/domains/device/core/usecase/StartRandomEventStreamUseCase.java src/main/java/com/intelbras/iotevents/domains/device/core/usecase/StopRandomEventStreamUseCase.java src/main/java/com/intelbras/iotevents/domains/device/core/usecase/GetRandomEventStreamStatusUseCase.java src/test/java/com/intelbras/iotevents/domains/device/core/RandomEventStreamConfigTest.java src/test/java/com/intelbras/iotevents/domains/device/core/usecase/RandomEventStreamUseCasesTest.java && git commit -m "feat: add random event stream config/status types, port, and use cases"`
- [ ] **Step 10: Merge** — `git checkout main && git merge --no-ff feat/random-event-stream-usecases && git branch -d feat/random-event-stream-usecases`

---

### Task 4: `RandomStreamMetricsPort` + `RandomEventStreamScheduler`

This is the core of the feature — the ticking scheduler, the unbounded worker pool, and the duplicate-replay history buffer.

**Files:**
- Create: `src/main/java/com/intelbras/iotevents/domains/device/core/port/RandomStreamMetricsPort.java`
- Modify: `src/main/java/com/intelbras/iotevents/common/metrics/EventMetrics.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/device/adapters/stream/RandomEventStreamScheduler.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/device/adapters/stream/RandomEventStreamSchedulerTest.java`

**Interfaces:**
- Consumes: `EventPublisherPort` (existing), `RandomDeviceEventFactory` (Task 2), `RandomEventStreamConfig`/`Status`/`Port` (Task 3).
- Produces: `RandomEventStreamScheduler implements RandomEventStreamPort`, package-private `int dueCountForTick(double eventsPerMinute)`, package-private `DeviceEvent decideEvent(RandomEventStreamConfig cfg, Random rnd)`, package-private field `final Deque<DeviceEvent> history` — Task 7's integration test exercises this adapter end-to-end through the REST API, not directly.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b feat/random-event-stream-scheduler`

- [ ] **Step 2: Failing tests**

```java
package com.intelbras.iotevents.domains.device.adapters.stream;

import com.intelbras.iotevents.domains.device.core.DeviceEvent;
import com.intelbras.iotevents.domains.device.core.EventCategory;
import com.intelbras.iotevents.domains.device.core.RandomDeviceEventFactory;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamConfig;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamStatus;
import com.intelbras.iotevents.domains.device.core.port.EventPublisherPort;
import com.intelbras.iotevents.domains.device.core.port.RandomStreamMetricsPort;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

class RandomEventStreamSchedulerTest {

    @Test
    void dueCountForTickShouldAccumulateFractionalRemainderForLowRates() {
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            mock(EventPublisherPort.class), new RandomDeviceEventFactory(), mock(RandomStreamMetricsPort.class));

        int totalDue = 0;
        for (int i = 0; i < 120; i++) {
            totalDue += scheduler.dueCountForTick(5.0);
        }

        assertThat(totalDue).isEqualTo(1);
    }

    @Test
    void dueCountForTickShouldMatchExactlyWhenRateDividesTicksEvenly() {
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            mock(EventPublisherPort.class), new RandomDeviceEventFactory(), mock(RandomStreamMetricsPort.class));

        int totalDue = 0;
        for (int i = 0; i < 100; i++) {
            totalDue += scheduler.dueCountForTick(600.0);
        }

        assertThat(totalDue).isEqualTo(100);
    }

    @Test
    void decideEventShouldGenerateFreshEventWhenHistoryIsEmptyEvenIfDuplicateRateIsOne() {
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            mock(EventPublisherPort.class), new RandomDeviceEventFactory(new Random(1)), mock(RandomStreamMetricsPort.class));
        RandomEventStreamConfig config = new RandomEventStreamConfig(600, 0.0, 1.0, List.of(EventCategory.PROPERTY_CHANGE));

        DeviceEvent event = scheduler.decideEvent(config, new Random(1));

        assertThat(event.deviceId()).startsWith("RANDOM-");
    }

    @Test
    void decideEventShouldReplayFromHistoryWhenDuplicateRateAlwaysFires() {
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            mock(EventPublisherPort.class), new RandomDeviceEventFactory(new Random(42)), mock(RandomStreamMetricsPort.class));
        RandomEventStreamConfig config = new RandomEventStreamConfig(600, 0.0, 1.0, List.of(EventCategory.PROPERTY_CHANGE));

        DeviceEvent first = scheduler.decideEvent(config, new Random(1));
        DeviceEvent second = scheduler.decideEvent(config, new Random(1));

        assertThat(second).isEqualTo(first);
    }

    @Test
    void decideEventShouldForceSimulateFailureWhenDlqRateAlwaysFires() {
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            mock(EventPublisherPort.class), new RandomDeviceEventFactory(new Random(42)), mock(RandomStreamMetricsPort.class));
        RandomEventStreamConfig config = new RandomEventStreamConfig(600, 1.0, 0.0, List.of(EventCategory.PROPERTY_CHANGE));

        DeviceEvent event = scheduler.decideEvent(config, new Random(1));

        assertThat(event.payload()).containsEntry("simulateFailure", true);
    }

    @Test
    void statusBeforeEverStartingShouldBeSafelyNotRunning() {
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            mock(EventPublisherPort.class), new RandomDeviceEventFactory(), mock(RandomStreamMetricsPort.class));

        RandomEventStreamStatus status = scheduler.status();

        assertThat(status.running()).isFalse();
        assertThat(status.requestedCount()).isZero();
        assertThat(status.publishedCount()).isZero();
    }

    @Test
    void stopWhenNothingIsRunningShouldBeIdempotentAndSafe() {
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            mock(EventPublisherPort.class), new RandomDeviceEventFactory(), mock(RandomStreamMetricsPort.class));

        RandomEventStreamStatus first = scheduler.stop();
        RandomEventStreamStatus second = scheduler.stop();

        assertThat(first.running()).isFalse();
        assertThat(second.running()).isFalse();
    }

    @Test
    void startShouldReportRunningImmediatelyWithTheGivenConfig() {
        EventPublisherPort publisherPort = mock(EventPublisherPort.class);
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            publisherPort, new RandomDeviceEventFactory(), mock(RandomStreamMetricsPort.class));
        RandomEventStreamConfig config = new RandomEventStreamConfig(60, 0.0, 0.0, List.of(EventCategory.PROPERTY_CHANGE));

        RandomEventStreamStatus status = scheduler.start(config);

        assertThat(status.running()).isTrue();
        assertThat(status.config()).isEqualTo(config);

        scheduler.stop();
    }

    @Test
    void startWhileAlreadyRunningShouldReplaceTheConfigAndResetCounts() {
        EventPublisherPort publisherPort = mock(EventPublisherPort.class);
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            publisherPort, new RandomDeviceEventFactory(), mock(RandomStreamMetricsPort.class));
        RandomEventStreamConfig first = new RandomEventStreamConfig(60, 0.0, 0.0, List.of(EventCategory.PROPERTY_CHANGE));
        RandomEventStreamConfig second = new RandomEventStreamConfig(120, 0.0, 0.0, List.of(EventCategory.MOTION_DETECTION));

        scheduler.start(first);
        RandomEventStreamStatus statusAfterSecondStart = scheduler.start(second);

        assertThat(statusAfterSecondStart.config()).isEqualTo(second);
        assertThat(statusAfterSecondStart.requestedCount()).isZero();

        scheduler.stop();
    }

    @Test
    void startShouldActuallyPublishEventsThroughTheRealTickLoop() {
        EventPublisherPort publisherPort = mock(EventPublisherPort.class);
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            publisherPort, new RandomDeviceEventFactory(), mock(RandomStreamMetricsPort.class));
        RandomEventStreamConfig config = new RandomEventStreamConfig(6000, 0.0, 0.0, List.of(EventCategory.PROPERTY_CHANGE));

        scheduler.start(config);

        verify(publisherPort, timeout(2000)).publish(any(DeviceEvent.class));

        scheduler.stop();
    }

    @Test
    void oneFailedPublishShouldNotStopLaterEventsFromBeingDispatched() {
        EventPublisherPort publisherPort = mock(EventPublisherPort.class);
        doThrow(new RuntimeException("transient broker hiccup")).when(publisherPort).publish(any(DeviceEvent.class));
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            publisherPort, new RandomDeviceEventFactory(), mock(RandomStreamMetricsPort.class));
        RandomEventStreamConfig config = new RandomEventStreamConfig(6000, 0.0, 0.0, List.of(EventCategory.PROPERTY_CHANGE));

        scheduler.start(config);

        verify(publisherPort, timeout(2000).atLeast(2)).publish(any(DeviceEvent.class));

        scheduler.stop();
    }
}
```

- [ ] **Step 3: Run, verify fails** — `mvn test -Dtest=RandomEventStreamSchedulerTest`
Expected: FAIL — `RandomStreamMetricsPort` and `RandomEventStreamScheduler` do not exist yet.

- [ ] **Step 4: `RandomStreamMetricsPort`**

```java
package com.intelbras.iotevents.domains.device.core.port;

public interface RandomStreamMetricsPort {
    void incrementRequested();

    void incrementPublished();
}
```

- [ ] **Step 5: Extend `EventMetrics` to also implement it**

```java
package com.intelbras.iotevents.common.metrics;

import com.intelbras.iotevents.domains.audit.core.port.MetricsPort;
import com.intelbras.iotevents.domains.device.core.port.RandomStreamMetricsPort;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class EventMetrics implements MetricsPort, RandomStreamMetricsPort {

    private final Counter processedCounter;
    private final Counter duplicateCounter;
    private final Counter dlqCounter;
    private final Counter streamRequestedCounter;
    private final Counter streamPublishedCounter;

    public EventMetrics(MeterRegistry registry) {
        this.processedCounter = Counter.builder("device_events_processed_total")
            .description("Number of device events successfully processed and audited").register(registry);
        this.duplicateCounter = Counter.builder("device_events_duplicate_total")
            .description("Number of device events skipped as duplicates").register(registry);
        this.dlqCounter = Counter.builder("device_events_dlq_total")
            .description("Number of device events sent to a dead-letter topic").register(registry);
        this.streamRequestedCounter = Counter.builder("random_stream_requested_total")
            .description("Number of events the random stream decided were due to be published").register(registry);
        this.streamPublishedCounter = Counter.builder("random_stream_published_total")
            .description("Number of events the random stream actually published successfully").register(registry);
    }

    @Override
    public void incrementProcessed() {
        processedCounter.increment();
    }

    @Override
    public void incrementDuplicate() {
        duplicateCounter.increment();
    }

    @Override
    public void incrementDlq() {
        dlqCounter.increment();
    }

    @Override
    public void incrementRequested() {
        streamRequestedCounter.increment();
    }

    @Override
    public void incrementPublished() {
        streamPublishedCounter.increment();
    }
}
```

- [ ] **Step 6: `RandomEventStreamScheduler`**

```java
package com.intelbras.iotevents.domains.device.adapters.stream;

import com.intelbras.iotevents.common.logging.ExecutionLog;
import com.intelbras.iotevents.domains.device.core.DeviceEvent;
import com.intelbras.iotevents.domains.device.core.EventCategory;
import com.intelbras.iotevents.domains.device.core.RandomDeviceEventFactory;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamConfig;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamStatus;
import com.intelbras.iotevents.domains.device.core.port.EventPublisherPort;
import com.intelbras.iotevents.domains.device.core.port.RandomEventStreamPort;
import com.intelbras.iotevents.domains.device.core.port.RandomStreamMetricsPort;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class RandomEventStreamScheduler implements RandomEventStreamPort {

    static final int HISTORY_LIMIT = 50;
    static final long TICK_INTERVAL_MILLIS = 100;

    private final EventPublisherPort eventPublisherPort;
    private final RandomDeviceEventFactory factory;
    private final RandomStreamMetricsPort metricsPort;
    private final Random random = new Random();
    private final Object lock = new Object();

    final Deque<DeviceEvent> history = new ArrayDeque<>();

    private ScheduledExecutorService tickExecutor;
    private ExecutorService workerPool;
    private volatile RandomEventStreamConfig currentConfig;
    private volatile Instant startedAt;
    private volatile boolean running = false;
    private final AtomicLong requestedCount = new AtomicLong();
    private final AtomicLong publishedCount = new AtomicLong();
    private double tickRemainder = 0.0;

    public RandomEventStreamScheduler(EventPublisherPort eventPublisherPort,
                                       RandomDeviceEventFactory factory,
                                       RandomStreamMetricsPort metricsPort) {
        this.eventPublisherPort = eventPublisherPort;
        this.factory = factory;
        this.metricsPort = metricsPort;
    }

    @Override
    public RandomEventStreamStatus start(RandomEventStreamConfig config) {
        ExecutionLog log = ExecutionLog.start(RandomEventStreamScheduler.class, "start")
            .context("eventsPerMinute", config.eventsPerMinute())
            .context("dlqRate", config.dlqRate())
            .context("duplicateRate", config.duplicateRate())
            .context("categories", config.categories());
        try {
            synchronized (lock) {
                stopInternal();
                this.currentConfig = config;
                this.startedAt = Instant.now();
                this.running = true;
                this.requestedCount.set(0);
                this.publishedCount.set(0);
                this.tickRemainder = 0.0;
                this.history.clear();
                this.workerPool = Executors.newCachedThreadPool();
                this.tickExecutor = Executors.newSingleThreadScheduledExecutor();
                this.tickExecutor.scheduleAtFixedRate(this::tick, 0, TICK_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
            }
            log.step("Started random event stream");
            RandomEventStreamStatus status = status();
            log.info("Random event stream started");
            return status;
        } catch (Exception ex) {
            log.step("Unhandled exception while starting random event stream");
            log.error("Failed to start random event stream", ex);
            throw ex;
        }
    }

    @Override
    public RandomEventStreamStatus stop() {
        ExecutionLog log = ExecutionLog.start(RandomEventStreamScheduler.class, "stop");
        try {
            synchronized (lock) {
                stopInternal();
            }
            log.step("Stopped random event stream");
            RandomEventStreamStatus status = status();
            log.info("Random event stream stopped");
            return status;
        } catch (Exception ex) {
            log.step("Unhandled exception while stopping random event stream");
            log.error("Failed to stop random event stream", ex);
            throw ex;
        }
    }

    private void stopInternal() {
        if (tickExecutor != null) {
            tickExecutor.shutdownNow();
            tickExecutor = null;
        }
        if (workerPool != null) {
            workerPool.shutdown();
            workerPool = null;
        }
        running = false;
    }

    @Override
    public RandomEventStreamStatus status() {
        return new RandomEventStreamStatus(running, currentConfig, startedAt, requestedCount.get(), publishedCount.get());
    }

    int dueCountForTick(double eventsPerMinute) {
        synchronized (lock) {
            double duePerTick = eventsPerMinute / 600.0;
            tickRemainder += duePerTick;
            int due = (int) Math.floor(tickRemainder);
            tickRemainder -= due;
            return due;
        }
    }

    void tick() {
        RandomEventStreamConfig cfg = currentConfig;
        ExecutorService pool = workerPool;
        if (cfg == null || pool == null) {
            return;
        }
        int dueCount = dueCountForTick(cfg.eventsPerMinute());
        for (int i = 0; i < dueCount; i++) {
            requestedCount.incrementAndGet();
            metricsPort.incrementRequested();
            pool.submit(() -> dispatchOne(cfg));
        }
    }

    void dispatchOne(RandomEventStreamConfig cfg) {
        try {
            DeviceEvent event = decideEvent(cfg, random);
            eventPublisherPort.publish(event);
            publishedCount.incrementAndGet();
            metricsPort.incrementPublished();
        } catch (Exception publishFailure) {
            // A single slot's failure must never stop the stream — requested-vs-published
            // divergence in /status is the visible signal, not a crashed scheduler.
        }
    }

    DeviceEvent decideEvent(RandomEventStreamConfig cfg, Random rnd) {
        synchronized (lock) {
            if (!history.isEmpty() && rnd.nextDouble() < cfg.duplicateRate()) {
                List<DeviceEvent> snapshot = List.copyOf(history);
                return snapshot.get(rnd.nextInt(snapshot.size()));
            }
        }
        List<EventCategory> categories = cfg.categories();
        EventCategory category = categories.get(rnd.nextInt(categories.size()));
        boolean forceDlq = rnd.nextDouble() < cfg.dlqRate();
        DeviceEvent event = factory.generate(category, forceDlq);
        synchronized (lock) {
            history.addLast(event);
            while (history.size() > HISTORY_LIMIT) {
                history.removeFirst();
            }
        }
        return event;
    }
}
```

- [ ] **Step 7: Run, verify passes** — `mvn test -Dtest=RandomEventStreamSchedulerTest` → 11 passed
- [ ] **Step 8: Run the full suite to confirm no regression** — `mvn test`
- [ ] **Step 9: Commit** — `git add src/main/java/com/intelbras/iotevents/domains/device/core/port/RandomStreamMetricsPort.java src/main/java/com/intelbras/iotevents/common/metrics/EventMetrics.java src/main/java/com/intelbras/iotevents/domains/device/adapters/stream/RandomEventStreamScheduler.java src/test/java/com/intelbras/iotevents/domains/device/adapters/stream/RandomEventStreamSchedulerTest.java && git commit -m "feat: add RandomEventStreamScheduler with uncapped producer throughput"`
- [ ] **Step 10: Merge** — `git checkout main && git merge --no-ff feat/random-event-stream-scheduler && git branch -d feat/random-event-stream-scheduler`

---

### Task 5: REST controller + DI wiring

**Files:**
- Create: `src/main/java/com/intelbras/iotevents/domains/device/adapters/web/StartRandomEventStreamRequest.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/device/adapters/web/RandomEventStreamController.java`
- Modify: `src/main/java/com/intelbras/iotevents/domains/device/DeviceUseCaseConfig.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/device/adapters/web/RandomEventStreamControllerTest.java`

**Interfaces:**
- Consumes: `Start/Stop/GetRandomEventStreamStatusUseCase` (Task 3), `RandomEventStreamConfig`/`Status` (Task 3).
- Produces: `POST /events/random-stream/start`, `POST /events/random-stream/stop`, `GET /events/random-stream/status`.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b feat/random-event-stream-controller`

- [ ] **Step 2: Failing test**

```java
package com.intelbras.iotevents.domains.device.adapters.web;

import com.intelbras.iotevents.domains.device.core.EventCategory;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamConfig;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamStatus;
import com.intelbras.iotevents.domains.device.core.usecase.GetRandomEventStreamStatusUseCase;
import com.intelbras.iotevents.domains.device.core.usecase.StartRandomEventStreamUseCase;
import com.intelbras.iotevents.domains.device.core.usecase.StopRandomEventStreamUseCase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(RandomEventStreamController.class)
class RandomEventStreamControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private StartRandomEventStreamUseCase startUseCase;

    @MockBean
    private StopRandomEventStreamUseCase stopUseCase;

    @MockBean
    private GetRandomEventStreamStatusUseCase getStatusUseCase;

    @Test
    void startShouldDefaultToAllCategoriesWhenOmitted() throws Exception {
        RandomEventStreamConfig expectedConfig = new RandomEventStreamConfig(
            600, 0.05, 0.1, List.of(EventCategory.values()));
        RandomEventStreamStatus status = new RandomEventStreamStatus(true, expectedConfig, Instant.now(), 0, 0);
        when(startUseCase.execute(expectedConfig)).thenReturn(status);

        mockMvc.perform(post("/events/random-stream/start")
                .contentType("application/json")
                .content("{\"eventsPerMinute\":600,\"dlqRate\":0.05,\"duplicateRate\":0.1}"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.running").value(true));
    }

    @Test
    void startShouldReturn400WhenEventsPerMinuteIsMissing() throws Exception {
        mockMvc.perform(post("/events/random-stream/start")
                .contentType("application/json")
                .content("{\"dlqRate\":0.05}"))
            .andExpect(status().isBadRequest());
    }

    @Test
    void startShouldReturn400ForAnUnrecognizedCategoryName() throws Exception {
        mockMvc.perform(post("/events/random-stream/start")
                .contentType("application/json")
                .content("{\"eventsPerMinute\":60,\"categories\":[\"BOGUS\"]}"))
            .andExpect(status().isBadRequest());
    }

    @Test
    void stopShouldReturnTheResultingStatus() throws Exception {
        when(stopUseCase.execute()).thenReturn(new RandomEventStreamStatus(false, null, null, 5, 5));

        mockMvc.perform(post("/events/random-stream/stop"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.running").value(false));
    }

    @Test
    void statusShouldReturnTheCurrentStatus() throws Exception {
        when(getStatusUseCase.execute()).thenReturn(new RandomEventStreamStatus(false, null, null, 0, 0));

        mockMvc.perform(get("/events/random-stream/status"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.running").value(false));
    }
}
```

- [ ] **Step 3: Run, verify fails** — `mvn test -Dtest=RandomEventStreamControllerTest`
Expected: FAIL — none of the new classes exist yet.

- [ ] **Step 4: `StartRandomEventStreamRequest`**

```java
package com.intelbras.iotevents.domains.device.adapters.web;

import com.intelbras.iotevents.domains.device.core.EventCategory;

import java.util.List;

public record StartRandomEventStreamRequest(Double eventsPerMinute, Double dlqRate, Double duplicateRate,
                                              List<EventCategory> categories) {
}
```

- [ ] **Step 5: `RandomEventStreamController`**

```java
package com.intelbras.iotevents.domains.device.adapters.web;

import com.intelbras.iotevents.domains.device.core.EventCategory;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamConfig;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamStatus;
import com.intelbras.iotevents.domains.device.core.usecase.GetRandomEventStreamStatusUseCase;
import com.intelbras.iotevents.domains.device.core.usecase.StartRandomEventStreamUseCase;
import com.intelbras.iotevents.domains.device.core.usecase.StopRandomEventStreamUseCase;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/events/random-stream")
public class RandomEventStreamController {

    private final StartRandomEventStreamUseCase startUseCase;
    private final StopRandomEventStreamUseCase stopUseCase;
    private final GetRandomEventStreamStatusUseCase getStatusUseCase;

    public RandomEventStreamController(StartRandomEventStreamUseCase startUseCase,
                                        StopRandomEventStreamUseCase stopUseCase,
                                        GetRandomEventStreamStatusUseCase getStatusUseCase) {
        this.startUseCase = startUseCase;
        this.stopUseCase = stopUseCase;
        this.getStatusUseCase = getStatusUseCase;
    }

    @PostMapping("/start")
    public ResponseEntity<RandomEventStreamStatus> start(@RequestBody StartRandomEventStreamRequest request) {
        List<EventCategory> categories = (request.categories() == null || request.categories().isEmpty())
            ? List.of(EventCategory.values())
            : request.categories();
        double eventsPerMinute = request.eventsPerMinute() == null ? 0 : request.eventsPerMinute();
        double dlqRate = request.dlqRate() == null ? 0.0 : request.dlqRate();
        double duplicateRate = request.duplicateRate() == null ? 0.0 : request.duplicateRate();

        RandomEventStreamConfig config = new RandomEventStreamConfig(eventsPerMinute, dlqRate, duplicateRate, categories);
        return ResponseEntity.ok(startUseCase.execute(config));
    }

    @PostMapping("/stop")
    public ResponseEntity<RandomEventStreamStatus> stop() {
        return ResponseEntity.ok(stopUseCase.execute());
    }

    @GetMapping("/status")
    public ResponseEntity<RandomEventStreamStatus> status() {
        return ResponseEntity.ok(getStatusUseCase.execute());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleValidationError(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(Map.of("error", ex.getMessage()));
    }
}
```

`startShouldReturn400ForAnUnrecognizedCategoryName` passes without any extra code: Jackson fails to deserialize `"BOGUS"` into `EventCategory` (not a valid enum constant), Spring wraps that in `HttpMessageNotReadableException`, which Spring Boot's default handling already maps to 400 — no custom handler needed for that case specifically, only for `RandomEventStreamConfig`'s own validation (the `IllegalArgumentException` case above).

- [ ] **Step 6: Wire the use case beans** — modify `DeviceUseCaseConfig`

```java
package com.intelbras.iotevents.domains.device;

import com.intelbras.iotevents.domains.device.core.port.EventPublisherPort;
import com.intelbras.iotevents.domains.device.core.port.EventSourcePort;
import com.intelbras.iotevents.domains.device.core.port.RandomEventStreamPort;
import com.intelbras.iotevents.domains.device.core.usecase.GetRandomEventStreamStatusUseCase;
import com.intelbras.iotevents.domains.device.core.usecase.SimulateIngestionUseCase;
import com.intelbras.iotevents.domains.device.core.usecase.StartRandomEventStreamUseCase;
import com.intelbras.iotevents.domains.device.core.usecase.StopRandomEventStreamUseCase;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DeviceUseCaseConfig {

    @Bean
    public SimulateIngestionUseCase simulateIngestionUseCase(EventSourcePort eventSourcePort,
                                                               EventPublisherPort eventPublisherPort) {
        return new SimulateIngestionUseCase(eventSourcePort, eventPublisherPort);
    }

    @Bean
    public StartRandomEventStreamUseCase startRandomEventStreamUseCase(RandomEventStreamPort port) {
        return new StartRandomEventStreamUseCase(port);
    }

    @Bean
    public StopRandomEventStreamUseCase stopRandomEventStreamUseCase(RandomEventStreamPort port) {
        return new StopRandomEventStreamUseCase(port);
    }

    @Bean
    public GetRandomEventStreamStatusUseCase getRandomEventStreamStatusUseCase(RandomEventStreamPort port) {
        return new GetRandomEventStreamStatusUseCase(port);
    }
}
```

- [ ] **Step 7: Run, verify passes** — `mvn test -Dtest=RandomEventStreamControllerTest` → 5 passed
- [ ] **Step 8: Run the full suite** — `mvn test`
- [ ] **Step 9: Commit** — `git add src/main/java/com/intelbras/iotevents/domains/device/adapters/web/StartRandomEventStreamRequest.java src/main/java/com/intelbras/iotevents/domains/device/adapters/web/RandomEventStreamController.java src/main/java/com/intelbras/iotevents/domains/device/DeviceUseCaseConfig.java src/test/java/com/intelbras/iotevents/domains/device/adapters/web/RandomEventStreamControllerTest.java && git commit -m "feat: add random event stream REST endpoints"`
- [ ] **Step 10: Merge** — `git checkout main && git merge --no-ff feat/random-event-stream-controller && git branch -d feat/random-event-stream-controller`

---

### Task 6: Grafana dashboard panel for the new metrics

**Files:**
- Modify: `observability/grafana/dashboards/device-events-dashboard.json`

**Interfaces:**
- Consumes: `random_stream_requested_total`, `random_stream_published_total` (Task 4).

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b chore/random-stream-dashboard-panel`

- [ ] **Step 2: Add a fifth panel**

Replace the file's contents with:

```json
{
  "title": "IoT Device Events",
  "schemaVersion": 39,
  "version": 1,
  "panels": [
    {"id": 1, "title": "Events Processed (rate)", "type": "timeseries", "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
     "targets": [{"expr": "rate(device_events_processed_total[1m])", "legendFormat": "processed"}]},
    {"id": 2, "title": "Duplicates Skipped (rate)", "type": "timeseries", "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
     "targets": [{"expr": "rate(device_events_duplicate_total[1m])", "legendFormat": "duplicates"}]},
    {"id": 3, "title": "Dead-lettered Events (rate)", "type": "timeseries", "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8},
     "targets": [{"expr": "rate(device_events_dlq_total[1m])", "legendFormat": "dlq"}]},
    {"id": 4, "title": "Kafka Consumer Lag", "type": "timeseries", "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8},
     "targets": [{"expr": "sum(kafka_consumer_fetch_manager_records_lag) by (topic)", "legendFormat": "{{topic}}"}]},
    {"id": 5, "title": "Random Stream: Requested vs Published (rate)", "type": "timeseries", "gridPos": {"h": 8, "w": 12, "x": 0, "y": 16},
     "targets": [
       {"expr": "rate(random_stream_requested_total[1m])", "legendFormat": "requested"},
       {"expr": "rate(random_stream_published_total[1m])", "legendFormat": "published"}
     ]}
  ]
}
```

- [ ] **Step 3: Verify the JSON is valid** — `python3 -m json.tool observability/grafana/dashboards/device-events-dashboard.json >/dev/null && echo OK`
Expected: `OK`

- [ ] **Step 4: Commit** — `git add observability/grafana/dashboards/device-events-dashboard.json && git commit -m "chore: add random stream requested-vs-published panel to the Grafana dashboard"`
- [ ] **Step 5: Merge** — `git checkout main && git merge --no-ff chore/random-stream-dashboard-panel && git branch -d chore/random-stream-dashboard-panel`

---

### Task 7: End-to-end Testcontainers integration test

**Files:**
- Create: `src/test/java/com/intelbras/iotevents/RandomEventStreamFlowIT.java`

**Interfaces:**
- Consumes: the full running application (all prior tasks), `AuditFlowIT`'s existing Testcontainers pattern.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b test/random-event-stream-integration`

- [ ] **Step 2: Failing integration test**

```java
package com.intelbras.iotevents;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RandomEventStreamFlowIT {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
        .withDatabaseName("iotevents").withUsername("iotevents").withPassword("iotevents");

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @LocalServerPort
    private int port;

    private final TestRestTemplate restTemplate = new TestRestTemplate();

    @AfterEach
    void stopStream() {
        restTemplate.postForEntity(url("/stop"), null, Map.class);
    }

    @Test
    @Order(1)
    void shouldPublishRandomEventsThatGetAuditedWithTheRandomDeviceIdPrefix() {
        Map<String, Object> body = Map.of("eventsPerMinute", 3000, "dlqRate", 0.0, "duplicateRate", 0.0);
        ResponseEntity<Map> startResponse = restTemplate.postForEntity(url("/start"), body, Map.class);
        assertThat(startResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(startResponse.getBody().get("running")).isEqualTo(true);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            ResponseEntity<List> audits = restTemplate.getForEntity(
                "http://localhost:" + port + "/audits?deviceId=RANDOM-00001", List.class);
            assertThat(audits.getBody()).isNotEmpty();
        });
    }

    @Test
    @Order(2)
    void duplicateRateOfOneShouldMakeAuditCountGrowSlowerThanRequestedCount() {
        Map<String, Object> body = Map.of("eventsPerMinute", 3000, "dlqRate", 0.0, "duplicateRate", 1.0);
        restTemplate.postForEntity(url("/start"), body, Map.class);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ResponseEntity<Map> status = restTemplate.getForEntity(url("/status"), Map.class);
            Number requested = (Number) status.getBody().get("requestedCount");
            assertThat(requested.longValue()).isGreaterThan(20);
        });

        ResponseEntity<Map> stopResponse = restTemplate.postForEntity(url("/stop"), null, Map.class);
        long requested = ((Number) stopResponse.getBody().get("requestedCount")).longValue();
        long published = ((Number) stopResponse.getBody().get("publishedCount")).longValue();

        // The very first generated event is never a duplicate (empty history); every one
        // after that replays, so published should be far smaller than requested.
        assertThat(published).isLessThan(requested);
    }

    private String url(String path) {
        return "http://localhost:" + port + "/events/random-stream" + path;
    }
}
```

- [ ] **Step 3: Confirm Docker is available** — `docker info`
- [ ] **Step 4: Run, verify passes** — `mvn test -Dtest=RandomEventStreamFlowIT` → 2 passed
- [ ] **Step 5: Run the full suite** — `mvn test`
- [ ] **Step 6: Commit** — `git add src/test/java/com/intelbras/iotevents/RandomEventStreamFlowIT.java && git commit -m "test: add end-to-end Testcontainers test for the random event stream"`
- [ ] **Step 7: Merge** — `git checkout main && git merge --no-ff test/random-event-stream-integration && git branch -d test/random-event-stream-integration`

---

### Task 8: README documentation

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b docs/random-event-stream-readme`

- [ ] **Step 2: Add a new section** after the existing "Step 7 — Prove the retry / dead-letter path" section and before "Step 8 — Run the tests locally":

```markdown
### Step 7b — Live demo: the random event stream

Beyond the one-shot CSV replay, the app can generate and publish random synthetic
events continuously, at a rate you control — useful for showing the pipeline handling
live, varied traffic, or for deliberately pushing past the consumer's current capacity
to show Kafka consumer lag grow (and later, scaling the consumer making it recover).

Start it:

```bash
curl -X POST http://localhost:8080/events/random-stream/start \
  -H "Content-Type: application/json" \
  -d '{"eventsPerMinute": 600, "dlqRate": 0.05, "duplicateRate": 0.1}'
```

`eventsPerMinute` is required; `dlqRate`/`duplicateRate` (0–1, default 0) control what
fraction of generated events are forced to fail (dead-lettered) or replay a previous
event (a true duplicate). `categories` is optional and defaults to all four. There is
**no rate cap** — this is also a load-testing tool. Calling `start` again replaces
whatever was running with the new configuration.

Check progress:

```bash
curl http://localhost:8080/events/random-stream/status
```

Generated events use device ids prefixed `RANDOM-` (vs. the CSV's `DEVICE-...` and
`SYNTHETIC-DLQ-...`), so you can tell them apart in `/audits` or in Grafana. Watch the
"Random Stream: Requested vs Published" panel — those two lines tracking closely means
the producer is keeping up; watch the "Kafka Consumer Lag" panel to see the consumer
fall behind at high rates.

Stop it:

```bash
curl -X POST http://localhost:8080/events/random-stream/stop
```
```

- [ ] **Step 3: Update the Reference section's REST API list** to add:

```markdown
  - `POST /events/random-stream/start` — body: `{eventsPerMinute, dlqRate?, duplicateRate?, categories?}`. Starts (or replaces) the continuous random event publisher.
  - `POST /events/random-stream/stop` — stops it.
  - `GET /events/random-stream/status` — current config and requested/published counts.
```

- [ ] **Step 4: Commit** — `git add README.md && git commit -m "docs: document the random event stream feature"`
- [ ] **Step 5: Merge** — `git checkout main && git merge --no-ff docs/random-event-stream-readme && git branch -d docs/random-event-stream-readme`
