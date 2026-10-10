# IoT Device Event Processing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Intelbras case backend: ingest the real 100-event CSV (plus 4 labeled synthetic rows for the DLQ demo), publish to Kafka (topic-per-`EventCategory`, derived from the real `msgType` field), consume in an independent `audit` domain with idempotent, retried, dead-lettered processing, persist an audit trail in Postgres, expose it over REST — observable and tested.

**Architecture:** Hexagonal, domain-per-feature (`domains/device`, `domains/audit`), each with a framework-free `core/` and a Spring-wired `adapters/`. Cross-domain communication happens only through the Kafka wire JSON. See CLAUDE.md for conventions (naming, `ExecutionLog`, one-branch-per-task git workflow).

**Tech Stack:** Java 21, Maven, Spring Boot 3.3.4 (web, kafka, data-jpa, actuator), PostgreSQL + Flyway, Apache Kafka (KRaft), Micrometer + Prometheus + Grafana, JUnit 5 + Mockito + Testcontainers + Awaitility, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-10-10-iot-device-event-kafka-design.md` (revised for the real dataset).

## Global Constraints

- Java 21, Maven, Spring Boot 3.3.4 parent, package root `com.intelbras.iotevents`.
- `core/` packages: zero Spring/Kafka/JPA imports.
- No cross-domain Java imports: `audit` never imports `device.core.*`.
- Ports are Java interfaces in `core/port/`, standard Spring autowiring.
- Classification is `EventCategory` (`PROPERTY_CHANGE`, `STATUS_CHANGE`, `MOTION_DETECTION`, `OTHER`), derived from the real `msgType` field — NOT a fabricated device type.
- Topics: `device.property-change.events`, `device.status-change.events`, `device.motion-detection.events`, `device.other.events` — 3 partitions each, keyed by `deviceId`. DLT per topic: `<topic>.DLT`.
- Consumer group `audit-service`, concurrency 3 per listener.
- Duplicates: log only, no audit row. Failures: log + DLQ only, no audit row, no `status` field on `AuditRecord`.
- The real CSV (`case_information/case_backend_senior_eventos_sanitizados 1.csv`, 100 rows) is copied into `src/main/resources/events.csv` with 4 appended synthetic rows (`SYNTHETIC-DLQ-001..004`, one per category, `simulateFailure: true`) to demonstrate the DLQ path without fabricating a failure inside real data.
- Timestamp source is the CSV's `timestamp_log` column, parsed via `OffsetDateTime.parse(...).toInstant()` (NOT `Instant.parse` — the format uses explicit `+00:00` offsets).
- Dedup id resolution order: `body_completo.id` → `body_completo.alarmId` → hash(deviceId, eventType, timestampRaw, payload).
- Every behaviour emits exactly one `ExecutionLog` terminal call. Git: one branch per task, merge after tests pass, commit messages are the message only, no trailers.

---

### Task 1: Project bootstrap

**Files:**
- Create: `pom.xml`, `src/main/resources/application.yml`, `src/main/java/com/intelbras/iotevents/IotEventsApplication.java`, `docker-compose.yml`
- Modify: `.gitignore`

**Interfaces:** Produces a compilable Maven project and `application.yml` properties every later task's beans rely on; `docker-compose.yml` services `kafka`/`postgres` for local dev.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b chore/project-bootstrap`

- [ ] **Step 2: `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.3.4</version>
        <relativePath/>
    </parent>
    <groupId>com.intelbras</groupId>
    <artifactId>iot-events</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <name>iot-events</name>
    <description>Intelbras IoT device event processing case</description>
    <properties>
        <java.version>21</java.version>
    </properties>
    <dependencies>
        <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-web</artifactId></dependency>
        <dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka</artifactId></dependency>
        <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-data-jpa</artifactId></dependency>
        <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-actuator</artifactId></dependency>
        <dependency><groupId>io.micrometer</groupId><artifactId>micrometer-registry-prometheus</artifactId></dependency>
        <dependency><groupId>org.postgresql</groupId><artifactId>postgresql</artifactId><scope>runtime</scope></dependency>
        <dependency><groupId>org.flywaydb</groupId><artifactId>flyway-core</artifactId></dependency>
        <dependency><groupId>org.flywaydb</groupId><artifactId>flyway-database-postgresql</artifactId></dependency>
        <dependency><groupId>org.apache.commons</groupId><artifactId>commons-csv</artifactId><version>1.11.0</version></dependency>
        <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-test</artifactId><scope>test</scope></dependency>
        <dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka-test</artifactId><scope>test</scope></dependency>
        <dependency><groupId>org.testcontainers</groupId><artifactId>junit-jupiter</artifactId><scope>test</scope></dependency>
        <dependency><groupId>org.testcontainers</groupId><artifactId>kafka</artifactId><scope>test</scope></dependency>
        <dependency><groupId>org.testcontainers</groupId><artifactId>postgresql</artifactId><scope>test</scope></dependency>
        <dependency><groupId>org.awaitility</groupId><artifactId>awaitility</artifactId><version>4.2.2</version><scope>test</scope></dependency>
    </dependencies>
    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.testcontainers</groupId>
                <artifactId>testcontainers-bom</artifactId>
                <version>1.20.1</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>
    <build>
        <plugins>
            <plugin><groupId>org.springframework.boot</groupId><artifactId>spring-boot-maven-plugin</artifactId></plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 3: `src/main/resources/application.yml`**

```yaml
server:
  port: 8080
spring:
  application:
    name: iot-events
  datasource:
    url: jdbc:postgresql://${DB_HOST:localhost}:${DB_PORT:5432}/${DB_NAME:iotevents}
    username: ${DB_USER:iotevents}
    password: ${DB_PASSWORD:iotevents}
  jpa:
    hibernate:
      ddl-auto: validate
    open-in-view: false
  flyway:
    enabled: true
    locations: classpath:db/migration
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
    consumer:
      group-id: audit-service
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      auto-offset-reset: earliest
management:
  endpoints:
    web:
      exposure:
        include: health,prometheus,metrics
  endpoint:
    health:
      probes:
        enabled: true
```

- [ ] **Step 4: Main class**

```java
package com.intelbras.iotevents;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class IotEventsApplication {
    public static void main(String[] args) {
        SpringApplication.run(IotEventsApplication.class, args);
    }
}
```

- [ ] **Step 5: `.gitignore`** — append:

```
# Maven
target/

# IDE
.idea/
*.iml
.vscode/
```

- [ ] **Step 6: `docker-compose.yml`** (Kafka + Postgres only; app/observability added in Task 16)

```yaml
services:
  kafka:
    image: apache/kafka:3.8.0
    container_name: iotevents-kafka
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  postgres:
    image: postgres:16-alpine
    container_name: iotevents-postgres
    ports: ["5432:5432"]
    environment:
      POSTGRES_DB: iotevents
      POSTGRES_USER: iotevents
      POSTGRES_PASSWORD: iotevents
    volumes: ["iotevents-postgres-data:/var/lib/postgresql/data"]
volumes:
  iotevents-postgres-data:
```

- [ ] **Step 7: Verify compile** — `mvn -q -DskipTests compile` → `BUILD SUCCESS`
- [ ] **Step 8: Commit** — `git add pom.xml src/main/resources/application.yml src/main/java/com/intelbras/iotevents/IotEventsApplication.java .gitignore docker-compose.yml && git commit -m "chore: bootstrap Spring Boot project skeleton"`
- [ ] **Step 9: Merge** — `git checkout main && git merge --no-ff chore/project-bootstrap && git branch -d chore/project-bootstrap`

---

### Task 2: Common structured logging (`ExecutionLog`)

**Files:** Create `common/logging/{LogStep,ExecutionLog,StructuredLogger}.java`; Test `ExecutionLogTest.java`.

**Interfaces:** `ExecutionLog.start(Class<?>, String)`, `.context(String,Object)`, `.step(String[, Map])`, `.info(String)`, `.error(String, Throwable)`. Every later use case/listener/controller depends on this.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b feat/execution-log`

- [ ] **Step 2: Failing test**

```java
package com.intelbras.iotevents.common.logging;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutionLogTest {

    private ListAppender<ILoggingEvent> appender;
    private Logger rootLogger;

    @BeforeEach
    void attachAppender() {
        appender = new ListAppender<>();
        appender.start();
        rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        rootLogger.detachAppender(appender);
    }

    @Test
    void shouldEmitExactlyOneInfoLineOnSuccessPath() {
        ExecutionLog log = ExecutionLog.start(ExecutionLogTest.class, "execute")
            .context("eventId", "evt-1")
            .step("Did something")
            .step("Did something else", Map.of("count", 3));

        log.info("Completed successfully");

        assertThat(appender.list).hasSize(1);
        assertThat(appender.list.get(0).getFormattedMessage()).contains("Completed successfully", "evt-1", "Did something else");
    }

    @Test
    void shouldEmitExactlyOneErrorLineOnFailurePath() {
        ExecutionLog log = ExecutionLog.start(ExecutionLogTest.class, "execute").context("eventId", "evt-2");
        log.error("Failed", new RuntimeException("boom"));

        assertThat(appender.list).hasSize(1);
        assertThat(appender.list.get(0).getFormattedMessage()).contains("Failed", "evt-2");
    }
}
```

- [ ] **Step 3: Run, verify fails** — `mvn -q test -Dtest=ExecutionLogTest`

- [ ] **Step 4: `LogStep`**

```java
package com.intelbras.iotevents.common.logging;

import java.util.Map;

public record LogStep(String message, Map<String, Object> data) {
}
```

- [ ] **Step 5: `StructuredLogger`**

```java
package com.intelbras.iotevents.common.logging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class StructuredLogger {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    private StructuredLogger() {
    }

    static void info(String message, String module, String method, Instant startedAt,
                      Map<String, Object> context, List<LogStep> steps) {
        LoggerFactory.getLogger(module).info(render(message, module, method, startedAt, context, steps, null));
    }

    static void error(String message, String module, String method, Instant startedAt,
                       Map<String, Object> context, List<LogStep> steps, Throwable error) {
        LoggerFactory.getLogger(module).error(render(message, module, method, startedAt, context, steps, error), error);
    }

    private static String render(String message, String module, String method, Instant startedAt,
                                  Map<String, Object> context, List<LogStep> steps, Throwable error) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("message", message);
        payload.put("module", module);
        payload.put("method", method);
        payload.put("durationMs", Duration.between(startedAt, Instant.now()).toMillis());
        payload.put("context", context);
        payload.put("steps", steps);
        if (error != null) {
            payload.put("errorType", error.getClass().getName());
            payload.put("errorMessage", error.getMessage());
        }
        try {
            return message + " | " + MAPPER.writeValueAsString(payload);
        } catch (Exception serializationError) {
            return message + " | " + payload;
        }
    }
}
```

- [ ] **Step 6: `ExecutionLog`**

```java
package com.intelbras.iotevents.common.logging;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ExecutionLog {

    private final String module;
    private final String method;
    private final Instant startedAt = Instant.now();
    private final Map<String, Object> context = new LinkedHashMap<>();
    private final List<LogStep> steps = new ArrayList<>();

    private ExecutionLog(String module, String method) {
        this.module = module;
        this.method = method;
    }

    public static ExecutionLog start(Class<?> module, String method) {
        return new ExecutionLog(module.getName(), method);
    }

    public ExecutionLog context(String key, Object value) {
        context.put(key, value);
        return this;
    }

    public ExecutionLog step(String message) {
        return step(message, Map.of());
    }

    public ExecutionLog step(String message, Map<String, Object> data) {
        steps.add(new LogStep(message, data));
        return this;
    }

    public void info(String message) {
        StructuredLogger.info(message, module, method, startedAt, context, steps);
    }

    public void error(String message, Throwable error) {
        StructuredLogger.error(message, module, method, startedAt, context, steps, error);
    }
}
```

- [ ] **Step 7: Run, verify passes** — `mvn -q test -Dtest=ExecutionLogTest` → 2 passed
- [ ] **Step 8: Commit** — `git add src/main/java/com/intelbras/iotevents/common/logging src/test/java/com/intelbras/iotevents/common/logging && git commit -m "feat: add ExecutionLog structured one-log-per-behaviour logging"`
- [ ] **Step 9: Merge** — `git checkout main && git merge --no-ff feat/execution-log && git branch -d feat/execution-log`

---

### Task 3: Device domain core models (`EventCategory`, real field mapping)

**Files:**
- Create: `domains/device/core/{EventCategory,DeviceEventRow,DeviceEvent,DedupKeyGenerator}.java`, `domains/device/core/error/{DeviceDomainError,DeviceEventValidationError}.java`
- Test: `DeviceEventTest.java`, `DedupKeyGeneratorTest.java`

**Interfaces:** `DeviceEvent(String eventId, String deviceId, EventCategory eventCategory, String eventType, Instant timestamp, Map<String,Object> payload)` with `DeviceEvent.from(DeviceEventRow)`; `DeviceEventRow(String id, String deviceId, String msgTypeRaw, String timestampRaw, Map<String,Object> payload)`; `EventCategory.fromMsgType(String): EventCategory`; `DedupKeyGenerator.generate(String deviceId, String eventType, String timestampRaw, Map<String,Object> payload): String`.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b feat/device-core-models`

- [ ] **Step 2: Failing tests**

```java
package com.intelbras.iotevents.domains.device.core;

import com.intelbras.iotevents.domains.device.core.error.DeviceEventValidationError;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeviceEventTest {

    @Test
    void shouldBuildDeviceEventFromRowWithOwnId() {
        DeviceEventRow row = new DeviceEventRow("evt-1", "DEVICE-00014", "iotProperty",
            "2026-09-25T08:47:51.157000+00:00", Map.of("properties", Map.of("11800", 3)));

        DeviceEvent event = DeviceEvent.from(row);

        assertThat(event.eventId()).isEqualTo("evt-1");
        assertThat(event.deviceId()).isEqualTo("DEVICE-00014");
        assertThat(event.eventCategory()).isEqualTo(EventCategory.PROPERTY_CHANGE);
        assertThat(event.eventType()).isEqualTo("iotProperty");
        assertThat(event.timestamp()).isEqualTo(Instant.parse("2026-09-25T08:47:51.157000Z"));
    }

    @Test
    void shouldFallBackToHashEventIdWhenRowHasNoId() {
        DeviceEventRow row = new DeviceEventRow(null, "DEVICE-00066", "videoMotion",
            "2026-09-25T09:30:11.000000+00:00", Map.of());

        DeviceEvent event = DeviceEvent.from(row);

        assertThat(event.eventId()).isEqualTo(
            DedupKeyGenerator.generate("DEVICE-00066", "videoMotion", "2026-09-25T09:30:11.000000+00:00", Map.of()));
    }

    @Test
    void shouldClassifyMotionDetectionEventsCorrectly() {
        DeviceEventRow row = new DeviceEventRow("evt-2", "DEVICE-00077", "mobileDetect",
            "2026-09-25T09:31:00.000000+00:00", Map.of());

        assertThat(DeviceEvent.from(row).eventCategory()).isEqualTo(EventCategory.MOTION_DETECTION);
    }

    @Test
    void shouldFallBackToOtherCategoryForUnrecognizedMsgType() {
        DeviceEventRow row = new DeviceEventRow("evt-3", "DEVICE-00099", "someFutureMsgType",
            "2026-09-25T09:32:00.000000+00:00", Map.of());

        assertThat(DeviceEvent.from(row).eventCategory()).isEqualTo(EventCategory.OTHER);
    }

    @Test
    void shouldRejectRowWithoutDeviceId() {
        DeviceEventRow row = new DeviceEventRow("evt-4", "", "iotProperty", "2026-09-25T08:47:51.157000+00:00", Map.of());

        assertThatThrownBy(() -> DeviceEvent.from(row)).isInstanceOf(DeviceEventValidationError.class);
    }

    @Test
    void shouldRejectRowWithInvalidTimestamp() {
        DeviceEventRow row = new DeviceEventRow("evt-5", "DEVICE-00014", "iotProperty", "not-a-timestamp", Map.of());

        assertThatThrownBy(() -> DeviceEvent.from(row)).isInstanceOf(DeviceEventValidationError.class);
    }
}
```

```java
package com.intelbras.iotevents.domains.device.core;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DedupKeyGeneratorTest {

    @Test
    void shouldBeDeterministicForTheSameInputs() {
        String first = DedupKeyGenerator.generate("DEVICE-00014", "iotProperty", "2026-09-25T08:47:51.157000+00:00", Map.of("x", 1));
        String second = DedupKeyGenerator.generate("DEVICE-00014", "iotProperty", "2026-09-25T08:47:51.157000+00:00", Map.of("x", 1));

        assertThat(first).isEqualTo(second);
    }

    @Test
    void shouldDifferWhenAnyInputDiffers() {
        String first = DedupKeyGenerator.generate("DEVICE-00014", "iotProperty", "2026-09-25T08:47:51.157000+00:00", Map.of());
        String second = DedupKeyGenerator.generate("DEVICE-00015", "iotProperty", "2026-09-25T08:47:51.157000+00:00", Map.of());

        assertThat(first).isNotEqualTo(second);
    }
}
```

- [ ] **Step 3: Run, verify fails** — `mvn -q test -Dtest=DeviceEventTest,DedupKeyGeneratorTest`

- [ ] **Step 4: Error classes**

```java
package com.intelbras.iotevents.domains.device.core.error;

public class DeviceDomainError extends RuntimeException {
    public DeviceDomainError(String message) {
        super(message);
    }
}
```

```java
package com.intelbras.iotevents.domains.device.core.error;

public class DeviceEventValidationError extends DeviceDomainError {
    public DeviceEventValidationError(String message) {
        super(message);
    }
}
```

- [ ] **Step 5: `EventCategory`**

```java
package com.intelbras.iotevents.domains.device.core;

import java.util.Set;

public enum EventCategory {
    PROPERTY_CHANGE, STATUS_CHANGE, MOTION_DETECTION, OTHER;

    private static final Set<String> PROPERTY_CHANGE_TYPES = Set.of("iotProperty");
    private static final Set<String> STATUS_CHANGE_TYPES = Set.of("online", "offline");
    private static final Set<String> MOTION_DETECTION_TYPES = Set.of("videoMotion", "mobileDetect", "human", "smartVideoMotion");

    public static EventCategory fromMsgType(String msgType) {
        if (msgType == null) {
            return OTHER;
        }
        String trimmed = msgType.trim();
        if (PROPERTY_CHANGE_TYPES.contains(trimmed)) {
            return PROPERTY_CHANGE;
        }
        if (STATUS_CHANGE_TYPES.contains(trimmed)) {
            return STATUS_CHANGE;
        }
        if (MOTION_DETECTION_TYPES.contains(trimmed)) {
            return MOTION_DETECTION;
        }
        return OTHER;
    }
}
```

- [ ] **Step 6: `DedupKeyGenerator`**

```java
package com.intelbras.iotevents.domains.device.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;

public final class DedupKeyGenerator {

    private DedupKeyGenerator() {
    }

    public static String generate(String deviceId, String eventType, String timestampRaw, Map<String, Object> payload) {
        String canonicalPayload = new TreeMap<>(payload).toString();
        String input = deviceId + "|" + eventType + "|" + timestampRaw + "|" + canonicalPayload;
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 not available", impossible);
        }
    }
}
```

- [ ] **Step 7: `DeviceEventRow`**

```java
package com.intelbras.iotevents.domains.device.core;

import java.util.Map;

public record DeviceEventRow(String id, String deviceId, String msgTypeRaw, String timestampRaw,
                              Map<String, Object> payload) {
}
```

- [ ] **Step 8: `DeviceEvent`**

```java
package com.intelbras.iotevents.domains.device.core;

import com.intelbras.iotevents.domains.device.core.error.DeviceEventValidationError;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Map;

public record DeviceEvent(String eventId, String deviceId, EventCategory eventCategory, String eventType,
                           Instant timestamp, Map<String, Object> payload) {

    public static DeviceEvent from(DeviceEventRow row) {
        if (row.deviceId() == null || row.deviceId().isBlank()) {
            throw new DeviceEventValidationError("deviceId is required, row=" + row);
        }
        if (row.timestampRaw() == null || row.timestampRaw().isBlank()) {
            throw new DeviceEventValidationError("timestamp is required, row=" + row);
        }

        Instant timestamp;
        try {
            timestamp = OffsetDateTime.parse(row.timestampRaw()).toInstant();
        } catch (DateTimeParseException invalidTimestamp) {
            throw new DeviceEventValidationError(
                "invalid timestamp '" + row.timestampRaw() + "' for deviceId=" + row.deviceId());
        }

        String eventType = (row.msgTypeRaw() == null || row.msgTypeRaw().isBlank())
            ? "unknown"
            : row.msgTypeRaw().trim();
        EventCategory eventCategory = EventCategory.fromMsgType(eventType);
        Map<String, Object> payload = row.payload() == null ? Map.of() : row.payload();

        String eventId = (row.id() != null && !row.id().isBlank())
            ? row.id()
            : DedupKeyGenerator.generate(row.deviceId(), eventType, row.timestampRaw(), payload);

        return new DeviceEvent(eventId, row.deviceId(), eventCategory, eventType, timestamp, payload);
    }
}
```

- [ ] **Step 9: Run, verify passes** — `mvn -q test -Dtest=DeviceEventTest,DedupKeyGeneratorTest` → 8 passed
- [ ] **Step 10: Commit** — `git add src/main/java/com/intelbras/iotevents/domains/device/core src/test/java/com/intelbras/iotevents/domains/device/core && git commit -m "feat: add device domain core models with real-data field mapping"`
- [ ] **Step 11: Merge** — `git checkout main && git merge --no-ff feat/device-core-models && git branch -d feat/device-core-models`

---

### Task 4: Device domain ports and `SimulateIngestionUseCase`

**Files:** Create `domains/device/core/port/{EventSourcePort,EventPublisherPort}.java`, `domains/device/core/usecase/SimulateIngestionUseCase.java`; Test `SimulateIngestionUseCaseTest.java`.

**Interfaces:** `EventSourcePort.readAll(): List<DeviceEvent>`; `EventPublisherPort.publish(DeviceEvent)`; `SimulateIngestionUseCase(EventSourcePort, EventPublisherPort).execute(): int`.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b feat/simulate-ingestion-usecase`

- [ ] **Step 2: Failing test**

```java
package com.intelbras.iotevents.domains.device.core.usecase;

import com.intelbras.iotevents.domains.device.core.DeviceEvent;
import com.intelbras.iotevents.domains.device.core.EventCategory;
import com.intelbras.iotevents.domains.device.core.port.EventPublisherPort;
import com.intelbras.iotevents.domains.device.core.port.EventSourcePort;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SimulateIngestionUseCaseTest {

    @Test
    void shouldPublishEveryEventReadFromTheSource() {
        DeviceEvent eventOne = new DeviceEvent("evt-1", "DEVICE-00014", EventCategory.PROPERTY_CHANGE, "iotProperty",
            Instant.parse("2026-09-25T08:47:51.157000Z"), Map.of());
        DeviceEvent eventTwo = new DeviceEvent("evt-2", "DEVICE-00066", EventCategory.MOTION_DETECTION, "videoMotion",
            Instant.parse("2026-09-25T09:30:11.000000Z"), Map.of());

        EventSourcePort source = () -> List.of(eventOne, eventTwo);
        List<DeviceEvent> published = new ArrayList<>();
        EventPublisherPort publisher = published::add;

        SimulateIngestionUseCase useCase = new SimulateIngestionUseCase(source, publisher);

        assertThat(useCase.execute()).isEqualTo(2);
        assertThat(published).containsExactly(eventOne, eventTwo);
    }

    @Test
    void shouldReturnZeroWhenSourceHasNoEvents() {
        EventSourcePort source = List::of;
        EventPublisherPort publisher = event -> { throw new AssertionError("should not publish anything"); };

        assertThat(new SimulateIngestionUseCase(source, publisher).execute()).isZero();
    }
}
```

- [ ] **Step 3: Run, verify fails** — `mvn -q test -Dtest=SimulateIngestionUseCaseTest`

- [ ] **Step 4: Ports**

```java
package com.intelbras.iotevents.domains.device.core.port;

import com.intelbras.iotevents.domains.device.core.DeviceEvent;

import java.util.List;

public interface EventSourcePort {
    List<DeviceEvent> readAll();
}
```

```java
package com.intelbras.iotevents.domains.device.core.port;

import com.intelbras.iotevents.domains.device.core.DeviceEvent;

public interface EventPublisherPort {
    void publish(DeviceEvent event);
}
```

- [ ] **Step 5: `SimulateIngestionUseCase`**

```java
package com.intelbras.iotevents.domains.device.core.usecase;

import com.intelbras.iotevents.common.logging.ExecutionLog;
import com.intelbras.iotevents.domains.device.core.DeviceEvent;
import com.intelbras.iotevents.domains.device.core.port.EventPublisherPort;
import com.intelbras.iotevents.domains.device.core.port.EventSourcePort;

import java.util.List;
import java.util.Map;

public class SimulateIngestionUseCase {

    private final EventSourcePort eventSourcePort;
    private final EventPublisherPort eventPublisherPort;

    public SimulateIngestionUseCase(EventSourcePort eventSourcePort, EventPublisherPort eventPublisherPort) {
        this.eventSourcePort = eventSourcePort;
        this.eventPublisherPort = eventPublisherPort;
    }

    public int execute() {
        ExecutionLog log = ExecutionLog.start(SimulateIngestionUseCase.class, "execute");
        try {
            List<DeviceEvent> events = eventSourcePort.readAll();
            log.step("Read events from source", Map.of("count", events.size()));

            for (DeviceEvent event : events) {
                eventPublisherPort.publish(event);
            }
            log.step("Published events to Kafka", Map.of("count", events.size()));

            log.info("Simulated ingestion completed");
            return events.size();
        } catch (Exception ex) {
            log.step("Unhandled exception while simulating ingestion");
            log.error("Failed to simulate ingestion", ex);
            throw ex;
        }
    }
}
```

- [ ] **Step 6: Run, verify passes** — `mvn -q test -Dtest=SimulateIngestionUseCaseTest` → 2 passed
- [ ] **Step 7: Commit** — `git add src/main/java/com/intelbras/iotevents/domains/device/core/port src/main/java/com/intelbras/iotevents/domains/device/core/usecase src/test/java/com/intelbras/iotevents/domains/device/core/usecase && git commit -m "feat: add device domain ports and SimulateIngestionUseCase"`
- [ ] **Step 8: Merge** — `git checkout main && git merge --no-ff feat/simulate-ingestion-usecase && git branch -d feat/simulate-ingestion-usecase`

---

### Task 5: CSV event source adapter + real dataset

**Files:**
- Create: `src/main/resources/events.csv` (real 100 rows + 4 synthetic)
- Create: `domains/device/adapters/csv/CsvEventSourceAdapter.java`
- Test: `CsvEventSourceAdapterTest.java`

**Interfaces:** `CsvEventSourceAdapter(ObjectMapper) implements EventSourcePort`.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b feat/csv-event-source-adapter`

- [ ] **Step 2: Build the dataset file** — copy the real CSV, then append 4 synthetic DLQ-demo rows:

```bash
cp "case_information/case_backend_senior_eventos_sanitizados 1.csv" src/main/resources/events.csv
cat >> src/main/resources/events.csv <<'EOF'
101,2026-09-25T10:00:00.000000+00:00,iotProperty,SYNTHETIC-DLQ-001,Synthetic DLQ Demo 1,PRODUCT-DEMO,,,20260925T100000,USER-DEMO,PROJECT-CASE,"{""simulateFailure"":true}","{""content"":{""simulateFailure"":true},""did"":""SYNTHETIC-DLQ-001"",""dname"":""Synthetic DLQ Demo 1"",""msgType"":""iotProperty"",""pid"":""PRODUCT-DEMO"",""utcTime"":""20260925T100000"",""appId"":""APP-CASE"",""userId"":""USER-DEMO"",""oauthProject"":""PROJECT-CASE""}"
102,2026-09-25T10:01:00.000000+00:00,online,SYNTHETIC-DLQ-002,Synthetic DLQ Demo 2,PRODUCT-DEMO,,,20260925T100100,USER-DEMO,PROJECT-CASE,"{""simulateFailure"":true}","{""content"":{""simulateFailure"":true},""did"":""SYNTHETIC-DLQ-002"",""dname"":""Synthetic DLQ Demo 2"",""msgType"":""online"",""pid"":""PRODUCT-DEMO"",""utcTime"":""20260925T100100"",""appId"":""APP-CASE"",""userId"":""USER-DEMO"",""oauthProject"":""PROJECT-CASE""}"
103,2026-09-25T10:02:00.000000+00:00,videoMotion,SYNTHETIC-DLQ-003,Synthetic DLQ Demo 3,,,,20260925T100200,USER-DEMO,PROJECT-CASE,"{""simulateFailure"":true}","{""accessType"":""PaaS"",""action"":""start"",""deviceType"":""IPC"",""did"":""SYNTHETIC-DLQ-003"",""dname"":""Synthetic DLQ Demo 3"",""labelType"":""motionAlarm"",""msgType"":""videoMotion"",""content"":{""simulateFailure"":true},""utcTime"":1790340000,""appId"":""APP-CASE"",""userId"":""USER-DEMO"",""oauthProject"":""PROJECT-CASE""}"
104,2026-09-25T10:03:00.000000+00:00,upgradeFail,SYNTHETIC-DLQ-004,Synthetic DLQ Demo 4,,,,20260925T100300,USER-DEMO,PROJECT-CASE,"{""simulateFailure"":true}","{""accessType"":""PaaS"",""deviceType"":""IPC"",""did"":""SYNTHETIC-DLQ-004"",""dname"":""Synthetic DLQ Demo 4"",""msgType"":""upgradeFail"",""content"":{""simulateFailure"":true},""utcTime"":1790340300,""appId"":""APP-CASE"",""userId"":""USER-DEMO"",""oauthProject"":""PROJECT-CASE""}"
EOF
```

These 4 rows are clearly labeled (`SYNTHETIC-DLQ-00N` ids), one per `EventCategory`, each carrying `simulateFailure: true` — see `DECISIONS.md` (Task 18) for why they exist.

- [ ] **Step 3: Failing test**

```java
package com.intelbras.iotevents.domains.device.adapters.csv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.intelbras.iotevents.domains.device.core.DeviceEvent;
import com.intelbras.iotevents.domains.device.core.EventCategory;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class CsvEventSourceAdapterTest {

    private final CsvEventSourceAdapter adapter = new CsvEventSourceAdapter(new ObjectMapper());

    @Test
    void shouldReadAllRealAndSyntheticRowsFromTheDataset() {
        assertThat(adapter.readAll()).hasSize(104);
    }

    @Test
    void shouldClassifyARealIotPropertyRowAsPropertyChange() {
        DeviceEvent event = adapter.readAll().stream()
            .filter(e -> e.deviceId().equals("DEVICE-00014"))
            .findFirst().orElseThrow();

        assertThat(event.eventCategory()).isEqualTo(EventCategory.PROPERTY_CHANGE);
        assertThat(event.eventType()).isEqualTo("iotProperty");
        assertThat(event.payload()).containsKey("properties");
    }

    @Test
    void shouldClassifyARealVideoMotionRowAsMotionDetection() {
        DeviceEvent event = adapter.readAll().stream()
            .filter(e -> e.deviceId().equals("DEVICE-00066"))
            .findFirst().orElseThrow();

        assertThat(event.eventCategory()).isEqualTo(EventCategory.MOTION_DETECTION);
    }

    @Test
    void shouldFallBackToBodyCompletoDeviceIdForBlankDidRows() {
        Set<String> informationTransferDeviceIds = adapter.readAll().stream()
            .filter(e -> e.eventType().equals("informationTransfer"))
            .map(DeviceEvent::deviceId)
            .collect(Collectors.toSet());

        assertThat(informationTransferDeviceIds).containsExactlyInAnyOrder("FJ1K2001704PS", "IA8L4001890ZN");
    }

    @Test
    void shouldCarrySimulateFailureFlagOnTheFourSyntheticDemoRowsAcrossAllCategories() {
        List<DeviceEvent> synthetic = adapter.readAll().stream()
            .filter(e -> e.deviceId().startsWith("SYNTHETIC-DLQ-"))
            .toList();

        assertThat(synthetic).hasSize(4);
        assertThat(synthetic).allSatisfy(e -> assertThat(e.payload()).containsEntry("simulateFailure", true));
        assertThat(synthetic.stream().map(DeviceEvent::eventCategory).collect(Collectors.toSet()))
            .containsExactlyInAnyOrder(EventCategory.PROPERTY_CHANGE, EventCategory.STATUS_CHANGE,
                EventCategory.MOTION_DETECTION, EventCategory.OTHER);
    }
}
```

- [ ] **Step 4: Run, verify fails** — `mvn -q test -Dtest=CsvEventSourceAdapterTest`

- [ ] **Step 5: `CsvEventSourceAdapter`**

```java
package com.intelbras.iotevents.domains.device.adapters.csv;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.intelbras.iotevents.common.logging.ExecutionLog;
import com.intelbras.iotevents.domains.device.core.DeviceEvent;
import com.intelbras.iotevents.domains.device.core.DeviceEventRow;
import com.intelbras.iotevents.domains.device.core.port.EventSourcePort;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class CsvEventSourceAdapter implements EventSourcePort {

    private static final String CSV_RESOURCE = "events.csv";

    private final ObjectMapper objectMapper;

    public CsvEventSourceAdapter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public List<DeviceEvent> readAll() {
        ExecutionLog log = ExecutionLog.start(CsvEventSourceAdapter.class, "readAll").context("resource", CSV_RESOURCE);
        List<DeviceEvent> events = new ArrayList<>();
        int skipped = 0;

        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(CSV_RESOURCE)) {
            if (inputStream == null) {
                throw new IllegalStateException("CSV resource not found: " + CSV_RESOURCE);
            }
            Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            CSVFormat format = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build();

            for (CSVRecord record : format.parse(reader)) {
                try {
                    events.add(DeviceEvent.from(toRow(record)));
                } catch (Exception rowError) {
                    skipped++;
                    log.step("Skipped malformed CSV row", Map.of(
                        "rowNumber", record.getRecordNumber(), "reason", String.valueOf(rowError.getMessage())));
                }
            }

            log.step("Parsed CSV rows", Map.of("valid", events.size(), "skipped", skipped));
            log.info("Read device events from CSV");
            return events;
        } catch (Exception ex) {
            log.step("Unhandled exception while reading CSV");
            log.error("Failed to read device events from CSV", ex);
            throw new IllegalStateException("Failed to read device events from CSV", ex);
        }
    }

    private DeviceEventRow toRow(CSVRecord record) {
        Map<String, Object> bodyCompleto = parseJsonObject(getOrNull(record, "body_completo"));
        String deviceId = resolveDeviceId(getOrNull(record, "did"), bodyCompleto);
        String nativeId = resolveNativeId(bodyCompleto);
        Map<String, Object> payload = parseJsonObject(getOrNull(record, "content"));

        return new DeviceEventRow(nativeId, deviceId, record.get("msgType"), record.get("timestamp_log"), payload);
    }

    private String resolveDeviceId(String did, Map<String, Object> bodyCompleto) {
        if (did != null && !did.isBlank()) {
            return did;
        }
        Object fallback = bodyCompleto.get("deviceId");
        return fallback != null ? String.valueOf(fallback) : "UNKNOWN";
    }

    private String resolveNativeId(Map<String, Object> bodyCompleto) {
        Object id = bodyCompleto.get("id");
        if (id != null) {
            return String.valueOf(id);
        }
        Object alarmId = bodyCompleto.get("alarmId");
        return alarmId != null ? String.valueOf(alarmId) : null;
    }

    private Map<String, Object> parseJsonObject(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {
            });
        } catch (Exception invalidJson) {
            return Map.of();
        }
    }

    private String getOrNull(CSVRecord record, String column) {
        return record.isMapped(column) && !record.get(column).isBlank() ? record.get(column) : null;
    }
}
```

- [ ] **Step 6: Run, verify passes** — `mvn -q test -Dtest=CsvEventSourceAdapterTest` → 4 passed
- [ ] **Step 7: Commit** — `git add src/main/resources/events.csv src/main/java/com/intelbras/iotevents/domains/device/adapters/csv src/test/java/com/intelbras/iotevents/domains/device/adapters/csv && git commit -m "feat: add CSV event source adapter over the real dataset"`
- [ ] **Step 8: Merge** — `git checkout main && git merge --no-ff feat/csv-event-source-adapter && git branch -d feat/csv-event-source-adapter`

---

### Task 6: Kafka topics configuration

**Files:** Create `config/KafkaTopics.java`, `config/KafkaTopicConfig.java`.

**Interfaces:** `KafkaTopics.DEVICE_PROPERTY_CHANGE_EVENTS`, `DEVICE_STATUS_CHANGE_EVENTS`, `DEVICE_MOTION_DETECTION_EVENTS`, `DEVICE_OTHER_EVENTS`, `DLT_SUFFIX`, `PARTITIONS`, `DLT_PARTITIONS`, `CONSUMER_GROUP`, `allTopics(): List<String>`.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b chore/kafka-topics-config`

- [ ] **Step 2: `KafkaTopics`**

```java
package com.intelbras.iotevents.config;

import java.util.List;

public final class KafkaTopics {

    public static final String DEVICE_PROPERTY_CHANGE_EVENTS = "device.property-change.events";
    public static final String DEVICE_STATUS_CHANGE_EVENTS = "device.status-change.events";
    public static final String DEVICE_MOTION_DETECTION_EVENTS = "device.motion-detection.events";
    public static final String DEVICE_OTHER_EVENTS = "device.other.events";
    public static final String DLT_SUFFIX = ".DLT";
    public static final int PARTITIONS = 3;
    public static final int DLT_PARTITIONS = 1;
    public static final String CONSUMER_GROUP = "audit-service";

    private KafkaTopics() {
    }

    public static List<String> allTopics() {
        return List.of(DEVICE_PROPERTY_CHANGE_EVENTS, DEVICE_STATUS_CHANGE_EVENTS,
            DEVICE_MOTION_DETECTION_EVENTS, DEVICE_OTHER_EVENTS);
    }
}
```

- [ ] **Step 3: `KafkaTopicConfig`**

```java
package com.intelbras.iotevents.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic devicePropertyChangeEventsTopic() {
        return topic(KafkaTopics.DEVICE_PROPERTY_CHANGE_EVENTS);
    }

    @Bean
    public NewTopic deviceStatusChangeEventsTopic() {
        return topic(KafkaTopics.DEVICE_STATUS_CHANGE_EVENTS);
    }

    @Bean
    public NewTopic deviceMotionDetectionEventsTopic() {
        return topic(KafkaTopics.DEVICE_MOTION_DETECTION_EVENTS);
    }

    @Bean
    public NewTopic deviceOtherEventsTopic() {
        return topic(KafkaTopics.DEVICE_OTHER_EVENTS);
    }

    @Bean
    public NewTopic devicePropertyChangeEventsDlt() {
        return dlt(KafkaTopics.DEVICE_PROPERTY_CHANGE_EVENTS);
    }

    @Bean
    public NewTopic deviceStatusChangeEventsDlt() {
        return dlt(KafkaTopics.DEVICE_STATUS_CHANGE_EVENTS);
    }

    @Bean
    public NewTopic deviceMotionDetectionEventsDlt() {
        return dlt(KafkaTopics.DEVICE_MOTION_DETECTION_EVENTS);
    }

    @Bean
    public NewTopic deviceOtherEventsDlt() {
        return dlt(KafkaTopics.DEVICE_OTHER_EVENTS);
    }

    private NewTopic topic(String name) {
        return TopicBuilder.name(name).partitions(KafkaTopics.PARTITIONS).replicas(1).build();
    }

    private NewTopic dlt(String baseTopicName) {
        return TopicBuilder.name(baseTopicName + KafkaTopics.DLT_SUFFIX).partitions(KafkaTopics.DLT_PARTITIONS).replicas(1).build();
    }
}
```

- [ ] **Step 4: Verify compile** — `mvn -q -DskipTests compile`
- [ ] **Step 5: Commit** — `git add src/main/java/com/intelbras/iotevents/config/KafkaTopics.java src/main/java/com/intelbras/iotevents/config/KafkaTopicConfig.java && git commit -m "chore: declare Kafka topics and DLT provisioning"`
- [ ] **Step 6: Merge** — `git checkout main && git merge --no-ff chore/kafka-topics-config && git branch -d chore/kafka-topics-config`

---

### Task 7: Kafka event publisher adapter + simulation REST endpoint

**Files:** Create `domains/device/adapters/kafka/KafkaEventPublisherAdapter.java`, `domains/device/adapters/web/EventSimulationController.java`, `domains/device/DeviceUseCaseConfig.java`; Test both adapter and controller.

**Interfaces:** `KafkaEventPublisherAdapter implements EventPublisherPort`; `POST /events/simulate` → `{"publishedCount": <int>}`.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b feat/kafka-publisher-and-simulation-endpoint`

- [ ] **Step 2: Failing publisher test**

```java
package com.intelbras.iotevents.domains.device.adapters.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.intelbras.iotevents.config.KafkaTopics;
import com.intelbras.iotevents.domains.device.core.DeviceEvent;
import com.intelbras.iotevents.domains.device.core.EventCategory;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class KafkaEventPublisherAdapterTest {

    @Test
    void shouldPublishToThePropertyChangeTopicKeyedByDeviceId() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        KafkaEventPublisherAdapter adapter = new KafkaEventPublisherAdapter(kafkaTemplate, new ObjectMapper());

        DeviceEvent event = new DeviceEvent("evt-1", "DEVICE-00014", EventCategory.PROPERTY_CHANGE, "iotProperty",
            Instant.parse("2026-09-25T08:47:51.157000Z"), Map.of("properties", Map.of("11800", 3)));

        adapter.publish(event);

        verify(kafkaTemplate).send(KafkaTopics.DEVICE_PROPERTY_CHANGE_EVENTS, "DEVICE-00014", anyString());
    }

    @Test
    void shouldPublishToTheOtherTopicForOtherCategory() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        KafkaEventPublisherAdapter adapter = new KafkaEventPublisherAdapter(kafkaTemplate, new ObjectMapper());

        DeviceEvent event = new DeviceEvent("evt-2", "SYNTHETIC-DLQ-004", EventCategory.OTHER, "upgradeFail",
            Instant.parse("2026-09-25T10:03:00Z"), Map.of());

        adapter.publish(event);

        verify(kafkaTemplate).send(KafkaTopics.DEVICE_OTHER_EVENTS, "SYNTHETIC-DLQ-004", anyString());
    }
}
```

- [ ] **Step 3: Failing controller slice test**

```java
package com.intelbras.iotevents.domains.device.adapters.web;

import com.intelbras.iotevents.domains.device.core.usecase.SimulateIngestionUseCase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(EventSimulationController.class)
class EventSimulationControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private SimulateIngestionUseCase simulateIngestionUseCase;

    @Test
    void shouldReturnPublishedCountFromTheUseCase() throws Exception {
        when(simulateIngestionUseCase.execute()).thenReturn(104);

        mockMvc.perform(post("/events/simulate"))
            .andExpect(status().isOk())
            .andExpect(content().json("{\"publishedCount\":104}"));
    }
}
```

- [ ] **Step 4: Run, verify both fail** — `mvn -q test -Dtest=KafkaEventPublisherAdapterTest,EventSimulationControllerTest`

- [ ] **Step 5: `KafkaEventPublisherAdapter`**

```java
package com.intelbras.iotevents.domains.device.adapters.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.intelbras.iotevents.common.logging.ExecutionLog;
import com.intelbras.iotevents.config.KafkaTopics;
import com.intelbras.iotevents.domains.device.core.DeviceEvent;
import com.intelbras.iotevents.domains.device.core.EventCategory;
import com.intelbras.iotevents.domains.device.core.port.EventPublisherPort;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class KafkaEventPublisherAdapter implements EventPublisherPort {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaEventPublisherAdapter(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public void publish(DeviceEvent event) {
        ExecutionLog log = ExecutionLog.start(KafkaEventPublisherAdapter.class, "publish")
            .context("eventId", event.eventId())
            .context("deviceId", event.deviceId())
            .context("eventCategory", event.eventCategory());
        try {
            String topic = resolveTopic(event.eventCategory());
            String payload = objectMapper.writeValueAsString(toWireMessage(event));
            log.context("topic", topic).step("Serialized device event to wire JSON");

            kafkaTemplate.send(topic, event.deviceId(), payload);
            log.step("Published event to Kafka", Map.of("topic", topic, "key", event.deviceId()));

            log.info("Published device event");
        } catch (Exception ex) {
            log.step("Unhandled exception while publishing device event");
            log.error("Failed to publish device event", ex);
            throw new IllegalStateException("Failed to publish device event eventId=" + event.eventId(), ex);
        }
    }

    private String resolveTopic(EventCategory eventCategory) {
        return switch (eventCategory) {
            case PROPERTY_CHANGE -> KafkaTopics.DEVICE_PROPERTY_CHANGE_EVENTS;
            case STATUS_CHANGE -> KafkaTopics.DEVICE_STATUS_CHANGE_EVENTS;
            case MOTION_DETECTION -> KafkaTopics.DEVICE_MOTION_DETECTION_EVENTS;
            case OTHER -> KafkaTopics.DEVICE_OTHER_EVENTS;
        };
    }

    private Map<String, Object> toWireMessage(DeviceEvent event) {
        return Map.of(
            "eventId", event.eventId(),
            "deviceId", event.deviceId(),
            "eventCategory", event.eventCategory().name(),
            "eventType", event.eventType(),
            "timestamp", event.timestamp().toString(),
            "payload", event.payload());
    }
}
```

- [ ] **Step 6: `EventSimulationController`**

```java
package com.intelbras.iotevents.domains.device.adapters.web;

import com.intelbras.iotevents.domains.device.core.usecase.SimulateIngestionUseCase;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/events")
public class EventSimulationController {

    private final SimulateIngestionUseCase simulateIngestionUseCase;

    public EventSimulationController(SimulateIngestionUseCase simulateIngestionUseCase) {
        this.simulateIngestionUseCase = simulateIngestionUseCase;
    }

    @PostMapping("/simulate")
    public ResponseEntity<SimulationResponse> simulate() {
        return ResponseEntity.ok(new SimulationResponse(simulateIngestionUseCase.execute()));
    }

    public record SimulationResponse(int publishedCount) {
    }
}
```

- [ ] **Step 7: Register the use case bean**

```java
package com.intelbras.iotevents.domains.device;

import com.intelbras.iotevents.domains.device.core.port.EventPublisherPort;
import com.intelbras.iotevents.domains.device.core.port.EventSourcePort;
import com.intelbras.iotevents.domains.device.core.usecase.SimulateIngestionUseCase;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DeviceUseCaseConfig {

    @Bean
    public SimulateIngestionUseCase simulateIngestionUseCase(EventSourcePort eventSourcePort,
                                                               EventPublisherPort eventPublisherPort) {
        return new SimulateIngestionUseCase(eventSourcePort, eventPublisherPort);
    }
}
```

- [ ] **Step 8: Run, verify both pass** — `mvn -q test -Dtest=KafkaEventPublisherAdapterTest,EventSimulationControllerTest` → 3 passed
- [ ] **Step 9: Commit** — `git add src/main/java/com/intelbras/iotevents/domains/device/adapters/kafka src/main/java/com/intelbras/iotevents/domains/device/adapters/web src/main/java/com/intelbras/iotevents/domains/device/DeviceUseCaseConfig.java src/test/java/com/intelbras/iotevents/domains/device/adapters/kafka src/test/java/com/intelbras/iotevents/domains/device/adapters/web && git commit -m "feat: add Kafka publisher adapter and simulation REST endpoint"`
- [ ] **Step 10: Merge** — `git checkout main && git merge --no-ff feat/kafka-publisher-and-simulation-endpoint && git branch -d feat/kafka-publisher-and-simulation-endpoint`

---

### Task 8: Audit domain core models

**Files:** Create `domains/audit/core/{IncomingDeviceEvent,AuditRecord,AuditQueryFilter,KafkaMessageContext}.java`, `domains/audit/core/error/{AuditDomainError,SimulatedProcessingFailureError}.java`, `domains/audit/core/port/AuditRepositoryPort.java`; Test `AuditRecordTest.java`.

**Interfaces:** `IncomingDeviceEvent(String eventId, String deviceId, String eventCategory, String eventType, Instant timestamp, Map<String,Object> payload)` — audit's own representation, never `device.core.DeviceEvent`; `AuditRecord(String dedupKey, String deviceId, String eventCategory, String eventType, String generalInfo, Instant consumedAt, Instant createdAt)` with `AuditRecord.processedNow(IncomingDeviceEvent, String)`; `AuditQueryFilter(String deviceId, String eventType, Instant from, Instant to)`; `KafkaMessageContext(String topic, int partition, long offset, String key)`; `AuditRepositoryPort.save/existsByDedupKey/query`.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b feat/audit-core-models`

- [ ] **Step 2: Failing test**

```java
package com.intelbras.iotevents.domains.audit.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AuditRecordTest {

    @Test
    void shouldBuildAProcessedAuditRecordFromAnIncomingEvent() {
        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-1", "DEVICE-00014", "PROPERTY_CHANGE", "iotProperty",
            Instant.parse("2026-09-25T08:47:51.157000Z"), Map.of("properties", Map.of("11800", 3)));

        AuditRecord record = AuditRecord.processedNow(event, "Device DEVICE-00014 reported a property change");

        assertThat(record.dedupKey()).isEqualTo("evt-1");
        assertThat(record.deviceId()).isEqualTo("DEVICE-00014");
        assertThat(record.eventCategory()).isEqualTo("PROPERTY_CHANGE");
        assertThat(record.eventType()).isEqualTo("iotProperty");
        assertThat(record.generalInfo()).isEqualTo("Device DEVICE-00014 reported a property change");
        assertThat(record.consumedAt()).isEqualTo(record.createdAt());
    }
}
```

- [ ] **Step 3: Run, verify fails** — `mvn -q test -Dtest=AuditRecordTest`

- [ ] **Step 4: `IncomingDeviceEvent`**

```java
package com.intelbras.iotevents.domains.audit.core;

import java.time.Instant;
import java.util.Map;

public record IncomingDeviceEvent(String eventId, String deviceId, String eventCategory, String eventType,
                                   Instant timestamp, Map<String, Object> payload) {
}
```

- [ ] **Step 5: `AuditRecord`**

```java
package com.intelbras.iotevents.domains.audit.core;

import java.time.Instant;

public record AuditRecord(String dedupKey, String deviceId, String eventCategory, String eventType,
                           String generalInfo, Instant consumedAt, Instant createdAt) {

    public static AuditRecord processedNow(IncomingDeviceEvent event, String generalInfo) {
        Instant now = Instant.now();
        return new AuditRecord(event.eventId(), event.deviceId(), event.eventCategory(), event.eventType(),
            generalInfo, now, now);
    }
}
```

- [ ] **Step 6: `AuditQueryFilter` and `KafkaMessageContext`**

```java
package com.intelbras.iotevents.domains.audit.core;

import java.time.Instant;

public record AuditQueryFilter(String deviceId, String eventType, Instant from, Instant to) {
}
```

```java
package com.intelbras.iotevents.domains.audit.core;

public record KafkaMessageContext(String topic, int partition, long offset, String key) {
}
```

- [ ] **Step 7: Error hierarchy**

```java
package com.intelbras.iotevents.domains.audit.core.error;

public class AuditDomainError extends RuntimeException {
    public AuditDomainError(String message) {
        super(message);
    }
}
```

```java
package com.intelbras.iotevents.domains.audit.core.error;

public class SimulatedProcessingFailureError extends AuditDomainError {
    public SimulatedProcessingFailureError(String message) {
        super(message);
    }
}
```

- [ ] **Step 8: `AuditRepositoryPort`**

```java
package com.intelbras.iotevents.domains.audit.core.port;

import com.intelbras.iotevents.domains.audit.core.AuditQueryFilter;
import com.intelbras.iotevents.domains.audit.core.AuditRecord;

import java.util.List;

public interface AuditRepositoryPort {
    void save(AuditRecord record);

    boolean existsByDedupKey(String eventId);

    List<AuditRecord> query(AuditQueryFilter filter);
}
```

- [ ] **Step 9: Run, verify passes** — `mvn -q test -Dtest=AuditRecordTest` → 1 passed
- [ ] **Step 10: Commit** — `git add src/main/java/com/intelbras/iotevents/domains/audit/core src/test/java/com/intelbras/iotevents/domains/audit/core && git commit -m "feat: add audit domain core models and repository port"`
- [ ] **Step 11: Merge** — `git checkout main && git merge --no-ff feat/audit-core-models && git branch -d feat/audit-core-models`

---

### Task 9: Audit event handlers (per `EventCategory`)

**Files:** Create `domains/audit/core/handler/{DeviceEventAuditHandler,PropertyChangeEventAuditHandler,StatusChangeEventAuditHandler,MotionDetectionEventAuditHandler,OtherEventAuditHandler}.java`; Test `DeviceEventAuditHandlersTest.java`.

**Interfaces:** `DeviceEventAuditHandler.supports(String eventCategory): boolean`, `.handle(IncomingDeviceEvent): String`. `OtherEventAuditHandler` is the designated fallback (Task 10 injects it separately).

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b feat/audit-device-event-handlers`

- [ ] **Step 2: Failing test**

```java
package com.intelbras.iotevents.domains.audit.core.handler;

import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DeviceEventAuditHandlersTest {

    private final PropertyChangeEventAuditHandler propertyChangeHandler = new PropertyChangeEventAuditHandler();
    private final StatusChangeEventAuditHandler statusChangeHandler = new StatusChangeEventAuditHandler();
    private final MotionDetectionEventAuditHandler motionDetectionHandler = new MotionDetectionEventAuditHandler();
    private final OtherEventAuditHandler otherHandler = new OtherEventAuditHandler();

    @Test
    void propertyChangeHandlerShouldSupportOnlyItsCategory() {
        assertThat(propertyChangeHandler.supports("PROPERTY_CHANGE")).isTrue();
        assertThat(propertyChangeHandler.supports("STATUS_CHANGE")).isFalse();
    }

    @Test
    void propertyChangeHandlerShouldDescribeTheEvent() {
        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-1", "DEVICE-00014", "PROPERTY_CHANGE", "iotProperty",
            Instant.parse("2026-09-25T08:47:51.157000Z"), Map.of("properties", Map.of("11800", 3)));

        assertThat(propertyChangeHandler.handle(event)).contains("DEVICE-00014").contains("iotProperty");
    }

    @Test
    void statusChangeHandlerShouldSupportOnlyItsCategory() {
        assertThat(statusChangeHandler.supports("STATUS_CHANGE")).isTrue();
        assertThat(statusChangeHandler.supports("MOTION_DETECTION")).isFalse();
    }

    @Test
    void motionDetectionHandlerShouldDescribeTheEvent() {
        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-2", "DEVICE-00066", "MOTION_DETECTION", "videoMotion",
            Instant.parse("2026-09-25T09:30:11Z"), Map.of());

        assertThat(motionDetectionHandler.handle(event)).contains("DEVICE-00066").contains("videoMotion");
    }

    @Test
    void otherHandlerShouldSupportOnlyOtherCategoryButDescribeAnyEvent() {
        assertThat(otherHandler.supports("OTHER")).isTrue();
        assertThat(otherHandler.supports("PROPERTY_CHANGE")).isFalse();

        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-3", "FJ1K2001704PS", "OTHER", "informationTransfer",
            Instant.parse("2026-09-25T09:45:37Z"), Map.of());

        assertThat(otherHandler.handle(event)).contains("FJ1K2001704PS");
    }
}
```

- [ ] **Step 3: Run, verify fails** — `mvn -q test -Dtest=DeviceEventAuditHandlersTest`

- [ ] **Step 4: `DeviceEventAuditHandler`**

```java
package com.intelbras.iotevents.domains.audit.core.handler;

import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;

public interface DeviceEventAuditHandler {
    boolean supports(String eventCategory);

    String handle(IncomingDeviceEvent event);
}
```

- [ ] **Step 5: The four handlers**

```java
package com.intelbras.iotevents.domains.audit.core.handler;

import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import org.springframework.stereotype.Component;

@Component
public class PropertyChangeEventAuditHandler implements DeviceEventAuditHandler {

    @Override
    public boolean supports(String eventCategory) {
        return "PROPERTY_CHANGE".equalsIgnoreCase(eventCategory);
    }

    @Override
    public String handle(IncomingDeviceEvent event) {
        return "Device " + event.deviceId() + " reported a property change (" + event.eventType()
            + ") with payload " + event.payload() + " at " + event.timestamp();
    }
}
```

```java
package com.intelbras.iotevents.domains.audit.core.handler;

import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import org.springframework.stereotype.Component;

@Component
public class StatusChangeEventAuditHandler implements DeviceEventAuditHandler {

    @Override
    public boolean supports(String eventCategory) {
        return "STATUS_CHANGE".equalsIgnoreCase(eventCategory);
    }

    @Override
    public String handle(IncomingDeviceEvent event) {
        return "Device " + event.deviceId() + " changed connectivity status to " + event.eventType()
            + " at " + event.timestamp();
    }
}
```

```java
package com.intelbras.iotevents.domains.audit.core.handler;

import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import org.springframework.stereotype.Component;

@Component
public class MotionDetectionEventAuditHandler implements DeviceEventAuditHandler {

    @Override
    public boolean supports(String eventCategory) {
        return "MOTION_DETECTION".equalsIgnoreCase(eventCategory);
    }

    @Override
    public String handle(IncomingDeviceEvent event) {
        return "Device " + event.deviceId() + " detected motion (" + event.eventType() + ") at " + event.timestamp();
    }
}
```

```java
package com.intelbras.iotevents.domains.audit.core.handler;

import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import org.springframework.stereotype.Component;

@Component
public class OtherEventAuditHandler implements DeviceEventAuditHandler {

    @Override
    public boolean supports(String eventCategory) {
        return "OTHER".equalsIgnoreCase(eventCategory);
    }

    @Override
    public String handle(IncomingDeviceEvent event) {
        return "Device " + event.deviceId() + " reported " + event.eventType() + " at " + event.timestamp();
    }
}
```

- [ ] **Step 6: Run, verify passes** — `mvn -q test -Dtest=DeviceEventAuditHandlersTest` → 5 passed
- [ ] **Step 7: Commit** — `git add src/main/java/com/intelbras/iotevents/domains/audit/core/handler src/test/java/com/intelbras/iotevents/domains/audit/core/handler && git commit -m "feat: add per-event-category audit handlers"`
- [ ] **Step 8: Merge** — `git checkout main && git merge --no-ff feat/audit-device-event-handlers && git branch -d feat/audit-device-event-handlers`

---

### Task 10: Audit use cases + metrics

**Files:** Create `common/metrics/EventMetrics.java`, `domains/audit/core/usecase/{RecordDeviceEventAuditUseCase,QueryAuditsUseCase}.java`; Test both.

**Interfaces:** `RecordDeviceEventAuditUseCase(AuditRepositoryPort, List<DeviceEventAuditHandler>, OtherEventAuditHandler, EventMetrics).execute(IncomingDeviceEvent, KafkaMessageContext)`; `QueryAuditsUseCase(AuditRepositoryPort).execute(AuditQueryFilter): List<AuditRecord>`.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b feat/audit-use-cases`

- [ ] **Step 2: Failing tests**

```java
package com.intelbras.iotevents.domains.audit.core.usecase;

import com.intelbras.iotevents.common.metrics.EventMetrics;
import com.intelbras.iotevents.domains.audit.core.AuditRecord;
import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import com.intelbras.iotevents.domains.audit.core.KafkaMessageContext;
import com.intelbras.iotevents.domains.audit.core.error.SimulatedProcessingFailureError;
import com.intelbras.iotevents.domains.audit.core.handler.DeviceEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.MotionDetectionEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.OtherEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.PropertyChangeEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.StatusChangeEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.port.AuditRepositoryPort;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RecordDeviceEventAuditUseCaseTest {

    private final OtherEventAuditHandler otherHandler = new OtherEventAuditHandler();
    private final List<DeviceEventAuditHandler> handlers = List.of(
        new PropertyChangeEventAuditHandler(), new StatusChangeEventAuditHandler(),
        new MotionDetectionEventAuditHandler(), otherHandler);
    private final EventMetrics eventMetrics = new EventMetrics(new SimpleMeterRegistry());
    private final KafkaMessageContext context = new KafkaMessageContext("device.property-change.events", 0, 1L, "DEVICE-00014");

    @Test
    void shouldPersistAnAuditRecordForANewEvent() {
        AuditRepositoryPort repositoryPort = mock(AuditRepositoryPort.class);
        when(repositoryPort.existsByDedupKey("evt-1")).thenReturn(false);
        RecordDeviceEventAuditUseCase useCase =
            new RecordDeviceEventAuditUseCase(repositoryPort, handlers, otherHandler, eventMetrics);

        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-1", "DEVICE-00014", "PROPERTY_CHANGE", "iotProperty",
            Instant.parse("2026-09-25T08:47:51.157000Z"), Map.of("properties", Map.of("11800", 3)));

        useCase.execute(event, context);

        verify(repositoryPort).save(any(AuditRecord.class));
    }

    @Test
    void shouldSkipDuplicateEventWithoutPersistingAudit() {
        AuditRepositoryPort repositoryPort = mock(AuditRepositoryPort.class);
        when(repositoryPort.existsByDedupKey("evt-1")).thenReturn(true);
        RecordDeviceEventAuditUseCase useCase =
            new RecordDeviceEventAuditUseCase(repositoryPort, handlers, otherHandler, eventMetrics);

        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-1", "DEVICE-00014", "PROPERTY_CHANGE", "iotProperty",
            Instant.parse("2026-09-25T08:47:51.157000Z"), Map.of());

        useCase.execute(event, context);

        verify(repositoryPort, never()).save(any(AuditRecord.class));
    }

    @Test
    void shouldThrowOnSimulateFailureFlagWithoutPersistingAudit() {
        AuditRepositoryPort repositoryPort = mock(AuditRepositoryPort.class);
        when(repositoryPort.existsByDedupKey("evt-1")).thenReturn(false);
        RecordDeviceEventAuditUseCase useCase =
            new RecordDeviceEventAuditUseCase(repositoryPort, handlers, otherHandler, eventMetrics);

        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-1", "SYNTHETIC-DLQ-001", "PROPERTY_CHANGE", "iotProperty",
            Instant.parse("2026-09-25T10:00:00Z"), Map.of("simulateFailure", true));

        assertThatThrownBy(() -> useCase.execute(event, context)).isInstanceOf(SimulatedProcessingFailureError.class);

        verify(repositoryPort, never()).save(any(AuditRecord.class));
    }

    @Test
    void shouldFallBackToOtherHandlerWhenNoSpecificHandlerMatches() {
        AuditRepositoryPort repositoryPort = mock(AuditRepositoryPort.class);
        when(repositoryPort.existsByDedupKey("evt-9")).thenReturn(false);
        RecordDeviceEventAuditUseCase useCase =
            new RecordDeviceEventAuditUseCase(repositoryPort, handlers, otherHandler, eventMetrics);

        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-9", "FJ1K2001704PS", "OTHER", "informationTransfer",
            Instant.parse("2026-09-25T09:45:37Z"), Map.of());

        useCase.execute(event, context);

        verify(repositoryPort).save(any(AuditRecord.class));
    }
}
```

```java
package com.intelbras.iotevents.domains.audit.core.usecase;

import com.intelbras.iotevents.domains.audit.core.AuditQueryFilter;
import com.intelbras.iotevents.domains.audit.core.AuditRecord;
import com.intelbras.iotevents.domains.audit.core.port.AuditRepositoryPort;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class QueryAuditsUseCaseTest {

    @Test
    void shouldReturnWhateverTheRepositoryPortReturnsForTheGivenFilter() {
        AuditRepositoryPort repositoryPort = mock(AuditRepositoryPort.class);
        AuditQueryFilter filter = new AuditQueryFilter("DEVICE-00014", null, null, null);
        AuditRecord record = new AuditRecord("evt-1", "DEVICE-00014", "PROPERTY_CHANGE", "iotProperty",
            "Device DEVICE-00014 reported a property change", Instant.now(), Instant.now());
        when(repositoryPort.query(filter)).thenReturn(List.of(record));

        assertThat(new QueryAuditsUseCase(repositoryPort).execute(filter)).containsExactly(record);
    }
}
```

- [ ] **Step 3: Run, verify fails** — `mvn -q test -Dtest=RecordDeviceEventAuditUseCaseTest,QueryAuditsUseCaseTest`

- [ ] **Step 4: `EventMetrics`**

```java
package com.intelbras.iotevents.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class EventMetrics {

    private final Counter processedCounter;
    private final Counter duplicateCounter;
    private final Counter dlqCounter;

    public EventMetrics(MeterRegistry registry) {
        this.processedCounter = Counter.builder("device_events_processed_total")
            .description("Number of device events successfully processed and audited").register(registry);
        this.duplicateCounter = Counter.builder("device_events_duplicate_total")
            .description("Number of device events skipped as duplicates").register(registry);
        this.dlqCounter = Counter.builder("device_events_dlq_total")
            .description("Number of device events sent to a dead-letter topic").register(registry);
    }

    public void incrementProcessed() {
        processedCounter.increment();
    }

    public void incrementDuplicate() {
        duplicateCounter.increment();
    }

    public void incrementDlq() {
        dlqCounter.increment();
    }
}
```

- [ ] **Step 5: `RecordDeviceEventAuditUseCase`**

```java
package com.intelbras.iotevents.domains.audit.core.usecase;

import com.intelbras.iotevents.common.logging.ExecutionLog;
import com.intelbras.iotevents.common.metrics.EventMetrics;
import com.intelbras.iotevents.domains.audit.core.AuditRecord;
import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import com.intelbras.iotevents.domains.audit.core.KafkaMessageContext;
import com.intelbras.iotevents.domains.audit.core.error.SimulatedProcessingFailureError;
import com.intelbras.iotevents.domains.audit.core.handler.DeviceEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.OtherEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.port.AuditRepositoryPort;

import java.util.List;
import java.util.Map;

public class RecordDeviceEventAuditUseCase {

    private final AuditRepositoryPort auditRepositoryPort;
    private final List<DeviceEventAuditHandler> handlers;
    private final OtherEventAuditHandler fallbackHandler;
    private final EventMetrics eventMetrics;

    public RecordDeviceEventAuditUseCase(AuditRepositoryPort auditRepositoryPort,
                                          List<DeviceEventAuditHandler> handlers,
                                          OtherEventAuditHandler fallbackHandler,
                                          EventMetrics eventMetrics) {
        this.auditRepositoryPort = auditRepositoryPort;
        this.handlers = handlers;
        this.fallbackHandler = fallbackHandler;
        this.eventMetrics = eventMetrics;
    }

    public void execute(IncomingDeviceEvent event, KafkaMessageContext kafkaCtx) {
        ExecutionLog log = ExecutionLog.start(RecordDeviceEventAuditUseCase.class, "execute")
            .context("eventId", event.eventId())
            .context("deviceId", event.deviceId())
            .context("eventCategory", event.eventCategory())
            .context("eventType", event.eventType())
            .context("topic", kafkaCtx.topic())
            .context("partition", kafkaCtx.partition())
            .context("offset", kafkaCtx.offset())
            .context("messageKey", kafkaCtx.key());

        try {
            boolean duplicate = auditRepositoryPort.existsByDedupKey(event.eventId());
            log.step("Checked dedup key against audit store", Map.of("isDuplicate", duplicate));

            if (duplicate) {
                eventMetrics.incrementDuplicate();
                log.step("Skipped processing because event is a duplicate");
                log.info("Skipped duplicate device event");
                return;
            }

            if (Boolean.TRUE.equals(event.payload().get("simulateFailure"))) {
                log.step("Detected simulateFailure flag in payload");
                throw new SimulatedProcessingFailureError("Simulated processing failure for eventId=" + event.eventId());
            }

            DeviceEventAuditHandler handler = resolveHandler(event.eventCategory());
            String generalInfo = handler.handle(event);
            log.step("Built general info via " + handler.getClass().getSimpleName(), Map.of("generalInfo", generalInfo));

            auditRepositoryPort.save(AuditRecord.processedNow(event, generalInfo));
            eventMetrics.incrementProcessed();
            log.step("Persisted audit record");

            log.info("Processed device event");
        } catch (Exception ex) {
            log.step("Unhandled exception while processing device event");
            log.error("Failed to process device event", ex);
            throw ex;
        }
    }

    private DeviceEventAuditHandler resolveHandler(String eventCategory) {
        return handlers.stream()
            .filter(handler -> handler != fallbackHandler && handler.supports(eventCategory))
            .findFirst()
            .orElse(fallbackHandler);
    }
}
```

- [ ] **Step 6: `QueryAuditsUseCase`**

```java
package com.intelbras.iotevents.domains.audit.core.usecase;

import com.intelbras.iotevents.common.logging.ExecutionLog;
import com.intelbras.iotevents.domains.audit.core.AuditQueryFilter;
import com.intelbras.iotevents.domains.audit.core.AuditRecord;
import com.intelbras.iotevents.domains.audit.core.port.AuditRepositoryPort;

import java.util.List;
import java.util.Map;

public class QueryAuditsUseCase {

    private final AuditRepositoryPort auditRepositoryPort;

    public QueryAuditsUseCase(AuditRepositoryPort auditRepositoryPort) {
        this.auditRepositoryPort = auditRepositoryPort;
    }

    public List<AuditRecord> execute(AuditQueryFilter filter) {
        ExecutionLog log = ExecutionLog.start(QueryAuditsUseCase.class, "execute")
            .context("deviceId", filter.deviceId())
            .context("eventType", filter.eventType())
            .context("from", filter.from())
            .context("to", filter.to());
        try {
            List<AuditRecord> results = auditRepositoryPort.query(filter);
            log.step("Queried audit store", Map.of("resultCount", results.size()));

            log.info("Queried audits");
            return results;
        } catch (Exception ex) {
            log.step("Unhandled exception while querying audits");
            log.error("Failed to query audits", ex);
            throw ex;
        }
    }
}
```

- [ ] **Step 7: Run, verify passes** — `mvn -q test -Dtest=RecordDeviceEventAuditUseCaseTest,QueryAuditsUseCaseTest` → 5 passed
- [ ] **Step 8: Commit** — `git add src/main/java/com/intelbras/iotevents/common/metrics src/main/java/com/intelbras/iotevents/domains/audit/core/usecase src/test/java/com/intelbras/iotevents/domains/audit/core/usecase && git commit -m "feat: add audit use cases and event metrics"`
- [ ] **Step 9: Merge** — `git checkout main && git merge --no-ff feat/audit-use-cases && git branch -d feat/audit-use-cases`

---

### Task 11: Audit persistence adapter (Postgres/JPA)

**Files:** Create `src/main/resources/db/migration/V1__create_audit_records_table.sql`, `domains/audit/adapters/persistence/{AuditJpaEntity,AuditJpaRepository,AuditMapper,JpaAuditRepositoryAdapter}.java`; Test `AuditMapperTest.java`.

**Interfaces:** `JpaAuditRepositoryAdapter implements AuditRepositoryPort`, backed by `audit_records` (column `event_category`, not `device_type`).

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b feat/audit-persistence-adapter`

- [ ] **Step 2: Flyway migration**

```sql
CREATE TABLE audit_records (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(255) NOT NULL,
    device_id VARCHAR(255) NOT NULL,
    event_category VARCHAR(50) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    general_info TEXT NOT NULL,
    consumed_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT uq_audit_records_event_id UNIQUE (event_id)
);

CREATE INDEX idx_audit_records_device_id ON audit_records (device_id);
CREATE INDEX idx_audit_records_event_type ON audit_records (event_type);
```

- [ ] **Step 3: Failing mapper test**

```java
package com.intelbras.iotevents.domains.audit.adapters.persistence;

import com.intelbras.iotevents.domains.audit.core.AuditRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class AuditMapperTest {

    @Test
    void shouldRoundTripBetweenDomainAndJpaEntity() {
        Instant now = Instant.now();
        AuditRecord record = new AuditRecord("evt-1", "DEVICE-00014", "PROPERTY_CHANGE", "iotProperty",
            "Device DEVICE-00014 reported a property change", now, now);

        AuditRecord roundTripped = AuditMapper.toDomain(AuditMapper.toEntity(record));

        assertThat(roundTripped).isEqualTo(record);
    }
}
```

- [ ] **Step 4: Run, verify fails** — `mvn -q test -Dtest=AuditMapperTest`

- [ ] **Step 5: `AuditJpaEntity`**

```java
package com.intelbras.iotevents.domains.audit.adapters.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import java.time.Instant;

@Entity
@Table(name = "audit_records", uniqueConstraints = @UniqueConstraint(name = "uq_audit_records_event_id", columnNames = "event_id"))
public class AuditJpaEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false)
    private String eventId;

    @Column(name = "device_id", nullable = false)
    private String deviceId;

    @Column(name = "event_category", nullable = false)
    private String eventCategory;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "general_info", nullable = false, columnDefinition = "TEXT")
    private String generalInfo;

    @Column(name = "consumed_at", nullable = false)
    private Instant consumedAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected AuditJpaEntity() {
    }

    public AuditJpaEntity(String eventId, String deviceId, String eventCategory, String eventType,
                           String generalInfo, Instant consumedAt, Instant createdAt) {
        this.eventId = eventId;
        this.deviceId = deviceId;
        this.eventCategory = eventCategory;
        this.eventType = eventType;
        this.generalInfo = generalInfo;
        this.consumedAt = consumedAt;
        this.createdAt = createdAt;
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public String getEventCategory() {
        return eventCategory;
    }

    public String getEventType() {
        return eventType;
    }

    public String getGeneralInfo() {
        return generalInfo;
    }

    public Instant getConsumedAt() {
        return consumedAt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
```

- [ ] **Step 6: `AuditMapper`**

```java
package com.intelbras.iotevents.domains.audit.adapters.persistence;

import com.intelbras.iotevents.domains.audit.core.AuditRecord;

public final class AuditMapper {

    private AuditMapper() {
    }

    public static AuditJpaEntity toEntity(AuditRecord record) {
        return new AuditJpaEntity(record.dedupKey(), record.deviceId(), record.eventCategory(), record.eventType(),
            record.generalInfo(), record.consumedAt(), record.createdAt());
    }

    public static AuditRecord toDomain(AuditJpaEntity entity) {
        return new AuditRecord(entity.getEventId(), entity.getDeviceId(), entity.getEventCategory(),
            entity.getEventType(), entity.getGeneralInfo(), entity.getConsumedAt(), entity.getCreatedAt());
    }
}
```

- [ ] **Step 7: Run, verify passes** — `mvn -q test -Dtest=AuditMapperTest` → 1 passed

- [ ] **Step 8: `AuditJpaRepository` and `JpaAuditRepositoryAdapter`** (proven through Task 15's integration test, not in isolation)

```java
package com.intelbras.iotevents.domains.audit.adapters.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

public interface AuditJpaRepository extends JpaRepository<AuditJpaEntity, Long>, JpaSpecificationExecutor<AuditJpaEntity> {
    boolean existsByEventId(String eventId);
}
```

```java
package com.intelbras.iotevents.domains.audit.adapters.persistence;

import com.intelbras.iotevents.domains.audit.core.AuditQueryFilter;
import com.intelbras.iotevents.domains.audit.core.AuditRecord;
import com.intelbras.iotevents.domains.audit.core.port.AuditRepositoryPort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class JpaAuditRepositoryAdapter implements AuditRepositoryPort {

    private final AuditJpaRepository repository;

    public JpaAuditRepositoryAdapter(AuditJpaRepository repository) {
        this.repository = repository;
    }

    @Override
    public void save(AuditRecord record) {
        repository.save(AuditMapper.toEntity(record));
    }

    @Override
    public boolean existsByDedupKey(String eventId) {
        return repository.existsByEventId(eventId);
    }

    @Override
    public List<AuditRecord> query(AuditQueryFilter filter) {
        Specification<AuditJpaEntity> spec = Specification.where(null);
        if (filter.deviceId() != null) {
            spec = spec.and((root, q, cb) -> cb.equal(root.get("deviceId"), filter.deviceId()));
        }
        if (filter.eventType() != null) {
            spec = spec.and((root, q, cb) -> cb.equal(root.get("eventType"), filter.eventType()));
        }
        if (filter.from() != null) {
            spec = spec.and((root, q, cb) -> cb.greaterThanOrEqualTo(root.get("consumedAt"), filter.from()));
        }
        if (filter.to() != null) {
            spec = spec.and((root, q, cb) -> cb.lessThanOrEqualTo(root.get("consumedAt"), filter.to()));
        }
        return repository.findAll(spec).stream().map(AuditMapper::toDomain).toList();
    }
}
```

- [ ] **Step 9: Verify compile** — `mvn -q -DskipTests compile`
- [ ] **Step 10: Commit** — `git add src/main/resources/db/migration src/main/java/com/intelbras/iotevents/domains/audit/adapters/persistence src/test/java/com/intelbras/iotevents/domains/audit/adapters/persistence && git commit -m "feat: add Postgres audit persistence adapter"`
- [ ] **Step 11: Merge** — `git checkout main && git merge --no-ff feat/audit-persistence-adapter && git branch -d feat/audit-persistence-adapter`

---

### Task 12: Kafka error handling + DLQ recoverer

**Files:** Create `domains/audit/adapters/kafka/DeviceEventDlqRecoverer.java`, `config/KafkaErrorHandlingConfig.java`; Test `DeviceEventDlqRecovererTest.java`.

**Interfaces:** `DeviceEventDlqRecoverer.recover(ConsumerRecord<?,?>, Exception, DeadLetterPublishingRecoverer)`; a `CommonErrorHandler` bean auto-applied to every `@KafkaListener` container.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b feat/kafka-dlq-recoverer`

- [ ] **Step 2: Failing test**

```java
package com.intelbras.iotevents.domains.audit.adapters.kafka;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.intelbras.iotevents.common.metrics.EventMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DeviceEventDlqRecovererTest {

    private ListAppender<ILoggingEvent> appender;
    private Logger rootLogger;

    @BeforeEach
    void attachAppender() {
        appender = new ListAppender<>();
        appender.start();
        rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        rootLogger.detachAppender(appender);
    }

    @Test
    void shouldDelegateToDeadLetterRecovererAndLogExactlyOnce() {
        DeviceEventDlqRecoverer recoverer = new DeviceEventDlqRecoverer(new EventMetrics(new SimpleMeterRegistry()));
        DeadLetterPublishingRecoverer delegate = mock(DeadLetterPublishingRecoverer.class);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("device.status-change.events", 0, 1L, "DEVICE-1", "{}");
        RuntimeException exception = new RuntimeException("boom");

        recoverer.recover(record, exception, delegate);

        verify(delegate).accept(record, exception);
        assertThat(appender.list).hasSize(1);
    }

    @Test
    void shouldLogExactlyOnceEvenWhenDelegateThrows() {
        DeviceEventDlqRecoverer recoverer = new DeviceEventDlqRecoverer(new EventMetrics(new SimpleMeterRegistry()));
        DeadLetterPublishingRecoverer delegate = mock(DeadLetterPublishingRecoverer.class);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("device.status-change.events", 0, 1L, "DEVICE-1", "{}");
        RuntimeException exception = new RuntimeException("boom");
        doThrow(new IllegalStateException("delegate failed")).when(delegate).accept(record, exception);

        try {
            recoverer.recover(record, exception, delegate);
        } catch (IllegalStateException expected) {
            // expected to propagate
        }

        assertThat(appender.list).hasSize(1);
    }
}
```

- [ ] **Step 3: Run, verify fails** — `mvn -q test -Dtest=DeviceEventDlqRecovererTest`

- [ ] **Step 4: `DeviceEventDlqRecoverer`**

```java
package com.intelbras.iotevents.domains.audit.adapters.kafka;

import com.intelbras.iotevents.common.logging.ExecutionLog;
import com.intelbras.iotevents.common.metrics.EventMetrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.stereotype.Component;

@Component
public class DeviceEventDlqRecoverer {

    private final EventMetrics eventMetrics;

    public DeviceEventDlqRecoverer(EventMetrics eventMetrics) {
        this.eventMetrics = eventMetrics;
    }

    public void recover(ConsumerRecord<?, ?> record, Exception exception, DeadLetterPublishingRecoverer delegate) {
        ExecutionLog log = ExecutionLog.start(DeviceEventDlqRecoverer.class, "recover")
            .context("topic", record.topic())
            .context("partition", record.partition())
            .context("offset", record.offset())
            .context("messageKey", record.key());
        try {
            log.step("Exhausted retries for device event, sending to dead-letter topic");
            delegate.accept(record, exception);
            eventMetrics.incrementDlq();
            log.step("Published record to dead-letter topic");

            log.error("Device event moved to dead-letter topic after exhausting retries", exception);
        } catch (Exception recoveryError) {
            log.step("Unhandled exception while recovering device event to DLQ");
            log.error("Failed to recover device event to dead-letter topic", recoveryError);
            throw recoveryError;
        }
    }
}
```

- [ ] **Step 5: Run, verify passes** — `mvn -q test -Dtest=DeviceEventDlqRecovererTest` → 2 passed

- [ ] **Step 6: `KafkaErrorHandlingConfig`**

```java
package com.intelbras.iotevents.config;

import com.intelbras.iotevents.domains.audit.adapters.kafka.DeviceEventDlqRecoverer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    public CommonErrorHandler deviceEventErrorHandler(KafkaTemplate<String, String> kafkaTemplate,
                                                        DeviceEventDlqRecoverer dlqRecoverer) {
        DeadLetterPublishingRecoverer deadLetterRecoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
            (record, ex) -> new TopicPartition(record.topic() + KafkaTopics.DLT_SUFFIX, 0));

        ConsumerRecordRecoverer recoverer = (record, ex) -> dlqRecoverer.recover(record, ex, deadLetterRecoverer);

        ExponentialBackOff backOff = new ExponentialBackOff(1000L, 2.0);
        backOff.setMaxAttempts(3);

        return new DefaultErrorHandler(recoverer, backOff);
    }
}
```

- [ ] **Step 7: Verify compile** — `mvn -q -DskipTests compile`
- [ ] **Step 8: Commit** — `git add src/main/java/com/intelbras/iotevents/domains/audit/adapters/kafka/DeviceEventDlqRecoverer.java src/main/java/com/intelbras/iotevents/config/KafkaErrorHandlingConfig.java src/test/java/com/intelbras/iotevents/domains/audit/adapters/kafka && git commit -m "feat: add Kafka retry and dead-letter error handling"`
- [ ] **Step 9: Merge** — `git checkout main && git merge --no-ff feat/kafka-dlq-recoverer && git branch -d feat/kafka-dlq-recoverer`

---

### Task 13: Audit Kafka consumer listeners

**Files:** Create `domains/audit/adapters/kafka/DeviceEventMessageParser.java`, `domains/audit/adapters/kafka/listener/{PropertyChangeEventListener,StatusChangeEventListener,MotionDetectionEventListener,OtherEventListener}.java`, `domains/audit/AuditUseCaseConfig.java`; Test parser + one listener.

**Interfaces:** Four `@KafkaListener`s wired to `audit-service`, each parsing the wire JSON (`eventCategory` key, not `deviceType`) and calling `RecordDeviceEventAuditUseCase.execute(...)`.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b feat/audit-kafka-listeners`

- [ ] **Step 2: Failing parser test**

```java
package com.intelbras.iotevents.domains.audit.adapters.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DeviceEventMessageParserTest {

    private final DeviceEventMessageParser parser = new DeviceEventMessageParser(new ObjectMapper());

    @Test
    void shouldParseTheWireJsonIntoAnIncomingDeviceEvent() {
        String json = """
            {"eventId":"evt-1","deviceId":"DEVICE-00014","eventCategory":"PROPERTY_CHANGE","eventType":"iotProperty",
             "timestamp":"2026-09-25T08:47:51.157000Z","payload":{"properties":{"11800":3}}}
            """;

        IncomingDeviceEvent event = parser.parse(json);

        assertThat(event.eventId()).isEqualTo("evt-1");
        assertThat(event.deviceId()).isEqualTo("DEVICE-00014");
        assertThat(event.eventCategory()).isEqualTo("PROPERTY_CHANGE");
        assertThat(event.eventType()).isEqualTo("iotProperty");
    }

    @Test
    void shouldDefaultToEmptyPayloadWhenMissing() {
        String json = """
            {"eventId":"evt-2","deviceId":"DEVICE-00056","eventCategory":"STATUS_CHANGE","eventType":"online",
             "timestamp":"2026-09-25T09:55:59Z"}
            """;

        assertThat(parser.parse(json).payload()).isEmpty();
    }
}
```

- [ ] **Step 3: Failing listener test**

```java
package com.intelbras.iotevents.domains.audit.adapters.kafka.listener;

import com.intelbras.iotevents.domains.audit.adapters.kafka.DeviceEventMessageParser;
import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import com.intelbras.iotevents.domains.audit.core.KafkaMessageContext;
import com.intelbras.iotevents.domains.audit.core.usecase.RecordDeviceEventAuditUseCase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PropertyChangeEventListenerTest {

    @Test
    void shouldParseTheRecordAndCallTheUseCaseWithKafkaContext() {
        DeviceEventMessageParser parser = mock(DeviceEventMessageParser.class);
        RecordDeviceEventAuditUseCase useCase = mock(RecordDeviceEventAuditUseCase.class);
        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-1", "DEVICE-00014", "PROPERTY_CHANGE", "iotProperty",
            Instant.parse("2026-09-25T08:47:51.157000Z"), Map.of());
        ConsumerRecord<String, String> record = new ConsumerRecord<>("device.property-change.events", 2, 42L, "DEVICE-00014", "{}");
        when(parser.parse("{}")).thenReturn(event);

        new PropertyChangeEventListener(parser, useCase).onMessage(record);

        verify(useCase).execute(eq(event), eq(new KafkaMessageContext("device.property-change.events", 2, 42L, "DEVICE-00014")));
    }
}
```

- [ ] **Step 4: Run, verify both fail** — `mvn -q test -Dtest=DeviceEventMessageParserTest,PropertyChangeEventListenerTest`

- [ ] **Step 5: `DeviceEventMessageParser`**

```java
package com.intelbras.iotevents.domains.audit.adapters.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;

@Component
public class DeviceEventMessageParser {

    private final ObjectMapper objectMapper;

    public DeviceEventMessageParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public IncomingDeviceEvent parse(String json) {
        try {
            Map<String, Object> raw = objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {
            });
            String eventId = (String) raw.get("eventId");
            String deviceId = (String) raw.get("deviceId");
            String eventCategory = (String) raw.get("eventCategory");
            String eventType = (String) raw.get("eventType");
            Instant timestamp = Instant.parse((String) raw.get("timestamp"));
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) raw.getOrDefault("payload", Map.of());
            return new IncomingDeviceEvent(eventId, deviceId, eventCategory, eventType, timestamp, payload);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to parse device event message: " + json, ex);
        }
    }
}
```

- [ ] **Step 6: The four listeners**

```java
package com.intelbras.iotevents.domains.audit.adapters.kafka.listener;

import com.intelbras.iotevents.config.KafkaTopics;
import com.intelbras.iotevents.domains.audit.adapters.kafka.DeviceEventMessageParser;
import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import com.intelbras.iotevents.domains.audit.core.KafkaMessageContext;
import com.intelbras.iotevents.domains.audit.core.usecase.RecordDeviceEventAuditUseCase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PropertyChangeEventListener {

    private final DeviceEventMessageParser parser;
    private final RecordDeviceEventAuditUseCase useCase;

    public PropertyChangeEventListener(DeviceEventMessageParser parser, RecordDeviceEventAuditUseCase useCase) {
        this.parser = parser;
        this.useCase = useCase;
    }

    @KafkaListener(topics = KafkaTopics.DEVICE_PROPERTY_CHANGE_EVENTS, groupId = KafkaTopics.CONSUMER_GROUP, concurrency = "3")
    public void onMessage(ConsumerRecord<String, String> record) {
        IncomingDeviceEvent event = parser.parse(record.value());
        useCase.execute(event, new KafkaMessageContext(record.topic(), record.partition(), record.offset(), record.key()));
    }
}
```

```java
package com.intelbras.iotevents.domains.audit.adapters.kafka.listener;

import com.intelbras.iotevents.config.KafkaTopics;
import com.intelbras.iotevents.domains.audit.adapters.kafka.DeviceEventMessageParser;
import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import com.intelbras.iotevents.domains.audit.core.KafkaMessageContext;
import com.intelbras.iotevents.domains.audit.core.usecase.RecordDeviceEventAuditUseCase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class StatusChangeEventListener {

    private final DeviceEventMessageParser parser;
    private final RecordDeviceEventAuditUseCase useCase;

    public StatusChangeEventListener(DeviceEventMessageParser parser, RecordDeviceEventAuditUseCase useCase) {
        this.parser = parser;
        this.useCase = useCase;
    }

    @KafkaListener(topics = KafkaTopics.DEVICE_STATUS_CHANGE_EVENTS, groupId = KafkaTopics.CONSUMER_GROUP, concurrency = "3")
    public void onMessage(ConsumerRecord<String, String> record) {
        IncomingDeviceEvent event = parser.parse(record.value());
        useCase.execute(event, new KafkaMessageContext(record.topic(), record.partition(), record.offset(), record.key()));
    }
}
```

```java
package com.intelbras.iotevents.domains.audit.adapters.kafka.listener;

import com.intelbras.iotevents.config.KafkaTopics;
import com.intelbras.iotevents.domains.audit.adapters.kafka.DeviceEventMessageParser;
import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import com.intelbras.iotevents.domains.audit.core.KafkaMessageContext;
import com.intelbras.iotevents.domains.audit.core.usecase.RecordDeviceEventAuditUseCase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MotionDetectionEventListener {

    private final DeviceEventMessageParser parser;
    private final RecordDeviceEventAuditUseCase useCase;

    public MotionDetectionEventListener(DeviceEventMessageParser parser, RecordDeviceEventAuditUseCase useCase) {
        this.parser = parser;
        this.useCase = useCase;
    }

    @KafkaListener(topics = KafkaTopics.DEVICE_MOTION_DETECTION_EVENTS, groupId = KafkaTopics.CONSUMER_GROUP, concurrency = "3")
    public void onMessage(ConsumerRecord<String, String> record) {
        IncomingDeviceEvent event = parser.parse(record.value());
        useCase.execute(event, new KafkaMessageContext(record.topic(), record.partition(), record.offset(), record.key()));
    }
}
```

```java
package com.intelbras.iotevents.domains.audit.adapters.kafka.listener;

import com.intelbras.iotevents.config.KafkaTopics;
import com.intelbras.iotevents.domains.audit.adapters.kafka.DeviceEventMessageParser;
import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import com.intelbras.iotevents.domains.audit.core.KafkaMessageContext;
import com.intelbras.iotevents.domains.audit.core.usecase.RecordDeviceEventAuditUseCase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OtherEventListener {

    private final DeviceEventMessageParser parser;
    private final RecordDeviceEventAuditUseCase useCase;

    public OtherEventListener(DeviceEventMessageParser parser, RecordDeviceEventAuditUseCase useCase) {
        this.parser = parser;
        this.useCase = useCase;
    }

    @KafkaListener(topics = KafkaTopics.DEVICE_OTHER_EVENTS, groupId = KafkaTopics.CONSUMER_GROUP, concurrency = "3")
    public void onMessage(ConsumerRecord<String, String> record) {
        IncomingDeviceEvent event = parser.parse(record.value());
        useCase.execute(event, new KafkaMessageContext(record.topic(), record.partition(), record.offset(), record.key()));
    }
}
```

- [ ] **Step 7: Register the audit use case beans**

```java
package com.intelbras.iotevents.domains.audit;

import com.intelbras.iotevents.common.metrics.EventMetrics;
import com.intelbras.iotevents.domains.audit.core.handler.DeviceEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.OtherEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.port.AuditRepositoryPort;
import com.intelbras.iotevents.domains.audit.core.usecase.QueryAuditsUseCase;
import com.intelbras.iotevents.domains.audit.core.usecase.RecordDeviceEventAuditUseCase;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class AuditUseCaseConfig {

    @Bean
    public RecordDeviceEventAuditUseCase recordDeviceEventAuditUseCase(AuditRepositoryPort auditRepositoryPort,
                                                                         List<DeviceEventAuditHandler> handlers,
                                                                         OtherEventAuditHandler fallbackHandler,
                                                                         EventMetrics eventMetrics) {
        return new RecordDeviceEventAuditUseCase(auditRepositoryPort, handlers, fallbackHandler, eventMetrics);
    }

    @Bean
    public QueryAuditsUseCase queryAuditsUseCase(AuditRepositoryPort auditRepositoryPort) {
        return new QueryAuditsUseCase(auditRepositoryPort);
    }
}
```

- [ ] **Step 8: Run, verify passes** — `mvn -q test -Dtest=DeviceEventMessageParserTest,PropertyChangeEventListenerTest` → 3 passed
- [ ] **Step 9: Commit** — `git add src/main/java/com/intelbras/iotevents/domains/audit/adapters/kafka src/main/java/com/intelbras/iotevents/domains/audit/AuditUseCaseConfig.java src/test/java/com/intelbras/iotevents/domains/audit/adapters/kafka && git commit -m "feat: add audit Kafka consumer listeners"`
- [ ] **Step 10: Merge** — `git checkout main && git merge --no-ff feat/audit-kafka-listeners && git branch -d feat/audit-kafka-listeners`

---

### Task 14: Audit REST query endpoint

**Files:** Create `domains/audit/adapters/web/AuditController.java`; Test `AuditControllerTest.java`.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b feat/audit-controller`

- [ ] **Step 2: Failing test**

```java
package com.intelbras.iotevents.domains.audit.adapters.web;

import com.intelbras.iotevents.domains.audit.core.AuditQueryFilter;
import com.intelbras.iotevents.domains.audit.core.AuditRecord;
import com.intelbras.iotevents.domains.audit.core.usecase.QueryAuditsUseCase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(AuditController.class)
class AuditControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private QueryAuditsUseCase queryAuditsUseCase;

    @Test
    void shouldReturnAuditsMatchingTheGivenFilter() throws Exception {
        AuditRecord record = new AuditRecord("evt-1", "DEVICE-00014", "PROPERTY_CHANGE", "iotProperty",
            "Device DEVICE-00014 reported a property change", Instant.parse("2026-09-25T08:47:51.157000Z"),
            Instant.parse("2026-09-25T08:47:51.157000Z"));
        when(queryAuditsUseCase.execute(new AuditQueryFilter("DEVICE-00014", null, null, null))).thenReturn(List.of(record));

        mockMvc.perform(get("/audits").param("deviceId", "DEVICE-00014"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[0].deviceId").value("DEVICE-00014"))
            .andExpect(jsonPath("$[0].eventCategory").value("PROPERTY_CHANGE"));
    }
}
```

- [ ] **Step 3: Run, verify fails** — `mvn -q test -Dtest=AuditControllerTest`

- [ ] **Step 4: `AuditController`**

```java
package com.intelbras.iotevents.domains.audit.adapters.web;

import com.intelbras.iotevents.domains.audit.core.AuditQueryFilter;
import com.intelbras.iotevents.domains.audit.core.AuditRecord;
import com.intelbras.iotevents.domains.audit.core.usecase.QueryAuditsUseCase;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/audits")
public class AuditController {

    private final QueryAuditsUseCase queryAuditsUseCase;

    public AuditController(QueryAuditsUseCase queryAuditsUseCase) {
        this.queryAuditsUseCase = queryAuditsUseCase;
    }

    @GetMapping
    public List<AuditRecord> getAudits(
            @RequestParam(required = false) String deviceId,
            @RequestParam(required = false) String eventType,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to) {
        return queryAuditsUseCase.execute(new AuditQueryFilter(deviceId, eventType, from, to));
    }
}
```

- [ ] **Step 5: Run, verify passes** — `mvn -q test -Dtest=AuditControllerTest` → 1 passed
- [ ] **Step 6: Commit** — `git add src/main/java/com/intelbras/iotevents/domains/audit/adapters/web src/test/java/com/intelbras/iotevents/domains/audit/adapters/web && git commit -m "feat: add audit query REST endpoint"`
- [ ] **Step 7: Merge** — `git checkout main && git merge --no-ff feat/audit-controller && git branch -d feat/audit-controller`

---

### Task 15: End-to-end Testcontainers integration tests

**Files:** Create `src/test/java/com/intelbras/iotevents/AuditFlowIT.java`, `src/test/resources/application-test.yml`.

**Interfaces:** Proves the happy path (100 real rows → 100 audits), duplicate path (re-ingest → no growth), and failure path (4 synthetic rows → 4 DLT messages, zero audits) against real Kafka + Postgres containers.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b test/audit-flow-integration-tests`

- [ ] **Step 2: `src/test/resources/application-test.yml`**

```yaml
spring:
  jpa:
    hibernate:
      ddl-auto: validate
  flyway:
    enabled: true
```

- [ ] **Step 3: Failing integration test**

```java
package com.intelbras.iotevents;

import com.intelbras.iotevents.config.KafkaTopics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AuditFlowIT {

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

    @Test
    void shouldProcessAllRealRowsAndDeadLetterTheFourSyntheticOnes() {
        ResponseEntity<Map> response = restTemplate.postForEntity("http://localhost:" + port + "/events/simulate", null, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().get("publishedCount")).isEqualTo(104);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            ResponseEntity<List> audits = restTemplate.getForEntity("http://localhost:" + port + "/audits", List.class);
            assertThat(audits.getBody()).hasSize(100);
        });

        for (String topic : KafkaTopics.allTopics()) {
            List<ConsumerRecord<String, String>> dltRecords = consumeFromDlt(topic + KafkaTopics.DLT_SUFFIX);
            assertThat(dltRecords).hasSize(1);
            assertThat(dltRecords.get(0).key()).startsWith("SYNTHETIC-DLQ-");
        }
    }

    @Test
    void shouldNotGrowTheAuditTableWhenTheSameEventsAreIngestedTwice() {
        restTemplate.postForEntity("http://localhost:" + port + "/events/simulate", null, Map.class);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            ResponseEntity<List> audits = restTemplate.getForEntity("http://localhost:" + port + "/audits", List.class);
            assertThat(audits.getBody()).hasSize(100);
        });

        restTemplate.postForEntity("http://localhost:" + port + "/events/simulate", null, Map.class);

        await().pollDelay(Duration.ofSeconds(5)).atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            ResponseEntity<List> audits = restTemplate.getForEntity("http://localhost:" + port + "/audits", List.class);
            assertThat(audits.getBody()).hasSize(100);
        });
    }

    private List<ConsumerRecord<String, String>> consumeFromDlt(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-dlt-reader-" + System.nanoTime());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            List<ConsumerRecord<String, String>> collected = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 15_000;
            while (collected.isEmpty() && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                records.records(topic).forEach(collected::add);
            }
            return collected;
        }
    }
}
```

- [ ] **Step 4: Confirm Docker is available** — `docker info` (start Docker if this fails; Testcontainers requires it)
- [ ] **Step 5: Run, verify passes** — `mvn -q test -Dtest=AuditFlowIT` → 2 passed (first run slower while images are pulled)
- [ ] **Step 6: Commit** — `git add src/test/java/com/intelbras/iotevents/AuditFlowIT.java src/test/resources/application-test.yml && git commit -m "test: add end-to-end Testcontainers audit flow tests"`
- [ ] **Step 7: Merge** — `git checkout main && git merge --no-ff test/audit-flow-integration-tests && git branch -d test/audit-flow-integration-tests`

---

### Task 16: Observability — Prometheus + Grafana in Docker Compose

**Files:** Modify `docker-compose.yml`; Create `observability/prometheus.yml`, `observability/grafana/provisioning/datasources/datasource.yml`, `observability/grafana/provisioning/dashboards/dashboards.yml`, `observability/grafana/dashboards/device-events-dashboard.json`, `Dockerfile`.

Unaffected by the dataset pivot — metric names are category-agnostic. Identical to the original plan.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b chore/observability-stack`

- [ ] **Step 2: `Dockerfile`**

```dockerfile
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/target/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 3: `observability/prometheus.yml`**

```yaml
global:
  scrape_interval: 5s
scrape_configs:
  - job_name: "iot-events-app"
    metrics_path: /actuator/prometheus
    static_configs:
      - targets: ["app:8080"]
```

- [ ] **Step 4: Grafana provisioning**

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

```yaml
apiVersion: 1
providers:
  - name: "Device Events"
    orgId: 1
    folder: ""
    type: file
    options:
      path: /etc/grafana/provisioning/dashboards-data
```

- [ ] **Step 5: `observability/grafana/dashboards/device-events-dashboard.json`**

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
     "targets": [{"expr": "sum(kafka_consumer_fetch_manager_records_lag) by (topic)", "legendFormat": "{{topic}}"}]}
  ]
}
```

- [ ] **Step 6: Update `docker-compose.yml`** (full replacement, adding `app`, `prometheus`, `grafana`)

```yaml
services:
  kafka:
    image: apache/kafka:3.8.0
    container_name: iotevents-kafka
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  postgres:
    image: postgres:16-alpine
    container_name: iotevents-postgres
    ports: ["5432:5432"]
    environment:
      POSTGRES_DB: iotevents
      POSTGRES_USER: iotevents
      POSTGRES_PASSWORD: iotevents
    volumes: ["iotevents-postgres-data:/var/lib/postgresql/data"]

  app:
    build: .
    container_name: iotevents-app
    depends_on: [kafka, postgres]
    ports: ["8080:8080"]
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      DB_HOST: postgres
      DB_PORT: 5432
      DB_NAME: iotevents
      DB_USER: iotevents
      DB_PASSWORD: iotevents

  prometheus:
    image: prom/prometheus:v2.54.1
    container_name: iotevents-prometheus
    depends_on: [app]
    ports: ["9090:9090"]
    volumes: ["./observability/prometheus.yml:/etc/prometheus/prometheus.yml"]

  grafana:
    image: grafana/grafana:11.2.0
    container_name: iotevents-grafana
    depends_on: [prometheus]
    ports: ["3000:3000"]
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - ./observability/grafana/provisioning/datasources:/etc/grafana/provisioning/datasources
      - ./observability/grafana/provisioning/dashboards:/etc/grafana/provisioning/dashboards
      - ./observability/grafana/dashboards:/etc/grafana/provisioning/dashboards-data

volumes:
  iotevents-postgres-data:
```

- [ ] **Step 7: Run and verify manually**

```bash
docker compose up -d --build
curl -s -X POST http://localhost:8080/events/simulate   # expect {"publishedCount":104}
curl -s http://localhost:8080/actuator/prometheus | grep device_events_processed_total   # expect 100.0 after all 4 synthetic rows dead-letter
```

Open `http://localhost:3000` (admin/admin) and confirm the "IoT Device Events" dashboard renders.

- [ ] **Step 8: Tear down** — `docker compose down`
- [ ] **Step 9: Commit** — `git add docker-compose.yml observability Dockerfile && git commit -m "chore: add Docker Compose observability stack with Prometheus and Grafana"`
- [ ] **Step 10: Merge** — `git checkout main && git merge --no-ff chore/observability-stack && git branch -d chore/observability-stack`

---

### Task 17: README run instructions

**Files:** Modify `README.md`.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b docs/readme-run-instructions`

- [ ] **Step 2: Rewrite `README.md`**

```markdown
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
```

- [ ] **Step 3: Commit** — `git add README.md && git commit -m "docs: add run instructions"`
- [ ] **Step 4: Merge** — `git checkout main && git merge --no-ff docs/readme-run-instructions && git branch -d docs/readme-run-instructions`

---

### Task 18: Bloco B & C deliverable documents

**Files:** Create `ARCHITECTURE.md`, `DECISIONS.md`.

- [ ] **Step 1: Branch** — `git checkout main && git checkout -b docs/bloco-b-c-deliverables`

- [ ] **Step 2: `ARCHITECTURE.md`**

```markdown
# Architecture (Bloco B)

## Diagram

\`\`\`mermaid
flowchart LR
    CSV[events.csv: 100 real + 4 synthetic] -->|POST /events/simulate| SIM[SimulateIngestionUseCase]
    SIM -->|publish, key=deviceId| T1[device.property-change.events]
    SIM -->|publish, key=deviceId| T2[device.status-change.events]
    SIM -->|publish, key=deviceId| T3[device.motion-detection.events]
    SIM -->|publish, key=deviceId| T4[device.other.events]

    T1 --> L1[PropertyChangeEventListener]
    T2 --> L2[StatusChangeEventListener]
    T3 --> L3[MotionDetectionEventListener]
    T4 --> L4[OtherEventListener]

    L1 --> UC[RecordDeviceEventAuditUseCase]
    L2 --> UC
    L3 --> UC
    L4 --> UC

    UC -->|duplicate| LOG1[log only]
    UC -->|success| DB[(Postgres audit_records)]
    UC -->|exhausted retries| DLT[per-topic .DLT]

    DB --> API[GET /audits]
\`\`\`

## Classification: EventCategory from msgType

The real dataset has no device-type column. Its `msgType` field (always present) is
grouped into the four kinds of event the case brief itself names: `PROPERTY_CHANGE`
(`iotProperty`), `STATUS_CHANGE` (`online`/`offline`), `MOTION_DETECTION`
(`videoMotion`/`mobileDetect`/`human`/`smartVideoMotion`), `OTHER` (everything else,
including any future unrecognized `msgType`).

## Scalability and Kafka partitioning strategy

Each category gets its own topic, partitioned by `deviceId`, 3 partitions at case
scale. At millions-of-events/day scale, partition count would grow per topic based on
observed throughput and target consumer parallelism; `PROPERTY_CHANGE` (45% of this
sample) would likely need more partitions than `OTHER` (10%).

## Consumer groups and parallelism

All four listeners share consumer group `audit-service` — one logical consuming
application across four topics. Concurrency (3, matching partition count) parallelizes
within each topic. Scaling horizontally means more `audit-service` instances, up to the
partition-count ceiling per topic.

## Ordering

Keying every record by `deviceId` guarantees one device's events land on the same
partition, consumed in order, by one consumer thread at a time. Different devices may
interleave, by design, for parallelism.

## Idempotency and duplicate handling

`eventId` is resolved once, in `device.core`: the dataset's own native id
(`body_completo.id`, present on camera/alarm-style events, or `body_completo.alarmId`
for `iotEvent`) when available, else a deterministic SHA-256 hash of
`deviceId|eventType|timestamp|payload`. `audit` checks `existsByDedupKey(eventId)`
before processing; a duplicate is logged and skipped. A database-level unique
constraint on `event_id` backs this up against race conditions.

## Retry and handling of unprocessable messages

A handler failure retries 3 times with exponential backoff (1s, 2s, 4s). If still
failing, the record is dead-lettered to `<topic>.DLT` and logged with full context —
no audit row is written. Replay from a DLT is a deliberate, manual operational action,
not automated by this application.

## Observability

Every behaviour emits exactly one structured log line (success or failure), carrying
topic/partition/offset/key context for Kafka-driven behaviours. Metrics
(`device_events_processed_total`, `device_events_duplicate_total`,
`device_events_dlq_total`, Kafka consumer lag) are scraped by Prometheus and shown in
a provisioned Grafana dashboard.

## Availability and fault tolerance

A single bad message never blocks a partition beyond its retry budget. The CSV adapter
tolerates malformed individual rows without aborting the batch (demonstrated by the
real dataset's 2 blank-`did` rows, resolved via a fallback rather than dropped). At
production scale, Kafka replication factor > 1 and multiple `audit-service` instances
would remove single points of failure (this case's local Kafka is a single-node KRaft
broker for simplicity).
```

- [ ] **Step 3: `DECISIONS.md`**

```markdown
# Technical Decisions & Trade-offs (Bloco C)

| Decision | Alternative considered | Why this way |
|---|---|---|
| `EventCategory` derived from the real `msgType` field | A fabricated `device_type` column (the original assumption, before the real CSV arrived) | The real data has no such column; `msgType` is the only always-present categorical field and maps directly onto the case brief's own four event kinds |
| 4 topics, grouping `msgType`'s 11 raw values | One topic per raw `msgType` | 11 topics for very uneven volumes (1 row up to 45 rows) fragments operations for no benefit at this scale |
| `timestamp_log` as the single timestamp source, parsed via `OffsetDateTime` | The nested `utcTime`/`time` fields inside `body_completo` | Those vary format (epoch seconds vs. compact strings) by message family; `timestamp_log` is uniform across all 100 rows |
| Native id (`body_completo.id`/`alarmId`) preferred over hash, when present | Always hash | Real native ids exist for 26/100 rows, verified unique; more faithful to the source system |
| Blank `did` → fallback to `body_completo.deviceId` → `"UNKNOWN"` | Reject the row at ingestion | Demonstrates RT04 tolerance on real, mildly inconsistent data instead of dropping 2 real rows |
| DLQ demo via 4 appended synthetic rows, clearly labeled | Force a failure into a real row; cover DLQ only in the integration test | The case explicitly allows adding events beyond the original 100; a labeled synthetic row keeps the real dataset unmodified and still demonstrates retry/DLQ live via `docker compose` |
| Two independent domains (`device`, `audit`) | One domain covering ingestion+processing+audit | Audit is one of potentially several independent consumers of the event stream (e.g. a future `notification` domain) |
| Ports as Java interfaces, standard Spring autowiring | Abstract classes; manual composition root | Idiomatic Java/Spring; same dependency-inversion guarantee |
| Duplicates/failures: log only, no audit row, no `status` field | Persist `DUPLICATE`/`FAILED` rows | The audit table should only ever reflect successfully processed events |
| PostgreSQL for the audit store | H2 | Real persistence across restarts; works with Testcontainers and the Docker Compose bonus |
| One structured `ExecutionLog` per behaviour | Scattered `logger.info` calls | Every behaviour's outcome is a single, greppable, structured line |

## Known limitations

- No authentication/authorization on the REST endpoints — out of scope for the case.
- DLT replay is manual, by design.
- A frontend for browsing `/audits` is a planned separate project, not part of this submission.
```

- [ ] **Step 4: Commit** — `git add ARCHITECTURE.md DECISIONS.md && git commit -m "docs: add Bloco B architecture and Bloco C decisions deliverables"`
- [ ] **Step 5: Merge** — `git checkout main && git merge --no-ff docs/bloco-b-c-deliverables && git branch -d docs/bloco-b-c-deliverables`
