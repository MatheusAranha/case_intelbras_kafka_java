# IoT Device Event Processing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Intelbras case backend: a Spring Boot app that ingests IoT device events from a CSV, publishes them to Kafka (topic-per-device-type), consumes them in an independent `audit` domain with idempotent, retried, dead-lettered processing, persists an audit trail in Postgres, and exposes it over REST — fully observable and tested.

**Architecture:** Hexagonal, domain-per-feature (`domains/device`, `domains/audit`), each with a framework-free `core/` and a Spring-wired `adapters/`. Cross-domain communication happens only through the Kafka wire JSON, never shared Java types. See CLAUDE.md for the conventions every task below follows (naming, the one-log-per-behaviour `ExecutionLog` pattern, one-branch-per-task git workflow).

**Tech Stack:** Java 21, Maven, Spring Boot 3.3.4 (web, kafka, data-jpa, actuator), PostgreSQL + Flyway, Apache Kafka (KRaft), Micrometer + Prometheus + Grafana, JUnit 5 + Mockito + Testcontainers (Kafka, Postgres) + Awaitility, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-10-10-iot-device-event-kafka-design.md`

## Global Constraints

- Java 21, Maven, Spring Boot 3.3.4 parent, package root `com.intelbras.iotevents`.
- `core/` packages: zero Spring/Kafka/JPA imports, unit-testable with no infra running.
- No cross-domain Java imports: `audit` never imports `device.core.*`. Only the Kafka JSON message crosses the boundary.
- Ports are Java interfaces in `core/port/`, implemented by `adapters/`, wired by standard Spring autowiring (constructor injection, no manual composition root).
- Topics: `device.camera.events`, `device.sensor.events`, `device.alarm.events`, `device.generic.events` — 3 partitions each, keyed by `deviceId`. DLT per topic: `<topic>.DLT`, 1 partition.
- Consumer group: `audit-service`, concurrency 3 per listener.
- Duplicates (dedup key already seen): log only, no audit row, no error.
- Failures (handler throws): retry with backoff, then dead-letter + log only — no audit row, no `status` field on `AuditRecord` at all.
- Every behaviour (use case `execute()`, listener `onMessage`, controller handler) emits exactly one `ExecutionLog` terminal call (`info` or `error`), never zero, never more than one. See CLAUDE.md "Logging" section for the full pattern and the `ExecutionLog`/`LogStep` shapes — every task below follows it verbatim.
- Git: every task starts on its own branch off `main` (`{type}/{kebab-case-description}`), merges back to `main` only after its tests pass; commit messages are the message only, no trailers.

---

### Task 1: Project bootstrap

**Files:**
- Create: `pom.xml`
- Create: `src/main/resources/application.yml`
- Create: `src/main/java/com/intelbras/iotevents/IotEventsApplication.java`
- Modify: `.gitignore`
- Create: `docker-compose.yml`

**Interfaces:**
- Produces: a compilable Maven project; `application.yml` properties (`spring.datasource.*`, `spring.kafka.*`, `server.port`) that every later task's Spring beans rely on; `docker-compose.yml` services `kafka` and `postgres` that later tasks' adapters connect to locally.

- [ ] **Step 1: Create the task branch**

```bash
git checkout main
git pull --ff-only || true
git checkout -b chore/project-bootstrap
```

- [ ] **Step 2: Write `pom.xml`**

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
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-registry-prometheus</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-database-postgresql</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.commons</groupId>
            <artifactId>commons-csv</artifactId>
            <version>1.11.0</version>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>kafka</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>postgresql</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <version>4.2.2</version>
            <scope>test</scope>
        </dependency>
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
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 3: Write `src/main/resources/application.yml`**

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

- [ ] **Step 4: Write the main class**

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

- [ ] **Step 5: Update `.gitignore`**

Add these lines to the existing `.gitignore`:

```
# Maven
target/

# IDE
.idea/
*.iml
.vscode/
```

- [ ] **Step 6: Write `docker-compose.yml` (Kafka + Postgres only for now — app/observability services added in later tasks)**

```yaml
services:
  kafka:
    image: apache/kafka:3.8.0
    container_name: iotevents-kafka
    ports:
      - "9092:9092"
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
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: iotevents
      POSTGRES_USER: iotevents
      POSTGRES_PASSWORD: iotevents
    volumes:
      - iotevents-postgres-data:/var/lib/postgresql/data

volumes:
  iotevents-postgres-data:
```

- [ ] **Step 7: Verify the project compiles**

Run: `mvn -q -DskipTests compile`
Expected: `BUILD SUCCESS`, no output beyond Maven's normal lifecycle logging.

- [ ] **Step 8: Commit**

```bash
git add pom.xml src/main/resources/application.yml src/main/java/com/intelbras/iotevents/IotEventsApplication.java .gitignore docker-compose.yml
git commit -m "chore: bootstrap Spring Boot project skeleton"
```

- [ ] **Step 9: Merge back to main**

```bash
git checkout main
git merge --no-ff chore/project-bootstrap
git branch -d chore/project-bootstrap
```

---

### Task 2: Common structured logging (`ExecutionLog`)

**Files:**
- Create: `src/main/java/com/intelbras/iotevents/common/logging/LogStep.java`
- Create: `src/main/java/com/intelbras/iotevents/common/logging/ExecutionLog.java`
- Create: `src/main/java/com/intelbras/iotevents/common/logging/StructuredLogger.java`
- Test: `src/test/java/com/intelbras/iotevents/common/logging/ExecutionLogTest.java`

**Interfaces:**
- Produces: `ExecutionLog.start(Class<?> module, String method)`, `.context(String, Object)`, `.step(String)`, `.step(String, Map<String,Object>)`, `.info(String)`, `.error(String, Throwable)`. Every use case, listener, and controller task from here on depends on this exact API.

- [ ] **Step 1: Create the task branch**

```bash
git checkout main
git checkout -b feat/execution-log
```

- [ ] **Step 2: Write the failing test**

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
        assertThat(appender.list.get(0).getFormattedMessage()).contains("Completed successfully");
        assertThat(appender.list.get(0).getFormattedMessage()).contains("evt-1");
        assertThat(appender.list.get(0).getFormattedMessage()).contains("Did something else");
    }

    @Test
    void shouldEmitExactlyOneErrorLineOnFailurePath() {
        ExecutionLog log = ExecutionLog.start(ExecutionLogTest.class, "execute")
            .context("eventId", "evt-2")
            .step("Attempted something");

        RuntimeException boom = new RuntimeException("boom");
        log.error("Failed", boom);

        assertThat(appender.list).hasSize(1);
        assertThat(appender.list.get(0).getFormattedMessage()).contains("Failed");
        assertThat(appender.list.get(0).getFormattedMessage()).contains("evt-2");
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `mvn -q test -Dtest=ExecutionLogTest`
Expected: FAIL — `ExecutionLog`, `LogStep`, `StructuredLogger` do not exist yet.

- [ ] **Step 4: Write `LogStep`**

```java
package com.intelbras.iotevents.common.logging;

import java.util.Map;

public record LogStep(String message, Map<String, Object> data) {
}
```

- [ ] **Step 5: Write `StructuredLogger`**

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
import java.util.Map;

final class StructuredLogger {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    private StructuredLogger() {
    }

    static void info(String message, String module, String method, Instant startedAt,
                      Map<String, Object> context, java.util.List<LogStep> steps) {
        Logger logger = LoggerFactory.getLogger(module);
        logger.info(render(message, module, method, startedAt, context, steps, null));
    }

    static void error(String message, String module, String method, Instant startedAt,
                       Map<String, Object> context, java.util.List<LogStep> steps, Throwable error) {
        Logger logger = LoggerFactory.getLogger(module);
        logger.error(render(message, module, method, startedAt, context, steps, error), error);
    }

    private static String render(String message, String module, String method, Instant startedAt,
                                  Map<String, Object> context, java.util.List<LogStep> steps, Throwable error) {
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

- [ ] **Step 6: Write `ExecutionLog`**

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

- [ ] **Step 7: Run the test to verify it passes**

Run: `mvn -q test -Dtest=ExecutionLogTest`
Expected: `Tests run: 2, Failures: 0, Errors: 0`

- [ ] **Step 8: Commit**

```bash
git add src/main/java/com/intelbras/iotevents/common/logging src/test/java/com/intelbras/iotevents/common/logging
git commit -m "feat: add ExecutionLog structured one-log-per-behaviour logging"
```

- [ ] **Step 9: Merge back to main**

```bash
git checkout main
git merge --no-ff feat/execution-log
git branch -d feat/execution-log
```

---

### Task 3: Device domain core models

**Files:**
- Create: `src/main/java/com/intelbras/iotevents/domains/device/core/DeviceType.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/device/core/DeviceEventRow.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/device/core/DeviceEvent.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/device/core/error/DeviceDomainError.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/device/core/error/DeviceEventValidationError.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/device/core/DedupKeyGenerator.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/device/core/DeviceEventTest.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/device/core/DedupKeyGeneratorTest.java`

**Interfaces:**
- Produces: `DeviceEvent(String eventId, String deviceId, DeviceType deviceType, String eventType, Instant timestamp, Map<String,Object> payload)` with static factory `DeviceEvent.from(DeviceEventRow row)`; `DeviceEventRow(String id, String deviceId, String deviceTypeRaw, String eventTypeRaw, String timestampRaw, Map<String,Object> payload)`; `DeviceType.fromRaw(String)`; `DedupKeyGenerator.generate(String deviceId, String eventType, String timestampRaw, Map<String,Object> payload)`.

- [ ] **Step 1: Create the task branch**

```bash
git checkout main
git checkout -b feat/device-core-models
```

- [ ] **Step 2: Write the failing tests**

```java
package com.intelbras.iotevents.domains.device.core;

import com.intelbras.iotevents.domains.device.core.error.DeviceEventValidationError;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeviceEventTest {

    @Test
    void shouldBuildDeviceEventFromRowWithOwnId() {
        DeviceEventRow row = new DeviceEventRow("evt-1", "cam-001", "camera", "motion_detected",
            "2026-01-10T10:00:00Z", Map.of("confidence", 0.9));

        DeviceEvent event = DeviceEvent.from(row);

        assertThat(event.eventId()).isEqualTo("evt-1");
        assertThat(event.deviceId()).isEqualTo("cam-001");
        assertThat(event.deviceType()).isEqualTo(DeviceType.CAMERA);
        assertThat(event.eventType()).isEqualTo("MOTION_DETECTED");
    }

    @Test
    void shouldFallBackToHashEventIdWhenRowHasNoId() {
        DeviceEventRow row = new DeviceEventRow(null, "cam-001", "camera", "motion_detected",
            "2026-01-10T10:00:00Z", Map.of("confidence", 0.9));

        DeviceEvent event = DeviceEvent.from(row);

        assertThat(event.eventId()).isNotBlank();
        assertThat(event.eventId()).isEqualTo(
            DedupKeyGenerator.generate("cam-001", "MOTION_DETECTED", "2026-01-10T10:00:00Z", Map.of("confidence", 0.9)));
    }

    @Test
    void shouldFallBackToGenericDeviceTypeWhenUnrecognized() {
        DeviceEventRow row = new DeviceEventRow("evt-2", "lock-001", "lock", "status_change",
            "2026-01-10T10:00:00Z", Map.of());

        DeviceEvent event = DeviceEvent.from(row);

        assertThat(event.deviceType()).isEqualTo(DeviceType.GENERIC);
    }

    @Test
    void shouldRejectRowWithoutDeviceId() {
        DeviceEventRow row = new DeviceEventRow("evt-3", "", "camera", "motion_detected",
            "2026-01-10T10:00:00Z", Map.of());

        assertThatThrownBy(() -> DeviceEvent.from(row))
            .isInstanceOf(DeviceEventValidationError.class);
    }

    @Test
    void shouldRejectRowWithInvalidTimestamp() {
        DeviceEventRow row = new DeviceEventRow("evt-4", "cam-001", "camera", "motion_detected",
            "not-a-timestamp", Map.of());

        assertThatThrownBy(() -> DeviceEvent.from(row))
            .isInstanceOf(DeviceEventValidationError.class);
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
        String first = DedupKeyGenerator.generate("cam-001", "MOTION_DETECTED", "2026-01-10T10:00:00Z", Map.of("confidence", 0.9));
        String second = DedupKeyGenerator.generate("cam-001", "MOTION_DETECTED", "2026-01-10T10:00:00Z", Map.of("confidence", 0.9));

        assertThat(first).isEqualTo(second);
    }

    @Test
    void shouldDifferWhenAnyInputDiffers() {
        String first = DedupKeyGenerator.generate("cam-001", "MOTION_DETECTED", "2026-01-10T10:00:00Z", Map.of("confidence", 0.9));
        String second = DedupKeyGenerator.generate("cam-002", "MOTION_DETECTED", "2026-01-10T10:00:00Z", Map.of("confidence", 0.9));

        assertThat(first).isNotEqualTo(second);
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `mvn -q test -Dtest=DeviceEventTest,DedupKeyGeneratorTest`
Expected: FAIL — classes under test do not exist yet.

- [ ] **Step 4: Write `DeviceDomainError` and `DeviceEventValidationError`**

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

- [ ] **Step 5: Write `DeviceType`**

```java
package com.intelbras.iotevents.domains.device.core;

public enum DeviceType {
    CAMERA, SENSOR, ALARM, GENERIC;

    public static DeviceType fromRaw(String raw) {
        if (raw == null || raw.isBlank()) {
            return GENERIC;
        }
        try {
            return DeviceType.valueOf(raw.trim().toUpperCase());
        } catch (IllegalArgumentException unrecognized) {
            return GENERIC;
        }
    }
}
```

- [ ] **Step 6: Write `DedupKeyGenerator`**

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

- [ ] **Step 7: Write `DeviceEventRow`**

```java
package com.intelbras.iotevents.domains.device.core;

import java.util.Map;

public record DeviceEventRow(String id, String deviceId, String deviceTypeRaw, String eventTypeRaw,
                              String timestampRaw, Map<String, Object> payload) {
}
```

- [ ] **Step 8: Write `DeviceEvent`**

```java
package com.intelbras.iotevents.domains.device.core;

import com.intelbras.iotevents.domains.device.core.error.DeviceEventValidationError;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;

public record DeviceEvent(String eventId, String deviceId, DeviceType deviceType, String eventType,
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
            timestamp = Instant.parse(row.timestampRaw());
        } catch (DateTimeParseException invalidTimestamp) {
            throw new DeviceEventValidationError(
                "invalid timestamp '" + row.timestampRaw() + "' for deviceId=" + row.deviceId());
        }

        DeviceType deviceType = DeviceType.fromRaw(row.deviceTypeRaw());
        String eventType = (row.eventTypeRaw() == null || row.eventTypeRaw().isBlank())
            ? "UNKNOWN"
            : row.eventTypeRaw().trim().toUpperCase();
        Map<String, Object> payload = row.payload() == null ? Map.of() : row.payload();

        String eventId = (row.id() != null && !row.id().isBlank())
            ? row.id()
            : DedupKeyGenerator.generate(row.deviceId(), eventType, row.timestampRaw(), payload);

        return new DeviceEvent(eventId, row.deviceId(), deviceType, eventType, timestamp, payload);
    }
}
```

- [ ] **Step 9: Run the tests to verify they pass**

Run: `mvn -q test -Dtest=DeviceEventTest,DedupKeyGeneratorTest`
Expected: `Tests run: 7, Failures: 0, Errors: 0`

- [ ] **Step 10: Commit**

```bash
git add src/main/java/com/intelbras/iotevents/domains/device/core src/test/java/com/intelbras/iotevents/domains/device/core
git commit -m "feat: add device domain core models and validation"
```

- [ ] **Step 11: Merge back to main**

```bash
git checkout main
git merge --no-ff feat/device-core-models
git branch -d feat/device-core-models
```

---

### Task 4: Device domain ports and `SimulateIngestionUseCase`

**Files:**
- Create: `src/main/java/com/intelbras/iotevents/domains/device/core/port/EventSourcePort.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/device/core/port/EventPublisherPort.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/device/core/usecase/SimulateIngestionUseCase.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/device/core/usecase/SimulateIngestionUseCaseTest.java`

**Interfaces:**
- Consumes: `DeviceEvent` from Task 3.
- Produces: `EventSourcePort.readAll(): List<DeviceEvent>`; `EventPublisherPort.publish(DeviceEvent event): void`; `SimulateIngestionUseCase(EventSourcePort, EventPublisherPort)` with `execute(): int` (count published). Task 7 (REST controller) and Task 5/7 (adapters) depend on these exact signatures.

- [ ] **Step 1: Create the task branch**

```bash
git checkout main
git checkout -b feat/simulate-ingestion-usecase
```

- [ ] **Step 2: Write the failing test using in-test fake adapters**

```java
package com.intelbras.iotevents.domains.device.core.usecase;

import com.intelbras.iotevents.domains.device.core.DeviceEvent;
import com.intelbras.iotevents.domains.device.core.DeviceType;
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
        DeviceEvent eventOne = new DeviceEvent("evt-1", "cam-001", DeviceType.CAMERA, "MOTION_DETECTED",
            Instant.parse("2026-01-10T10:00:00Z"), Map.of());
        DeviceEvent eventTwo = new DeviceEvent("evt-2", "sensor-001", DeviceType.SENSOR, "STATUS_CHANGE",
            Instant.parse("2026-01-10T10:01:00Z"), Map.of());

        EventSourcePort source = () -> List.of(eventOne, eventTwo);
        List<DeviceEvent> published = new ArrayList<>();
        EventPublisherPort publisher = published::add;

        SimulateIngestionUseCase useCase = new SimulateIngestionUseCase(source, publisher);

        int publishedCount = useCase.execute();

        assertThat(publishedCount).isEqualTo(2);
        assertThat(published).containsExactly(eventOne, eventTwo);
    }

    @Test
    void shouldReturnZeroWhenSourceHasNoEvents() {
        EventSourcePort source = List::of;
        EventPublisherPort publisher = event -> { throw new AssertionError("should not publish anything"); };

        SimulateIngestionUseCase useCase = new SimulateIngestionUseCase(source, publisher);

        assertThat(useCase.execute()).isZero();
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `mvn -q test -Dtest=SimulateIngestionUseCaseTest`
Expected: FAIL — `EventSourcePort`, `EventPublisherPort`, `SimulateIngestionUseCase` do not exist yet.

- [ ] **Step 4: Write the ports**

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

- [ ] **Step 5: Write `SimulateIngestionUseCase`**

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

- [ ] **Step 6: Run the test to verify it passes**

Run: `mvn -q test -Dtest=SimulateIngestionUseCaseTest`
Expected: `Tests run: 2, Failures: 0, Errors: 0`

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/intelbras/iotevents/domains/device/core/port src/main/java/com/intelbras/iotevents/domains/device/core/usecase src/test/java/com/intelbras/iotevents/domains/device/core/usecase
git commit -m "feat: add device domain ports and SimulateIngestionUseCase"
```

- [ ] **Step 8: Merge back to main**

```bash
git checkout main
git merge --no-ff feat/simulate-ingestion-usecase
git branch -d feat/simulate-ingestion-usecase
```

---

### Task 5: CSV event source adapter + sample dataset

**Files:**
- Create: `src/main/resources/events.csv`
- Create: `src/main/java/com/intelbras/iotevents/domains/device/adapters/csv/CsvEventSourceAdapter.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/device/adapters/csv/CsvEventSourceAdapterTest.java`

**Interfaces:**
- Consumes: `EventSourcePort` (Task 4), `DeviceEvent`/`DeviceEventRow` (Task 3).
- Produces: a Spring `@Component` implementing `EventSourcePort`, reading `events.csv` from the classpath.

- [ ] **Step 1: Create the task branch**

```bash
git checkout main
git checkout -b feat/csv-event-source-adapter
```

- [ ] **Step 2: Write the sample dataset `src/main/resources/events.csv`**

This stands in for the real 100-event CSV until it arrives (per the design spec). It covers all four device types, one unrecognized device type (`lock`, falls back to `GENERIC`), and one row carrying `simulateFailure: true` to exercise the retry/DLQ path end-to-end later.

```csv
id,device_id,device_type,event_type,timestamp,payload_json
evt-001,cam-001,camera,motion_detected,2026-01-10T10:00:00Z,"{""confidence"":0.92}"
evt-002,cam-002,camera,property_change,2026-01-10T10:01:00Z,"{""resolution"":""1080p""}"
evt-003,sensor-001,sensor,motion_detected,2026-01-10T10:02:00Z,"{""zone"":""living_room""}"
evt-004,sensor-002,sensor,status_change,2026-01-10T10:03:00Z,"{""battery"":""low""}"
evt-005,alarm-001,alarm,status_change,2026-01-10T10:04:00Z,"{""status"":""ARMED""}"
evt-006,alarm-002,alarm,status_change,2026-01-10T10:05:00Z,"{""status"":""TRIGGERED""}"
evt-007,lock-001,lock,status_change,2026-01-10T10:06:00Z,"{""status"":""LOCKED""}"
evt-008,cam-001,camera,motion_detected,2026-01-10T10:07:00Z,"{""confidence"":0.75}"
evt-009,sensor-001,sensor,motion_detected,2026-01-10T10:08:00Z,"{""zone"":""kitchen""}"
evt-010,alarm-001,alarm,status_change,2026-01-10T10:09:00Z,"{""simulateFailure"":true}"
evt-011,cam-003,camera,motion_detected,2026-01-10T10:10:00Z,"{""confidence"":0.60}"
evt-012,sensor-003,sensor,status_change,2026-01-10T10:11:00Z,"{""battery"":""full""}"
```

- [ ] **Step 3: Write the failing test**

```java
package com.intelbras.iotevents.domains.device.adapters.csv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.intelbras.iotevents.domains.device.core.DeviceEvent;
import com.intelbras.iotevents.domains.device.core.DeviceType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CsvEventSourceAdapterTest {

    private final CsvEventSourceAdapter adapter = new CsvEventSourceAdapter(new ObjectMapper());

    @Test
    void shouldReadAllValidRowsFromTheSampleCsv() {
        List<DeviceEvent> events = adapter.readAll();

        assertThat(events).hasSize(12);
        assertThat(events).anySatisfy(event -> {
            assertThat(event.eventId()).isEqualTo("evt-001");
            assertThat(event.deviceType()).isEqualTo(DeviceType.CAMERA);
            assertThat(event.payload()).containsEntry("confidence", 0.92);
        });
    }

    @Test
    void shouldFallBackToGenericDeviceTypeForUnrecognizedDeviceType() {
        List<DeviceEvent> events = adapter.readAll();

        DeviceEvent lockEvent = events.stream()
            .filter(event -> event.eventId().equals("evt-007"))
            .findFirst()
            .orElseThrow();

        assertThat(lockEvent.deviceType()).isEqualTo(DeviceType.GENERIC);
    }

    @Test
    void shouldCarryForcedFailureFlagThroughUnchanged() {
        List<DeviceEvent> events = adapter.readAll();

        DeviceEvent failingEvent = events.stream()
            .filter(event -> event.eventId().equals("evt-010"))
            .findFirst()
            .orElseThrow();

        assertThat(failingEvent.payload()).containsEntry("simulateFailure", true);
    }
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `mvn -q test -Dtest=CsvEventSourceAdapterTest`
Expected: FAIL — `CsvEventSourceAdapter` does not exist yet.

- [ ] **Step 5: Write `CsvEventSourceAdapter`**

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
        ExecutionLog log = ExecutionLog.start(CsvEventSourceAdapter.class, "readAll")
            .context("resource", CSV_RESOURCE);
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
                        "rowNumber", record.getRecordNumber(),
                        "reason", String.valueOf(rowError.getMessage())));
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
        return new DeviceEventRow(
            blankToNull(getOrNull(record, "id")),
            record.get("device_id"),
            record.get("device_type"),
            record.get("event_type"),
            record.get("timestamp"),
            parsePayload(getOrNull(record, "payload_json")));
    }

    private Map<String, Object> parsePayload(String payloadJson) {
        if (payloadJson == null || payloadJson.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(payloadJson, new TypeReference<Map<String, Object>>() {
            });
        } catch (Exception invalidJson) {
            return Map.of();
        }
    }

    private String getOrNull(CSVRecord record, String column) {
        return record.isMapped(column) ? record.get(column) : null;
    }

    private String blankToNull(String value) {
        return (value == null || value.isBlank()) ? null : value;
    }
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `mvn -q test -Dtest=CsvEventSourceAdapterTest`
Expected: `Tests run: 3, Failures: 0, Errors: 0`

- [ ] **Step 7: Commit**

```bash
git add src/main/resources/events.csv src/main/java/com/intelbras/iotevents/domains/device/adapters/csv src/test/java/com/intelbras/iotevents/domains/device/adapters/csv
git commit -m "feat: add CSV event source adapter and sample dataset"
```

- [ ] **Step 8: Merge back to main**

```bash
git checkout main
git merge --no-ff feat/csv-event-source-adapter
git branch -d feat/csv-event-source-adapter
```

---

### Task 6: Kafka topics configuration

**Files:**
- Create: `src/main/java/com/intelbras/iotevents/config/KafkaTopics.java`
- Create: `src/main/java/com/intelbras/iotevents/config/KafkaTopicConfig.java`

**Interfaces:**
- Produces: string constants `KafkaTopics.DEVICE_CAMERA_EVENTS`, `DEVICE_SENSOR_EVENTS`, `DEVICE_ALARM_EVENTS`, `DEVICE_GENERIC_EVENTS`, `DLT_SUFFIX`, `PARTITIONS`, `DLT_PARTITIONS`, `CONSUMER_GROUP`, and `KafkaTopics.allTopics(): List<String>`. Task 7 (publisher), Task 12 (DLQ config), and Task 13 (listeners) all reference these constants directly (listener `@KafkaListener(topics = ...)` annotations require compile-time constants, which `public static final String` satisfies).

This task has no independent runtime behaviour to unit test — it's Spring `@Bean` wiring, validated when the broker is up (Task 14's integration test confirms the topics actually get created). No test step; "deliverable" is a successful compile and a quick manual check against a running Kafka.

- [ ] **Step 1: Create the task branch**

```bash
git checkout main
git checkout -b chore/kafka-topics-config
```

- [ ] **Step 2: Write `KafkaTopics`**

```java
package com.intelbras.iotevents.config;

import java.util.List;

public final class KafkaTopics {

    public static final String DEVICE_CAMERA_EVENTS = "device.camera.events";
    public static final String DEVICE_SENSOR_EVENTS = "device.sensor.events";
    public static final String DEVICE_ALARM_EVENTS = "device.alarm.events";
    public static final String DEVICE_GENERIC_EVENTS = "device.generic.events";
    public static final String DLT_SUFFIX = ".DLT";
    public static final int PARTITIONS = 3;
    public static final int DLT_PARTITIONS = 1;
    public static final String CONSUMER_GROUP = "audit-service";

    private KafkaTopics() {
    }

    public static List<String> allTopics() {
        return List.of(DEVICE_CAMERA_EVENTS, DEVICE_SENSOR_EVENTS, DEVICE_ALARM_EVENTS, DEVICE_GENERIC_EVENTS);
    }
}
```

- [ ] **Step 3: Write `KafkaTopicConfig`**

```java
package com.intelbras.iotevents.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic deviceCameraEventsTopic() {
        return topic(KafkaTopics.DEVICE_CAMERA_EVENTS);
    }

    @Bean
    public NewTopic deviceSensorEventsTopic() {
        return topic(KafkaTopics.DEVICE_SENSOR_EVENTS);
    }

    @Bean
    public NewTopic deviceAlarmEventsTopic() {
        return topic(KafkaTopics.DEVICE_ALARM_EVENTS);
    }

    @Bean
    public NewTopic deviceGenericEventsTopic() {
        return topic(KafkaTopics.DEVICE_GENERIC_EVENTS);
    }

    @Bean
    public NewTopic deviceCameraEventsDlt() {
        return dlt(KafkaTopics.DEVICE_CAMERA_EVENTS);
    }

    @Bean
    public NewTopic deviceSensorEventsDlt() {
        return dlt(KafkaTopics.DEVICE_SENSOR_EVENTS);
    }

    @Bean
    public NewTopic deviceAlarmEventsDlt() {
        return dlt(KafkaTopics.DEVICE_ALARM_EVENTS);
    }

    @Bean
    public NewTopic deviceGenericEventsDlt() {
        return dlt(KafkaTopics.DEVICE_GENERIC_EVENTS);
    }

    private NewTopic topic(String name) {
        return TopicBuilder.name(name).partitions(KafkaTopics.PARTITIONS).replicas(1).build();
    }

    private NewTopic dlt(String baseTopicName) {
        return TopicBuilder.name(baseTopicName + KafkaTopics.DLT_SUFFIX).partitions(KafkaTopics.DLT_PARTITIONS).replicas(1).build();
    }
}
```

- [ ] **Step 4: Verify the project compiles**

Run: `mvn -q -DskipTests compile`
Expected: `BUILD SUCCESS`

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/intelbras/iotevents/config/KafkaTopics.java src/main/java/com/intelbras/iotevents/config/KafkaTopicConfig.java
git commit -m "chore: declare Kafka topics and DLT provisioning"
```

- [ ] **Step 6: Merge back to main**

```bash
git checkout main
git merge --no-ff chore/kafka-topics-config
git branch -d chore/kafka-topics-config
```

---

### Task 7: Kafka event publisher adapter + simulation REST endpoint

**Files:**
- Create: `src/main/java/com/intelbras/iotevents/domains/device/adapters/kafka/KafkaEventPublisherAdapter.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/device/adapters/web/EventSimulationController.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/device/adapters/kafka/KafkaEventPublisherAdapterTest.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/device/adapters/web/EventSimulationControllerTest.java`

**Interfaces:**
- Consumes: `EventPublisherPort` (Task 4), `DeviceEvent`/`DeviceType` (Task 3), `KafkaTopics` (Task 6), `SimulateIngestionUseCase` (Task 4).
- Produces: `KafkaEventPublisherAdapter implements EventPublisherPort`; `POST /events/simulate` returning `{"publishedCount": <int>}`.

- [ ] **Step 1: Create the task branch**

```bash
git checkout main
git checkout -b feat/kafka-publisher-and-simulation-endpoint
```

- [ ] **Step 2: Write the failing publisher test (mocked `KafkaTemplate`, no broker needed)**

```java
package com.intelbras.iotevents.domains.device.adapters.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.intelbras.iotevents.config.KafkaTopics;
import com.intelbras.iotevents.domains.device.core.DeviceEvent;
import com.intelbras.iotevents.domains.device.core.DeviceType;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class KafkaEventPublisherAdapterTest {

    @Test
    void shouldPublishToTheCameraTopicKeyedByDeviceId() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        KafkaEventPublisherAdapter adapter = new KafkaEventPublisherAdapter(kafkaTemplate, new ObjectMapper());

        DeviceEvent event = new DeviceEvent("evt-1", "cam-001", DeviceType.CAMERA, "MOTION_DETECTED",
            Instant.parse("2026-01-10T10:00:00Z"), Map.of("confidence", 0.9));

        adapter.publish(event);

        verify(kafkaTemplate).send(KafkaTopics.DEVICE_CAMERA_EVENTS, "cam-001", anyString());
    }

    @Test
    void shouldPublishToTheGenericTopicForGenericDeviceType() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        KafkaEventPublisherAdapter adapter = new KafkaEventPublisherAdapter(kafkaTemplate, new ObjectMapper());

        DeviceEvent event = new DeviceEvent("evt-2", "lock-001", DeviceType.GENERIC, "STATUS_CHANGE",
            Instant.parse("2026-01-10T10:00:00Z"), Map.of());

        adapter.publish(event);

        verify(kafkaTemplate).send(KafkaTopics.DEVICE_GENERIC_EVENTS, "lock-001", anyString());
    }
}
```

- [ ] **Step 3: Write the failing controller slice test**

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
        when(simulateIngestionUseCase.execute()).thenReturn(12);

        mockMvc.perform(post("/events/simulate"))
            .andExpect(status().isOk())
            .andExpect(content().json("{\"publishedCount\":12}"));
    }
}
```

- [ ] **Step 4: Run both tests to verify they fail**

Run: `mvn -q test -Dtest=KafkaEventPublisherAdapterTest,EventSimulationControllerTest`
Expected: FAIL — `KafkaEventPublisherAdapter` and `EventSimulationController` do not exist yet.

- [ ] **Step 5: Write `KafkaEventPublisherAdapter`**

```java
package com.intelbras.iotevents.domains.device.adapters.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.intelbras.iotevents.common.logging.ExecutionLog;
import com.intelbras.iotevents.config.KafkaTopics;
import com.intelbras.iotevents.domains.device.core.DeviceEvent;
import com.intelbras.iotevents.domains.device.core.DeviceType;
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
            .context("deviceType", event.deviceType());
        try {
            String topic = resolveTopic(event.deviceType());
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

    private String resolveTopic(DeviceType deviceType) {
        return switch (deviceType) {
            case CAMERA -> KafkaTopics.DEVICE_CAMERA_EVENTS;
            case SENSOR -> KafkaTopics.DEVICE_SENSOR_EVENTS;
            case ALARM -> KafkaTopics.DEVICE_ALARM_EVENTS;
            case GENERIC -> KafkaTopics.DEVICE_GENERIC_EVENTS;
        };
    }

    private Map<String, Object> toWireMessage(DeviceEvent event) {
        return Map.of(
            "eventId", event.eventId(),
            "deviceId", event.deviceId(),
            "deviceType", event.deviceType().name(),
            "eventType", event.eventType(),
            "timestamp", event.timestamp().toString(),
            "payload", event.payload());
    }
}
```

- [ ] **Step 6: Write `EventSimulationController`**

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
        int publishedCount = simulateIngestionUseCase.execute();
        return ResponseEntity.ok(new SimulationResponse(publishedCount));
    }

    public record SimulationResponse(int publishedCount) {
    }
}
```

- [ ] **Step 7: Register `SimulateIngestionUseCase` as a Spring bean**

Create `src/main/java/com/intelbras/iotevents/domains/device/DeviceUseCaseConfig.java`:

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

- [ ] **Step 8: Run both tests to verify they pass**

Run: `mvn -q test -Dtest=KafkaEventPublisherAdapterTest,EventSimulationControllerTest`
Expected: `Tests run: 3, Failures: 0, Errors: 0`

- [ ] **Step 9: Commit**

```bash
git add src/main/java/com/intelbras/iotevents/domains/device/adapters/kafka src/main/java/com/intelbras/iotevents/domains/device/adapters/web src/main/java/com/intelbras/iotevents/domains/device/DeviceUseCaseConfig.java src/test/java/com/intelbras/iotevents/domains/device/adapters/kafka src/test/java/com/intelbras/iotevents/domains/device/adapters/web
git commit -m "feat: add Kafka publisher adapter and simulation REST endpoint"
```

- [ ] **Step 10: Merge back to main**

```bash
git checkout main
git merge --no-ff feat/kafka-publisher-and-simulation-endpoint
git branch -d feat/kafka-publisher-and-simulation-endpoint
```

---

### Task 8: Audit domain core models

**Files:**
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/core/IncomingDeviceEvent.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/core/AuditRecord.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/core/AuditQueryFilter.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/core/KafkaMessageContext.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/core/error/AuditDomainError.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/core/error/SimulatedProcessingFailureError.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/core/port/AuditRepositoryPort.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/audit/core/AuditRecordTest.java`

**Interfaces:**
- Produces: `IncomingDeviceEvent(String eventId, String deviceId, String deviceType, String eventType, Instant timestamp, Map<String,Object> payload)` — audit's own representation, deliberately not importing `device.core.DeviceEvent`; `AuditRecord(String dedupKey, String deviceId, String deviceType, String eventType, String generalInfo, Instant consumedAt, Instant createdAt)` with static factory `AuditRecord.processedNow(IncomingDeviceEvent, String generalInfo)`; `AuditQueryFilter(String deviceId, String eventType, Instant from, Instant to)`; `KafkaMessageContext(String topic, int partition, long offset, String key)`; `AuditRepositoryPort.save(AuditRecord)`, `.existsByDedupKey(String): boolean`, `.query(AuditQueryFilter): List<AuditRecord>`.

- [ ] **Step 1: Create the task branch**

```bash
git checkout main
git checkout -b feat/audit-core-models
```

- [ ] **Step 2: Write the failing test**

```java
package com.intelbras.iotevents.domains.audit.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AuditRecordTest {

    @Test
    void shouldBuildAProcessedAuditRecordFromAnIncomingEvent() {
        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-1", "cam-001", "CAMERA", "MOTION_DETECTED",
            Instant.parse("2026-01-10T10:00:00Z"), Map.of("confidence", 0.9));

        AuditRecord record = AuditRecord.processedNow(event, "Camera cam-001 reported MOTION_DETECTED");

        assertThat(record.dedupKey()).isEqualTo("evt-1");
        assertThat(record.deviceId()).isEqualTo("cam-001");
        assertThat(record.deviceType()).isEqualTo("CAMERA");
        assertThat(record.eventType()).isEqualTo("MOTION_DETECTED");
        assertThat(record.generalInfo()).isEqualTo("Camera cam-001 reported MOTION_DETECTED");
        assertThat(record.consumedAt()).isEqualTo(record.createdAt());
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `mvn -q test -Dtest=AuditRecordTest`
Expected: FAIL — `IncomingDeviceEvent` and `AuditRecord` do not exist yet.

- [ ] **Step 4: Write `IncomingDeviceEvent`**

```java
package com.intelbras.iotevents.domains.audit.core;

import java.time.Instant;
import java.util.Map;

public record IncomingDeviceEvent(String eventId, String deviceId, String deviceType, String eventType,
                                   Instant timestamp, Map<String, Object> payload) {
}
```

- [ ] **Step 5: Write `AuditRecord`**

```java
package com.intelbras.iotevents.domains.audit.core;

import java.time.Instant;

public record AuditRecord(String dedupKey, String deviceId, String deviceType, String eventType,
                           String generalInfo, Instant consumedAt, Instant createdAt) {

    public static AuditRecord processedNow(IncomingDeviceEvent event, String generalInfo) {
        Instant now = Instant.now();
        return new AuditRecord(event.eventId(), event.deviceId(), event.deviceType(), event.eventType(),
            generalInfo, now, now);
    }
}
```

- [ ] **Step 6: Write `AuditQueryFilter` and `KafkaMessageContext`**

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

- [ ] **Step 7: Write the error hierarchy**

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

- [ ] **Step 8: Write `AuditRepositoryPort`**

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

- [ ] **Step 9: Run the test to verify it passes**

Run: `mvn -q test -Dtest=AuditRecordTest`
Expected: `Tests run: 1, Failures: 0, Errors: 0`

- [ ] **Step 10: Commit**

```bash
git add src/main/java/com/intelbras/iotevents/domains/audit/core src/test/java/com/intelbras/iotevents/domains/audit/core
git commit -m "feat: add audit domain core models and repository port"
```

- [ ] **Step 11: Merge back to main**

```bash
git checkout main
git merge --no-ff feat/audit-core-models
git branch -d feat/audit-core-models
```

---

### Task 9: Audit event handlers (per device type)

**Files:**
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/core/handler/DeviceEventAuditHandler.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/core/handler/CameraEventAuditHandler.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/core/handler/SensorEventAuditHandler.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/core/handler/AlarmEventAuditHandler.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/core/handler/GenericEventAuditHandler.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/audit/core/handler/DeviceEventAuditHandlersTest.java`

**Interfaces:**
- Consumes: `IncomingDeviceEvent` (Task 8).
- Produces: `DeviceEventAuditHandler.supports(String deviceType): boolean` and `.handle(IncomingDeviceEvent): String`, implemented by four `@Component`s keyed to `"CAMERA"`, `"SENSOR"`, `"ALARM"`, `"GENERIC"` respectively. Task 10's use case resolves one of these four by `supports()`, with the `GenericEventAuditHandler` instance injected separately as the explicit fallback.

- [ ] **Step 1: Create the task branch**

```bash
git checkout main
git checkout -b feat/audit-device-event-handlers
```

- [ ] **Step 2: Write the failing test**

```java
package com.intelbras.iotevents.domains.audit.core.handler;

import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DeviceEventAuditHandlersTest {

    private final CameraEventAuditHandler cameraHandler = new CameraEventAuditHandler();
    private final SensorEventAuditHandler sensorHandler = new SensorEventAuditHandler();
    private final AlarmEventAuditHandler alarmHandler = new AlarmEventAuditHandler();
    private final GenericEventAuditHandler genericHandler = new GenericEventAuditHandler();

    @Test
    void cameraHandlerShouldSupportOnlyCameraDeviceType() {
        assertThat(cameraHandler.supports("CAMERA")).isTrue();
        assertThat(cameraHandler.supports("SENSOR")).isFalse();
    }

    @Test
    void cameraHandlerShouldDescribeTheEvent() {
        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-1", "cam-001", "CAMERA", "MOTION_DETECTED",
            Instant.parse("2026-01-10T10:00:00Z"), Map.of("confidence", 0.9));

        String generalInfo = cameraHandler.handle(event);

        assertThat(generalInfo).contains("cam-001").contains("MOTION_DETECTED");
    }

    @Test
    void sensorHandlerShouldSupportOnlySensorDeviceType() {
        assertThat(sensorHandler.supports("SENSOR")).isTrue();
        assertThat(sensorHandler.supports("CAMERA")).isFalse();
    }

    @Test
    void alarmHandlerShouldIncludeStatusFromPayload() {
        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-2", "alarm-001", "ALARM", "STATUS_CHANGE",
            Instant.parse("2026-01-10T10:00:00Z"), Map.of("status", "ARMED"));

        String generalInfo = alarmHandler.handle(event);

        assertThat(generalInfo).contains("alarm-001").contains("ARMED");
    }

    @Test
    void genericHandlerShouldSupportOnlyGenericDeviceTypeButDescribeAnyEvent() {
        assertThat(genericHandler.supports("GENERIC")).isTrue();
        assertThat(genericHandler.supports("CAMERA")).isFalse();

        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-3", "lock-001", "GENERIC", "STATUS_CHANGE",
            Instant.parse("2026-01-10T10:00:00Z"), Map.of());

        assertThat(genericHandler.handle(event)).contains("lock-001");
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `mvn -q test -Dtest=DeviceEventAuditHandlersTest`
Expected: FAIL — handler classes do not exist yet.

- [ ] **Step 4: Write `DeviceEventAuditHandler`**

```java
package com.intelbras.iotevents.domains.audit.core.handler;

import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;

public interface DeviceEventAuditHandler {
    boolean supports(String deviceType);

    String handle(IncomingDeviceEvent event);
}
```

- [ ] **Step 5: Write the four handlers**

```java
package com.intelbras.iotevents.domains.audit.core.handler;

import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import org.springframework.stereotype.Component;

@Component
public class CameraEventAuditHandler implements DeviceEventAuditHandler {

    @Override
    public boolean supports(String deviceType) {
        return "CAMERA".equalsIgnoreCase(deviceType);
    }

    @Override
    public String handle(IncomingDeviceEvent event) {
        return "Camera " + event.deviceId() + " reported " + event.eventType() + " at " + event.timestamp();
    }
}
```

```java
package com.intelbras.iotevents.domains.audit.core.handler;

import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import org.springframework.stereotype.Component;

@Component
public class SensorEventAuditHandler implements DeviceEventAuditHandler {

    @Override
    public boolean supports(String deviceType) {
        return "SENSOR".equalsIgnoreCase(deviceType);
    }

    @Override
    public String handle(IncomingDeviceEvent event) {
        return "Sensor " + event.deviceId() + " triggered " + event.eventType() + " at " + event.timestamp();
    }
}
```

```java
package com.intelbras.iotevents.domains.audit.core.handler;

import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import org.springframework.stereotype.Component;

@Component
public class AlarmEventAuditHandler implements DeviceEventAuditHandler {

    @Override
    public boolean supports(String deviceType) {
        return "ALARM".equalsIgnoreCase(deviceType);
    }

    @Override
    public String handle(IncomingDeviceEvent event) {
        Object status = event.payload().get("status");
        return "Alarm " + event.deviceId() + " changed status to " + status
            + " (" + event.eventType() + ") at " + event.timestamp();
    }
}
```

```java
package com.intelbras.iotevents.domains.audit.core.handler;

import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import org.springframework.stereotype.Component;

@Component
public class GenericEventAuditHandler implements DeviceEventAuditHandler {

    @Override
    public boolean supports(String deviceType) {
        return "GENERIC".equalsIgnoreCase(deviceType);
    }

    @Override
    public String handle(IncomingDeviceEvent event) {
        return "Device " + event.deviceId() + " (" + event.deviceType() + ") reported "
            + event.eventType() + " at " + event.timestamp();
    }
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `mvn -q test -Dtest=DeviceEventAuditHandlersTest`
Expected: `Tests run: 5, Failures: 0, Errors: 0`

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/intelbras/iotevents/domains/audit/core/handler src/test/java/com/intelbras/iotevents/domains/audit/core/handler
git commit -m "feat: add per-device-type audit event handlers"
```

- [ ] **Step 8: Merge back to main**

```bash
git checkout main
git merge --no-ff feat/audit-device-event-handlers
git branch -d feat/audit-device-event-handlers
```

---

### Task 10: Audit use cases + metrics

**Files:**
- Create: `src/main/java/com/intelbras/iotevents/common/metrics/EventMetrics.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/core/usecase/RecordDeviceEventAuditUseCase.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/core/usecase/QueryAuditsUseCase.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/audit/core/usecase/RecordDeviceEventAuditUseCaseTest.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/audit/core/usecase/QueryAuditsUseCaseTest.java`

**Interfaces:**
- Consumes: `AuditRepositoryPort`, `IncomingDeviceEvent`, `AuditRecord`, `AuditQueryFilter`, `KafkaMessageContext` (Task 8); `DeviceEventAuditHandler` + four impls (Task 9).
- Produces: `RecordDeviceEventAuditUseCase(AuditRepositoryPort, List<DeviceEventAuditHandler>, GenericEventAuditHandler, EventMetrics)` with `execute(IncomingDeviceEvent, KafkaMessageContext): void`; `QueryAuditsUseCase(AuditRepositoryPort)` with `execute(AuditQueryFilter): List<AuditRecord>`. Task 13 (listeners) and Task 14 (controller) call these directly.

- [ ] **Step 1: Create the task branch**

```bash
git checkout main
git checkout -b feat/audit-use-cases
```

- [ ] **Step 2: Write the failing tests**

```java
package com.intelbras.iotevents.domains.audit.core.usecase;

import com.intelbras.iotevents.common.metrics.EventMetrics;
import com.intelbras.iotevents.domains.audit.core.AuditRecord;
import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import com.intelbras.iotevents.domains.audit.core.KafkaMessageContext;
import com.intelbras.iotevents.domains.audit.core.error.SimulatedProcessingFailureError;
import com.intelbras.iotevents.domains.audit.core.handler.AlarmEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.CameraEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.DeviceEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.GenericEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.SensorEventAuditHandler;
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

    private final GenericEventAuditHandler genericHandler = new GenericEventAuditHandler();
    private final List<DeviceEventAuditHandler> handlers = List.of(
        new CameraEventAuditHandler(), new SensorEventAuditHandler(), new AlarmEventAuditHandler(), genericHandler);
    private final EventMetrics eventMetrics = new EventMetrics(new SimpleMeterRegistry());
    private final KafkaMessageContext context = new KafkaMessageContext("device.camera.events", 0, 1L, "cam-001");

    @Test
    void shouldPersistAnAuditRecordForANewEvent() {
        AuditRepositoryPort repositoryPort = mock(AuditRepositoryPort.class);
        when(repositoryPort.existsByDedupKey("evt-1")).thenReturn(false);
        RecordDeviceEventAuditUseCase useCase =
            new RecordDeviceEventAuditUseCase(repositoryPort, handlers, genericHandler, eventMetrics);

        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-1", "cam-001", "CAMERA", "MOTION_DETECTED",
            Instant.parse("2026-01-10T10:00:00Z"), Map.of("confidence", 0.9));

        useCase.execute(event, context);

        verify(repositoryPort).save(any(AuditRecord.class));
    }

    @Test
    void shouldSkipDuplicateEventWithoutPersistingAudit() {
        AuditRepositoryPort repositoryPort = mock(AuditRepositoryPort.class);
        when(repositoryPort.existsByDedupKey("evt-1")).thenReturn(true);
        RecordDeviceEventAuditUseCase useCase =
            new RecordDeviceEventAuditUseCase(repositoryPort, handlers, genericHandler, eventMetrics);

        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-1", "cam-001", "CAMERA", "MOTION_DETECTED",
            Instant.parse("2026-01-10T10:00:00Z"), Map.of());

        useCase.execute(event, context);

        verify(repositoryPort, never()).save(any(AuditRecord.class));
    }

    @Test
    void shouldThrowOnSimulateFailureFlagWithoutPersistingAudit() {
        AuditRepositoryPort repositoryPort = mock(AuditRepositoryPort.class);
        when(repositoryPort.existsByDedupKey("evt-1")).thenReturn(false);
        RecordDeviceEventAuditUseCase useCase =
            new RecordDeviceEventAuditUseCase(repositoryPort, handlers, genericHandler, eventMetrics);

        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-1", "alarm-001", "ALARM", "STATUS_CHANGE",
            Instant.parse("2026-01-10T10:00:00Z"), Map.of("simulateFailure", true));

        assertThatThrownBy(() -> useCase.execute(event, context))
            .isInstanceOf(SimulatedProcessingFailureError.class);

        verify(repositoryPort, never()).save(any(AuditRecord.class));
    }

    @Test
    void shouldFallBackToGenericHandlerWhenNoSpecificHandlerMatches() {
        AuditRepositoryPort repositoryPort = mock(AuditRepositoryPort.class);
        when(repositoryPort.existsByDedupKey("evt-9")).thenReturn(false);
        RecordDeviceEventAuditUseCase useCase =
            new RecordDeviceEventAuditUseCase(repositoryPort, handlers, genericHandler, eventMetrics);

        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-9", "lock-001", "LOCK", "STATUS_CHANGE",
            Instant.parse("2026-01-10T10:00:00Z"), Map.of());

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
        AuditQueryFilter filter = new AuditQueryFilter("cam-001", null, null, null);
        AuditRecord record = new AuditRecord("evt-1", "cam-001", "CAMERA", "MOTION_DETECTED",
            "Camera cam-001 reported MOTION_DETECTED", Instant.now(), Instant.now());
        when(repositoryPort.query(filter)).thenReturn(List.of(record));

        QueryAuditsUseCase useCase = new QueryAuditsUseCase(repositoryPort);

        assertThat(useCase.execute(filter)).containsExactly(record);
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `mvn -q test -Dtest=RecordDeviceEventAuditUseCaseTest,QueryAuditsUseCaseTest`
Expected: FAIL — `EventMetrics`, `RecordDeviceEventAuditUseCase`, `QueryAuditsUseCase` do not exist yet.

- [ ] **Step 4: Write `EventMetrics`**

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
            .description("Number of device events successfully processed and audited")
            .register(registry);
        this.duplicateCounter = Counter.builder("device_events_duplicate_total")
            .description("Number of device events skipped as duplicates")
            .register(registry);
        this.dlqCounter = Counter.builder("device_events_dlq_total")
            .description("Number of device events sent to a dead-letter topic")
            .register(registry);
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

- [ ] **Step 5: Write `RecordDeviceEventAuditUseCase`**

```java
package com.intelbras.iotevents.domains.audit.core.usecase;

import com.intelbras.iotevents.common.logging.ExecutionLog;
import com.intelbras.iotevents.common.metrics.EventMetrics;
import com.intelbras.iotevents.domains.audit.core.AuditRecord;
import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import com.intelbras.iotevents.domains.audit.core.KafkaMessageContext;
import com.intelbras.iotevents.domains.audit.core.error.SimulatedProcessingFailureError;
import com.intelbras.iotevents.domains.audit.core.handler.DeviceEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.GenericEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.port.AuditRepositoryPort;

import java.util.List;
import java.util.Map;

public class RecordDeviceEventAuditUseCase {

    private final AuditRepositoryPort auditRepositoryPort;
    private final List<DeviceEventAuditHandler> handlers;
    private final GenericEventAuditHandler fallbackHandler;
    private final EventMetrics eventMetrics;

    public RecordDeviceEventAuditUseCase(AuditRepositoryPort auditRepositoryPort,
                                          List<DeviceEventAuditHandler> handlers,
                                          GenericEventAuditHandler fallbackHandler,
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
            .context("deviceType", event.deviceType())
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
                throw new SimulatedProcessingFailureError(
                    "Simulated processing failure for eventId=" + event.eventId());
            }

            DeviceEventAuditHandler handler = resolveHandler(event.deviceType());
            String generalInfo = handler.handle(event);
            log.step("Built general info via " + handler.getClass().getSimpleName(),
                Map.of("generalInfo", generalInfo));

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

    private DeviceEventAuditHandler resolveHandler(String deviceType) {
        return handlers.stream()
            .filter(handler -> handler != fallbackHandler && handler.supports(deviceType))
            .findFirst()
            .orElse(fallbackHandler);
    }
}
```

- [ ] **Step 6: Write `QueryAuditsUseCase`**

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

- [ ] **Step 7: Run the tests to verify they pass**

Run: `mvn -q test -Dtest=RecordDeviceEventAuditUseCaseTest,QueryAuditsUseCaseTest`
Expected: `Tests run: 5, Failures: 0, Errors: 0`

- [ ] **Step 8: Commit**

```bash
git add src/main/java/com/intelbras/iotevents/common/metrics src/main/java/com/intelbras/iotevents/domains/audit/core/usecase src/test/java/com/intelbras/iotevents/domains/audit/core/usecase
git commit -m "feat: add audit use cases and event metrics"
```

- [ ] **Step 9: Merge back to main**

```bash
git checkout main
git merge --no-ff feat/audit-use-cases
git branch -d feat/audit-use-cases
```

---

### Task 11: Audit persistence adapter (Postgres/JPA)

**Files:**
- Create: `src/main/resources/db/migration/V1__create_audit_records_table.sql`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/adapters/persistence/AuditJpaEntity.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/adapters/persistence/AuditJpaRepository.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/adapters/persistence/AuditMapper.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/adapters/persistence/JpaAuditRepositoryAdapter.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/audit/adapters/persistence/AuditMapperTest.java`

**Interfaces:**
- Consumes: `AuditRecord`, `AuditQueryFilter`, `AuditRepositoryPort` (Task 8).
- Produces: `JpaAuditRepositoryAdapter implements AuditRepositoryPort`, backed by `audit_records` table. No `core` type ever exposes `AuditJpaEntity`.

- [ ] **Step 1: Create the task branch**

```bash
git checkout main
git checkout -b feat/audit-persistence-adapter
```

- [ ] **Step 2: Write the Flyway migration**

```sql
CREATE TABLE audit_records (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(255) NOT NULL,
    device_id VARCHAR(255) NOT NULL,
    device_type VARCHAR(50) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    general_info TEXT NOT NULL,
    consumed_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT uq_audit_records_event_id UNIQUE (event_id)
);

CREATE INDEX idx_audit_records_device_id ON audit_records (device_id);
CREATE INDEX idx_audit_records_event_type ON audit_records (event_type);
```

- [ ] **Step 3: Write the failing mapper test (pure mapping, no DB)**

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
        AuditRecord record = new AuditRecord("evt-1", "cam-001", "CAMERA", "MOTION_DETECTED",
            "Camera cam-001 reported MOTION_DETECTED", now, now);

        AuditJpaEntity entity = AuditMapper.toEntity(record);
        AuditRecord roundTripped = AuditMapper.toDomain(entity);

        assertThat(roundTripped).isEqualTo(record);
    }
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `mvn -q test -Dtest=AuditMapperTest`
Expected: FAIL — `AuditJpaEntity` and `AuditMapper` do not exist yet.

- [ ] **Step 5: Write `AuditJpaEntity`**

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

    @Column(name = "device_type", nullable = false)
    private String deviceType;

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

    public AuditJpaEntity(String eventId, String deviceId, String deviceType, String eventType,
                           String generalInfo, Instant consumedAt, Instant createdAt) {
        this.eventId = eventId;
        this.deviceId = deviceId;
        this.deviceType = deviceType;
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

    public String getDeviceType() {
        return deviceType;
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

- [ ] **Step 6: Write `AuditMapper`**

```java
package com.intelbras.iotevents.domains.audit.adapters.persistence;

import com.intelbras.iotevents.domains.audit.core.AuditRecord;

public final class AuditMapper {

    private AuditMapper() {
    }

    public static AuditJpaEntity toEntity(AuditRecord record) {
        return new AuditJpaEntity(record.dedupKey(), record.deviceId(), record.deviceType(), record.eventType(),
            record.generalInfo(), record.consumedAt(), record.createdAt());
    }

    public static AuditRecord toDomain(AuditJpaEntity entity) {
        return new AuditRecord(entity.getEventId(), entity.getDeviceId(), entity.getDeviceType(),
            entity.getEventType(), entity.getGeneralInfo(), entity.getConsumedAt(), entity.getCreatedAt());
    }
}
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `mvn -q test -Dtest=AuditMapperTest`
Expected: `Tests run: 1, Failures: 0, Errors: 0`

- [ ] **Step 8: Write `AuditJpaRepository` and `JpaAuditRepositoryAdapter`** (verified by the Task 14 integration test, not in isolation — per CLAUDE.md's testing strategy, repository adapters are proven through the full flow, not mocked)

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

- [ ] **Step 9: Verify the project compiles**

Run: `mvn -q -DskipTests compile`
Expected: `BUILD SUCCESS`

- [ ] **Step 10: Commit**

```bash
git add src/main/resources/db/migration src/main/java/com/intelbras/iotevents/domains/audit/adapters/persistence src/test/java/com/intelbras/iotevents/domains/audit/adapters/persistence
git commit -m "feat: add Postgres audit persistence adapter"
```

- [ ] **Step 11: Merge back to main**

```bash
git checkout main
git merge --no-ff feat/audit-persistence-adapter
git branch -d feat/audit-persistence-adapter
```

---

### Task 12: Kafka error handling + DLQ recoverer

**Files:**
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/adapters/kafka/DeviceEventDlqRecoverer.java`
- Create: `src/main/java/com/intelbras/iotevents/config/KafkaErrorHandlingConfig.java`
- Modify: `src/main/resources/application.yml`
- Test: `src/test/java/com/intelbras/iotevents/domains/audit/adapters/kafka/DeviceEventDlqRecovererTest.java`

**Interfaces:**
- Consumes: `EventMetrics` (Task 10), `KafkaTopics` (Task 6).
- Produces: `DeviceEventDlqRecoverer.recover(ConsumerRecord<?,?>, Exception, DeadLetterPublishingRecoverer): void`; a `CommonErrorHandler` bean that Spring Boot auto-applies to every `@KafkaListener` container (Task 13 listeners rely on this being in place for retry/DLQ to work).

- [ ] **Step 1: Create the task branch**

```bash
git checkout main
git checkout -b feat/kafka-dlq-recoverer
```

- [ ] **Step 2: Write the failing test**

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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doThrow;

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
        EventMetrics eventMetrics = new EventMetrics(new SimpleMeterRegistry());
        DeviceEventDlqRecoverer recoverer = new DeviceEventDlqRecoverer(eventMetrics);
        DeadLetterPublishingRecoverer delegate = mock(DeadLetterPublishingRecoverer.class);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("device.alarm.events", 0, 1L, "alarm-001", "{}");
        RuntimeException exception = new RuntimeException("boom");

        recoverer.recover(record, exception, delegate);

        verify(delegate).accept(record, exception);
        assertThat(appender.list).hasSize(1);
    }

    @Test
    void shouldLogExactlyOnceEvenWhenDelegateThrows() {
        EventMetrics eventMetrics = new EventMetrics(new SimpleMeterRegistry());
        DeviceEventDlqRecoverer recoverer = new DeviceEventDlqRecoverer(eventMetrics);
        DeadLetterPublishingRecoverer delegate = mock(DeadLetterPublishingRecoverer.class);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("device.alarm.events", 0, 1L, "alarm-001", "{}");
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

- [ ] **Step 3: Run the test to verify it fails**

Run: `mvn -q test -Dtest=DeviceEventDlqRecovererTest`
Expected: FAIL — `DeviceEventDlqRecoverer` does not exist yet.

- [ ] **Step 4: Write `DeviceEventDlqRecoverer`**

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

- [ ] **Step 5: Run the test to verify it passes**

Run: `mvn -q test -Dtest=DeviceEventDlqRecovererTest`
Expected: `Tests run: 2, Failures: 0, Errors: 0`

- [ ] **Step 6: Write `KafkaErrorHandlingConfig`**

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

        ConsumerRecordRecoverer recoverer = (record, ex) ->
            dlqRecoverer.recover(record, ex, deadLetterRecoverer);

        ExponentialBackOff backOff = new ExponentialBackOff(1000L, 2.0);
        backOff.setMaxAttempts(3);

        return new DefaultErrorHandler(recoverer, backOff);
    }
}
```

- [ ] **Step 7: Verify the project compiles**

Run: `mvn -q -DskipTests compile`
Expected: `BUILD SUCCESS`

- [ ] **Step 8: Commit**

```bash
git add src/main/java/com/intelbras/iotevents/domains/audit/adapters/kafka/DeviceEventDlqRecoverer.java src/main/java/com/intelbras/iotevents/config/KafkaErrorHandlingConfig.java src/test/java/com/intelbras/iotevents/domains/audit/adapters/kafka
git commit -m "feat: add Kafka retry and dead-letter error handling"
```

- [ ] **Step 9: Merge back to main**

```bash
git checkout main
git merge --no-ff feat/kafka-dlq-recoverer
git branch -d feat/kafka-dlq-recoverer
```

---

### Task 13: Audit Kafka consumer listeners

**Files:**
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/adapters/kafka/DeviceEventMessageParser.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/adapters/kafka/listener/CameraEventListener.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/adapters/kafka/listener/SensorEventListener.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/adapters/kafka/listener/AlarmEventListener.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/adapters/kafka/listener/GenericEventListener.java`
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/AuditUseCaseConfig.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/audit/adapters/kafka/DeviceEventMessageParserTest.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/audit/adapters/kafka/listener/CameraEventListenerTest.java`

**Interfaces:**
- Consumes: `RecordDeviceEventAuditUseCase` (Task 10), `IncomingDeviceEvent`/`KafkaMessageContext` (Task 8), `KafkaTopics` (Task 6).
- Produces: four `@KafkaListener`-annotated classes wired to the consumer group `audit-service`, each parsing the wire JSON and calling `RecordDeviceEventAuditUseCase.execute(...)`.

- [ ] **Step 1: Create the task branch**

```bash
git checkout main
git checkout -b feat/audit-kafka-listeners
```

- [ ] **Step 2: Write the failing parser test**

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
            {"eventId":"evt-1","deviceId":"cam-001","deviceType":"CAMERA","eventType":"MOTION_DETECTED",
             "timestamp":"2026-01-10T10:00:00Z","payload":{"confidence":0.9}}
            """;

        IncomingDeviceEvent event = parser.parse(json);

        assertThat(event.eventId()).isEqualTo("evt-1");
        assertThat(event.deviceId()).isEqualTo("cam-001");
        assertThat(event.deviceType()).isEqualTo("CAMERA");
        assertThat(event.eventType()).isEqualTo("MOTION_DETECTED");
        assertThat(event.payload()).containsEntry("confidence", 0.9);
    }

    @Test
    void shouldDefaultToEmptyPayloadWhenMissing() {
        String json = """
            {"eventId":"evt-2","deviceId":"alarm-001","deviceType":"ALARM","eventType":"STATUS_CHANGE",
             "timestamp":"2026-01-10T10:00:00Z"}
            """;

        IncomingDeviceEvent event = parser.parse(json);

        assertThat(event.payload()).isEmpty();
    }
}
```

- [ ] **Step 3: Write the failing listener test**

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

class CameraEventListenerTest {

    @Test
    void shouldParseTheRecordAndCallTheUseCaseWithKafkaContext() {
        DeviceEventMessageParser parser = mock(DeviceEventMessageParser.class);
        RecordDeviceEventAuditUseCase useCase = mock(RecordDeviceEventAuditUseCase.class);
        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-1", "cam-001", "CAMERA", "MOTION_DETECTED",
            Instant.parse("2026-01-10T10:00:00Z"), Map.of());
        ConsumerRecord<String, String> record = new ConsumerRecord<>("device.camera.events", 2, 42L, "cam-001", "{}");
        when(parser.parse("{}")).thenReturn(event);

        CameraEventListener listener = new CameraEventListener(parser, useCase);
        listener.onMessage(record);

        verify(useCase).execute(eq(event), eq(new KafkaMessageContext("device.camera.events", 2, 42L, "cam-001")));
    }
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `mvn -q test -Dtest=DeviceEventMessageParserTest,CameraEventListenerTest`
Expected: FAIL — `DeviceEventMessageParser` and `CameraEventListener` do not exist yet.

- [ ] **Step 5: Add an `equals`/`hashCode`-friendly `KafkaMessageContext` check**

`KafkaMessageContext` is already a `record` (Task 8), so structural equality is free — no change needed here; confirming it in this step for the listener test above to pass.

- [ ] **Step 6: Write `DeviceEventMessageParser`**

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
            String deviceType = (String) raw.get("deviceType");
            String eventType = (String) raw.get("eventType");
            Instant timestamp = Instant.parse((String) raw.get("timestamp"));
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) raw.getOrDefault("payload", Map.of());
            return new IncomingDeviceEvent(eventId, deviceId, deviceType, eventType, timestamp, payload);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to parse device event message: " + json, ex);
        }
    }
}
```

- [ ] **Step 7: Write the four listeners**

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
public class CameraEventListener {

    private final DeviceEventMessageParser parser;
    private final RecordDeviceEventAuditUseCase useCase;

    public CameraEventListener(DeviceEventMessageParser parser, RecordDeviceEventAuditUseCase useCase) {
        this.parser = parser;
        this.useCase = useCase;
    }

    @KafkaListener(topics = KafkaTopics.DEVICE_CAMERA_EVENTS, groupId = KafkaTopics.CONSUMER_GROUP, concurrency = "3")
    public void onMessage(ConsumerRecord<String, String> record) {
        IncomingDeviceEvent event = parser.parse(record.value());
        KafkaMessageContext context = new KafkaMessageContext(record.topic(), record.partition(), record.offset(), record.key());
        useCase.execute(event, context);
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
public class SensorEventListener {

    private final DeviceEventMessageParser parser;
    private final RecordDeviceEventAuditUseCase useCase;

    public SensorEventListener(DeviceEventMessageParser parser, RecordDeviceEventAuditUseCase useCase) {
        this.parser = parser;
        this.useCase = useCase;
    }

    @KafkaListener(topics = KafkaTopics.DEVICE_SENSOR_EVENTS, groupId = KafkaTopics.CONSUMER_GROUP, concurrency = "3")
    public void onMessage(ConsumerRecord<String, String> record) {
        IncomingDeviceEvent event = parser.parse(record.value());
        KafkaMessageContext context = new KafkaMessageContext(record.topic(), record.partition(), record.offset(), record.key());
        useCase.execute(event, context);
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
public class AlarmEventListener {

    private final DeviceEventMessageParser parser;
    private final RecordDeviceEventAuditUseCase useCase;

    public AlarmEventListener(DeviceEventMessageParser parser, RecordDeviceEventAuditUseCase useCase) {
        this.parser = parser;
        this.useCase = useCase;
    }

    @KafkaListener(topics = KafkaTopics.DEVICE_ALARM_EVENTS, groupId = KafkaTopics.CONSUMER_GROUP, concurrency = "3")
    public void onMessage(ConsumerRecord<String, String> record) {
        IncomingDeviceEvent event = parser.parse(record.value());
        KafkaMessageContext context = new KafkaMessageContext(record.topic(), record.partition(), record.offset(), record.key());
        useCase.execute(event, context);
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
public class GenericEventListener {

    private final DeviceEventMessageParser parser;
    private final RecordDeviceEventAuditUseCase useCase;

    public GenericEventListener(DeviceEventMessageParser parser, RecordDeviceEventAuditUseCase useCase) {
        this.parser = parser;
        this.useCase = useCase;
    }

    @KafkaListener(topics = KafkaTopics.DEVICE_GENERIC_EVENTS, groupId = KafkaTopics.CONSUMER_GROUP, concurrency = "3")
    public void onMessage(ConsumerRecord<String, String> record) {
        IncomingDeviceEvent event = parser.parse(record.value());
        KafkaMessageContext context = new KafkaMessageContext(record.topic(), record.partition(), record.offset(), record.key());
        useCase.execute(event, context);
    }
}
```

- [ ] **Step 8: Register the audit use cases as Spring beans**

```java
package com.intelbras.iotevents.domains.audit;

import com.intelbras.iotevents.common.metrics.EventMetrics;
import com.intelbras.iotevents.domains.audit.core.handler.DeviceEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.GenericEventAuditHandler;
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
                                                                         GenericEventAuditHandler fallbackHandler,
                                                                         EventMetrics eventMetrics) {
        return new RecordDeviceEventAuditUseCase(auditRepositoryPort, handlers, fallbackHandler, eventMetrics);
    }

    @Bean
    public QueryAuditsUseCase queryAuditsUseCase(AuditRepositoryPort auditRepositoryPort) {
        return new QueryAuditsUseCase(auditRepositoryPort);
    }
}
```

- [ ] **Step 9: Run the tests to verify they pass**

Run: `mvn -q test -Dtest=DeviceEventMessageParserTest,CameraEventListenerTest`
Expected: `Tests run: 3, Failures: 0, Errors: 0`

- [ ] **Step 10: Commit**

```bash
git add src/main/java/com/intelbras/iotevents/domains/audit/adapters/kafka src/main/java/com/intelbras/iotevents/domains/audit/AuditUseCaseConfig.java src/test/java/com/intelbras/iotevents/domains/audit/adapters/kafka
git commit -m "feat: add audit Kafka consumer listeners"
```

- [ ] **Step 11: Merge back to main**

```bash
git checkout main
git merge --no-ff feat/audit-kafka-listeners
git branch -d feat/audit-kafka-listeners
```

---

### Task 14: Audit REST query endpoint

**Files:**
- Create: `src/main/java/com/intelbras/iotevents/domains/audit/adapters/web/AuditController.java`
- Test: `src/test/java/com/intelbras/iotevents/domains/audit/adapters/web/AuditControllerTest.java`

**Interfaces:**
- Consumes: `QueryAuditsUseCase`, `AuditQueryFilter`, `AuditRecord` (Tasks 8, 10).
- Produces: `GET /audits?deviceId=&eventType=&from=&to=` returning a JSON array of `AuditRecord`.

- [ ] **Step 1: Create the task branch**

```bash
git checkout main
git checkout -b feat/audit-controller
```

- [ ] **Step 2: Write the failing test**

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
        AuditRecord record = new AuditRecord("evt-1", "cam-001", "CAMERA", "MOTION_DETECTED",
            "Camera cam-001 reported MOTION_DETECTED", Instant.parse("2026-01-10T10:00:00Z"),
            Instant.parse("2026-01-10T10:00:00Z"));
        when(queryAuditsUseCase.execute(new AuditQueryFilter("cam-001", null, null, null)))
            .thenReturn(List.of(record));

        mockMvc.perform(get("/audits").param("deviceId", "cam-001"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[0].deviceId").value("cam-001"))
            .andExpect(jsonPath("$[0].generalInfo").value("Camera cam-001 reported MOTION_DETECTED"));
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `mvn -q test -Dtest=AuditControllerTest`
Expected: FAIL — `AuditController` does not exist yet.

- [ ] **Step 4: Write `AuditController`**

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

- [ ] **Step 5: Run the test to verify it passes**

Run: `mvn -q test -Dtest=AuditControllerTest`
Expected: `Tests run: 1, Failures: 0, Errors: 0`

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/intelbras/iotevents/domains/audit/adapters/web src/test/java/com/intelbras/iotevents/domains/audit/adapters/web
git commit -m "feat: add audit query REST endpoint"
```

- [ ] **Step 7: Merge back to main**

```bash
git checkout main
git merge --no-ff feat/audit-controller
git branch -d feat/audit-controller
```

---

### Task 15: End-to-end Testcontainers integration tests

**Files:**
- Create: `src/test/java/com/intelbras/iotevents/AuditFlowIT.java`
- Create: `src/test/resources/application-test.yml`

**Interfaces:**
- Consumes: the full running application (all prior tasks) against real Kafka + Postgres containers.
- Produces: proof that the happy path, duplicate path, and forced-failure/DLQ path all work end-to-end, satisfying RT05/RT06 and the Kafka-integration-test bonus.

- [ ] **Step 1: Create the task branch**

```bash
git checkout main
git checkout -b test/audit-flow-integration-tests
```

- [ ] **Step 2: Write `src/test/resources/application-test.yml`** (keeps the test profile from trying to run Flyway against a fixed host before Testcontainers assigns ports — properties are overridden dynamically in the test class instead)

```yaml
spring:
  jpa:
    hibernate:
      ddl-auto: validate
  flyway:
    enabled: true
```

- [ ] **Step 3: Write the failing integration test**

```java
package com.intelbras.iotevents;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
        .withDatabaseName("iotevents")
        .withUsername("iotevents")
        .withPassword("iotevents");

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @LocalServerPort
    private int port;

    private TestRestTemplate restTemplate = new TestRestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void shouldProcessAllEventsFromTheSampleCsvAndExposeThemViaAudits() {
        ResponseEntity<Map> response = restTemplate.postForEntity(
            "http://localhost:" + port + "/events/simulate", null, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().get("publishedCount")).isEqualTo(12);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            ResponseEntity<List> audits = restTemplate.getForEntity(
                "http://localhost:" + port + "/audits?deviceId=cam-001", List.class);
            assertThat(audits.getBody()).hasSize(2);
        });
    }

    @Test
    void shouldPersistOnlyOneAuditRowWhenTheSameEventIsIngestedTwice() {
        restTemplate.postForEntity("http://localhost:" + port + "/events/simulate", null, Map.class);
        restTemplate.postForEntity("http://localhost:" + port + "/events/simulate", null, Map.class);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            ResponseEntity<List> audits = restTemplate.getForEntity(
                "http://localhost:" + port + "/audits?deviceId=cam-001", List.class);
            assertThat(audits.getBody()).hasSize(2);
        });
    }

    @Test
    void shouldDeadLetterTheForcedFailureEventWithoutPersistingAnAudit() {
        restTemplate.postForEntity("http://localhost:" + port + "/events/simulate", null, Map.class);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            List<ConsumerRecord<String, String>> dltRecords = consumeFromDlt("device.alarm.events.DLT");
            assertThat(dltRecords).anySatisfy(record -> assertThat(record.key()).isEqualTo("alarm-001"));
        });

        ResponseEntity<List> audits = restTemplate.getForEntity(
            "http://localhost:" + port + "/audits?deviceId=alarm-001", List.class);
        assertThat(audits.getBody()).hasSize(1);
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
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            return records.records(topic);
        }
    }
}
```

- [ ] **Step 4: Run the test to verify it fails (or does not compile) before Docker is available**

Run: `mvn -q test -Dtest=AuditFlowIT`
Expected: if Docker is not running locally, FAIL with a Testcontainers startup error — note this and move to Step 5 anyway, since the code must still be correct; re-run once Docker is confirmed available (see Step 6).

- [ ] **Step 5: Confirm Docker is available**

Run: `docker info`
Expected: a running Docker daemon. If this fails, start Docker Desktop (or the local Docker service) before continuing — Testcontainers requires it.

- [ ] **Step 6: Run the test to verify it passes with Docker available**

Run: `mvn -q test -Dtest=AuditFlowIT`
Expected: `Tests run: 3, Failures: 0, Errors: 0` (first run will be slower while Testcontainers pulls the Kafka and Postgres images).

- [ ] **Step 7: Commit**

```bash
git add src/test/java/com/intelbras/iotevents/AuditFlowIT.java src/test/resources/application-test.yml
git commit -m "test: add end-to-end Testcontainers audit flow tests"
```

- [ ] **Step 8: Merge back to main**

```bash
git checkout main
git merge --no-ff test/audit-flow-integration-tests
git branch -d test/audit-flow-integration-tests
```

---

### Task 16: Observability — Prometheus + Grafana in Docker Compose

**Files:**
- Modify: `docker-compose.yml`
- Create: `observability/prometheus.yml`
- Create: `observability/grafana/provisioning/datasources/datasource.yml`
- Create: `observability/grafana/provisioning/dashboards/dashboards.yml`
- Create: `observability/grafana/dashboards/device-events-dashboard.json`
- Create: `Dockerfile`

**Interfaces:**
- Consumes: `/actuator/prometheus` (auto-exposed by `spring-boot-starter-actuator` + `micrometer-registry-prometheus`, already on the classpath since Task 1).
- Produces: a full local stack — `docker compose up` brings up Kafka, Postgres, the app, Prometheus, and Grafana with a pre-provisioned dashboard.

- [ ] **Step 1: Create the task branch**

```bash
git checkout main
git checkout -b chore/observability-stack
```

- [ ] **Step 2: Write `Dockerfile`**

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

- [ ] **Step 3: Write `observability/prometheus.yml`**

```yaml
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: "iot-events-app"
    metrics_path: /actuator/prometheus
    static_configs:
      - targets: ["app:8080"]
```

- [ ] **Step 4: Write Grafana provisioning files**

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

- [ ] **Step 5: Write `observability/grafana/dashboards/device-events-dashboard.json`**

```json
{
  "title": "IoT Device Events",
  "schemaVersion": 39,
  "version": 1,
  "panels": [
    {
      "id": 1,
      "title": "Events Processed (rate)",
      "type": "timeseries",
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
      "targets": [
        {"expr": "rate(device_events_processed_total[1m])", "legendFormat": "processed"}
      ]
    },
    {
      "id": 2,
      "title": "Duplicates Skipped (rate)",
      "type": "timeseries",
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
      "targets": [
        {"expr": "rate(device_events_duplicate_total[1m])", "legendFormat": "duplicates"}
      ]
    },
    {
      "id": 3,
      "title": "Dead-lettered Events (rate)",
      "type": "timeseries",
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8},
      "targets": [
        {"expr": "rate(device_events_dlq_total[1m])", "legendFormat": "dlq"}
      ]
    },
    {
      "id": 4,
      "title": "Kafka Consumer Lag",
      "type": "timeseries",
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8},
      "targets": [
        {"expr": "sum(kafka_consumer_fetch_manager_records_lag) by (topic)", "legendFormat": "{{topic}}"}
      ]
    }
  ]
}
```

- [ ] **Step 6: Update `docker-compose.yml` to add `app`, `prometheus`, and `grafana`**

Replace the full file content with:

```yaml
services:
  kafka:
    image: apache/kafka:3.8.0
    container_name: iotevents-kafka
    ports:
      - "9092:9092"
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
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: iotevents
      POSTGRES_USER: iotevents
      POSTGRES_PASSWORD: iotevents
    volumes:
      - iotevents-postgres-data:/var/lib/postgresql/data

  app:
    build: .
    container_name: iotevents-app
    depends_on:
      - kafka
      - postgres
    ports:
      - "8080:8080"
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
    depends_on:
      - app
    ports:
      - "9090:9090"
    volumes:
      - ./observability/prometheus.yml:/etc/prometheus/prometheus.yml

  grafana:
    image: grafana/grafana:11.2.0
    container_name: iotevents-grafana
    depends_on:
      - prometheus
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - ./observability/grafana/provisioning/datasources:/etc/grafana/provisioning/datasources
      - ./observability/grafana/provisioning/dashboards:/etc/grafana/provisioning/dashboards
      - ./observability/grafana/dashboards:/etc/grafana/provisioning/dashboards-data

volumes:
  iotevents-postgres-data:
```

- [ ] **Step 7: Run the full stack and verify manually**

Run: `docker compose up -d --build`
Expected: all five containers reach a running state (`docker compose ps` shows no restarting/exited containers).

Run: `curl -s -X POST http://localhost:8080/events/simulate`
Expected: `{"publishedCount":12}`

Run: `curl -s http://localhost:8080/actuator/prometheus | grep device_events_processed_total`
Expected: a line like `device_events_processed_total 11.0` (11, not 12 — `evt-010`'s simulated failure is dead-lettered, not processed).

Open `http://localhost:3000` (Grafana, login `admin`/`admin`) and confirm the "IoT Device Events" dashboard shows data on all four panels.

- [ ] **Step 8: Tear down**

Run: `docker compose down`

- [ ] **Step 9: Commit**

```bash
git add docker-compose.yml observability Dockerfile
git commit -m "chore: add Docker Compose observability stack with Prometheus and Grafana"
```

- [ ] **Step 10: Merge back to main**

```bash
git checkout main
git merge --no-ff chore/observability-stack
git branch -d chore/observability-stack
```

---

### Task 17: README run instructions

**Files:**
- Modify: `README.md`

**Interfaces:**
- Produces: documentation only, no code interfaces.

- [ ] **Step 1: Create the task branch**

```bash
git checkout main
git checkout -b docs/readme-run-instructions
```

- [ ] **Step 2: Rewrite `README.md`**

```markdown
# case_intelbras_kafka_java

Backend case for the Intelbras Backend Sênior process: an IoT device event processing
pipeline built with Java, Spring Boot, and Apache Kafka. Full architecture rationale is
in `docs/superpowers/specs/2026-10-10-iot-device-event-kafka-design.md`; coding
conventions are in `CLAUDE.md`.

## Running locally (Docker Compose)

```bash
docker compose up -d --build
```

This starts Kafka (KRaft mode), Postgres, the application, Prometheus, and Grafana.

Once all containers are up:

1. Trigger ingestion of the sample CSV (`src/main/resources/events.csv`):

   ```bash
   curl -X POST http://localhost:8080/events/simulate
   ```

2. Query the resulting audit trail:

   ```bash
   curl http://localhost:8080/audits
   curl "http://localhost:8080/audits?deviceId=cam-001"
   ```

3. Metrics: `http://localhost:8080/actuator/prometheus`
4. Grafana dashboard: `http://localhost:3000` (login `admin`/`admin`), "IoT Device Events" dashboard.

Re-running step 1 demonstrates idempotency (no new audit rows for already-seen events).
`evt-010` in the sample CSV carries a `simulateFailure` flag and demonstrates the
retry → dead-letter-topic path; it never produces an audit row.

## Running tests

```bash
mvn test
```

This includes unit tests (no infra required) and `AuditFlowIT`, an end-to-end
Testcontainers integration test that spins up real Kafka and Postgres containers —
requires a running Docker daemon.

## Tearing down

```bash
docker compose down -v
```
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: add run instructions"
```

- [ ] **Step 4: Merge back to main**

```bash
git checkout main
git merge --no-ff docs/readme-run-instructions
git branch -d docs/readme-run-instructions
```

---

### Task 18: Bloco B & C deliverable documents

**Files:**
- Create: `ARCHITECTURE.md`
- Create: `DECISIONS.md`

**Interfaces:**
- Produces: the case's required Bloco B (architecture diagram + production-scale write-up) and Bloco C (technical decisions + trade-offs) deliverables, derived from the already-approved design spec.

- [ ] **Step 1: Create the task branch**

```bash
git checkout main
git checkout -b docs/bloco-b-c-deliverables
```

- [ ] **Step 2: Write `ARCHITECTURE.md`**

```markdown
# Architecture (Bloco B)

## Diagram

\`\`\`mermaid
flowchart LR
    CSV[events.csv] -->|POST /events/simulate| SIM[SimulateIngestionUseCase]
    SIM -->|publish, key=deviceId| T1[device.camera.events]
    SIM -->|publish, key=deviceId| T2[device.sensor.events]
    SIM -->|publish, key=deviceId| T3[device.alarm.events]
    SIM -->|publish, key=deviceId| T4[device.generic.events]

    T1 --> L1[CameraEventListener]
    T2 --> L2[SensorEventListener]
    T3 --> L3[AlarmEventListener]
    T4 --> L4[GenericEventListener]

    L1 --> UC[RecordDeviceEventAuditUseCase]
    L2 --> UC
    L3 --> UC
    L4 --> UC

    UC -->|duplicate| LOG1[log only]
    UC -->|success| DB[(Postgres audit_records)]
    UC -->|exhausted retries| DLT[per-topic .DLT]

    DB --> API[GET /audits]
\`\`\`

## Scalability and Kafka partitioning strategy

Each device type gets its own topic (`device.camera.events`, `device.sensor.events`,
`device.alarm.events`, `device.generic.events`), partitioned by `deviceId`. At case
scale (100 events) each topic runs 3 partitions; at millions-of-events/day scale,
partition count would grow based on per-topic throughput and target consumer
parallelism (rule of thumb: partitions >= peak consumers you want to run
concurrently in the group). Topic-per-device-type lets each device domain's traffic
scale independently — a camera-heavy deployment can repartition `device.camera.events`
without touching the others.

## Consumer groups and parallelism

All four listeners share one consumer group, `audit-service` — they are the same
logical consuming application, just subscribed to different topics. Concurrency
(3, matching partition count) lets each topic's partitions be consumed in parallel
within the group. At higher scale, horizontally scaling the audit service (more
application instances, same group id) increases the number of partitions each
instance's listener threads can claim, up to the partition count ceiling.

## Ordering

Keying every record by `deviceId` guarantees Kafka delivers all of one device's
events to the same partition, in produce order, and that partition is consumed by
exactly one consumer thread at a time within the group — so one device's event
history is processed in order. Different devices may be processed out of order
relative to each other, by design, for parallelism.

## Idempotency and duplicate handling

Every event carries an `eventId` (the CSV's own id, or a deterministic SHA-256 hash
of `deviceId|eventType|timestamp|payload` when absent), computed once at the
`device` domain and carried in the wire message. `audit` checks
`existsByDedupKey(eventId)` before processing; a duplicate is logged and skipped,
never reprocessed, never given a new audit row. The `event_id` column also carries
a database-level unique constraint as a second line of defense against a race
between the check and the insert.

## Retry and handling of unprocessable messages

A handler failure retries 3 times with exponential backoff (1s, 2s, 4s) via Spring
Kafka's `DefaultErrorHandler`. If still failing, the record is published to that
topic's dead-letter topic (`<topic>.DLT`) and the failure is logged with full
context (topic, partition, offset, key, exception) — no audit row is written for a
failed event. Replay from a DLT is a deliberate, manual operational action (e.g.
inspecting the DLT, fixing the root cause, republishing), not automated by this
application, to avoid silently reprocessing something that may need a human
decision first.

## Observability

Every behaviour (`SimulateIngestionUseCase`, `RecordDeviceEventAuditUseCase`,
`QueryAuditsUseCase`, each Kafka listener) emits exactly one structured log line —
success or failure — carrying topic/partition/offset/key context for Kafka-driven
behaviours. Metrics (`device_events_processed_total`,
`device_events_duplicate_total`, `device_events_dlq_total`, plus Kafka consumer lag)
are exposed via Micrometer/Actuator at `/actuator/prometheus`, scraped by Prometheus,
and visualized in a provisioned Grafana dashboard.

## Availability and fault tolerance

A single bad message (malformed payload, forced failure) never blocks a partition
beyond its retry budget — it's dead-lettered and the partition continues with the
next offset. The CSV adapter tolerates malformed individual rows at ingestion
without aborting the whole batch. At production scale, running Kafka with a
replication factor > 1 (this case's local Kafka is a single-node KRaft broker for
simplicity) and multiple audit-service instances would remove single points of
failure on both the broker and consumer sides.
```

- [ ] **Step 3: Write `DECISIONS.md`**

```markdown
# Technical Decisions & Trade-offs (Bloco C)

| Decision | Alternative considered | Why this way |
|---|---|---|
| Topic-per-device-type | Single topic with a type field; one topic per event type | Matches how device domains naturally evolve/scale independently; still preserves per-device ordering via the `deviceId` key |
| Two independent domains (`device`, `audit`), communicating only via Kafka JSON | One domain covering ingestion, processing, and audit | Models audit as one of potentially several independent consumers of the event stream; a future `notification` domain could consume the same topics with zero coupling |
| Ports as Java interfaces, standard Spring autowiring | Abstract classes; manual composition root | Idiomatic Java/Spring; same dependency-inversion guarantee without fighting the framework |
| `eventId` computed once in `device.core`, carried as wire data | Each consumer re-derives its own dedup key | Keeps identity computation in one place; consumers trust the message instead of re-deriving it |
| Duplicates: log only, no audit row | Persist a `DUPLICATE`-status row | The audit table should only ever reflect successfully processed events |
| Failures: log + DLQ only, no audit row, no `status` field on `AuditRecord` | Persist a `FAILED`-status row | Simplifies the schema since only one outcome (success) is ever persisted |
| PostgreSQL for the audit store | H2 | Real persistence across restarts; works with Testcontainers and the Docker Compose bonus |
| Spring Kafka `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` | Manual try/catch per listener | Standard, well-documented retry/DLQ pattern |
| One structured `ExecutionLog` per behaviour | Scattered `logger.info` calls | Every behaviour's outcome is a single, greppable, structured line — easy to trace one event's path through logs |
| Sample CSV + CSV adapter as an isolated seam | Hardcoding the real dataset's assumptions everywhere | The real 100-event CSV wasn't available during development; only `CsvEventSourceAdapter` and the sample file should need to change when it arrives |

## Known limitations (acknowledged, not fixed in this submission)

- The real CSV dataset was not provided; `events.csv` is a representative sample built
  from the case's own description (camera/sensor/alarm events, one unrecognized
  device type, one forced-failure row).
- No authentication/authorization on the REST endpoints — out of scope for the case.
- DLT replay is manual, by design — not automated.
- A frontend for browsing `/audits` is a planned separate project, not part of this
  submission.
```

- [ ] **Step 4: Commit**

```bash
git add ARCHITECTURE.md DECISIONS.md
git commit -m "docs: add Bloco B architecture and Bloco C decisions deliverables"
```

- [ ] **Step 5: Merge back to main**

```bash
git checkout main
git merge --no-ff docs/bloco-b-c-deliverables
git branch -d docs/bloco-b-c-deliverables
```
