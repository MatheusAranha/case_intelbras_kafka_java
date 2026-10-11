package com.intelbras.iotevents;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
    private MeterRegistry meterRegistry;

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
        ResponseEntity<List> auditsBefore = restTemplate.getForEntity("http://localhost:" + port + "/audits", List.class);
        int auditCountBefore = auditsBefore.getBody().size();
        double duplicatesBefore = readCounterMetric("device_events_duplicate_total");

        Map<String, Object> body = Map.of("eventsPerMinute", 3000, "dlqRate", 0.0, "duplicateRate", 1.0);
        restTemplate.postForEntity(url("/start"), body, Map.class);

        // Confirm at least one fresh event really landed as an audit row before judging
        // anything about growth staying slow — otherwise "growth < requested" could pass
        // trivially just because the consumer hasn't caught up yet.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ResponseEntity<List> audits = restTemplate.getForEntity("http://localhost:" + port + "/audits", List.class);
            assertThat(audits.getBody().size()).isGreaterThan(auditCountBefore);
        });

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ResponseEntity<Map> status = restTemplate.getForEntity(url("/status"), Map.class);
            Number requested = (Number) status.getBody().get("requestedCount");
            assertThat(requested.longValue()).isGreaterThan(100);
        });

        ResponseEntity<Map> stopResponse = restTemplate.postForEntity(url("/stop"), null, Map.class);
        long requested = ((Number) stopResponse.getBody().get("requestedCount")).longValue();

        // duplicateRate=1.0 means history starts empty, so the handful of workers dispatched
        // on the very first tick (bounded by eventsPerMinute/600) may each independently see
        // an empty history and generate a fresh event; every dispatch after that always
        // replays one of those few, so audit growth should stay small and well under
        // requested no matter how long the stream kept running.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ResponseEntity<List> auditsAfter = restTemplate.getForEntity("http://localhost:" + port + "/audits", List.class);
            long auditGrowth = auditsAfter.getBody().size() - auditCountBefore;
            assertThat(auditGrowth).isLessThanOrEqualTo(10);
            assertThat(auditGrowth).isLessThan(requested);
            assertThat(readCounterMetric("device_events_duplicate_total")).isGreaterThan(duplicatesBefore);
        });
    }

    private double readCounterMetric(String metricName) {
        try {
            return meterRegistry.get(metricName).counter().count();
        } catch (MeterNotFoundException notRegisteredYet) {
            return 0.0;
        }
    }

    private String url(String path) {
        return "http://localhost:" + port + "/events/random-stream" + path;
    }
}
