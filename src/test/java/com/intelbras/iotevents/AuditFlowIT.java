package com.intelbras.iotevents;

import com.intelbras.iotevents.config.KafkaTopics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
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
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
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
    @Order(1)
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
    @Order(2)
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
