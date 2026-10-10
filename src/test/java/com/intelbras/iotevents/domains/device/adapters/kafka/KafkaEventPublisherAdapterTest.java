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
import static org.mockito.ArgumentMatchers.eq;
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

        verify(kafkaTemplate).send(eq(KafkaTopics.DEVICE_PROPERTY_CHANGE_EVENTS), eq("DEVICE-00014"), anyString());
    }

    @Test
    void shouldPublishToTheOtherTopicForOtherCategory() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        KafkaEventPublisherAdapter adapter = new KafkaEventPublisherAdapter(kafkaTemplate, new ObjectMapper());

        DeviceEvent event = new DeviceEvent("evt-2", "SYNTHETIC-DLQ-004", EventCategory.OTHER, "upgradeFail",
            Instant.parse("2026-09-25T10:03:00Z"), Map.of());

        adapter.publish(event);

        verify(kafkaTemplate).send(eq(KafkaTopics.DEVICE_OTHER_EVENTS), eq("SYNTHETIC-DLQ-004"), anyString());
    }
}
