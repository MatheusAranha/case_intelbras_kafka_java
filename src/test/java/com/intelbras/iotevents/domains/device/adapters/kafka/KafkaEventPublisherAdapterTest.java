package com.intelbras.iotevents.domains.device.adapters.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.intelbras.iotevents.config.KafkaTopics;
import com.intelbras.iotevents.domains.device.core.DeviceEvent;
import com.intelbras.iotevents.domains.device.core.EventCategory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaEventPublisherAdapterTest {

    @Test
    void shouldPublishToThePropertyChangeTopicKeyedByDeviceId() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(completedSend());
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
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(completedSend());
        KafkaEventPublisherAdapter adapter = new KafkaEventPublisherAdapter(kafkaTemplate, new ObjectMapper());

        DeviceEvent event = new DeviceEvent("evt-2", "SYNTHETIC-DLQ-004", EventCategory.OTHER, "upgradeFail",
            Instant.parse("2026-09-25T10:03:00Z"), Map.of());

        adapter.publish(event);

        verify(kafkaTemplate).send(eq(KafkaTopics.DEVICE_OTHER_EVENTS), eq("SYNTHETIC-DLQ-004"), anyString());
    }

    @Test
    void shouldPropagateFailureWhenTheBrokerNeverAcknowledgesTheSend() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        CompletableFuture<SendResult<String, String>> failedSend = new CompletableFuture<>();
        failedSend.completeExceptionally(new RuntimeException("broker unreachable"));
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(failedSend);
        KafkaEventPublisherAdapter adapter = new KafkaEventPublisherAdapter(kafkaTemplate, new ObjectMapper());

        DeviceEvent event = new DeviceEvent("evt-3", "DEVICE-00014", EventCategory.PROPERTY_CHANGE, "iotProperty",
            Instant.parse("2026-09-25T08:47:51.157000Z"), Map.of());

        assertThatThrownBy(() -> adapter.publish(event)).isInstanceOf(IllegalStateException.class);
    }

    private CompletableFuture<SendResult<String, String>> completedSend() {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("topic", "key", "value");
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("topic", 0), 0, 0, 0, 0, 0);
        return CompletableFuture.completedFuture(new SendResult<>(producerRecord, metadata));
    }
}
