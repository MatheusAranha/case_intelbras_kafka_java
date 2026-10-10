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
import java.util.concurrent.TimeUnit;

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

            kafkaTemplate.send(topic, event.deviceId(), payload).get(10, TimeUnit.SECONDS);
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
