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
