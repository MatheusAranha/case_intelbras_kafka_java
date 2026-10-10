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
