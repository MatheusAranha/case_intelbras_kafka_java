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
