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
        assertThat(record.eventTimestamp()).isEqualTo(Instant.parse("2026-09-25T08:47:51.157000Z"));
        assertThat(record.consumedAt()).isEqualTo(record.createdAt());
    }
}
