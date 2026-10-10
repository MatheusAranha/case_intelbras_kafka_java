package com.intelbras.iotevents.domains.device.core;

import com.intelbras.iotevents.domains.device.core.error.DeviceEventValidationError;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeviceEventTest {

    @Test
    void shouldBuildDeviceEventFromRowWithOwnId() {
        DeviceEventRow row = new DeviceEventRow("evt-1", "DEVICE-00014", "iotProperty",
            "2026-09-25T08:47:51.157000+00:00", Map.of("properties", Map.of("11800", 3)));

        DeviceEvent event = DeviceEvent.from(row);

        assertThat(event.eventId()).isEqualTo("evt-1");
        assertThat(event.deviceId()).isEqualTo("DEVICE-00014");
        assertThat(event.eventCategory()).isEqualTo(EventCategory.PROPERTY_CHANGE);
        assertThat(event.eventType()).isEqualTo("iotProperty");
        assertThat(event.timestamp()).isEqualTo(Instant.parse("2026-09-25T08:47:51.157000Z"));
    }

    @Test
    void shouldFallBackToHashEventIdWhenRowHasNoId() {
        DeviceEventRow row = new DeviceEventRow(null, "DEVICE-00066", "videoMotion",
            "2026-09-25T09:30:11.000000+00:00", Map.of());

        DeviceEvent event = DeviceEvent.from(row);

        assertThat(event.eventId()).isEqualTo(
            DedupKeyGenerator.generate("DEVICE-00066", "videoMotion", "2026-09-25T09:30:11.000000+00:00", Map.of()));
    }

    @Test
    void shouldClassifyMotionDetectionEventsCorrectly() {
        DeviceEventRow row = new DeviceEventRow("evt-2", "DEVICE-00077", "mobileDetect",
            "2026-09-25T09:31:00.000000+00:00", Map.of());

        assertThat(DeviceEvent.from(row).eventCategory()).isEqualTo(EventCategory.MOTION_DETECTION);
    }

    @Test
    void shouldFallBackToOtherCategoryForUnrecognizedMsgType() {
        DeviceEventRow row = new DeviceEventRow("evt-3", "DEVICE-00099", "someFutureMsgType",
            "2026-09-25T09:32:00.000000+00:00", Map.of());

        assertThat(DeviceEvent.from(row).eventCategory()).isEqualTo(EventCategory.OTHER);
    }

    @Test
    void shouldRejectRowWithoutDeviceId() {
        DeviceEventRow row = new DeviceEventRow("evt-4", "", "iotProperty", "2026-09-25T08:47:51.157000+00:00", Map.of());

        assertThatThrownBy(() -> DeviceEvent.from(row)).isInstanceOf(DeviceEventValidationError.class);
    }

    @Test
    void shouldRejectRowWithInvalidTimestamp() {
        DeviceEventRow row = new DeviceEventRow("evt-5", "DEVICE-00014", "iotProperty", "not-a-timestamp", Map.of());

        assertThatThrownBy(() -> DeviceEvent.from(row)).isInstanceOf(DeviceEventValidationError.class);
    }
}
