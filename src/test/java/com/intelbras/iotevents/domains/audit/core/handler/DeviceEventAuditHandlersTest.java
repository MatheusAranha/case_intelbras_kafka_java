package com.intelbras.iotevents.domains.audit.core.handler;

import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DeviceEventAuditHandlersTest {

    private final PropertyChangeEventAuditHandler propertyChangeHandler = new PropertyChangeEventAuditHandler();
    private final StatusChangeEventAuditHandler statusChangeHandler = new StatusChangeEventAuditHandler();
    private final MotionDetectionEventAuditHandler motionDetectionHandler = new MotionDetectionEventAuditHandler();
    private final OtherEventAuditHandler otherHandler = new OtherEventAuditHandler();

    @Test
    void propertyChangeHandlerShouldSupportOnlyItsCategory() {
        assertThat(propertyChangeHandler.supports("PROPERTY_CHANGE")).isTrue();
        assertThat(propertyChangeHandler.supports("STATUS_CHANGE")).isFalse();
    }

    @Test
    void propertyChangeHandlerShouldDescribeTheEvent() {
        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-1", "DEVICE-00014", "PROPERTY_CHANGE", "iotProperty",
            Instant.parse("2026-09-25T08:47:51.157000Z"), Map.of("properties", Map.of("11800", 3)));

        assertThat(propertyChangeHandler.handle(event)).contains("DEVICE-00014").contains("iotProperty");
    }

    @Test
    void statusChangeHandlerShouldSupportOnlyItsCategory() {
        assertThat(statusChangeHandler.supports("STATUS_CHANGE")).isTrue();
        assertThat(statusChangeHandler.supports("MOTION_DETECTION")).isFalse();
    }

    @Test
    void motionDetectionHandlerShouldDescribeTheEvent() {
        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-2", "DEVICE-00066", "MOTION_DETECTION", "videoMotion",
            Instant.parse("2026-09-25T09:30:11Z"), Map.of());

        assertThat(motionDetectionHandler.handle(event)).contains("DEVICE-00066").contains("videoMotion");
    }

    @Test
    void otherHandlerShouldSupportOnlyOtherCategoryButDescribeAnyEvent() {
        assertThat(otherHandler.supports("OTHER")).isTrue();
        assertThat(otherHandler.supports("PROPERTY_CHANGE")).isFalse();

        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-3", "FJ1K2001704PS", "OTHER", "informationTransfer",
            Instant.parse("2026-09-25T09:45:37Z"), Map.of());

        assertThat(otherHandler.handle(event)).contains("FJ1K2001704PS");
    }
}
