package com.intelbras.iotevents.domains.device.core;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class RandomDeviceEventFactoryTest {

    @Test
    void shouldGenerateAnEventWithARandomDeviceIdPrefix() {
        RandomDeviceEventFactory factory = new RandomDeviceEventFactory(new Random(42));

        DeviceEvent event = factory.generate(EventCategory.PROPERTY_CHANGE, false);

        assertThat(event.deviceId()).startsWith("RANDOM-");
    }

    @Test
    void shouldGenerateAnEventWhoseMsgTypeClassifiesBackToTheRequestedCategory() {
        RandomDeviceEventFactory factory = new RandomDeviceEventFactory(new Random(7));

        for (EventCategory category : EventCategory.values()) {
            DeviceEvent event = factory.generate(category, false);
            assertThat(event.eventCategory()).isEqualTo(category);
            assertThat(category.sampleMsgTypes()).contains(event.eventType());
        }
    }

    @Test
    void shouldNotSetSimulateFailureWhenForceDlqIsFalse() {
        RandomDeviceEventFactory factory = new RandomDeviceEventFactory(new Random(1));

        DeviceEvent event = factory.generate(EventCategory.MOTION_DETECTION, false);

        assertThat(event.payload()).doesNotContainKey("simulateFailure");
    }

    @Test
    void shouldSetSimulateFailureWhenForceDlqIsTrue() {
        RandomDeviceEventFactory factory = new RandomDeviceEventFactory(new Random(1));

        DeviceEvent event = factory.generate(EventCategory.MOTION_DETECTION, true);

        assertThat(event.payload()).containsEntry("simulateFailure", true);
    }

    @Test
    void shouldGenerateDistinctEventIdsAcrossCalls() {
        RandomDeviceEventFactory factory = new RandomDeviceEventFactory(new Random(99));

        DeviceEvent first = factory.generate(EventCategory.OTHER, false);
        DeviceEvent second = factory.generate(EventCategory.OTHER, false);

        assertThat(first.eventId()).isNotEqualTo(second.eventId());
    }
}
