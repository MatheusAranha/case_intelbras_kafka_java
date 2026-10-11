package com.intelbras.iotevents.domains.device.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EventCategoryTest {

    @Test
    void shouldClassifyEveryMsgTypeItSamplesBackToItself() {
        for (EventCategory category : EventCategory.values()) {
            if (category == EventCategory.OTHER) {
                continue;
            }
            for (String msgType : category.sampleMsgTypes()) {
                assertThat(EventCategory.fromMsgType(msgType)).isEqualTo(category);
            }
        }
    }

    @Test
    void shouldSampleAtLeastOneMsgTypePerCategory() {
        for (EventCategory category : EventCategory.values()) {
            assertThat(category.sampleMsgTypes()).isNotEmpty();
        }
    }

    @Test
    void shouldStillClassifyKnownMsgTypesCorrectly() {
        assertThat(EventCategory.fromMsgType("iotProperty")).isEqualTo(EventCategory.PROPERTY_CHANGE);
        assertThat(EventCategory.fromMsgType("online")).isEqualTo(EventCategory.STATUS_CHANGE);
        assertThat(EventCategory.fromMsgType("videoMotion")).isEqualTo(EventCategory.MOTION_DETECTION);
        assertThat(EventCategory.fromMsgType("somethingUnrecognized")).isEqualTo(EventCategory.OTHER);
    }
}
