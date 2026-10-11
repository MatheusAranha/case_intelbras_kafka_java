package com.intelbras.iotevents.domains.device.core;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RandomEventStreamConfigTest {

    @Test
    void shouldAcceptAValidConfig() {
        RandomEventStreamConfig config =
            new RandomEventStreamConfig(600, 0.05, 0.1, List.of(EventCategory.PROPERTY_CHANGE));

        assertThat(config.eventsPerMinute()).isEqualTo(600);
    }

    @Test
    void shouldRejectNonPositiveEventsPerMinute() {
        assertThatThrownBy(() -> new RandomEventStreamConfig(0, 0.0, 0.0, List.of(EventCategory.OTHER)))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RandomEventStreamConfig(-1, 0.0, 0.0, List.of(EventCategory.OTHER)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectRatesOutsideZeroToOne() {
        assertThatThrownBy(() -> new RandomEventStreamConfig(60, 1.5, 0.0, List.of(EventCategory.OTHER)))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RandomEventStreamConfig(60, 0.0, -0.1, List.of(EventCategory.OTHER)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectEmptyCategories() {
        assertThatThrownBy(() -> new RandomEventStreamConfig(60, 0.0, 0.0, List.of()))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RandomEventStreamConfig(60, 0.0, 0.0, null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectCategoriesContainingNullEntries() {
        assertThatThrownBy(() -> new RandomEventStreamConfig(60, 0.0, 0.0, Arrays.asList(EventCategory.OTHER, null)))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
