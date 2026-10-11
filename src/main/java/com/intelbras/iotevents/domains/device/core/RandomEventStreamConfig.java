package com.intelbras.iotevents.domains.device.core;

import java.util.List;

public record RandomEventStreamConfig(double eventsPerMinute, double dlqRate, double duplicateRate,
                                       List<EventCategory> categories) {

    public RandomEventStreamConfig {
        if (eventsPerMinute <= 0) {
            throw new IllegalArgumentException("eventsPerMinute must be positive, got " + eventsPerMinute);
        }
        if (dlqRate < 0 || dlqRate > 1) {
            throw new IllegalArgumentException("dlqRate must be between 0 and 1, got " + dlqRate);
        }
        if (duplicateRate < 0 || duplicateRate > 1) {
            throw new IllegalArgumentException("duplicateRate must be between 0 and 1, got " + duplicateRate);
        }
        if (categories == null || categories.isEmpty()) {
            throw new IllegalArgumentException("categories must not be empty");
        }
    }
}
