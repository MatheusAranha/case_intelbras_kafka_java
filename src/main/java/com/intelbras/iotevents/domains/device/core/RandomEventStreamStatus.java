package com.intelbras.iotevents.domains.device.core;

import java.time.Instant;

public record RandomEventStreamStatus(boolean running, RandomEventStreamConfig config, Instant startedAt,
                                       long requestedCount, long publishedCount) {
}
