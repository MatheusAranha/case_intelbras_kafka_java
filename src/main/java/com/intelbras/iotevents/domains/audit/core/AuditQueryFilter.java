package com.intelbras.iotevents.domains.audit.core;

import java.time.Instant;

public record AuditQueryFilter(String deviceId, String eventType, Instant from, Instant to) {
}
