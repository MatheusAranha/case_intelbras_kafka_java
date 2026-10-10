package com.intelbras.iotevents.domains.audit.core;

import java.time.Instant;
import java.util.Map;

public record IncomingDeviceEvent(String eventId, String deviceId, String eventCategory, String eventType,
                                   Instant timestamp, Map<String, Object> payload) {
}
