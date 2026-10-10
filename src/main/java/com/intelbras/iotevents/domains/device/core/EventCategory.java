package com.intelbras.iotevents.domains.device.core;

import java.util.Set;

public enum EventCategory {
    PROPERTY_CHANGE, STATUS_CHANGE, MOTION_DETECTION, OTHER;

    private static final Set<String> PROPERTY_CHANGE_TYPES = Set.of("iotProperty");
    private static final Set<String> STATUS_CHANGE_TYPES = Set.of("online", "offline");
    private static final Set<String> MOTION_DETECTION_TYPES = Set.of("videoMotion", "mobileDetect", "human", "smartVideoMotion");

    public static EventCategory fromMsgType(String msgType) {
        if (msgType == null) {
            return OTHER;
        }
        String trimmed = msgType.trim();
        if (PROPERTY_CHANGE_TYPES.contains(trimmed)) {
            return PROPERTY_CHANGE;
        }
        if (STATUS_CHANGE_TYPES.contains(trimmed)) {
            return STATUS_CHANGE;
        }
        if (MOTION_DETECTION_TYPES.contains(trimmed)) {
            return MOTION_DETECTION;
        }
        return OTHER;
    }
}
