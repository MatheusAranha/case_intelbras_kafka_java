package com.intelbras.iotevents.domains.device.core;

import java.util.Map;
import java.util.Set;

public enum EventCategory {
    PROPERTY_CHANGE, STATUS_CHANGE, MOTION_DETECTION, OTHER;

    private static final Map<EventCategory, Set<String>> MSG_TYPES_BY_CATEGORY = Map.of(
        PROPERTY_CHANGE, Set.of("iotProperty"),
        STATUS_CHANGE, Set.of("online", "offline"),
        MOTION_DETECTION, Set.of("videoMotion", "mobileDetect", "human", "smartVideoMotion"),
        OTHER, Set.of("iotEvent", "informationTransfer", "openCamera", "upgradeFail")
    );

    public static EventCategory fromMsgType(String msgType) {
        if (msgType == null) {
            return OTHER;
        }
        String trimmed = msgType.trim();
        for (EventCategory category : values()) {
            if (category != OTHER && MSG_TYPES_BY_CATEGORY.get(category).contains(trimmed)) {
                return category;
            }
        }
        return OTHER;
    }

    public Set<String> sampleMsgTypes() {
        return MSG_TYPES_BY_CATEGORY.get(this);
    }
}
