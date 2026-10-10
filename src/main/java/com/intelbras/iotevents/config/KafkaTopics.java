package com.intelbras.iotevents.config;

import java.util.List;

public final class KafkaTopics {

    public static final String DEVICE_PROPERTY_CHANGE_EVENTS = "device.property-change.events";
    public static final String DEVICE_STATUS_CHANGE_EVENTS = "device.status-change.events";
    public static final String DEVICE_MOTION_DETECTION_EVENTS = "device.motion-detection.events";
    public static final String DEVICE_OTHER_EVENTS = "device.other.events";
    public static final String DLT_SUFFIX = ".DLT";
    public static final int PARTITIONS = 3;
    public static final int DLT_PARTITIONS = 1;
    public static final String CONSUMER_GROUP = "audit-service";

    private KafkaTopics() {
    }

    public static List<String> allTopics() {
        return List.of(DEVICE_PROPERTY_CHANGE_EVENTS, DEVICE_STATUS_CHANGE_EVENTS,
            DEVICE_MOTION_DETECTION_EVENTS, DEVICE_OTHER_EVENTS);
    }
}
