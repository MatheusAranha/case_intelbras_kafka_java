package com.intelbras.iotevents.domains.device.core;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class RandomDeviceEventFactory {

    private final Random random;
    private final AtomicLong deviceSequence = new AtomicLong();

    public RandomDeviceEventFactory() {
        this(new Random());
    }

    public RandomDeviceEventFactory(Random random) {
        this.random = random;
    }

    public DeviceEvent generate(EventCategory category, boolean forceDlq) {
        List<String> msgTypes = List.copyOf(category.sampleMsgTypes());
        String msgType = msgTypes.get(random.nextInt(msgTypes.size()));
        String deviceId = "RANDOM-" + String.format("%05d", deviceSequence.incrementAndGet());
        String timestampRaw = Instant.now().toString();
        Map<String, Object> payload = new HashMap<>(buildPayload(category));
        if (forceDlq) {
            payload.put("simulateFailure", true);
        }

        DeviceEventRow row = new DeviceEventRow(null, deviceId, msgType, timestampRaw, payload);
        return DeviceEvent.from(row);
    }

    private Map<String, Object> buildPayload(EventCategory category) {
        return switch (category) {
            case PROPERTY_CHANGE -> Map.of("properties", Map.of(
                String.valueOf(10000 + random.nextInt(9999)), random.nextInt(100)));
            case STATUS_CHANGE -> Map.of("status", random.nextBoolean() ? "ONLINE" : "OFFLINE");
            case MOTION_DETECTION -> Map.of("confidence", Math.round(random.nextDouble() * 100) / 100.0);
            case OTHER -> Map.of();
        };
    }
}
