package com.intelbras.iotevents.domains.device.core;

import java.util.Map;

public record DeviceEventRow(String id, String deviceId, String msgTypeRaw, String timestampRaw,
                              Map<String, Object> payload) {
}
