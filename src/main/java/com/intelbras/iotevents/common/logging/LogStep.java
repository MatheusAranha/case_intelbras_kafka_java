package com.intelbras.iotevents.common.logging;

import java.util.Map;

public record LogStep(String message, Map<String, Object> data) {
}
