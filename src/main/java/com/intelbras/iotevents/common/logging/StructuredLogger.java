package com.intelbras.iotevents.common.logging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class StructuredLogger {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    private StructuredLogger() {
    }

    static void info(String message, String module, String method, Instant startedAt,
                      Map<String, Object> context, List<LogStep> steps) {
        LoggerFactory.getLogger(module).info(render(message, module, method, startedAt, context, steps, null));
    }

    static void error(String message, String module, String method, Instant startedAt,
                       Map<String, Object> context, List<LogStep> steps, Throwable error) {
        LoggerFactory.getLogger(module).error(render(message, module, method, startedAt, context, steps, error), error);
    }

    private static String render(String message, String module, String method, Instant startedAt,
                                  Map<String, Object> context, List<LogStep> steps, Throwable error) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("message", message);
        payload.put("module", module);
        payload.put("method", method);
        payload.put("durationMs", Duration.between(startedAt, Instant.now()).toMillis());
        payload.put("context", context);
        payload.put("steps", steps);
        if (error != null) {
            payload.put("errorType", error.getClass().getName());
            payload.put("errorMessage", error.getMessage());
        }
        try {
            return message + " | " + MAPPER.writeValueAsString(payload);
        } catch (Exception serializationError) {
            return message + " | " + payload;
        }
    }
}
