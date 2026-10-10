package com.intelbras.iotevents.common.logging;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ExecutionLog {

    private final String module;
    private final String method;
    private final Instant startedAt = Instant.now();
    private final Map<String, Object> context = new LinkedHashMap<>();
    private final List<LogStep> steps = new ArrayList<>();

    private ExecutionLog(String module, String method) {
        this.module = module;
        this.method = method;
    }

    public static ExecutionLog start(Class<?> module, String method) {
        return new ExecutionLog(module.getName(), method);
    }

    public ExecutionLog context(String key, Object value) {
        context.put(key, value);
        return this;
    }

    public ExecutionLog step(String message) {
        return step(message, Map.of());
    }

    public ExecutionLog step(String message, Map<String, Object> data) {
        steps.add(new LogStep(message, data));
        return this;
    }

    public void info(String message) {
        StructuredLogger.info(message, module, method, startedAt, context, steps);
    }

    public void error(String message, Throwable error) {
        StructuredLogger.error(message, module, method, startedAt, context, steps, error);
    }
}
