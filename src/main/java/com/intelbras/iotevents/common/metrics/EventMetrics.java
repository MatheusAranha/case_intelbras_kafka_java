package com.intelbras.iotevents.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class EventMetrics {

    private final Counter processedCounter;
    private final Counter duplicateCounter;
    private final Counter dlqCounter;

    public EventMetrics(MeterRegistry registry) {
        this.processedCounter = Counter.builder("device_events_processed_total")
            .description("Number of device events successfully processed and audited").register(registry);
        this.duplicateCounter = Counter.builder("device_events_duplicate_total")
            .description("Number of device events skipped as duplicates").register(registry);
        this.dlqCounter = Counter.builder("device_events_dlq_total")
            .description("Number of device events sent to a dead-letter topic").register(registry);
    }

    public void incrementProcessed() {
        processedCounter.increment();
    }

    public void incrementDuplicate() {
        duplicateCounter.increment();
    }

    public void incrementDlq() {
        dlqCounter.increment();
    }
}
