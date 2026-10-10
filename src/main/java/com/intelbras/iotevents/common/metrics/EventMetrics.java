package com.intelbras.iotevents.common.metrics;

import com.intelbras.iotevents.domains.audit.core.port.MetricsPort;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class EventMetrics implements MetricsPort {

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

    @Override
    public void incrementProcessed() {
        processedCounter.increment();
    }

    @Override
    public void incrementDuplicate() {
        duplicateCounter.increment();
    }

    @Override
    public void incrementDlq() {
        dlqCounter.increment();
    }
}
