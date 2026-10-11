package com.intelbras.iotevents.common.metrics;

import com.intelbras.iotevents.domains.audit.core.port.MetricsPort;
import com.intelbras.iotevents.domains.device.core.port.RandomStreamMetricsPort;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class EventMetrics implements MetricsPort, RandomStreamMetricsPort {

    private final Counter processedCounter;
    private final Counter duplicateCounter;
    private final Counter dlqCounter;
    private final Counter streamRequestedCounter;
    private final Counter streamPublishedCounter;

    public EventMetrics(MeterRegistry registry) {
        this.processedCounter = Counter.builder("device_events_processed_total")
            .description("Number of device events successfully processed and audited").register(registry);
        this.duplicateCounter = Counter.builder("device_events_duplicate_total")
            .description("Number of device events skipped as duplicates").register(registry);
        this.dlqCounter = Counter.builder("device_events_dlq_total")
            .description("Number of device events sent to a dead-letter topic").register(registry);
        this.streamRequestedCounter = Counter.builder("random_stream_requested_total")
            .description("Number of events the random stream decided were due to be published").register(registry);
        this.streamPublishedCounter = Counter.builder("random_stream_published_total")
            .description("Number of events the random stream actually published successfully").register(registry);
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

    @Override
    public void incrementRequested() {
        streamRequestedCounter.increment();
    }

    @Override
    public void incrementPublished() {
        streamPublishedCounter.increment();
    }
}
