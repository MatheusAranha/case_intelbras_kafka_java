package com.intelbras.iotevents.domains.audit.adapters.kafka;

import com.intelbras.iotevents.common.logging.ExecutionLog;
import com.intelbras.iotevents.common.metrics.EventMetrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.stereotype.Component;

@Component
public class DeviceEventDlqRecoverer {

    private final EventMetrics eventMetrics;

    public DeviceEventDlqRecoverer(EventMetrics eventMetrics) {
        this.eventMetrics = eventMetrics;
    }

    public void recover(ConsumerRecord<?, ?> record, Exception exception, DeadLetterPublishingRecoverer delegate) {
        ExecutionLog log = ExecutionLog.start(DeviceEventDlqRecoverer.class, "recover")
            .context("topic", record.topic())
            .context("partition", record.partition())
            .context("offset", record.offset())
            .context("messageKey", record.key());
        try {
            log.step("Exhausted retries for device event, sending to dead-letter topic");
            delegate.accept(record, exception);
            eventMetrics.incrementDlq();
            log.step("Published record to dead-letter topic");

            log.error("Device event moved to dead-letter topic after exhausting retries", exception);
        } catch (Exception recoveryError) {
            log.step("Unhandled exception while recovering device event to DLQ");
            log.error("Failed to recover device event to dead-letter topic", recoveryError);
            throw recoveryError;
        }
    }
}
