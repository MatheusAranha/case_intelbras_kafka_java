package com.intelbras.iotevents.domains.audit.adapters.kafka;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.intelbras.iotevents.common.metrics.EventMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DeviceEventDlqRecovererTest {

    private ListAppender<ILoggingEvent> appender;
    private Logger rootLogger;

    @BeforeEach
    void attachAppender() {
        appender = new ListAppender<>();
        appender.start();
        rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        rootLogger.detachAppender(appender);
    }

    @Test
    void shouldDelegateToDeadLetterRecovererAndLogExactlyOnce() {
        DeviceEventDlqRecoverer recoverer = new DeviceEventDlqRecoverer(new EventMetrics(new SimpleMeterRegistry()));
        DeadLetterPublishingRecoverer delegate = mock(DeadLetterPublishingRecoverer.class);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("device.status-change.events", 0, 1L, "DEVICE-1", "{}");
        RuntimeException exception = new RuntimeException("boom");

        recoverer.recover(record, exception, delegate);

        verify(delegate).accept(record, exception);
        assertThat(appender.list).hasSize(1);
    }

    @Test
    void shouldLogExactlyOnceEvenWhenDelegateThrows() {
        DeviceEventDlqRecoverer recoverer = new DeviceEventDlqRecoverer(new EventMetrics(new SimpleMeterRegistry()));
        DeadLetterPublishingRecoverer delegate = mock(DeadLetterPublishingRecoverer.class);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("device.status-change.events", 0, 1L, "DEVICE-1", "{}");
        RuntimeException exception = new RuntimeException("boom");
        doThrow(new IllegalStateException("delegate failed")).when(delegate).accept(record, exception);

        try {
            recoverer.recover(record, exception, delegate);
        } catch (IllegalStateException expected) {
            // expected to propagate
        }

        assertThat(appender.list).hasSize(1);
    }
}
