package com.intelbras.iotevents.common.logging;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutionLogTest {

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
    void shouldEmitExactlyOneInfoLineOnSuccessPath() {
        ExecutionLog log = ExecutionLog.start(ExecutionLogTest.class, "execute")
            .context("eventId", "evt-1")
            .step("Did something")
            .step("Did something else", Map.of("count", 3));

        log.info("Completed successfully");

        assertThat(appender.list).hasSize(1);
        assertThat(appender.list.get(0).getFormattedMessage()).contains("Completed successfully", "evt-1", "Did something else");
    }

    @Test
    void shouldEmitExactlyOneErrorLineOnFailurePath() {
        ExecutionLog log = ExecutionLog.start(ExecutionLogTest.class, "execute").context("eventId", "evt-2");
        log.error("Failed", new RuntimeException("boom"));

        assertThat(appender.list).hasSize(1);
        assertThat(appender.list.get(0).getFormattedMessage()).contains("Failed", "evt-2");
    }
}
