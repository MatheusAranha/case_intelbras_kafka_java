package com.intelbras.iotevents.domains.audit.core.usecase;

import com.intelbras.iotevents.common.metrics.EventMetrics;
import com.intelbras.iotevents.domains.audit.core.AuditRecord;
import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import com.intelbras.iotevents.domains.audit.core.KafkaMessageContext;
import com.intelbras.iotevents.domains.audit.core.error.SimulatedProcessingFailureError;
import com.intelbras.iotevents.domains.audit.core.handler.DeviceEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.MotionDetectionEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.OtherEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.PropertyChangeEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.StatusChangeEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.port.AuditRepositoryPort;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RecordDeviceEventAuditUseCaseTest {

    private final OtherEventAuditHandler otherHandler = new OtherEventAuditHandler();
    private final List<DeviceEventAuditHandler> handlers = List.of(
        new PropertyChangeEventAuditHandler(), new StatusChangeEventAuditHandler(),
        new MotionDetectionEventAuditHandler(), otherHandler);
    private final EventMetrics eventMetrics = new EventMetrics(new SimpleMeterRegistry());
    private final KafkaMessageContext context = new KafkaMessageContext("device.property-change.events", 0, 1L, "DEVICE-00014");

    @Test
    void shouldPersistAnAuditRecordForANewEvent() {
        AuditRepositoryPort repositoryPort = mock(AuditRepositoryPort.class);
        when(repositoryPort.existsByDedupKey("evt-1")).thenReturn(false);
        RecordDeviceEventAuditUseCase useCase =
            new RecordDeviceEventAuditUseCase(repositoryPort, handlers, otherHandler, eventMetrics);

        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-1", "DEVICE-00014", "PROPERTY_CHANGE", "iotProperty",
            Instant.parse("2026-09-25T08:47:51.157000Z"), Map.of("properties", Map.of("11800", 3)));

        useCase.execute(event, context);

        verify(repositoryPort).save(any(AuditRecord.class));
    }

    @Test
    void shouldSkipDuplicateEventWithoutPersistingAudit() {
        AuditRepositoryPort repositoryPort = mock(AuditRepositoryPort.class);
        when(repositoryPort.existsByDedupKey("evt-1")).thenReturn(true);
        RecordDeviceEventAuditUseCase useCase =
            new RecordDeviceEventAuditUseCase(repositoryPort, handlers, otherHandler, eventMetrics);

        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-1", "DEVICE-00014", "PROPERTY_CHANGE", "iotProperty",
            Instant.parse("2026-09-25T08:47:51.157000Z"), Map.of());

        useCase.execute(event, context);

        verify(repositoryPort, never()).save(any(AuditRecord.class));
    }

    @Test
    void shouldThrowOnSimulateFailureFlagWithoutPersistingAudit() {
        AuditRepositoryPort repositoryPort = mock(AuditRepositoryPort.class);
        when(repositoryPort.existsByDedupKey("evt-1")).thenReturn(false);
        RecordDeviceEventAuditUseCase useCase =
            new RecordDeviceEventAuditUseCase(repositoryPort, handlers, otherHandler, eventMetrics);

        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-1", "SYNTHETIC-DLQ-001", "PROPERTY_CHANGE", "iotProperty",
            Instant.parse("2026-09-25T10:00:00Z"), Map.of("simulateFailure", true));

        assertThatThrownBy(() -> useCase.execute(event, context)).isInstanceOf(SimulatedProcessingFailureError.class);

        verify(repositoryPort, never()).save(any(AuditRecord.class));
    }

    @Test
    void shouldFallBackToOtherHandlerWhenNoSpecificHandlerMatches() {
        AuditRepositoryPort repositoryPort = mock(AuditRepositoryPort.class);
        when(repositoryPort.existsByDedupKey("evt-9")).thenReturn(false);
        RecordDeviceEventAuditUseCase useCase =
            new RecordDeviceEventAuditUseCase(repositoryPort, handlers, otherHandler, eventMetrics);

        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-9", "FJ1K2001704PS", "OTHER", "informationTransfer",
            Instant.parse("2026-09-25T09:45:37Z"), Map.of());

        useCase.execute(event, context);

        verify(repositoryPort).save(any(AuditRecord.class));
    }
}
