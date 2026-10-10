package com.intelbras.iotevents.domains.audit.core.usecase;

import com.intelbras.iotevents.common.logging.ExecutionLog;
import com.intelbras.iotevents.common.metrics.EventMetrics;
import com.intelbras.iotevents.domains.audit.core.AuditRecord;
import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import com.intelbras.iotevents.domains.audit.core.KafkaMessageContext;
import com.intelbras.iotevents.domains.audit.core.error.SimulatedProcessingFailureError;
import com.intelbras.iotevents.domains.audit.core.handler.DeviceEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.OtherEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.port.AuditRepositoryPort;

import java.util.List;
import java.util.Map;

public class RecordDeviceEventAuditUseCase {

    private final AuditRepositoryPort auditRepositoryPort;
    private final List<DeviceEventAuditHandler> handlers;
    private final OtherEventAuditHandler fallbackHandler;
    private final EventMetrics eventMetrics;

    public RecordDeviceEventAuditUseCase(AuditRepositoryPort auditRepositoryPort,
                                          List<DeviceEventAuditHandler> handlers,
                                          OtherEventAuditHandler fallbackHandler,
                                          EventMetrics eventMetrics) {
        this.auditRepositoryPort = auditRepositoryPort;
        this.handlers = handlers;
        this.fallbackHandler = fallbackHandler;
        this.eventMetrics = eventMetrics;
    }

    public void execute(IncomingDeviceEvent event, KafkaMessageContext kafkaCtx) {
        ExecutionLog log = ExecutionLog.start(RecordDeviceEventAuditUseCase.class, "execute")
            .context("eventId", event.eventId())
            .context("deviceId", event.deviceId())
            .context("eventCategory", event.eventCategory())
            .context("eventType", event.eventType())
            .context("topic", kafkaCtx.topic())
            .context("partition", kafkaCtx.partition())
            .context("offset", kafkaCtx.offset())
            .context("messageKey", kafkaCtx.key());

        try {
            boolean duplicate = auditRepositoryPort.existsByDedupKey(event.eventId());
            log.step("Checked dedup key against audit store", Map.of("isDuplicate", duplicate));

            if (duplicate) {
                eventMetrics.incrementDuplicate();
                log.step("Skipped processing because event is a duplicate");
                log.info("Skipped duplicate device event");
                return;
            }

            if (Boolean.TRUE.equals(event.payload().get("simulateFailure"))) {
                log.step("Detected simulateFailure flag in payload");
                throw new SimulatedProcessingFailureError("Simulated processing failure for eventId=" + event.eventId());
            }

            DeviceEventAuditHandler handler = resolveHandler(event.eventCategory());
            String generalInfo = handler.handle(event);
            log.step("Built general info via " + handler.getClass().getSimpleName(), Map.of("generalInfo", generalInfo));

            auditRepositoryPort.save(AuditRecord.processedNow(event, generalInfo));
            eventMetrics.incrementProcessed();
            log.step("Persisted audit record");

            log.info("Processed device event");
        } catch (Exception ex) {
            log.step("Unhandled exception while processing device event");
            log.error("Failed to process device event", ex);
            throw ex;
        }
    }

    private DeviceEventAuditHandler resolveHandler(String eventCategory) {
        return handlers.stream()
            .filter(handler -> handler != fallbackHandler && handler.supports(eventCategory))
            .findFirst()
            .orElse(fallbackHandler);
    }
}
