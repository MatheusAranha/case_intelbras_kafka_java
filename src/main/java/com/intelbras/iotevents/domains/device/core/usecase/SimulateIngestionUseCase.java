package com.intelbras.iotevents.domains.device.core.usecase;

import com.intelbras.iotevents.common.logging.ExecutionLog;
import com.intelbras.iotevents.domains.device.core.DeviceEvent;
import com.intelbras.iotevents.domains.device.core.port.EventPublisherPort;
import com.intelbras.iotevents.domains.device.core.port.EventSourcePort;

import java.util.List;
import java.util.Map;

public class SimulateIngestionUseCase {

    private final EventSourcePort eventSourcePort;
    private final EventPublisherPort eventPublisherPort;

    public SimulateIngestionUseCase(EventSourcePort eventSourcePort, EventPublisherPort eventPublisherPort) {
        this.eventSourcePort = eventSourcePort;
        this.eventPublisherPort = eventPublisherPort;
    }

    public int execute() {
        ExecutionLog log = ExecutionLog.start(SimulateIngestionUseCase.class, "execute");
        try {
            List<DeviceEvent> events = eventSourcePort.readAll();
            log.step("Read events from source", Map.of("count", events.size()));

            for (DeviceEvent event : events) {
                eventPublisherPort.publish(event);
            }
            log.step("Published events to Kafka", Map.of("count", events.size()));

            log.info("Simulated ingestion completed");
            return events.size();
        } catch (Exception ex) {
            log.step("Unhandled exception while simulating ingestion");
            log.error("Failed to simulate ingestion", ex);
            throw ex;
        }
    }
}
