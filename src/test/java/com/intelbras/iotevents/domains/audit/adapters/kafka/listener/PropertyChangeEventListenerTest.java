package com.intelbras.iotevents.domains.audit.adapters.kafka.listener;

import com.intelbras.iotevents.domains.audit.adapters.kafka.DeviceEventMessageParser;
import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import com.intelbras.iotevents.domains.audit.core.KafkaMessageContext;
import com.intelbras.iotevents.domains.audit.core.usecase.RecordDeviceEventAuditUseCase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PropertyChangeEventListenerTest {

    @Test
    void shouldParseTheRecordAndCallTheUseCaseWithKafkaContext() {
        DeviceEventMessageParser parser = mock(DeviceEventMessageParser.class);
        RecordDeviceEventAuditUseCase useCase = mock(RecordDeviceEventAuditUseCase.class);
        IncomingDeviceEvent event = new IncomingDeviceEvent("evt-1", "DEVICE-00014", "PROPERTY_CHANGE", "iotProperty",
            Instant.parse("2026-09-25T08:47:51.157000Z"), Map.of());
        ConsumerRecord<String, String> record = new ConsumerRecord<>("device.property-change.events", 2, 42L, "DEVICE-00014", "{}");
        when(parser.parse("{}")).thenReturn(event);

        new PropertyChangeEventListener(parser, useCase).onMessage(record);

        verify(useCase).execute(eq(event), eq(new KafkaMessageContext("device.property-change.events", 2, 42L, "DEVICE-00014")));
    }
}
