package com.intelbras.iotevents.domains.audit.adapters.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DeviceEventMessageParserTest {

    private final DeviceEventMessageParser parser = new DeviceEventMessageParser(new ObjectMapper());

    @Test
    void shouldParseTheWireJsonIntoAnIncomingDeviceEvent() {
        String json = """
            {"eventId":"evt-1","deviceId":"DEVICE-00014","eventCategory":"PROPERTY_CHANGE","eventType":"iotProperty",
             "timestamp":"2026-09-25T08:47:51.157000Z","payload":{"properties":{"11800":3}}}
            """;

        IncomingDeviceEvent event = parser.parse(json);

        assertThat(event.eventId()).isEqualTo("evt-1");
        assertThat(event.deviceId()).isEqualTo("DEVICE-00014");
        assertThat(event.eventCategory()).isEqualTo("PROPERTY_CHANGE");
        assertThat(event.eventType()).isEqualTo("iotProperty");
    }

    @Test
    void shouldDefaultToEmptyPayloadWhenMissing() {
        String json = """
            {"eventId":"evt-2","deviceId":"DEVICE-00056","eventCategory":"STATUS_CHANGE","eventType":"online",
             "timestamp":"2026-09-25T09:55:59Z"}
            """;

        assertThat(parser.parse(json).payload()).isEmpty();
    }
}
