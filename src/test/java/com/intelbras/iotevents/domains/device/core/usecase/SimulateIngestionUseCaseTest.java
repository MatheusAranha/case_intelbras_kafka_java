package com.intelbras.iotevents.domains.device.core.usecase;

import com.intelbras.iotevents.domains.device.core.DeviceEvent;
import com.intelbras.iotevents.domains.device.core.EventCategory;
import com.intelbras.iotevents.domains.device.core.port.EventPublisherPort;
import com.intelbras.iotevents.domains.device.core.port.EventSourcePort;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SimulateIngestionUseCaseTest {

    @Test
    void shouldPublishEveryEventReadFromTheSource() {
        DeviceEvent eventOne = new DeviceEvent("evt-1", "DEVICE-00014", EventCategory.PROPERTY_CHANGE, "iotProperty",
            Instant.parse("2026-09-25T08:47:51.157000Z"), Map.of());
        DeviceEvent eventTwo = new DeviceEvent("evt-2", "DEVICE-00066", EventCategory.MOTION_DETECTION, "videoMotion",
            Instant.parse("2026-09-25T09:30:11.000000Z"), Map.of());

        EventSourcePort source = () -> List.of(eventOne, eventTwo);
        List<DeviceEvent> published = new ArrayList<>();
        EventPublisherPort publisher = published::add;

        SimulateIngestionUseCase useCase = new SimulateIngestionUseCase(source, publisher);

        assertThat(useCase.execute()).isEqualTo(2);
        assertThat(published).containsExactly(eventOne, eventTwo);
    }

    @Test
    void shouldReturnZeroWhenSourceHasNoEvents() {
        EventSourcePort source = List::of;
        EventPublisherPort publisher = event -> { throw new AssertionError("should not publish anything"); };

        assertThat(new SimulateIngestionUseCase(source, publisher).execute()).isZero();
    }
}
