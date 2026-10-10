package com.intelbras.iotevents.domains.device.adapters.csv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.intelbras.iotevents.domains.device.core.DeviceEvent;
import com.intelbras.iotevents.domains.device.core.EventCategory;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class CsvEventSourceAdapterTest {

    private final CsvEventSourceAdapter adapter = new CsvEventSourceAdapter(new ObjectMapper());

    @Test
    void shouldReadAllRealAndSyntheticRowsFromTheDataset() {
        assertThat(adapter.readAll()).hasSize(104);
    }

    @Test
    void shouldClassifyARealIotPropertyRowAsPropertyChange() {
        DeviceEvent event = adapter.readAll().stream()
            .filter(e -> e.deviceId().equals("DEVICE-00014"))
            .findFirst().orElseThrow();

        assertThat(event.eventCategory()).isEqualTo(EventCategory.PROPERTY_CHANGE);
        assertThat(event.eventType()).isEqualTo("iotProperty");
        assertThat(event.payload()).containsKey("properties");
    }

    @Test
    void shouldClassifyARealVideoMotionRowAsMotionDetection() {
        DeviceEvent event = adapter.readAll().stream()
            .filter(e -> e.deviceId().equals("DEVICE-00066"))
            .findFirst().orElseThrow();

        assertThat(event.eventCategory()).isEqualTo(EventCategory.MOTION_DETECTION);
    }

    @Test
    void shouldFallBackToBodyCompletoDeviceIdForBlankDidRows() {
        Set<String> informationTransferDeviceIds = adapter.readAll().stream()
            .filter(e -> e.eventType().equals("informationTransfer"))
            .map(DeviceEvent::deviceId)
            .collect(Collectors.toSet());

        assertThat(informationTransferDeviceIds).containsExactlyInAnyOrder("FJ1K2001704PS", "IA8L4001890ZN");
    }

    @Test
    void shouldCarrySimulateFailureFlagOnTheFourSyntheticDemoRowsAcrossAllCategories() {
        List<DeviceEvent> synthetic = adapter.readAll().stream()
            .filter(e -> e.deviceId().startsWith("SYNTHETIC-DLQ-"))
            .toList();

        assertThat(synthetic).hasSize(4);
        assertThat(synthetic).allSatisfy(e -> assertThat(e.payload()).containsEntry("simulateFailure", true));
        assertThat(synthetic.stream().map(DeviceEvent::eventCategory).collect(Collectors.toSet()))
            .containsExactlyInAnyOrder(EventCategory.PROPERTY_CHANGE, EventCategory.STATUS_CHANGE,
                EventCategory.MOTION_DETECTION, EventCategory.OTHER);
    }
}
