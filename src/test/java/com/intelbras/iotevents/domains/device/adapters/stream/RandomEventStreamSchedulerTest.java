package com.intelbras.iotevents.domains.device.adapters.stream;

import com.intelbras.iotevents.domains.device.core.DeviceEvent;
import com.intelbras.iotevents.domains.device.core.EventCategory;
import com.intelbras.iotevents.domains.device.core.RandomDeviceEventFactory;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamConfig;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamStatus;
import com.intelbras.iotevents.domains.device.core.port.EventPublisherPort;
import com.intelbras.iotevents.domains.device.core.port.RandomStreamMetricsPort;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

class RandomEventStreamSchedulerTest {

    @Test
    void dueCountForTickShouldAccumulateFractionalRemainderForLowRates() {
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            mock(EventPublisherPort.class), new RandomDeviceEventFactory(), mock(RandomStreamMetricsPort.class));

        // 75/min => 0.125/tick, exactly representable in binary floating point,
        // so 8 ticks accumulate to exactly 1.0 with no drift.
        int totalDue = 0;
        for (int i = 0; i < 8; i++) {
            totalDue += scheduler.dueCountForTick(75.0);
        }

        assertThat(totalDue).isEqualTo(1);
    }

    @Test
    void dueCountForTickShouldMatchExactlyWhenRateDividesTicksEvenly() {
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            mock(EventPublisherPort.class), new RandomDeviceEventFactory(), mock(RandomStreamMetricsPort.class));

        int totalDue = 0;
        for (int i = 0; i < 100; i++) {
            totalDue += scheduler.dueCountForTick(600.0);
        }

        assertThat(totalDue).isEqualTo(100);
    }

    @Test
    void decideEventShouldGenerateFreshEventWhenHistoryIsEmptyEvenIfDuplicateRateIsOne() {
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            mock(EventPublisherPort.class), new RandomDeviceEventFactory(new Random(1)), mock(RandomStreamMetricsPort.class));
        RandomEventStreamConfig config = new RandomEventStreamConfig(600, 0.0, 1.0, List.of(EventCategory.PROPERTY_CHANGE));

        DeviceEvent event = scheduler.decideEvent(config, new Random(1));

        assertThat(event.deviceId()).startsWith("RANDOM-");
    }

    @Test
    void decideEventShouldReplayFromHistoryWhenDuplicateRateAlwaysFires() {
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            mock(EventPublisherPort.class), new RandomDeviceEventFactory(new Random(42)), mock(RandomStreamMetricsPort.class));
        RandomEventStreamConfig config = new RandomEventStreamConfig(600, 0.0, 1.0, List.of(EventCategory.PROPERTY_CHANGE));

        DeviceEvent first = scheduler.decideEvent(config, new Random(1));
        DeviceEvent second = scheduler.decideEvent(config, new Random(1));

        assertThat(second).isEqualTo(first);
    }

    @Test
    void decideEventShouldForceSimulateFailureWhenDlqRateAlwaysFires() {
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            mock(EventPublisherPort.class), new RandomDeviceEventFactory(new Random(42)), mock(RandomStreamMetricsPort.class));
        RandomEventStreamConfig config = new RandomEventStreamConfig(600, 1.0, 0.0, List.of(EventCategory.PROPERTY_CHANGE));

        DeviceEvent event = scheduler.decideEvent(config, new Random(1));

        assertThat(event.payload()).containsEntry("simulateFailure", true);
    }

    @Test
    void decideEventShouldNeverReplayAForcedDlqEvent() {
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            mock(EventPublisherPort.class), new RandomDeviceEventFactory(new Random(5)), mock(RandomStreamMetricsPort.class));
        RandomEventStreamConfig dlqConfig = new RandomEventStreamConfig(600, 1.0, 0.0, List.of(EventCategory.PROPERTY_CHANGE));
        RandomEventStreamConfig replayConfig = new RandomEventStreamConfig(600, 0.0, 1.0, List.of(EventCategory.PROPERTY_CHANGE));

        DeviceEvent forcedDlqEvent = scheduler.decideEvent(dlqConfig, new Random(1));
        assertThat(forcedDlqEvent.payload()).containsEntry("simulateFailure", true);

        DeviceEvent next = scheduler.decideEvent(replayConfig, new Random(1));

        assertThat(next.payload()).doesNotContainKey("simulateFailure");
    }

    @Test
    void statusBeforeEverStartingShouldBeSafelyNotRunning() {
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            mock(EventPublisherPort.class), new RandomDeviceEventFactory(), mock(RandomStreamMetricsPort.class));

        RandomEventStreamStatus status = scheduler.status();

        assertThat(status.running()).isFalse();
        assertThat(status.requestedCount()).isZero();
        assertThat(status.publishedCount()).isZero();
    }

    @Test
    void stopWhenNothingIsRunningShouldBeIdempotentAndSafe() {
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            mock(EventPublisherPort.class), new RandomDeviceEventFactory(), mock(RandomStreamMetricsPort.class));

        RandomEventStreamStatus first = scheduler.stop();
        RandomEventStreamStatus second = scheduler.stop();

        assertThat(first.running()).isFalse();
        assertThat(second.running()).isFalse();
    }

    @Test
    void startShouldReportRunningImmediatelyWithTheGivenConfig() {
        EventPublisherPort publisherPort = mock(EventPublisherPort.class);
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            publisherPort, new RandomDeviceEventFactory(), mock(RandomStreamMetricsPort.class));
        RandomEventStreamConfig config = new RandomEventStreamConfig(60, 0.0, 0.0, List.of(EventCategory.PROPERTY_CHANGE));

        RandomEventStreamStatus status = scheduler.start(config);

        assertThat(status.running()).isTrue();
        assertThat(status.config()).isEqualTo(config);

        scheduler.stop();
    }

    @Test
    void startWhileAlreadyRunningShouldReplaceTheConfigAndResetCounts() {
        EventPublisherPort publisherPort = mock(EventPublisherPort.class);
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            publisherPort, new RandomDeviceEventFactory(), mock(RandomStreamMetricsPort.class));
        RandomEventStreamConfig first = new RandomEventStreamConfig(60, 0.0, 0.0, List.of(EventCategory.PROPERTY_CHANGE));
        RandomEventStreamConfig second = new RandomEventStreamConfig(120, 0.0, 0.0, List.of(EventCategory.MOTION_DETECTION));

        scheduler.start(first);
        RandomEventStreamStatus statusAfterSecondStart = scheduler.start(second);

        assertThat(statusAfterSecondStart.config()).isEqualTo(second);
        assertThat(statusAfterSecondStart.requestedCount()).isZero();

        scheduler.stop();
    }

    @Test
    void startShouldActuallyPublishEventsThroughTheRealTickLoop() {
        EventPublisherPort publisherPort = mock(EventPublisherPort.class);
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            publisherPort, new RandomDeviceEventFactory(), mock(RandomStreamMetricsPort.class));
        RandomEventStreamConfig config = new RandomEventStreamConfig(6000, 0.0, 0.0, List.of(EventCategory.PROPERTY_CHANGE));

        scheduler.start(config);

        verify(publisherPort, timeout(2000).atLeastOnce()).publish(any(DeviceEvent.class));

        scheduler.stop();
    }

    @Test
    void shutdownShouldStopARunningStream() {
        EventPublisherPort publisherPort = mock(EventPublisherPort.class);
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            publisherPort, new RandomDeviceEventFactory(), mock(RandomStreamMetricsPort.class));
        RandomEventStreamConfig config = new RandomEventStreamConfig(60, 0.0, 0.0, List.of(EventCategory.PROPERTY_CHANGE));
        scheduler.start(config);

        scheduler.shutdown();

        assertThat(scheduler.status().running()).isFalse();
    }

    @Test
    void oneFailedPublishShouldNotStopLaterEventsFromBeingDispatched() {
        EventPublisherPort publisherPort = mock(EventPublisherPort.class);
        doThrow(new RuntimeException("transient broker hiccup")).when(publisherPort).publish(any(DeviceEvent.class));
        RandomEventStreamScheduler scheduler = new RandomEventStreamScheduler(
            publisherPort, new RandomDeviceEventFactory(), mock(RandomStreamMetricsPort.class));
        RandomEventStreamConfig config = new RandomEventStreamConfig(6000, 0.0, 0.0, List.of(EventCategory.PROPERTY_CHANGE));

        scheduler.start(config);

        verify(publisherPort, timeout(2000).atLeast(2)).publish(any(DeviceEvent.class));

        scheduler.stop();
    }
}
