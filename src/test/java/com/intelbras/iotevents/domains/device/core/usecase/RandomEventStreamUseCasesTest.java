package com.intelbras.iotevents.domains.device.core.usecase;

import com.intelbras.iotevents.domains.device.core.EventCategory;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamConfig;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamStatus;
import com.intelbras.iotevents.domains.device.core.port.RandomEventStreamPort;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RandomEventStreamUseCasesTest {

    @Test
    void startUseCaseShouldDelegateToThePort() {
        RandomEventStreamPort port = mock(RandomEventStreamPort.class);
        RandomEventStreamConfig config = new RandomEventStreamConfig(60, 0.0, 0.0, List.of(EventCategory.OTHER));
        RandomEventStreamStatus expected = new RandomEventStreamStatus(true, config, Instant.now(), 0, 0);
        when(port.start(config)).thenReturn(expected);

        RandomEventStreamStatus result = new StartRandomEventStreamUseCase(port).execute(config);

        assertThat(result).isEqualTo(expected);
        verify(port).start(config);
    }

    @Test
    void stopUseCaseShouldDelegateToThePort() {
        RandomEventStreamPort port = mock(RandomEventStreamPort.class);
        RandomEventStreamStatus expected = new RandomEventStreamStatus(false, null, null, 3, 3);
        when(port.stop()).thenReturn(expected);

        RandomEventStreamStatus result = new StopRandomEventStreamUseCase(port).execute();

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void getStatusUseCaseShouldDelegateToThePort() {
        RandomEventStreamPort port = mock(RandomEventStreamPort.class);
        RandomEventStreamStatus expected = new RandomEventStreamStatus(false, null, null, 0, 0);
        when(port.status()).thenReturn(expected);

        RandomEventStreamStatus result = new GetRandomEventStreamStatusUseCase(port).execute();

        assertThat(result).isEqualTo(expected);
    }
}
