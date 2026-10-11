package com.intelbras.iotevents.domains.device;

import com.intelbras.iotevents.domains.device.core.port.EventPublisherPort;
import com.intelbras.iotevents.domains.device.core.port.EventSourcePort;
import com.intelbras.iotevents.domains.device.core.port.RandomEventStreamPort;
import com.intelbras.iotevents.domains.device.core.usecase.GetRandomEventStreamStatusUseCase;
import com.intelbras.iotevents.domains.device.core.usecase.SimulateIngestionUseCase;
import com.intelbras.iotevents.domains.device.core.usecase.StartRandomEventStreamUseCase;
import com.intelbras.iotevents.domains.device.core.usecase.StopRandomEventStreamUseCase;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DeviceUseCaseConfig {

    @Bean
    public SimulateIngestionUseCase simulateIngestionUseCase(EventSourcePort eventSourcePort,
                                                               EventPublisherPort eventPublisherPort) {
        return new SimulateIngestionUseCase(eventSourcePort, eventPublisherPort);
    }

    @Bean
    public StartRandomEventStreamUseCase startRandomEventStreamUseCase(RandomEventStreamPort port) {
        return new StartRandomEventStreamUseCase(port);
    }

    @Bean
    public StopRandomEventStreamUseCase stopRandomEventStreamUseCase(RandomEventStreamPort port) {
        return new StopRandomEventStreamUseCase(port);
    }

    @Bean
    public GetRandomEventStreamStatusUseCase getRandomEventStreamStatusUseCase(RandomEventStreamPort port) {
        return new GetRandomEventStreamStatusUseCase(port);
    }
}
