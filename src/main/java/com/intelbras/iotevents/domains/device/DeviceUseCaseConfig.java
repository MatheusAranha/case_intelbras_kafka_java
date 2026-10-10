package com.intelbras.iotevents.domains.device;

import com.intelbras.iotevents.domains.device.core.port.EventPublisherPort;
import com.intelbras.iotevents.domains.device.core.port.EventSourcePort;
import com.intelbras.iotevents.domains.device.core.usecase.SimulateIngestionUseCase;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DeviceUseCaseConfig {

    @Bean
    public SimulateIngestionUseCase simulateIngestionUseCase(EventSourcePort eventSourcePort,
                                                               EventPublisherPort eventPublisherPort) {
        return new SimulateIngestionUseCase(eventSourcePort, eventPublisherPort);
    }
}
