package com.intelbras.iotevents.domains.device.core.usecase;

import com.intelbras.iotevents.domains.device.core.RandomEventStreamStatus;
import com.intelbras.iotevents.domains.device.core.port.RandomEventStreamPort;

public class GetRandomEventStreamStatusUseCase {

    private final RandomEventStreamPort port;

    public GetRandomEventStreamStatusUseCase(RandomEventStreamPort port) {
        this.port = port;
    }

    public RandomEventStreamStatus execute() {
        return port.status();
    }
}
