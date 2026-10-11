package com.intelbras.iotevents.domains.device.core.usecase;

import com.intelbras.iotevents.domains.device.core.RandomEventStreamConfig;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamStatus;
import com.intelbras.iotevents.domains.device.core.port.RandomEventStreamPort;

public class StartRandomEventStreamUseCase {

    private final RandomEventStreamPort port;

    public StartRandomEventStreamUseCase(RandomEventStreamPort port) {
        this.port = port;
    }

    public RandomEventStreamStatus execute(RandomEventStreamConfig config) {
        return port.start(config);
    }
}
