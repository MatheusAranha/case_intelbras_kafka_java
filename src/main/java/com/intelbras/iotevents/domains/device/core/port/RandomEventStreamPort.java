package com.intelbras.iotevents.domains.device.core.port;

import com.intelbras.iotevents.domains.device.core.RandomEventStreamConfig;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamStatus;

public interface RandomEventStreamPort {
    RandomEventStreamStatus start(RandomEventStreamConfig config);

    RandomEventStreamStatus stop();

    RandomEventStreamStatus status();
}
