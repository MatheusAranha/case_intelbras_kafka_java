package com.intelbras.iotevents.domains.device.core.port;

import com.intelbras.iotevents.domains.device.core.DeviceEvent;

public interface EventPublisherPort {
    void publish(DeviceEvent event);
}
