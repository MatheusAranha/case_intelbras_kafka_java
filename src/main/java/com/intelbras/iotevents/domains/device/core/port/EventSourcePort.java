package com.intelbras.iotevents.domains.device.core.port;

import com.intelbras.iotevents.domains.device.core.DeviceEvent;

import java.util.List;

public interface EventSourcePort {
    List<DeviceEvent> readAll();
}
