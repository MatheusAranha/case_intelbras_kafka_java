package com.intelbras.iotevents.domains.audit.core.handler;

import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;

public interface DeviceEventAuditHandler {
    boolean supports(String eventCategory);

    String handle(IncomingDeviceEvent event);
}
