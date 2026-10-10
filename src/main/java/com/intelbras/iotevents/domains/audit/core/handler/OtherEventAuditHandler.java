package com.intelbras.iotevents.domains.audit.core.handler;

import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;

public class OtherEventAuditHandler implements DeviceEventAuditHandler {

    @Override
    public boolean supports(String eventCategory) {
        return "OTHER".equalsIgnoreCase(eventCategory);
    }

    @Override
    public String handle(IncomingDeviceEvent event) {
        return "Device " + event.deviceId() + " reported " + event.eventType() + " at " + event.timestamp();
    }
}
