package com.intelbras.iotevents.domains.audit.core.handler;

import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;

public class StatusChangeEventAuditHandler implements DeviceEventAuditHandler {

    @Override
    public boolean supports(String eventCategory) {
        return "STATUS_CHANGE".equalsIgnoreCase(eventCategory);
    }

    @Override
    public String handle(IncomingDeviceEvent event) {
        return "Device " + event.deviceId() + " changed connectivity status to " + event.eventType()
            + " at " + event.timestamp();
    }
}
