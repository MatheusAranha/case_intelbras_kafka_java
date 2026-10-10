package com.intelbras.iotevents.domains.audit.core.handler;

import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import org.springframework.stereotype.Component;

@Component
public class PropertyChangeEventAuditHandler implements DeviceEventAuditHandler {

    @Override
    public boolean supports(String eventCategory) {
        return "PROPERTY_CHANGE".equalsIgnoreCase(eventCategory);
    }

    @Override
    public String handle(IncomingDeviceEvent event) {
        return "Device " + event.deviceId() + " reported a property change (" + event.eventType()
            + ") with payload " + event.payload() + " at " + event.timestamp();
    }
}
