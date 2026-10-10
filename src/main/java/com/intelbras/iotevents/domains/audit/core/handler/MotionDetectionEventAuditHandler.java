package com.intelbras.iotevents.domains.audit.core.handler;

import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import org.springframework.stereotype.Component;

@Component
public class MotionDetectionEventAuditHandler implements DeviceEventAuditHandler {

    @Override
    public boolean supports(String eventCategory) {
        return "MOTION_DETECTION".equalsIgnoreCase(eventCategory);
    }

    @Override
    public String handle(IncomingDeviceEvent event) {
        return "Device " + event.deviceId() + " detected motion (" + event.eventType() + ") at " + event.timestamp();
    }
}
