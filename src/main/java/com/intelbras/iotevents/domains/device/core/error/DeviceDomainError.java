package com.intelbras.iotevents.domains.device.core.error;

public class DeviceDomainError extends RuntimeException {
    public DeviceDomainError(String message) {
        super(message);
    }
}
