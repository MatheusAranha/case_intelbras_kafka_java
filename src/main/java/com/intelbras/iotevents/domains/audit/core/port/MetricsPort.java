package com.intelbras.iotevents.domains.audit.core.port;

public interface MetricsPort {
    void incrementProcessed();

    void incrementDuplicate();

    void incrementDlq();
}
