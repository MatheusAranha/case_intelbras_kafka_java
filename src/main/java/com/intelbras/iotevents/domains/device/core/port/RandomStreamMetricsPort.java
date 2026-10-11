package com.intelbras.iotevents.domains.device.core.port;

public interface RandomStreamMetricsPort {
    void incrementRequested();

    void incrementPublished();
}
