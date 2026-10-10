package com.intelbras.iotevents.domains.audit.core;

public record KafkaMessageContext(String topic, int partition, long offset, String key) {
}
