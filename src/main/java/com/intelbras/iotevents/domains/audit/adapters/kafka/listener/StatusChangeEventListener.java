package com.intelbras.iotevents.domains.audit.adapters.kafka.listener;

import com.intelbras.iotevents.config.KafkaTopics;
import com.intelbras.iotevents.domains.audit.adapters.kafka.DeviceEventMessageParser;
import com.intelbras.iotevents.domains.audit.core.IncomingDeviceEvent;
import com.intelbras.iotevents.domains.audit.core.KafkaMessageContext;
import com.intelbras.iotevents.domains.audit.core.usecase.RecordDeviceEventAuditUseCase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class StatusChangeEventListener {

    private final DeviceEventMessageParser parser;
    private final RecordDeviceEventAuditUseCase useCase;

    public StatusChangeEventListener(DeviceEventMessageParser parser, RecordDeviceEventAuditUseCase useCase) {
        this.parser = parser;
        this.useCase = useCase;
    }

    @KafkaListener(topics = KafkaTopics.DEVICE_STATUS_CHANGE_EVENTS, groupId = KafkaTopics.CONSUMER_GROUP, concurrency = "${app.kafka.listener-concurrency}")
    public void onMessage(ConsumerRecord<String, String> record) {
        IncomingDeviceEvent event = parser.parse(record.value());
        useCase.execute(event, new KafkaMessageContext(record.topic(), record.partition(), record.offset(), record.key()));
    }
}
