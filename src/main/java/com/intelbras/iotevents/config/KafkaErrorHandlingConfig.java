package com.intelbras.iotevents.config;

import com.intelbras.iotevents.domains.audit.adapters.kafka.DeviceEventDlqRecoverer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    public CommonErrorHandler deviceEventErrorHandler(KafkaTemplate<String, String> kafkaTemplate,
                                                        DeviceEventDlqRecoverer dlqRecoverer) {
        DeadLetterPublishingRecoverer deadLetterRecoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
            (record, ex) -> new TopicPartition(record.topic() + KafkaTopics.DLT_SUFFIX, 0));

        ConsumerRecordRecoverer recoverer = (record, ex) -> dlqRecoverer.recover(record, ex, deadLetterRecoverer);

        ExponentialBackOff backOff = new ExponentialBackOff(1000L, 2.0);
        backOff.setMaxAttempts(3);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        // A malformed message can never succeed on retry — skip straight to the dead-letter topic.
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        return errorHandler;
    }
}
