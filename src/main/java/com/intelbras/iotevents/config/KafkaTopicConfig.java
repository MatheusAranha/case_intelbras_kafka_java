package com.intelbras.iotevents.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic devicePropertyChangeEventsTopic() {
        return topic(KafkaTopics.DEVICE_PROPERTY_CHANGE_EVENTS);
    }

    @Bean
    public NewTopic deviceStatusChangeEventsTopic() {
        return topic(KafkaTopics.DEVICE_STATUS_CHANGE_EVENTS);
    }

    @Bean
    public NewTopic deviceMotionDetectionEventsTopic() {
        return topic(KafkaTopics.DEVICE_MOTION_DETECTION_EVENTS);
    }

    @Bean
    public NewTopic deviceOtherEventsTopic() {
        return topic(KafkaTopics.DEVICE_OTHER_EVENTS);
    }

    @Bean
    public NewTopic devicePropertyChangeEventsDlt() {
        return dlt(KafkaTopics.DEVICE_PROPERTY_CHANGE_EVENTS);
    }

    @Bean
    public NewTopic deviceStatusChangeEventsDlt() {
        return dlt(KafkaTopics.DEVICE_STATUS_CHANGE_EVENTS);
    }

    @Bean
    public NewTopic deviceMotionDetectionEventsDlt() {
        return dlt(KafkaTopics.DEVICE_MOTION_DETECTION_EVENTS);
    }

    @Bean
    public NewTopic deviceOtherEventsDlt() {
        return dlt(KafkaTopics.DEVICE_OTHER_EVENTS);
    }

    private NewTopic topic(String name) {
        return TopicBuilder.name(name).partitions(KafkaTopics.PARTITIONS).replicas(1).build();
    }

    private NewTopic dlt(String baseTopicName) {
        return TopicBuilder.name(baseTopicName + KafkaTopics.DLT_SUFFIX).partitions(KafkaTopics.DLT_PARTITIONS).replicas(1).build();
    }
}
