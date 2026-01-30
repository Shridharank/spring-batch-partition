package com.shri.spring_batch.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.List;

@Configuration
@EnableConfigurationProperties(KafkaTopicProperties.class)
public class KafkaTopicConfig {

    @Bean
    public KafkaAdmin.NewTopics kafkaTopics(KafkaTopicProperties topicProperties) {
        List<NewTopic> topicList = topicProperties.getTopics()
                .values().stream()
                .map(topicName ->
                        TopicBuilder.name(topicName)
                                .partitions(1)
                                .replicas(1)
                                .build()
                ).toList();
        return new KafkaAdmin.NewTopics(topicList.toArray(NewTopic[]::new));
    }
}
