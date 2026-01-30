package com.shri.spring_batch.kafka.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@Data
@ConfigurationProperties(prefix = "spring.batch.kafka")
public class KafkaTopicProperties {

    private Map<String, String> topics = new HashMap<>();

    @Data
    public static class Topics {
        private String customerProcessedData;
        private String customerErrorData;
    }

}
