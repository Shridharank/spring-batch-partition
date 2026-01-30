package com.shri.kafka.config;

import com.shri.kafka.model.model.ElasticCustomer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.mapping.DefaultJacksonJavaTypeMapper;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-server}")
    private String bootstrapServer;

    @Bean
    public ConsumerFactory<String, ElasticCustomer> consumerFactory() {
        Map<String, Object> configmap = new HashMap<>();
        configmap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        configmap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configmap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);

        // 1. Manually configure the JsonDeserializer
        JacksonJsonDeserializer<ElasticCustomer> valueJsonDeserializer =
                new JacksonJsonDeserializer<>(ElasticCustomer.class);
        // Essential for cross-app communication
        valueJsonDeserializer.setTypeMapper(new DefaultJacksonJavaTypeMapper());
        valueJsonDeserializer.addTrustedPackages("*");

        return new DefaultKafkaConsumerFactory<>(configmap,
                new StringDeserializer(),
                valueJsonDeserializer);
    }
}
