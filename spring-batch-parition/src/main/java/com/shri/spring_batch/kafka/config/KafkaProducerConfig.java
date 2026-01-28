package com.shri.spring_batch.kafka.config;

import com.fasterxml.jackson.databind.JsonSerializer;
import com.shri.spring_batch.model.ElasticCustomer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, ElasticCustomer> producerFactory(
            @Value("${spring.kafka.bootstrap-server}") String bootstrapServer) {

        Map<String, Object> configMap = new HashMap<>();

        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(configMap);
    }

    @Bean
    public KafkaTemplate<String, ElasticCustomer> kafkaTemplate(
            ProducerFactory<String, ElasticCustomer> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
