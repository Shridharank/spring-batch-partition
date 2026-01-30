package com.shri.spring_batch.kafka.listener;

import com.shri.spring_batch.kafka.config.KafkaTopicProperties;
import com.shri.spring_batch.model.CustomerWrapper;
import com.shri.spring_batch.model.ElasticCustomer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@RequiredArgsConstructor
@StepScope
public class KafkaItemWriter implements ItemWriter<CustomerWrapper> {

    private final KafkaTemplate<String, ElasticCustomer> kafkaTemplate;
    private final KafkaTopicProperties topicProperties;

    @Override
    public void write(Chunk<? extends CustomerWrapper> items) throws Exception {
        var topicName = topicProperties.getTopics().get("customer-processed-data");
        System.out.println("Topic Name: "+topicName);
        items.getItems().stream()
                .map(CustomerWrapper::getElasticCustomer)
                .forEach(elasticCustomer -> {
                    ProducerRecord<String, ElasticCustomer> record = new ProducerRecord<>(
                            topicName,
                            String.valueOf(elasticCustomer.getId()),
                            elasticCustomer
                    );
                    record.headers().add("source-app", "spring-batch-partition".getBytes());
                    record.headers().add("correlation-id", UUID.randomUUID().toString().getBytes());
                    kafkaTemplate.send(record);
                });
    }

}
