package com.shri.spring_batch.kafka.listener;

import com.shri.spring_batch.model.CustomerWrapper;
import com.shri.spring_batch.model.ElasticCustomer;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@StepScope
public class KafkaItemWriter implements ItemWriter<CustomerWrapper> {

    private final KafkaTemplate<String, ElasticCustomer> kafkaTemplate;

    @Value("${spring.batch.kafka.topic}")
    private String topicName;

    @Override
    public void write(Chunk<? extends CustomerWrapper> items) throws Exception {
        System.out.println("Topic Name: "+topicName);
        items.getItems().stream()
                .map(CustomerWrapper::getElasticCustomer)
                .forEach(elasticCustomer -> {
                    kafkaTemplate.send(topicName, String.valueOf(elasticCustomer.getId()), elasticCustomer);
                });
    }

}
