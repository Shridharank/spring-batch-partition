package com.shri.kafka.comsumer;

import com.shri.kafka.event.EventConsumer;
import com.shri.kafka.model.model.ElasticCustomer;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class CustomerBatchListener {

    private EventConsumer consumer;

    private static final Logger log = LoggerFactory.getLogger(CustomerBatchListener.class);

    @KafkaListener(topics = "${spring.kafka.topic.customer-processed-data}",
    groupId = "${spring.kafka.consumer-group-prefix}_batch_process")
    public void processCustomerBatchData(
            @Payload String customer,
            @Header("source-app") String sourceApp,
            @Header("correlation-id") String key) {
        try {
            ElasticCustomer elasticCustomer =
                    consumer.eventFromMessage(customer, ElasticCustomer.class);
            log.info("Processing key: {} from source: {}", key, sourceApp);
            System.out.println("Elastic repo customer: "+elasticCustomer);
        } catch (Exception e) {
            log.error("Exception while reading batch data {}", e.getMessage());
        }
    }
}
