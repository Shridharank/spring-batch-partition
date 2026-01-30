package com.shri.kafka.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;


@Component
public class EventConsumer {

    private static final ObjectMapper objectMapper =
            new ObjectMapper();

    public  <T> T eventFromMessage(String json, Class<T> targetClass) {
        try {
            return objectMapper.readValue(json, targetClass);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to convert JSON to "+ targetClass.getName(), e);
        }

    }
}
