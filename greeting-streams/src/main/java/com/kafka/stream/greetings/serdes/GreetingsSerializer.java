package com.kafka.stream.greetings.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.stream.greetings.domain.Greetings;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
public class GreetingsSerializer implements Serializer<Greetings> {

    private final ObjectMapper objectMapper;

    public GreetingsSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(String s, Greetings greetings) {

        try {
            return objectMapper.writeValueAsBytes(greetings);
        } catch (JsonProcessingException e) {

            log.error("JsonProcessingException: {}", e.getMessage(), e);
            throw new RuntimeException(e);

        } catch (Exception e) {

            log.error("Exception: {}", e.getMessage(), e);
            throw new RuntimeException(e);

        }
    }
}
