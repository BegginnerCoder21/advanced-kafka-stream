package com.kafka.stream.greetings.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.stream.greetings.domain.Greetings;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

@Slf4j
public class GreetingsDeserializer implements Deserializer<Greetings> {

    private final ObjectMapper objectMapper;

    public GreetingsDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Greetings deserialize(String s, byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, Greetings.class);
        } catch (IOException e) {

            log.error("IOException : {}", e.getMessage(), e);
            throw new RuntimeException(e);

        } catch (Exception e) {

            log.error("Exception : {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

}
