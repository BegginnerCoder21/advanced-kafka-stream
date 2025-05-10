package com.kafka.stream.greetings.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kafka.stream.greetings.domain.Greetings;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class GreetingSerdes implements Serde<Greetings> {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    @Override
    public Serializer<Greetings> serializer() {
        return new GreetingsSerializer(objectMapper);
    }

    @Override
    public Deserializer<Greetings> deserializer() {
        return new GreetingsDeserializer(objectMapper);
    }
}
