package com.kafka.stream.greetings.serdes;

import com.kafka.stream.greetings.domain.Greetings;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {
    private SerdesFactory(){}

    public static Serde<Greetings> greetingSerdes() {
        return new GreetingSerdes();
    }

    public static Serde<Greetings> greetingSerdesUsingGeneric() {
        JsonSerializer<Greetings> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Greetings> jsonDeserializer = new JsonDeserializer<>(Greetings.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }
}
