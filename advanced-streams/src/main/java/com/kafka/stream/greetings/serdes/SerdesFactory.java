package com.kafka.stream.greetings.serdes;

import com.kafka.stream.greetings.utils.AlphabetWordAggregate;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    public static Serde<AlphabetWordAggregate> aggregateSerdes() {
        JsonSerializer<AlphabetWordAggregate> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<AlphabetWordAggregate> jsonDeserializer = new JsonDeserializer<>(AlphabetWordAggregate.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }
}
